from . import Scraper
import logging
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, parse_athlete_name, FailedItemWriter
from checkpoint import CheckpointManager
import json
import csv
import os
import urllib.parse
import asyncio

logger = logging.getLogger("HeavyAggregator")

class NasgaScraper(Scraper):
    BASE_URL = "http://www.nasgaweb.com/dbase/main.asp"
    RESULTS_URL = "http://www.nasgaweb.com/dbase/results2.asp"
    ATHLETE_URL = "http://www.nasgaweb.com/dbase/resultsathlete3.asp"

    def __init__(self, settings):
        super().__init__(settings)
        self.checkpoint = CheckpointManager()

    async def run(self):
        logger.info("Starting NASGA Scraper (Async)...")
        session = await get_async_session(self.settings)
        
        # Setup Output Files
        base_output_dir = os.path.join('output', 'nasga')
        if not os.path.exists(base_output_dir):
            os.makedirs(base_output_dir)

        max_lines = self.settings.get('max_output_line_count', 0)
        
        # Initialize Writers (Failures at base)
        failure_logger = FailedItemWriter(base_output_dir, 'nasga_failed_retrievals.json')
        
        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)

        # Athlete Aggregation DB
        self.athlete_db = {}

        try:
            # 1. Get Years
            logger.info("Fetching available years...")
            years = await self.get_years(session)
            if not years:
                logger.error("No years found. Exiting.")
                return

            logger.info(f"Found {len(years)} years to process: {years}")
            
            # Resume Logic for Years
            start_year_idx = 0
            saved_year = self.checkpoint.get("nasga_last_completed_year")
            if saved_year and saved_year in years:
                try:
                    idx = years.index(saved_year)
                    start_year_idx = idx + 1 # Start with next
                    logger.info(f"Resuming Games scraping after year {saved_year}...")
                except ValueError:
                    pass

            # 2. Iterate Years to collect Games and Build Athlete History
            for i in range(start_year_idx, len(years)):
                year = years[i]
                logger.info(f"Scanning Year: {year}")
                
                # Setup Year Directory
                year_dir = os.path.join(base_output_dir, str(year))
                if not os.path.exists(year_dir):
                    os.makedirs(year_dir)
                    
                year_games_writer = StreamingJSONWriter(year_dir, 'nasga_games.json', max_lines)
                
                try:
                    year_url = f"{self.BASE_URL}?resultsyear={year}"
                    resp_text = await async_fetch_url(session, year_url, settings=self.settings)
                    if not resp_text: continue
    
                    soup = BeautifulSoup(resp_text, 'html.parser')
                    
                    # Get Games
                    games = self.get_dropdown_options(soup, 'gamesid')
                    valid_games = {}
                    for name, value in games.items():
                         if value and value not in ['0', 'none', ''] and not name.startswith('Select') and not name.startswith('---'):
                             valid_games[value] = name
    
                    logger.info(f"  Found {len(valid_games)} games.")
    
                    # Scrape Games for this Year (Parallel)
                    game_tasks = []
                    for game_id, game_name in valid_games.items():
                        game_tasks.append(self.scrape_game_async(session, game_id, game_name, year, semaphore, failure_logger))
                    
                    game_results = await asyncio.gather(*game_tasks)
                    
                    for res in game_results:
                        if res:
                            year_games_writer.write_item(res)
                            # Update Athlete DB
                            self._update_athlete_db(res)
                            
                finally:
                    year_games_writer.close()
                
                # Save/Update Accumulated Athletes
                self._save_athletes(base_output_dir)
                logger.info(f"  Saved {len(self.athlete_db)} athletes to nasga_athletes.json")
                
                # Save Checkpoint
                self.checkpoint.save("nasga_last_completed_year", year)

            logger.info("NASGA Scraping Complete.")

        finally:
            await session.close()

    def _update_athlete_db(self, game_result):
        # Extract athletes from game results
        # Structure: game_result['results'] = { 'Class': [ { 'Athlete': 'Name', ... } ] }
        game_info = {
            'Date': game_result.get('name', '').split(',')[-1].strip(), # Approximate date from name
            'Game': game_result.get('name'),
            'GameID': game_result.get('id'),
            'Year': game_result.get('year')
        }
        
        for cls, athletes in game_result.get('results', {}).items():
            for ath in athletes:
                name = ath.get('Athlete')
                if not name: continue
                
                # Normalize name?
                # Using cleaned name as key
                if name not in self.athlete_db:
                    self.athlete_db[name] = {
                        'name': name,
                        'history': []
                    }
                
                # Create History Entry
                entry = {
                    'Date': game_info['Date'],
                    'Game': game_info['Game'],
                    'GameID': game_info['GameID'], # Added for dedupe
                    'Class': cls,
                    'Place': ath.get('Place'),
                    'Points': ath.get('GamesPoints')
                }
                
                # Check for duplicates
                # Same GameID and Class?
                exists = False
                for existing in self.athlete_db[name]['history']:
                    if existing.get('GameID') == entry['GameID'] and existing.get('Class') == entry['Class']:
                         exists = True
                         break
                    # Fallback for older entries without GameID? (Unlikely in this refactor)
                    if not existing.get('GameID') and existing.get('Date') == entry['Date'] and existing.get('Game') == entry['Game'] and existing.get('Class') == entry['Class']:
                         exists = True
                         break
                
                if not exists:
                    self.athlete_db[name]['history'].append(entry)

    def _save_athletes(self, base_dir):
        # Overwrite the athletes file with current DB
        path = os.path.join(base_dir, 'nasga_athletes.json')
        try:
            # Convert dict to list
            data = list(self.athlete_db.values())
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save athletes file: {e}")

    async def get_years(self, session):
        resp_text = await async_fetch_url(session, self.BASE_URL, settings=self.settings)
        if not resp_text:
            return []
        soup = BeautifulSoup(resp_text, 'html.parser')
        years = self.get_dropdown_options(soup, 'resultsyear')
        
        valid = []
        for text, value in years.items():
            year_candidate = text.strip()
            if year_candidate.isdigit() and len(year_candidate) == 4:
                valid.append(year_candidate)
        return sorted(valid, reverse=True)

    def get_dropdown_options(self, soup, select_name):
        options = {}
        select = soup.find('select', {'name': select_name})
        if select:
            for opt in select.find_all('option'):
                txt = opt.get_text(strip=True)
                val = opt.get('value', '').strip()
                if val:
                    options[txt] = val
        return options

    def clean_text(self, text):
        if isinstance(text, str):
            return text.replace('\u00a0', ' ').strip()
        return text

    def parse_number(self, text, dtype='float'):
        if not text: return None
        text = str(text).strip()
        if text.upper().startswith('T') and text[1:].isdigit():
            text = text[1:]
        try:
            if dtype == 'int': return int(float(text))
            else: return float(text)
        except ValueError:
            return text

    def parse_distance(self, text):
        import re
        if not text: return None
        text = str(text).strip()
        if text.upper() in ['NT', 'DNS', '-', '']: return None
        if ':' in text: return text
        match = re.match(r"(\d+)'\s*-?\s*(\d*\.?\d*)\"?", text)
        if match:
            feet = float(match.group(1))
            inches_str = match.group(2)
            inches = float(inches_str) if inches_str else 0
            return round(feet + (inches / 12.0), 3)
        match_ft = re.match(r"(\d+)'$", text)
        if match_ft: return float(match_ft.group(1))
        try: return float(text)
        except ValueError: pass
        return text

    def parse_game_tables(self, tables):
        structured_results = {}
        current_class = "Unknown"
        current_headers = []
        all_rows = []
        for table in tables:
            for row in table:
                cleaned_row = [self.clean_text(cell) for cell in row]
                if not any(cleaned_row): continue
                all_rows.append(cleaned_row)

        for row in all_rows:
            non_empty = [c for c in row if c]
            if len(non_empty) <= 2 and len(non_empty) > 0:
                val = non_empty[0]
                if val not in ["Athlete", "Dist", "Pts"] and \
                   not any(x in val for x in ["Notes:", "View the log", "Copyright", "Database Main", "Home|"]):
                        if not val.replace('.','').isdigit():
                            current_class = val
                            if current_class not in structured_results:
                                structured_results[current_class] = []
                            continue

            if "Athlete" in row:
                current_headers = row
                continue
            if not non_empty or "Dist" in row or "Pts" in row: continue
            if any(x in non_empty[0] for x in ["Notes:", "View the log", "Copyright", "Database Main", "Home|"]): continue

            if current_headers:
                if "Athlete" in row: continue
                athlete_data = {}
                if len(row) > 0: athlete_data['Athlete'] = row[0]
                if len(row) > 1: athlete_data['Place'] = self.parse_number(row[1], dtype='int')
                if len(row) > 2: athlete_data['GamesPoints'] = self.parse_number(row[2], dtype='float')
                
                # Identify event columns by index from the header row
                event_col_indices = []
                for idx, h in enumerate(current_headers):
                    if idx >= 3 and h and h not in ['Pts', 'Points', 'GamesPoints']:
                        event_col_indices.append((idx, h))
                
                for idx, evt in event_col_indices:
                    if idx < len(row):
                         raw_val = row[idx]
                         val = self.parse_distance(raw_val)
                         if val is not None:
                             athlete_data[evt] = val
                
                if athlete_data.get('Athlete') and current_class != "Unknown":
                     structured_results[current_class].append(athlete_data)
        return structured_results

    async def scrape_game_async(self, session, game_id, game_name, year, semaphore, failure_logger=None):
        async with semaphore:
            logger.info(f"    Scraping Game: {game_name} ({game_id})")
            data = {'gamesid': game_id, 'Submit': 'Select'}
            resp_text = await async_fetch_url(session, self.RESULTS_URL, method='POST', data=data, settings=self.settings)
            if not resp_text:
                if failure_logger:
                    failure_logger.log_failure(self.RESULTS_URL, game_id, f"Failed to fetch game results for {game_name} ({year})")
                return None
            
            soup = BeautifulSoup(resp_text, 'html.parser')
            tables_data = []
            for table in soup.find_all('table'):
                 t_rows = []
                 for row in table.find_all('tr'):
                     cols = [ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])]
                     t_rows.append(cols)
                 if t_rows:
                     tables_data.append(t_rows)

            structured_data = self.parse_game_tables(tables_data)
            return {
                'id': game_id,
                'name': game_name,
                'year': year,
                'results': structured_data
            }





