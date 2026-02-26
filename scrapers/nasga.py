from . import Scraper
import logging
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, parse_athlete_name, FailedItemWriter, parse_distance
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
        logger.info("  [VERIFIED] NasgaScraper initialized with POST fixes and Dist filtering.")

    async def run(self):
        logger.info("Starting NASGA Scraper (Async)...")
        # Setup Session with Specific Referer for Results
        session = await get_async_session(self.settings)
        session.headers.update({
            'Referer': 'http://www.nasgaweb.com/dbase/main.asp'
        })
        
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
        
        total_games = 0

        try:
            # 1. Get Years
            logger.info("Fetching available years...")
            years = await self.get_years(session)
            if not years:
                logger.error("No years found. Exiting.")
                return {'site': 'Nasga', 'games_count': 0, 'athletes_count': 0}

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
                    total_games += len(valid_games)
    
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
            return {
                'site': 'Nasga',
                'games_count': total_games,
                'athletes_count': len(self.athlete_db)
            }

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
        path = os.path.join(base_dir, 'nasga_athletes.json')
        try:
            data = list(self.athlete_db.values())
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save athletes file: {e}")

    async def get_years(self, session):
        resp_text = await async_fetch_url(session, self.BASE_URL, settings=self.settings)
        if not resp_text: return []
        
        soup = BeautifulSoup(resp_text, 'html.parser')
        years_dict = self.get_dropdown_options(soup, 'resultsyear')
        
        # Convert to sorted list of ints
        years = []
        for y_str in years_dict.values():
            if 'resultsyear=' in y_str:
                y_str = y_str.split('resultsyear=')[-1]
            
            if y_str.isdigit():
                years.append(int(y_str))
        
        return sorted(years)

    def get_dropdown_options(self, soup, select_name):
        options = {}
        select = soup.find('select', {'name': select_name})
        if select:
            for opt in select.find_all('option'):
                val = opt.get('value')
                text = opt.get_text(strip=True)
                options[text] = val
        return options

    def parse_distance(self, text):
        return parse_distance(text)

    async def scrape_game_async(self, session, game_id, game_name, year, semaphore, failure_logger=None):
        async with semaphore:
            # ORIGINAL LOGIC: Use POST
            # url = f"{self.RESULTS_URL}?gamesid={game_id}&resultsyear={year}" 
            # POST doesn't use query params, but data payload.
            
            data = {
                'gamesid': game_id,
                'Submit': 'Select'
            }
            
            logger.info(f"    Scraping Game: {game_name} ({game_id})")
            
            # Use self.RESULTS_URL directly
            resp_text = await async_fetch_url(session, self.RESULTS_URL, method='POST', data=data, settings=self.settings)
            
            if not resp_text:
                if failure_logger:
                    failure_logger.log_failure(self.RESULTS_URL, game_id, f"Failed to fetch game: {game_name} ({year})")
                return None
            
            soup = BeautifulSoup(resp_text, 'html.parser')

            # Find data table
            table = None
            for t in soup.find_all('table'):
                if "Athlete" in t.get_text() and "Place" in t.get_text():
                    table = t
                    break
            
            if not table:
                return None

            results = {}
            current_class = "Unknown"
            headers = []
            
            rows = table.find_all('tr')
            for row in rows:
                cols = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                if not cols: continue
                
                if 'Athlete' in cols:
                    headers = cols
                    continue
                
                # Exclude header-like rows or metadata
                if 'Dist' in cols or 'Pts' in cols: continue
                if any(x in cols[0] for x in ["Notes:", "View the log", "Copyright", "Database Main", "Home|"]): continue
                    
                if len(cols) == 1:
                     current_class = cols[0]
                     if current_class not in results:
                         results[current_class] = []
                     continue
                
                if headers and 'Athlete' in headers:
                    entry = {}
                    athlete_name = ""
                    
                    # Map columns to headers
                    for i, col in enumerate(cols):
                        if i >= len(headers): break
                        header = headers[i]
                        
                        if header == 'Athlete':
                            athlete_name = col
                        elif header == 'Place':
                            entry['Place'] = col
                        elif header == 'Points':
                            entry['GamesPoints'] = col
                        else:
                             # Event Result
                             entry[header] = self.parse_distance(col)
                    
                    if athlete_name:
                         # Use raw string for compatibility with _update_athlete_db
                         # Clean unicode non-breaking spaces
                         entry['Athlete'] = athlete_name.replace('\u00a0', ' ').strip()
                         
                         if current_class not in results:
                             results[current_class] = []
                         
                         results[current_class].append(entry)

            return {
                'id': game_id,
                'name': game_name,
                'year': year,
                'results': results
            }
