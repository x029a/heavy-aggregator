from . import Scraper
import logging
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter
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
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        max_lines = self.settings.get('max_output_line_count', 0)
        
        # Initialize Writers
        games_writer = StreamingJSONWriter(output_dir, f'nasga_games_{date_str}.json', max_lines)
        athletes_writer = StreamingJSONWriter(output_dir, f'nasga_athletes_{date_str}.json', max_lines)
        
        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)

        try:
            # 1. Get Years
            logger.info("Fetching available years...")
            years = await self.get_years(session)
            if not years:
                logger.error("No years found. Exiting.")
                return

            logger.info(f"Found {len(years)} years to process: {years}")

            unique_athletes = set()
            
            # Resume Logic for Years
            start_year_idx = 0
            saved_year = self.checkpoint.get("nasga_last_completed_year")
            if saved_year and saved_year in years:
                # If we saved 2000, we want to start at the NEXT one? 
                # Or simply years handled. Since years list is reversed (2025, 2024...), 
                # identifying index is safer.
                try:
                    idx = years.index(saved_year)
                    start_year_idx = idx + 1 # Start with next
                    logger.info(f"Resuming Games scraping after year {saved_year}...")
                except ValueError:
                    pass

            # 2. Iterate Years to collect Games and Athletes
            # Phase 1: Games and Athlete Discovery
            for i in range(start_year_idx, len(years)):
                year = years[i]
                logger.info(f"Scanning Year: {year}")
                
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
                    game_tasks.append(self.scrape_game_async(session, game_id, game_name, year, semaphore))
                
                game_results = await asyncio.gather(*game_tasks)
                
                for res in game_results:
                    if res:
                        games_writer.write_item(res)
                
                # Get Athletes (Collect only)
                # We need to collect ALL athletes across all years first before Phase 2?
                # Or can we assume we scrape athletes at the end?
                # Yes, standard flow is: Games (Year 1..N) -> Then Athletes (A..Z)
                # We save unique_athletes in memory. If crashed, we might lose 'discovered' athletes?
                # Ideally we should checkpoint discovered athletes too?
                # For simplicity: Re-scanning years to discover athletes is fast. 
                # Re-scraping games is slow. We checkpoint "Games Done".
                
                athletes = self.get_dropdown_options(soup, 'athletename')
                for name, value in athletes.items():
                    if value and value != '0' and not name.startswith('Select'):
                        unique_athletes.add(value)
                
                logger.info(f"  Accumulated {len(unique_athletes)} unique athletes so far.")
                
                # Save Checkpoint
                self.checkpoint.save("nasga_last_completed_year", year)

            # Phase 2: Athlete Details
            sorted_athletes = sorted(list(unique_athletes))
            logger.info(f"Scraping details for {len(sorted_athletes)} athletes...")
            
            # Resume Logic for Athletes
            # If we crashed during athlete phase, we check `nasga_athlete_index`
            start_ath_idx = self.checkpoint.get("nasga_athlete_index", 0)
            if start_ath_idx > 0:
                 logger.info(f"Resuming Athlete scraping from index {start_ath_idx}...")

            # Process in chunks to avoid massive memory usage / task list
            # But asyncio.gather is fine for thousands if semaphore used.
            # Let's simple loop with semaphore passing or chunks.
            
            BATCH_SIZE = 50
            for i in range(start_ath_idx, len(sorted_athletes), BATCH_SIZE):
                batch = sorted_athletes[i : i + BATCH_SIZE]
                tasks = []
                for ath_name in batch:
                     tasks.append(self.scrape_athlete_async(session, ath_name, semaphore))
                
                results = await asyncio.gather(*tasks)
                
                for res in results:
                    if res:
                        athletes_writer.write_item(res)
                
                # Checkpoint
                self.checkpoint.save("nasga_athlete_index", i + len(batch))
                if i % 100 == 0:
                     logger.info(f"  Processed {i + len(batch)}/{len(sorted_athletes)} athletes...")
            
            logger.info("NASGA Scraping Complete.")
            
            # Clear checkpoint on success?
            # self.checkpoint.clear()
            # Maybe keep it to prevent accidental re-run? 
            # User can delete if they want fresh start.

        finally:
            games_writer.close()
            athletes_writer.close()
            await session.close()

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
                
                event_headers = [h for i, h in enumerate(current_headers) if i >= 3 and h]
                data_col_idx = 3
                for evt in event_headers:
                    if data_col_idx < len(row):
                         raw_val = row[data_col_idx]
                         val = self.parse_distance(raw_val)
                         if val is not None:
                             athlete_data[evt] = val
                    data_col_idx += 2
                
                if athlete_data.get('Athlete') and current_class != "Unknown":
                     structured_results[current_class].append(athlete_data)
        return structured_results

    async def scrape_game_async(self, session, game_id, game_name, year, semaphore):
        async with semaphore:
            logger.info(f"    Scraping Game: {game_name} ({game_id})")
            data = {'gamesid': game_id, 'Submit': 'Select'}
            resp_text = await async_fetch_url(session, self.RESULTS_URL, method='POST', data=data, settings=self.settings)
            if not resp_text: return None
            
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

    async def scrape_athlete_async(self, session, ath_name, semaphore):
        async with semaphore:
            encoded_name = urllib.parse.quote(ath_name)
            url = f"{self.ATHLETE_URL}?athletename={encoded_name}"
            resp_text = await async_fetch_url(session, url, settings=self.settings)
            if not resp_text: return None
            
            soup = BeautifulSoup(resp_text, 'html.parser')
            tables_data = []
            for table in soup.find_all('table'):
                 t_rows = []
                 for row in table.find_all('tr'):
                     cols = [ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])]
                     t_rows.append(cols)
                 if t_rows:
                     tables_data.append(t_rows)
            
            return {
                'name': ath_name,
                'history': tables_data
            }

logger = logging.getLogger("HeavyAggregator")

class NasgaScraper(Scraper):
    BASE_URL = "http://www.nasgaweb.com/dbase/main.asp"
    RESULTS_URL = "http://www.nasgaweb.com/dbase/results2.asp"
    ATHLETE_URL = "http://www.nasgaweb.com/dbase/resultsathlete3.asp"

    def run(self):
        logger.info("Starting NASGA Scraper...")
        session = get_session(self.settings)
        
        # Setup Output Files for Streaming via Helper Class
        from datetime import datetime
        from utils import StreamingJSONWriter
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        max_lines = self.settings.get('max_output_line_count', 0)
        
        # Initialize Writers
        games_writer = StreamingJSONWriter(output_dir, f'nasga_games_{date_str}.json', max_lines)
        athletes_writer = StreamingJSONWriter(output_dir, f'nasga_athletes_{date_str}.json', max_lines)
        
        try:
            # 1. Get Years
            logger.info("Fetching available years...")
            years = self.get_years(session)
            logger.info(f"Found {len(years)} years to process: {years}")

            unique_athletes = set()

            # 2. Iterate Years to collect Games and Athletes
            for year in years:
                logger.info(f"Scanning Year: {year}")
                
                year_url = f"{self.BASE_URL}?resultsyear={year}"
                resp = fetch_url(session, year_url, settings=self.settings)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # Get Games
                games = self.get_dropdown_options(soup, 'gamesid')
                valid_games = {}
                for name, value in games.items():
                     if value and value not in ['0', 'none', ''] and not name.startswith('Select') and not name.startswith('---'):
                         valid_games[value] = name

                logger.info(f"  Found {len(valid_games)} games.")

                # Scrape Games for this Year AND STREAM
                for game_id, game_name in valid_games.items():
                    logger.info(f"    Scraping Game: {game_name} ({game_id})")
                    game_data = self.scrape_game(session, game_id, game_name, year)
                    if game_data:
                        games_writer.write_item(game_data)
                
                # Get Athletes (Collect only)
                athletes = self.get_dropdown_options(soup, 'athletename')
                for name, value in athletes.items():
                    if value and value != '0' and not name.startswith('Select'):
                        unique_athletes.add(value)
                
                logger.info(f"  Accumulated {len(unique_athletes)} unique athletes so far.")

            # 3. Scrape Athlete Details AND STREAM
            sorted_athletes = sorted(list(unique_athletes))
            logger.info(f"Scraping details for {len(sorted_athletes)} athletes...")
            
            for i, ath_name in enumerate(sorted_athletes):
                logger.info(f"  [{i+1}/{len(sorted_athletes)}] Scraping Athlete: {ath_name}")
                data = self.scrape_athlete(session, ath_name)
                if data:
                    athletes_writer.write_item(data)
            
            logger.info("NASGA Scraping Complete.")

        finally:
            # Close Writers
            games_writer.close()
            athletes_writer.close()

    # save_results removed as it is now handled via streaming in run()

    def get_years(self, session):
        resp = fetch_url(session, self.BASE_URL, settings=self.settings)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # DEBUG: Dump HTML if parsing fails
        # with open('debug_main.html', 'w') as f:
        #     f.write(soup.prettify())
            
        years = self.get_dropdown_options(soup, 'resultsyear')
        
        if not years:
            logger.warning("No years found in dropdown 'resultsyear'. Dumping HTML to debug_main.html")
            with open('debug_main.html', 'w') as f:
                f.write(soup.prettify())
        else:
            logger.info(f"Raw years found: {list(years.items())[:10]}...") # Log first 10

        # Filter valid years (4 digits)
        # The dropdown values are URLs (e.g. main.asp?resultsyear=2024), but text is the year.
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
            # Replace whitespace chars
            return text.replace('\u00a0', ' ').strip()
        return text

    def parse_number(self, text, dtype='float'):
        if not text:
            return None
        text = str(text).strip()
        
        # Handle "T1" (Tie 1) -> 1
        if text.upper().startswith('T') and text[1:].isdigit():
            text = text[1:]
            
        try:
            if dtype == 'int':
                return int(float(text)) # float first to handle 1.0 -> 1
            else:
                return float(text)
        except ValueError:
            return text # Return original string if fail (e.g. "DQ")

    def parse_distance(self, text):
        import re
        if not text:
            return None
            
        text = str(text).strip()
        
        # Handle Null-like values
        if text.upper() in ['NT', 'DNS', '-', '']:
            return None
        
        # Caber toss (Time format) - keep as is
        if ':' in text:
            return text
            
        # Feet and Inches: 20'-4", 20'-4.5"
        match = re.match(r"(\d+)'\s*-?\s*(\d*\.?\d*)\"?", text)
        if match:
            feet = float(match.group(1))
            inches_str = match.group(2)
            inches = float(inches_str) if inches_str else 0
            return round(feet + (inches / 12.0), 3)
        
        # Just Feet: 20'
        match_ft = re.match(r"(\d+)'$", text)
        if match_ft:
            return float(match_ft.group(1))

        # Just Number -> float
        try:
            return float(text)
        except ValueError:
            pass

        return text

    def parse_game_tables(self, tables):
        structured_results = {}
        current_class = "Unknown"
        current_headers = []
        
        all_rows = []
        for table in tables:
            for row in table:
                cleaned_row = [self.clean_text(cell) for cell in row]
                # Filter out empty rows
                if not any(cleaned_row):
                    continue
                all_rows.append(cleaned_row)

        for row in all_rows:
            non_empty = [c for c in row if c]
            
            # Heuristic 1: Class Header (Single column or distinct)
            if len(non_empty) <= 2 and len(non_empty) > 0:
                val = non_empty[0]
                if val not in ["Athlete", "Dist", "Pts"] and \
                   not any(x in val for x in ["Notes:", "View the log", "Copyright", "Database Main", "Home|"]):
                        # Check digits to avoid just numbers/scores being headers
                        # Classes usually contain text (Men, Women, Masters...)
                        if not val.replace('.','').isdigit():
                            current_class = val
                            if current_class not in structured_results:
                                structured_results[current_class] = []
                            continue

            # Heuristic 2: Main Header Row
            if "Athlete" in row:
                current_headers = row
                continue
                
            # Heuristic 3: Sub-Headers / Junk
            if not non_empty or "Dist" in row or "Pts" in row:
                continue
            if any(x in non_empty[0] for x in ["Notes:", "View the log", "Copyright", "Database Main", "Home|"]):
                continue
                
            # Data Row
            if current_headers:
                if "Athlete" in row: 
                    continue
                    
                athlete_data = {}
                # Basic info: Athlete(0), Place(1), Points(2)
                if len(row) > 0: athlete_data['Athlete'] = row[0]
                if len(row) > 1: 
                    athlete_data['Place'] = self.parse_number(row[1], dtype='int')
                if len(row) > 2: 
                    athlete_data['GamesPoints'] = self.parse_number(row[2], dtype='float')
                
                # Events: Start at Index 3 (usually OpenStone or similar)
                # Parse available headers starting from index 3
                event_headers = [h for i, h in enumerate(current_headers) if i >= 3 and h]
                
                # Map using stride=2 (Dist, Pts)
                data_col_idx = 3
                for evt in event_headers:
                    if data_col_idx < len(row):
                         raw_val = row[data_col_idx]
                         val = self.parse_distance(raw_val)
                         # Only add non-nulls? Or keep null keys? 
                         # Keeping null keys makes schema consistent for CSV/Analyst
                         # But dense JSON is smaller. Let's strictly add it.
                         if val is not None:
                             athlete_data[evt] = val
                    data_col_idx += 2 # Skip Pts column
                
                if athlete_data.get('Athlete') and current_class != "Unknown":
                     structured_results[current_class].append(athlete_data)

        return structured_results

    def scrape_game(self, session, game_id, game_name, year):
        # Games results are POSTed to results2.asp
        data = {
            'gamesid': game_id,
            'Submit': 'Select' 
        }
        resp = fetch_url(session, self.RESULTS_URL, method='POST', data=data, settings=self.settings)
        if not resp:
            return None
        
        # Parse Game Results
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        tables_data = []
        for table in soup.find_all('table'):
             t_rows = []
             for row in table.find_all('tr'):
                 cols = [ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])]
                 t_rows.append(cols)
             if t_rows:
                 tables_data.append(t_rows)

        # Parse the tables into structure
        structured_data = self.parse_game_tables(tables_data)

        return {
            'id': game_id,
            'name': game_name,
            'year': year,
            'results': structured_data
        }

    def scrape_athlete(self, session, ath_name):
        # Athlete results are GET
        # Encoded name
        encoded_name = urllib.parse.quote(ath_name)
        # However, requests handles params nicely, but the site might expect specific formatting (comma)
        # The browser steps used: resultsathlete3.asp?athletename=Anthony%2CLaura
        
        params = {'athletename': ath_name}
        # Using params in fetch_url would require modifying utils, so let's construct URL manually or update utils.
        # The `fetch_url` doesn't support params kwarg in my simple definition, but `requests` does.
        # Let's just construct the URL to be safe/explicit.
        url = f"{self.ATHLETE_URL}?athletename={encoded_name}"
        
        resp = fetch_url(session, url, settings=self.settings)
        if not resp:
            return None
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Extract tables
        tables_data = []
        for table in soup.find_all('table'):
             t_rows = []
             for row in table.find_all('tr'):
                 cols = [ele.get_text(strip=True) for ele in row.find_all(['td', 'th'])]
                 t_rows.append(cols)
             if t_rows:
                 tables_data.append(t_rows)
                 
        return {
            'name': ath_name,
            'history': tables_data
        }

    def save_results(self, games, athletes):
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        fmt = self.settings.get('output_format', 'json')
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        if fmt == 'json':
            with open(f'{output_dir}/nasga_games_{date_str}.json', 'w') as f:
                json.dump(games, f, indent=2)
            with open(f'{output_dir}/nasga_athletes_{date_str}.json', 'w') as f:
                json.dump(athletes, f, indent=2)
        elif fmt == 'csv':
            # Flattening this structure to CSV is complex.
            # We'll do a simplified Dump or just recommend JSON.
            # For now, let's just log a warning and dump JSON anyway or try to flatten.
            logger.warning("CSV output for complex nested structures is experimental. Saving JSON backup.")
            with open(f'{output_dir}/nasga_data_{date_str}.json', 'w') as f:
                 json.dump({'games': games, 'athletes': athletes}, f, indent=2)

