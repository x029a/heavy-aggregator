from . import Scraper
import logging

logger = logging.getLogger("HeavyAggregator")

from . import Scraper
import logging
from bs4 import BeautifulSoup
from utils import get_session, fetch_url
import json
import csv
import os
import urllib.parse

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

