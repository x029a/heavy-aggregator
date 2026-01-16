import logging
import time
from datetime import datetime
from bs4 import BeautifulSoup
from utils import get_session, fetch_url, StreamingJSONWriter, ColoredFormatter
import re
import os

logger = logging.getLogger("HeavyAggregator")

class HeavyAthleteScraper:
    BASE_URL = "https://heavyathlete.com"

    def __init__(self, settings):
        self.settings = settings

    def clean_text(self, text):
        if isinstance(text, str):
            return text.replace('\u00a0', ' ').strip()
        return text

    def parse_number(self, text, dtype='float'):
        if not text:
            return None
        text = str(text).strip()
        if text.upper().startswith('T') and text[1:].isdigit():
            text = text[1:]
        try:
            if dtype == 'int':
                return int(float(text))
            else:
                return float(text)
        except ValueError:
            return text

    def parse_distance(self, text):
        if not text:
            return None
        text = str(text).strip()
        if text.upper() in ['NT', 'DNS', '-', '', 'F']:
            return None
        
        # Already checked NASGA logic, re-using robust regexes
        if ':' in text: return text # Time
        
        # 20'-4"
        match = re.match(r"(\d+)'\s*-?\s*(\d*\.?\d*)\"?", text)
        if match:
            feet = float(match.group(1))
            inches_str = match.group(2)
            inches = float(inches_str) if inches_str else 0
            return round(feet + (inches / 12.0), 3)
        
        # 20'
        match_ft = re.match(r"(\d+)'$", text)
        if match_ft:
            return float(match_ft.group(1))

        # Check for numeric
        try:
            return float(text)
        except ValueError:
            pass
            
        return text

    def parse_scores_html(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        structured_results = {}
        current_class = "Unknown"
        event_headers = []
        
        # HeavyAthlete uses Bootstrap tables
        # Look for the table
        table = soup.find('table')
        if not table:
            return {}

        rows = table.find_all('tr')
        
        for row in rows:
            # Check for Class Header (single th usually)
            # Example: <tr><th>Amateur A</th></tr>
            th_cells = row.find_all('th')
            td_cells = row.find_all('td')
            
            all_cells = th_cells + td_cells
            clean_cells = [self.clean_text(c.get_text()) for c in all_cells]
            non_empty = [c for c in clean_cells if c]

            if not non_empty: continue

            # Class Header Identification
            # If it's a single TH cell with text, likely a class name
            if len(th_cells) == 1 and not td_cells:
                val = non_empty[0]
                # Filter out "Historic Scores" or other meta headers if needed
                if "Historic Scores" not in val and "NASGA Clone" not in val:
                    current_class = val
                    if current_class not in structured_results:
                        structured_results[current_class] = []
                continue

            # Event Header Identification
            # <th class="no-wrap">Athlete Name</th> ...
            if "Athlete Name" in clean_cells:
                event_headers = clean_cells
                continue

            # Data Row
            # Must have data cells and we must have a current class
            if td_cells and current_class != "Unknown" and event_headers:
                athlete_data = {}
                
                # Map headers to values by index
                # clean_cells matches event_headers length ideally
                # But sometimes colspan usage might mess it up? 
                # HeavyAthlete tables seem regular.
                
                # Identify Athlete Name index
                try:
                    name_idx = event_headers.index("Athlete Name")
                except ValueError:
                    continue # Skip if we can't find name column

                if name_idx < len(clean_cells):
                    athlete_name = clean_cells[name_idx]
                    if not athlete_name: continue
                    
                    athlete_data['Athlete'] = athlete_name
                    
                    # Parse other columns
                    for i, header in enumerate(event_headers):
                        if i == name_idx: continue
                        if i < len(clean_cells):
                            val = clean_cells[i]
                            
                            # Determine if Place, Points or Event
                            if header in ['Place', 'Rank']:
                                athlete_data['Place'] = self.parse_number(val, 'int')
                            elif header in ['Pts', 'Points', 'Total']:
                                athlete_data['GamesPoints'] = self.parse_number(val, 'float')
                            else:
                                # Likely an Event
                                parsed_val = self.parse_distance(val)
                                if parsed_val is not None:
                                    athlete_data[header] = parsed_val
                    
                    structured_results[current_class].append(athlete_data)

        return structured_results

    def run(self):
        logger.info("Starting Heavy Athlete Scraper...")
        session = get_session(self.settings)

        # Output Setup
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        max_lines = self.settings.get('max_output_line_count', 0)
        games_writer = StreamingJSONWriter(output_dir, f'heavyathlete_games_{date_str}.json', max_lines)
        
        # Date Range: 1999 to Current Year + 1 
        current_year = datetime.now().year
        start_year = 1999
        years = range(start_year, current_year + 2) # +2 to include next year (scheduled games)
        
        try:
            for year in years:
                logger.info(f"Scanning Year: {year}")
                for month in range(1, 13):
                    # Discovery URL: /game/calendar_list/YYYY/M/
                    calendar_url = f"{self.BASE_URL}/game/calendar_list/{year}/{month}/"
                    resp = fetch_url(session, calendar_url, settings=self.settings)
                    if not resp: continue
                    
                    # Regex to find game links: <a href="/game/1234/">
                    # Pattern: href="/game/(\d+)/"
                    game_ids = set(re.findall(r'href="/game/(\d+)/"', resp.text))
                    
                    if game_ids:
                        logger.info(f"  Month {month}: Found {len(game_ids)} games.")
                    
                    for gid in game_ids:
                        # Scrape Game details
                        # We need Name and Metadata roughly. 
                        # The calendar page has names, but messy to parse with regex only.
                        # Let's hit the game page or scores page?
                        # The Scores HTMX is best for data.
                        # Metadata (Name, Date, Location) is in the main /game/{id}/ page.
                        # For efficiency, we might just grab the name from the Calendar page if regex permits?
                        # Regex for ID and Name: <a href="/game/5630/">Central Florida Highland Games</a>
                        
                        # Let's just create a basic entry and fill data from scores.
                        # Ideally we want Game Name.
                        # Try to find Name in calendar text:
                        match_name = re.search(f'href="/game/{gid}/">([^<]+)</a>', resp.text)
                        game_name = match_name.group(1).strip() if match_name else f"Game {gid}"
                        
                        logger.info(f"    Scraping Game: {game_name} ({gid})")
                        
                        # Fetch Scores (HTMX endpoint)
                        scores_url = f"{self.BASE_URL}/game/{gid}/scores_htmx/"
                        scores_resp = fetch_url(session, scores_url, settings=self.settings)
                        
                        structured_results = {}
                        if scores_resp:
                            structured_results = self.parse_scores_html(scores_resp.text)
                        
                        game_obj = {
                            'id': gid,
                            'name': game_name,
                            'year': str(year),
                            'month': str(month),
                            'source': 'heavyathlete.com',
                            'results': structured_results
                        }
                        
                        games_writer.write_item(game_obj)

        finally:
            games_writer.close()
            logger.info("Heavy Athlete Scraping Complete.")
