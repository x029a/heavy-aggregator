from . import Scraper
import logging
import asyncio
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, parse_athlete_name, FailedItemWriter, parse_distance
from checkpoint import CheckpointManager

# ... (rest of imports)


import os
import urllib.parse
import re
import json

logger = logging.getLogger("HeavyAggregator")

class ScottishScoresScraper(Scraper):
    BASE_URL = "https://scottishscores.com"
    SESSION_SET_URL = "https://scottishscores.com/SessionYrSet.cfm"
    INDEX_URL = "https://scottishscores.com/index.cfm"
    ATHLETE_LIST_URL = "https://scottishscores.com/prMenu.cfm?FC=0"

    def __init__(self, settings):
        super().__init__(settings)
        self.checkpoint = CheckpointManager()

    async def run(self):
        logger.info("Starting Scottish Scores Scraper (Async)...")
        session = await get_async_session(self.settings)

        # Output Setup
        from datetime import datetime
        now = datetime.now()
        # Base Output Directory
        base_output_dir = os.path.join('output', 'scottishscores')
        if not os.path.exists(base_output_dir): os.makedirs(base_output_dir)

        max_lines = self.settings.get('max_output_line_count', 0)

        # Initialize Writers (Failures at base)
        failure_logger = FailedItemWriter(base_output_dir, 'scottishscores_failed_retrievals.json')
        
        # Athlete DB
        self.athlete_db = {}

        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)

        total_games = 0

        try:
            # 1. Scrape Games (Iterate Years)
            start_year = 1990
            end_year = now.year + 1
            years = list(range(start_year, end_year + 1))
            
            # Resume Year
            saved_year = self.checkpoint.get("scottishscores_year")
            if saved_year:
                logger.info(f"Resuming form year {saved_year}...")
                try:
                    idx = years.index(int(saved_year))
                    years = years[idx:]
                except ValueError:
                    pass # Reset if invalid

            for year in years:
                logger.info(f"Scanning Year: {year}")
                
                # Setup Year Directory
                year_dir = os.path.join(base_output_dir, str(year))
                if not os.path.exists(year_dir):
                    os.makedirs(year_dir)

                # Year-specific Games Writer
                year_games_writer = StreamingJSONWriter(year_dir, 'scottishscores_games.json', max_lines)
                
                try:
                    # Switch Session Year
                    post_data = {'FilterYear': str(year)}
                    await async_fetch_url(session, self.SESSION_SET_URL, method='POST', data=post_data, settings=self.settings)
                    
                    # Now fetch Index to get games for this year
                    idx_resp = await async_fetch_url(session, self.INDEX_URL, settings=self.settings)
                    if not idx_resp:
                        logger.warning(f"Failed to fetch index for {year}")
                        continue
    
                    games = self.parse_games_list(idx_resp)
                    logger.info(f"  Found {len(games)} games in {year}.")
                    total_games += len(games)
                    
                    # Scrape detailed games in parallel
                    game_tasks = [self.scrape_game_detail(session, g, semaphore, failure_logger) for g in games]
                    results = await asyncio.gather(*game_tasks)
                    
                    for res in results:
                        if res:
                            year_games_writer.write_item(res)
                            self._update_athlete_db(res, year)
                            
                finally:
                    year_games_writer.close()
                
                # Save Athletes
                self._save_athletes(base_output_dir)
                logger.info(f"  Saved {len(self.athlete_db)} athletes to scottishscores_athletes.json")
                
                self.checkpoint.save("scottishscores_year", year)

            logger.info("ScottishScores Scraping Complete.")
            return {
                'site': 'Scottish Scores',
                'games_count': total_games,
                'athletes_count': len(self.athlete_db)
            }

        finally:
            await session.close()

    async def scrape_game_detail(self, session, game, semaphore, failure_logger=None):
        async with semaphore:
            url = f"{self.BASE_URL}/{game['url']}"
            if game['url'].startswith('http'):
                url = game['url']
            elif game['url'].startswith('/'):
                 url = f"{self.BASE_URL}{game['url']}"
            
            logger.info(f"    Scraping Game: {game['name']} ({game['id']})")
            resp_text = await async_fetch_url(session, url, settings=self.settings)
            
            if not resp_text:
                if failure_logger:
                    failure_logger.log_failure(url, game['id'], f"Failed to fetch game details for {game['name']}")
                return None
            
            soup = BeautifulSoup(resp_text, 'html.parser')
            # Extract year/date from page if possible, or fallback to passed year?
            # actually run() passes year to _update_athlete_db, so we just need results here.
            
            results = self.parse_game_results_table(soup)
            
            return {
                'id': game['id'],
                'name': game['name'],
                'date': game.get('date'),
                'url': url,
                'results': results
            }

    def _update_athlete_db(self, game_result, year):
        # game_result: {id, name, date, results: {Class: [entries]}}
        
        game_name = game_result.get('name', 'Unknown Game')
        # Use explicit date if available, else fallback
        date_str = game_result.get('date')
        if not date_str:
             date_str = f"01/01/{year}" # Default fallback

        
        game_log_info = {
            'Date': date_str,
            'Game': game_name,
            'GameID': game_result.get('id'),
            'Year': year
        }

        for cls, athletes in game_result.get('results', {}).items():
            for ath in athletes:
                name_data = ath.get('Athlete')
                if not name_data: continue
                
                # Handle name as dict or string
                if isinstance(name_data, dict):
                    name = f"{name_data.get('firstName', '')} {name_data.get('lastName', '')}".strip()
                else:
                    name = str(name_data).strip()

                if not name: continue
                
                if name not in self.athlete_db:
                    self.athlete_db[name] = {
                        'name': name,
                        'history': []
                    }
                
                entry = {
                    'Date': game_log_info['Date'],
                    'Game': game_log_info['Game'],
                    'GameID': game_log_info['GameID'], # Added for dedupe
                    'Class': cls,
                    'Place': ath.get('Place'),
                    'Points': ath.get('GamesPoints')
                }
                
                # Check for duplicates
                exists = False
                for existing in self.athlete_db[name]['history']:
                    if existing.get('GameID') == entry['GameID'] and existing.get('Class') == entry['Class']:
                         exists = True
                         break
                
                if not exists:
                    self.athlete_db[name]['history'].append(entry)

    def _save_athletes(self, base_dir):
        path = os.path.join(base_dir, 'scottishscores_athletes.json')
        try:
            data = list(self.athlete_db.values())
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save athletes file: {e}")

    def parse_games_list(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        games = []
        # Games are in links like classesListNew.cfm?GameCode=XYZ
        # Usually inside a table
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'classesListNew.cfm' in href and 'GameCode=' in href:
                try:
                    parsed = urllib.parse.urlparse(href)
                    qs = urllib.parse.parse_qs(parsed.query)
                    code = qs.get('GameCode', [''])[0]
                    if code:
                        # Fix for "View" as name
                        name = a.get_text(strip=True)
                        date_str = None
                        
                        if name == "View":
                            # Try to find name in the row
                            row = a.find_parent('tr')
                            if row:
                                cols = row.find_all('td')
                                if cols and len(cols) > 0:
                                     # First column is the Game Name
                                     name = cols[0].get_text(strip=True)
                                     # Third column (index 2) is the Date
                                     if len(cols) > 2:
                                         raw_date = cols[2].get_text(strip=True)
                                         # Check for concatenated dates: 10 chars + 10 chars
                                         # e.g. 02/21/202602/22/2026
                                         # Regex for MM/DD/YYYYMM/DD/YYYY
                                         match = re.search(r'(\d{2}/\d{2}/\d{4})(\d{2}/\d{2}/\d{4})', raw_date)
                                         if match:
                                             date_str = f"{match.group(1)} - {match.group(2)}"
                                         else:
                                             date_str = raw_date
                                     
                        games.append({'id': code, 'name': name, 'date': date_str, 'url': href})
                except Exception:
                    pass
        return games



    def parse_clean_distance(self, text):
        return parse_distance(text)

    def clean_text(self, text):
        if isinstance(text, str):
            # Replace non-breaking space with space, strip whitespace
            return text.replace('\u00a0', ' ').strip()
        return text

    def parse_game_results_table(self, soup):
        structured = {}
        current_class = "Unknown"
        event_headers = []
        current_pts_idx = 2 # Default index for Points column
        
        # Iterate all rows in document order
        # The site structure is inconsistent, so we stream through TRs
        all_rows = []
        for table in soup.find_all('table'):
            # Skip tables that are layout tables containing other tables
            if table.find('table'):
                continue
                
            for tr in table.find_all('tr'):
                # Clean every cell immediately
                cols = [self.clean_text(td.get_text()) for td in tr.find_all(['td', 'th'])]
                if cols:
                    all_rows.append(cols)
        
        for row in all_rows:
            if not row: continue
            first_cell = row[0]
            
            # Heuristic: Class Header
            # Often single column (or few), upper case, not "Athlete"
            # e.g., "MENS PROFESSIONAL"
            if len(row) < 3 and len(first_cell) > 3 and not "Athlete" in row and not "Print" in first_cell:
                 # Check if it's junk
                 if "View" in first_cell or "Done" in first_cell: continue
                 
                 # Fix for "JUNIORS    Christena..."
                 # Split by multiple spaces if present, or just assume first token if it looks like a class?
                 # Better: If > 2 spaces? Or just split on double space.
                 # "JUNIORS\u00a0\u00a0..." -> "JUNIORS" (already cleaned to "JUNIORS  ...")
                 # Using regex to split on 2 or more whitespace characters
                 parts = re.split(r'\s{2,}', first_cell)
                 potential_class = parts[0].strip()
                 
                 current_class = potential_class
                 
                 if current_class not in structured:
                     structured[current_class] = []
                 event_headers = [] # Reset headers for new class
                 current_pts_idx = 2 # Reset to default
                 continue
                 
            # Heuristic: Header Row
            if "Athlete" in row:
                # Find index of events starting after "Points"
                try:
                    pts_idx = row.index("Points")
                    current_pts_idx = pts_idx # Update using discovered index
                    # Events are after Points: [Braemar, Open, ...]
                    # Warning: Headers might be condensed or have weird names
                    event_headers = row[pts_idx+1:]
                    
                    # Sanitize Headers
                    cleaned_headers = []
                    for h in event_headers:
                        # Fix Caber weirdness
                        if "Caber0-0" in h or "0 lbs" in h:
                            h = "Caber"
                        # Fix Sheaf0
                        elif h == "Sheaf0":
                            h = "Sheaf"
                        elif h.endswith("0") and len(h) > 1 and not h[-2].isdigit():
                             # e.g. "Hammer0" -> "Hammer"? Be careful with "10"
                             # But "Sheaf0" ends with 0. "Sheaf10" does too.
                             # Sheaf0 check above handles it.
                             pass
                        
                        cleaned_headers.append(h)
                    event_headers = cleaned_headers
                except ValueError:
                    pass
                continue
                
            # Heuristic: Junk Row
            if "Print Class Results" in first_cell or "Extra Throws" in first_cell:
                continue
            if first_cell == "Athlete": continue # Should be caught by Header check but safe to double check
            
            # Heuristic: Data Row
            # Needs to have Athlete Name, Place (1st, 2nd..), Points (num)
            if len(row) >= 3 and event_headers:
                ath_name = row[0]
                place_raw = row[1] # "1st"
                
                # Use dynamic points index if valid
                if len(row) > current_pts_idx:
                    points_raw = row[current_pts_idx]
                else:
                    points_raw = None
                
                # Check if valid data row (Place usually ends in st/nd/rd/th or is digit)
                if not (place_raw[-2:] in ['st','nd','rd','th'] or place_raw.isdigit()):
                    continue
                    
                entry = {
                    'Athlete': parse_athlete_name(ath_name),
                    'Place': place_raw,
                    'GamesPoints': points_raw, # Renamed from Points
                }
                
                # Parse Events
                # Data columns correspond to event_headers
                # Row: [Name, Place, Points, Evt1, Evt2...]
                # Use current_pts_idx instead of hardcoded 3
                data_values = row[current_pts_idx+1:]
                
                for i, val in enumerate(data_values):
                    if i < len(event_headers):
                        evt_name = event_headers[i]
                        parsed_val = self.parse_clean_distance(val)
                        if parsed_val:
                            # Flatten: Add directly to entry
                            entry[evt_name] = parsed_val
                
                if current_class not in structured:
                    structured[current_class] = []
                structured[current_class].append(entry)
                
        return structured
