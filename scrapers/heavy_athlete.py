import logging
import time
from datetime import datetime
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, ColoredFormatter, parse_athlete_name, FailedItemWriter, parse_distance
from checkpoint import CheckpointManager
import re
import os
import asyncio
import json

logger = logging.getLogger("HeavyAggregator")

class HeavyAthleteScraper:
    BASE_URL = "https://heavyathlete.com"

    def __init__(self, settings):
        self.settings = settings
        self.checkpoint = CheckpointManager()

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
        return parse_distance(text)

    def parse_scores_html(self, html_content):
        if not html_content: return {}
        soup = BeautifulSoup(html_content, 'html.parser')
        structured_results = {}
        current_class = "Unknown"
        event_headers = []
        
        table = soup.find('table')
        if not table:
            return {}

        rows = table.find_all('tr')
        
        for row in rows:
            th_cells = row.find_all('th')
            td_cells = row.find_all('td')
            
            all_cells = th_cells + td_cells
            clean_cells = [self.clean_text(c.get_text()) for c in all_cells]
            non_empty = [c for c in clean_cells if c]

            if not non_empty: continue

            # Class Header Identification
            if len(th_cells) == 1 and not td_cells:
                val = non_empty[0]
                if "Historic Scores" not in val and "NASGA Clone" not in val:
                    current_class = val
                    if current_class not in structured_results:
                        structured_results[current_class] = []
                continue

            # Event Header Identification
            if "Athlete Name" in clean_cells:
                event_headers = clean_cells
                continue

            # Data Row
            if td_cells and current_class != "Unknown" and event_headers:
                athlete_data = {}
                
                try:
                    name_idx = event_headers.index("Athlete Name")
                except ValueError:
                    continue

                if name_idx < len(clean_cells):
                    athlete_name = clean_cells[name_idx]
                    if not athlete_name: continue
                    
                    athlete_data['Athlete'] = parse_athlete_name(athlete_name)
                    
                    for i, header in enumerate(event_headers):
                        if i == name_idx: continue
                        if i < len(clean_cells):
                            val = clean_cells[i]
                            
                            if header in ['Place', 'Rank']:
                                athlete_data['Place'] = self.parse_number(val, 'int')
                            elif header in ['Pts', 'Points', 'Total']:
                                athlete_data['GamesPoints'] = self.parse_number(val, 'float')
                            else:
                                parsed_val = self.parse_distance(val)
                                if parsed_val is not None:
                                    athlete_data[header] = parsed_val
                    
                    structured_results[current_class].append(athlete_data)

        return structured_results

    async def _fetch_month_games(self, session, year, month):
        url = f"{self.BASE_URL}/game/calendar_list/{year}/{month}/"
        resp_text = await async_fetch_url(session, url, settings=self.settings)
        if not resp_text: return []
        
        games = []
        
        # Use BeautifulSoup instead of Regex
        try:
            soup = BeautifulSoup(resp_text, 'html.parser')
            # Look for links like /game/123/
            # They are usually valid game links if they have a numeric ID
            
            # Pattern: <a href="/game/123/">Name</a>
            for a in soup.find_all('a', href=True):
                 href = a['href']
                 # Expecting /game/123/ or /game/123
                 if '/game/' in href:
                     # Extract digits
                     # This accounts for /game/123/ and /game/123
                     match = re.search(r'/game/(\d+)/?', href)
                     if match:
                         gid = match.group(1)
                         name = a.get_text(strip=True)
                         if not name: name = f"Game {gid}"
                         
                         # Dedup if multiple links to same game (e.g. "View" and "Title")
                         if not any(g['id'] == gid for g in games):
                            games.append({'id': gid, 'name': name, 'year': str(year), 'month': str(month)})
                            
        except Exception as e:
            logger.warning(f"Error parsing game list with BS4 for {year}/{month}: {e}")
            # Fallback to Regex if BS4 fails heavily?
            
        return games

    async def _scrape_game(self, session, game_info, semaphore, failure_logger=None):
        async with semaphore:
            gid = game_info['id']
            name = game_info['name']
            logger.info(f"    Scraping Game: {name} ({gid})")
            
            scores_url = f"{self.BASE_URL}/game/{gid}/scores_htmx/"
            resp_text = await async_fetch_url(session, scores_url, settings=self.settings)
            
            if not resp_text:
                if failure_logger:
                    failure_logger.log_failure(scores_url, gid, f"Failed to fetch game scores: {name}")
                return None
            
            game_entry = {
                'id': gid,
                'name': name,
                'year': game_info['year'],
                'month': game_info['month'],
                'source': 'heavyathlete.com',
                'results': self.parse_scores_html(resp_text)
            }
            return game_entry

    async def run(self):
        logger.info("Starting Heavy Athlete Scraper (Async)...")
        
        # Checkpoint Resume
        start_year = 1999
        saved_year = self.checkpoint.get("heavyathlete_year")
        if saved_year:
            logger.info(f"Found checkpoint. Resuming from Year: {saved_year}")
            start_year = int(saved_year)

        current_year = datetime.now().year
        years = range(start_year, current_year + 2)
        
        # Output Setup
        # Base Output Directory
        base_output_dir = os.path.join('output', 'heavyathlete')
        if not os.path.exists(base_output_dir): os.makedirs(base_output_dir)
        
        max_lines = self.settings.get('max_output_line_count', 0)
        
        # Failure logger at base
        failure_logger = FailedItemWriter(base_output_dir, 'heavyathlete_failed_retrievals.json')
        
        # Athlete DB
        self.athlete_db = {}

        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)
        
        session = await get_async_session(self.settings)
        
        total_games = 0

        try:
            for year in years:
                logger.info(f"Scanning Year: {year}")
                
                # Setup Year Directory
                year_dir = os.path.join(base_output_dir, str(year))
                if not os.path.exists(year_dir):
                    os.makedirs(year_dir)
                
                # Year-specific Games Writer
                year_games_writer = StreamingJSONWriter(year_dir, 'heavyathlete_games.json', max_lines)
                
                try:
                    # 1. Fetch all months in parallel to find games
                    month_tasks = [self._fetch_month_games(session, year, m) for m in range(1, 13)]
                    month_results = await asyncio.gather(*month_tasks)
                    
                    # Flatten list of games
                    year_games = [g for months in month_results for g in months]
                    
                    if year_games:
                        count = len(year_games)
                        logger.info(f"  Found {count} games in {year}. Fetching details...")
                        total_games += count
                        
                        # 2. Fetch game details in parallel
                        game_tasks = [self._scrape_game(session, g, semaphore, failure_logger) for g in year_games]
                        results = await asyncio.gather(*game_tasks)
                        
                        for res in results:
                            if res:
                                year_games_writer.write_item(res)
                                self._update_athlete_db(res)
                                
                finally:
                    year_games_writer.close()
                
                # Save Athletes
                self._save_athletes(base_output_dir)
                logger.info(f"  Saved {len(self.athlete_db)} athletes to heavyathlete_athletes.json")
                
                # Update Checkpoint
                self.checkpoint.save("heavyathlete_year", year + 1)
        
            logger.info("HeavyAthlete Scraping Complete.")
            return {
                'site': 'Heavy Athlete',
                'games_count': total_games,
                'athletes_count': len(self.athlete_db)
            }
                
        except Exception as e:
            logger.exception(f"Error during scraping: {e}")
            raise
        finally:
            await session.close()
            logger.info("Heavy Athlete Scraping Complete.")

    def _update_athlete_db(self, game_result):
        # game_result: {id, name, year, month, source, results: {Class: [entries]}}
        month = game_result.get('month', '1')
        year = game_result.get('year', '2000')
        # Approximate date
        date_str = f"{month}/1/{year}"
        
        game_log_info = {
            'Date': date_str,
            'Game': game_result.get('name'),
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
        path = os.path.join(base_dir, 'heavyathlete_athletes.json')
        try:
            data = list(self.athlete_db.values())
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save athletes file: {e}")

