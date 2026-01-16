import logging
import time
from datetime import datetime
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, ColoredFormatter
from checkpoint import CheckpointManager
import re
import os
import asyncio

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
        if not text:
            return None
        text = str(text).strip()
        if text.upper() in ['NT', 'DNS', '-', '', 'F']:
            return None
        
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
                    
                    athlete_data['Athlete'] = athlete_name
                    
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
        
        # Find all game links
        # Returns list of (gid, match_object) tuples
        games = []
        # Pattern for ID and optional Name
        # <a href="/game/5630/">Central Florida Highland Games</a>
        # We find just IDs first then grep name
        
        links = re.findall(r'href="/game/(\d+)/">([^<]+)</a>', resp_text)
        for gid, name in links:
             games.append({'id': gid, 'name': name.strip(), 'year': str(year), 'month': str(month)})
             
        # Also catch just IDs if name regex fails?
        # links_simple = re.findall(r'href="/game/(\d+)/"', resp_text)
        # But consistent naming is nice.
        
        if not links:
             # Fallback: check for IDs without name capture (unlikely structure but safe)
             ids = re.findall(r'href="/game/(\d+)/"', resp_text)
             for gid in ids:
                 # Check if we already have it
                 if not any(g['id'] == gid for g in games):
                     games.append({'id': gid, 'name': f"Game {gid}", 'year': str(year), 'month': str(month)})
        
        return games

    async def _scrape_game(self, session, game_info, semaphore):
        async with semaphore:
            gid = game_info['id']
            name = game_info['name']
            logger.info(f"    Scraping Game: {name} ({gid})")
            
            scores_url = f"{self.BASE_URL}/game/{gid}/scores_htmx/"
            resp_text = await async_fetch_url(session, scores_url, settings=self.settings)
            
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
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = 'output'
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        max_lines = self.settings.get('max_output_line_count', 0)
        games_writer = StreamingJSONWriter(output_dir, f'heavyathlete_games_{date_str}.json', max_lines)

        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)
        
        session = await get_async_session(self.settings)
        
        try:
            for year in years:
                logger.info(f"Scanning Year: {year}")
                
                # 1. Fetch all months in parallel to find games
                month_tasks = [self._fetch_month_games(session, year, m) for m in range(1, 13)]
                month_results = await asyncio.gather(*month_tasks)
                
                # Flatten list of games
                year_games = [g for months in month_results for g in months]
                
                if year_games:
                    logger.info(f"  Found {len(year_games)} games in {year}. Fetching details...")
                    
                    # 2. Fetch game details in parallel
                    game_tasks = [self._scrape_game(session, g, semaphore) for g in year_games]
                    results = await asyncio.gather(*game_tasks)
                    
                    for res in results:
                        if res:
                            games_writer.write_item(res)
                
                # Update Checkpoint
                self.checkpoint.save("heavyathlete_year", year + 1)
                
        except Exception as e:
            logger.exception(f"Error during scraping: {e}")
            raise
        finally:
            games_writer.close()
            await session.close()
            logger.info("Heavy Athlete Scraping Complete.")

