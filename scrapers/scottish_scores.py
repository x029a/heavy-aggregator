from . import Scraper
import logging
import asyncio
from bs4 import BeautifulSoup
from utils import get_async_session, async_fetch_url, StreamingJSONWriter, parse_athlete_name
from checkpoint import CheckpointManager

# ... (rest of imports)


import os
import urllib.parse
import re

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
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = 'output'
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        max_lines = self.settings.get('max_output_line_count', 0)

        games_writer = StreamingJSONWriter(output_dir, f'scottishscores_games_{date_str}.json', max_lines)
        athletes_writer = StreamingJSONWriter(output_dir, f'scottishscores_athletes_{date_str}.json', max_lines)

        concurrency = self.settings.get('concurrency', 5)
        semaphore = asyncio.Semaphore(concurrency)

        try:
            # 1. Scrape Games (Iterate Years)
            start_year = 1990
            end_year = datetime.now().year + 1
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
                
                # Switch Session Year
                # The site uses POST to SessionYrSet.cfm to set the year in session
                post_data = {'FilterYear': str(year)}
                # We don't really care about the response body, just the cookie/session update
                await async_fetch_url(session, self.SESSION_SET_URL, method='POST', data=post_data, settings=self.settings)
                
                # Now fetch Index to get games for this year
                idx_resp = await async_fetch_url(session, self.INDEX_URL, settings=self.settings)
                if not idx_resp:
                    logger.warning(f"Failed to fetch index for {year}")
                    continue

                games = self.parse_games_list(idx_resp)
                logger.info(f"  Found {len(games)} games in {year}.")
                
                # Scrape detailed games in parallel
                game_tasks = [self.scrape_game_detail(session, g, semaphore) for g in games]
                results = await asyncio.gather(*game_tasks)
                
                for res in results:
                    if res:
                        games_writer.write_item(res)
                
                self.checkpoint.save("scottishscores_year", year)

            # 2. Scrape Athletes (Master List)
            # Strategy: Get master list -> Resume/Filter -> Scrape Details
            logger.info("Fetching Athlete Master List...")
            master_resp = await async_fetch_url(session, self.ATHLETE_LIST_URL, settings=self.settings)
            
            if master_resp:
                athletes = self.parse_athlete_list(master_resp)
                logger.info(f"Found {len(athletes)} unique athletes.")
                
                # Resume Athlete Index
                start_ath = self.checkpoint.get("scottishscores_athlete_idx", 0)
                if start_ath > 0:
                    logger.info(f"Resuming athletes from index {start_ath}...")
                    athletes = athletes[start_ath:]
                
                # Chunking
                BATCH_SIZE = 50
                total = len(athletes)
                base_idx = start_ath
                
                for i in range(0, len(athletes), BATCH_SIZE):
                    batch = athletes[i : i + BATCH_SIZE]
                    tasks = [self.scrape_athlete_detail(session, a, semaphore) for a in batch]
                    results = await asyncio.gather(*tasks)
                    
                    for res in results:
                        if res:
                            athletes_writer.write_item(res)
                    
                    current_processed = base_idx + i + len(batch)
                    self.checkpoint.save("scottishscores_athlete_idx", current_processed)
                    if i % 100 == 0:
                        logger.info(f"  Processed {current_processed}/{total + base_idx} athletes...")

        finally:
            games_writer.close()
            athletes_writer.close()
            await session.close()
            logger.info("ScottishScores Scraping Complete.")

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
                        name = a.get_text(strip=True)
                        games.append({'id': code, 'name': name, 'url': href})
                except Exception:
                    pass
        return games

    def parse_athlete_list(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        athletes = []
        # Links: rankingHistory.cfm?FN=...&LN=...&SysID=...
        seen_ids = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'rankingHistory.cfm' in href and 'SysID=' in href:
                 try:
                    parsed = urllib.parse.urlparse(href)
                    qs = urllib.parse.parse_qs(parsed.query)
                    sys_id = qs.get('SysID', [''])[0]
                    fn = qs.get('FN', [''])[0]
                    ln = qs.get('LN', [''])[0]
                    
                    if sys_id and sys_id not in seen_ids:
                        seen_ids.add(sys_id)
                        athletes.append({
                            'id': sys_id,
                            'first_name': fn,
                            'last_name': ln,
                            'url': href
                        })
                 except Exception:
                     pass
        return athletes

    async def scrape_game_detail(self, session, game_meta, semaphore):
        async with semaphore:
            url = f"{self.BASE_URL}/{game_meta['url']}"
            resp = await async_fetch_url(session, url, settings=self.settings)
            if not resp: return None
            
            soup = BeautifulSoup(resp, 'html.parser')
            # Look for tables with results
            # Similar logic to NASGA: Identify Class headers and rows
            
            results = self.parse_game_results_table(soup)
            
            return {
                'id': game_meta['id'],
                'name': game_meta['name'],
                'results': results
            }

    async def scrape_athlete_detail(self, session, ath_meta, semaphore):
        async with semaphore:
            # url is relative
            url = f"{self.BASE_URL}/{ath_meta['url']}"
            resp = await async_fetch_url(session, url, settings=self.settings)
            if not resp: return None
            
            soup = BeautifulSoup(resp, 'html.parser')
            # Look for history table
            history = []
            # Simplified table parsing: dump rows
            for table in soup.find_all('table'):
                rows = []
                for tr in table.find_all('tr'):
                     cols = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                     if cols: rows.append(cols)
                if len(rows) > 1: # Header + Data
                    history.append(rows)
            
            return {
                'id': ath_meta['id'],
                'name': parse_athlete_name(f"{ath_meta['first_name']} {ath_meta['last_name']}"),
                'history': history
            }

    def parse_clean_distance(self, text):
        # Handle "44 - 9" -> 44.75
        if not text: return None
        text = text.strip()
        if not text or text == '-': return None
        
        # Format: "44 - 9" or "44 - 9.5"
        parts = text.split('-')
        if len(parts) == 2:
            try:
                ft = float(parts[0].strip())
                inch = float(parts[1].strip())
                return round(ft + (inch / 12.0), 3)
            except ValueError:
                pass
        
        # Try pure float
        try:
            return float(text)
        except ValueError:
            pass
            
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
            for tr in table.find_all('tr'):
                cols = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
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
                 current_class = first_cell
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
                    'Points': points_raw,
                    'Results': {}
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
                            entry['Results'][evt_name] = parsed_val
                
                if current_class not in structured:
                    structured[current_class] = []
                structured[current_class].append(entry)
                
        return structured
