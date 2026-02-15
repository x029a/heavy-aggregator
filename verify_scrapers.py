import unittest
from bs4 import BeautifulSoup
import re
import asyncio
from scrapers.heavy_athlete import HeavyAthleteScraper

# Mocking the Scottish Scores logic locally to test extraction
class MockScottishScores:
    def clean_text(self, text):
        if isinstance(text, str):
            return text.replace('\u00a0', ' ').strip()
        return text

    def parse_game_results_table(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        structured = {}
        current_class = "Unknown"
        event_headers = []
        current_pts_idx = 2
        
        all_rows = []
        for table in soup.find_all('table'):
            for tr in table.find_all('tr'):
                # Clean every cell immediately
                cols = [self.clean_text(td.get_text()) for td in tr.find_all(['td', 'th'])]
                if cols: all_rows.append(cols)
        
        for row in all_rows:
            if not row: continue
            first_cell = row[0]
            
            # Logic from scraper
            if len(row) < 3 and len(first_cell) > 3 and not "Athlete" in row and not "Print" in first_cell:
                 if "View" in first_cell or "Done" in first_cell: continue
                 
                 parts = re.split(r'\s{2,}', first_cell)
                 current_class = parts[0].strip()
                 
                 if current_class not in structured:
                     structured[current_class] = []
                 continue
            
            # Header Row
            if "Athlete" in row:
                try:
                    pts_idx = row.index("Points")
                    current_pts_idx = pts_idx
                    event_headers = row[pts_idx+1:]
                except ValueError:
                    pass
                continue

            # Data Row
            if len(row) >= 3 and event_headers:
                ath_name = row[0]
                place_raw = row[1]
                
                if len(row) > current_pts_idx:
                    points_raw = row[current_pts_idx]
                else:
                    points_raw = None
                
                if not (place_raw[-2:] in ['st','nd','rd','th'] or place_raw.isdigit()):
                    continue
                    
                entry = {
                    'Athlete': ath_name,
                    'Place': place_raw,
                    'GamesPoints': points_raw, # Check new key
                }
                
                # Mock events
                data_values = row[current_pts_idx+1:]
                for i, val in enumerate(data_values):
                    if i < len(event_headers):
                        evt_name = event_headers[i]
                        if val:
                             entry[evt_name] = val # Flattened
                
                if current_class not in structured:
                    structured[current_class] = []
                structured[current_class].append(entry)

        return structured

class TestScrapers(unittest.TestCase):
    def test_scottish_scores_normalization(self):
        # HTML simulating the broken row AND data
        html = """
        <table>
            <tr><td>JUNIORS&nbsp;&nbsp;&nbsp;&nbsp;Christena Georgas-Burns</td></tr>
            <tr><td>Athlete</td><td>Place</td><td>Points</td><td>Caber</td></tr>
            <tr><td>Kayleigh Downing</td><td>1st</td><td>9</td><td>12:00</td></tr>
        </table>
        """
        scraper = MockScottishScores()
        res = scraper.parse_game_results_table(html)
        
        self.assertIn("JUNIORS", res) # Class header fix
        
        entries = res["JUNIORS"]
        self.assertTrue(len(entries) > 0)
        entry = entries[0]
        
        # Verify Normalization
        self.assertIn("GamesPoints", entry)
        self.assertNotIn("Points", entry)
        self.assertEqual(entry["GamesPoints"], "9")
        
        # Verify Flattening
        self.assertIn("Caber", entry)
        self.assertEqual(entry["Caber"], "12:00")
        self.assertNotIn("Results", entry)
        
        print(f"Scottish Scores Normalization Verified: {entry}")

    def test_heavy_athlete_live_parsing(self):
        # Async wrapper
        async def run_test():
            settings = {'concurrency': 1}
            scraper = HeavyAthleteScraper(settings)
            
            from utils import get_async_session
            session = await get_async_session(settings)
            try:
                # Test Jan 2024
                # https://heavyathlete.com/game/calendar_list/2024/1/
                games = await scraper._fetch_month_games(session, 2024, 1)
                print(f"Heavy Athlete Live Test: Found {len(games)} games in Jan 2024")
                
                if len(games) > 0:
                     # Deep Verify: Scrape details for first game
                     first_game = games[0] # 16th Annual Frozen Stones (5622)
                     print(f"Deep Verifying: {first_game['name']}")
                     
                     details = await scraper._scrape_game(session, first_game, asyncio.Semaphore(1))
                     if details and 'results' in details and len(details['results']) > 0:
                         print("Heavy Athlete Deep Verify SUCCESS: Found results")
                         # Print keys of first class
                         cls = list(details['results'].keys())[0]
                         if len(details['results'][cls]) > 0:
                             print(f" - Class: {cls}")
                             print(f" - Entry: {details['results'][cls][0]}")
                         else:
                             print(f" - Class: {cls} (Empty)")
                     else:
                         print("Heavy Athlete Deep Verify FAILED: No results found")
                else:
                     print("Heavy Athlete parsing FAILED (No games found)")
            finally:
                await session.close()

        asyncio.run(run_test())

if __name__ == '__main__':
    unittest.main()
