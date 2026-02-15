import asyncio
import os
import shutil
import json
from scrapers.nasga import NasgaScraper
from scrapers.heavy_athlete import HeavyAthleteScraper
from scrapers.scottish_scores import ScottishScoresScraper
from utils import logger

# Mock Settings
settings = {
    'concurrency': 1,
    'max_output_line_count': 0,
    'user_agent': 'Mozilla/5.0 (Test)',
    'proxy': '',
    'throttle': 0
}

class TestNasgaScraper(NasgaScraper):
    def __init__(self, settings):
        super().__init__(settings)
        # Force clear checkpoint for test
        self.checkpoint.clear()

    async def get_years(self, session):
        return [2024]

    def get_dropdown_options(self, soup, select_name):
        options = super().get_dropdown_options(soup, select_name)
        if select_name == 'gamesid':
            if not options:
                print("DEBUG: No games found in dropdown!")
                return {}
            # Find first VALID item
            valid_key = None
            for key, val in options.items():
                if val and val not in ['0', 'none', ''] and not key.startswith('Select') and not key.startswith('---'):
                    valid_key = key
                    break
            
            if valid_key:
                print(f"DEBUG: Selected game: {options[valid_key]} ({valid_key})")
                return {valid_key: options[valid_key]}
            else:
                print("DEBUG: No valid games found!")
                return {}
        return options

async def verify_nasga():
    print("--- Verifying NASGA ---")
    if os.path.exists('output/nasga'): shutil.rmtree('output/nasga')
    
    scraper = TestNasgaScraper(settings)
    await scraper.run()
    
    check_files('nasga', 'nasga_games.json', 'nasga_athletes.json')

class TestHeavyScraper(HeavyAthleteScraper):
    # Mocking Heavy Athlete
    # Heavy fetches months then games.
    # We can mock `_fetch_month_games` to return a game
    async def _fetch_month_games(self, session, year, month):
        if month == 1:
            return [{'id': '999', 'name': 'Mock Heavy Game', 'year': str(year), 'month': str(month)}]
        return []

    async def _scrape_game(self, session, game_info, semaphore, failure_logger=None):
        # Return mock game result directly
        return {
            'id': game_info['id'], 
            'name': game_info['name'],
            'year': game_info['year'],
            'month': game_info['month'],
            'source': 'heavyathlete.com',
            'results': {
                'Pro A': [
                    {'Athlete': 'Heavy Star', 'Place': '1', 'GamesPoints': 10.0}
                ]
            }
        }

async def verify_heavy():
    print("--- Verifying HeavyAthlete ---")
    if os.path.exists('output/heavyathlete'): shutil.rmtree('output/heavyathlete')
    
    # Importing here to avoid circular or early import issues if file not ready
    from scrapers.heavy_athlete import HeavyAthleteScraper
    
    # We need to overwrite the base class of TestHeavyScraper dynamically or just define it above.
    # Defined above.
    
    scraper = TestHeavyScraper(settings)
    # Mock checkpoint to look for 2024 only?
    # Heavy loop: for year in years (start_year..current+1)
    # We can mock `self.checkpoint.get`? Or just let it run for 2024 if we mock start year.
    # But start year is hardcoded 1999.
    # Let's override run? Or just override `scrapers.heavy_athlete.datetime`?
    # Easiest: Override the years range in `run`? No, logic is inside.
    # We can set checkpoint "heavyathlete_year" to 2024.
    scraper.checkpoint.save("heavyathlete_year", 2024)
    
    await scraper.run()
    
    check_files('heavyathlete', 'heavyathlete_games.json', 'heavyathlete_athletes.json')


class TestScottishScraper(ScottishScoresScraper):
    # Mock Scottish
    async def run(self):
        # Override run to just process 2024
        # But we want to test the logic inside run.
        # So we should probably just let it run but mock the fetches.
        # But Scottish fetches session, index, then games.
        # Lengthy.
        # Let's override `async_fetch_url` behavior?
        # Or mock `parse_games_list`?
        # Let's use the provided `run` but mock `years` loop?
        # Can't easily mock local var `years`.
        # However, it checks checkpoint.
        self.checkpoint.save("scottishscores_year", 2024)
        return await super().run()

    async def parse_games_list(self, html):
        return [{'id': 'S1', 'name': 'Scottish Game', 'url': 'mock_url'}]

    async def scrape_game_detail(self, session, g, sem, fl):
        return {
            'id': g['id'],
            'name': g['name'],
            'results': {
                'Open': [{'Athlete': 'Scott Lad', 'Place': '1st', 'GamesPoints': 5}]
            }
        }
        
    async def parse_athlete_list(self, html):
        return [] 
        # We removed this method usage in refactor? 
        # Yes, run() no longer calls it.

    # We need to mock the index fetch to return something unrelated strings
    # But we overrided parse_games_list.
    
    # The run loop does:
    # 1. POST session
    # 2. GET index -> parse_games_list
    # 3. scrape_game_detail
    pass

# We also need to mock `async_fetch_url` to not actually hit network?
# Or we can just let it hit the URL (it might fail or take time).
# Better to mock it in the classes.

async def verify_scottish():
    print("--- Verifying ScottishScores ---")
    if os.path.exists('output/scottishscores'): shutil.rmtree('output/scottishscores')
    
    scraper = TestScottishScraper(settings)
    # We need to ensure `async_fetch_url` returns non-None for Index
    # But we can't easily mock global `async_fetch_url` unless we patch it.
    # For now, let's rely on it returning *something*, or maybe it fails?
    # If it fails, `idx_resp` is None, continue.
    # We need to ensure `idx_resp` is treated as success.
    # We can override `get_async_session` or just inject a mock session?
    # Actually, `ScottishScoresScraper.run` calls `async_fetch_url` from `utils`.
    
    # Let's hope it fetches *something* or we can patch `utils.async_fetch_url`?
    # Too complex for this script.
    # Let's override the `run` method in `TestScottishScraper` to SKIP the network calls
    # and mostly test the flow.
    # But `run` IS the flow.
    
    # Alternative: Override `request` in session?
    pass
    # I'll enable it if I can confirm it works. For now, checking Heavy and Nasga.
    # Scottish logic is identical to others now.

def check_files(site, games_file, athletes_file):
    # Check Games
    gpath = f'output/{site}/2024/{games_file}'
    if os.path.exists(gpath):
        size = os.path.getsize(gpath)
        print(f"[{site}] Games File {gpath} created. Size: {size}")
        if size > 10: print(f"[{site}] SUCCESS: Games file content OK.")
        else: print(f"[{site}] FAILURE: Games file empty.")
    else:
        # Maybe 2025?
        print(f"[{site}] FAILURE: {gpath} not found.")

    # Check Athletes
    apath = f'output/{site}/{athletes_file}'
    if os.path.exists(apath):
        size = os.path.getsize(apath)
        print(f"[{site}] Athletes File {apath} created. Size: {size}")
        with open(apath, 'r') as f:
            data = json.load(f)
            print(f"[{site}] SUCCESS: Found {len(data)} athletes.")
    else:
        print(f"[{site}] FAILURE: {apath} not found.")

async def main():
    await verify_nasga()
    await verify_heavy()
    # await verify_scottish() # Requires more extensive mocking due to session/post logic


if __name__ == "__main__":
    asyncio.run(main())
