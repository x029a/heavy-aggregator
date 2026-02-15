import asyncio
import logging
from utils import setup_logging
from scrapers.nasga import NasgaScraper
from scrapers.heavy_athlete import HeavyAthleteScraper
from scrapers.scottish_scores import ScottishScoresScraper
from settings import get_settings

async def main():
    logger = setup_logging()
    settings = get_settings()
    settings['site'] = 'all' # Force all
    
    # Initialize
    nasga = NasgaScraper(settings)
    heavy = HeavyAthleteScraper(settings)
    scottish = ScottishScoresScraper(settings)
    
    # Run concurrently
    tasks = [nasga.run(), heavy.run(), scottish.run()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
