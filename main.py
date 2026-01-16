import sys
import logging
from settings import get_settings
from scrapers.nasga import NasgaScraper
from scrapers.heavy_athlete import HeavyAthleteScraper
from utils import logger # formatting already applied in utils

# Setup logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
# logger = logging.getLogger("HeavyAggregator") # Now imported from utils

def main():
    logger.info("Heavy Aggregator Starting...")
    
    # 1. Load Settings
    settings = get_settings()
    
    # Determine if we should be interactive
    # If arguments were passed (other than script name), assume non-interactive for settings
    # unless site is missing.
    interactive_mode = len(sys.argv) == 1

    # 2. Interactive Prompt if Site not selected
    if not settings.get('site'):
        print("\nWhich site would you like to scrape?")
        print("1. Nasga (nasgaweb.com)")
        print("2. Heavy Athlete (heavyathlete.com)")
        choice = input("Enter 1 or 2: ").strip()
        
        if choice == '1':
            settings['site'] = 'nasga'
        elif choice == '2':
            settings['site'] = 'heavyathlete'
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)

    # 3. Interactive Prompts for Settings (Only in interactive mode)
    if interactive_mode:
        print("\n--- Configuration (Press Enter to accept default) ---")
        
        # Proxy
        default = settings.get('proxy') or 'NONE'
        val = input(f"Proxy [{default}]: ").strip()
        if val:
            settings['proxy'] = val if val.lower() != 'none' else ''

        # User Agent
        default = settings.get('user_agent')
        # Truncate for display if too long
        display_ua = (default[:50] + '..') if len(default) > 50 else default
        val = input(f"User Agent [{display_ua}]: ").strip()
        if val:
            settings['user_agent'] = val

        # Retry Count
        default = settings.get('retry_count')
        val = input(f"Retry Count [{default}]: ").strip()
        if val:
            try:
                settings['retry_count'] = int(val)
            except ValueError:
                print("Invalid integer, keeping default.")

        # Throttle
        default = settings.get('throttle')
        val = input(f"Throttle (ms) [{default}]: ").strip()
        if val:
            try:
                settings['throttle'] = int(val)
            except ValueError:
                print("Invalid integer, keeping default.")

        # Output Format
        default = settings.get('output_format')
        val = input(f"Output Format (json/csv) [{default}]: ").strip().lower()
        if val:
            if val in ['json', 'csv']:
                settings['output_format'] = val
            else:
                print("Invalid format, keeping default.")

    logger.info(f"Configuration: Site={settings['site']}, Proxy={settings.get('proxy') or 'None'}, Throttle={settings.get('throttle')}")

    # 4. Run Scraper
    if settings['site'] == 'nasga':
        scraper = NasgaScraper(settings)
    elif settings['site'] == 'heavyathlete':
        scraper = HeavyAthleteScraper(settings)
    else:
        logger.error(f"Unknown site: {settings['site']}")
        sys.exit(1)
        
    scraper.run()

if __name__ == "__main__":
    main()
