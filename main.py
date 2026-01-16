import sys
import logging
from settings import get_settings
from scrapers.nasga import NasgaScraper
from scrapers.heavy_athlete import HeavyAthleteScraper
from utils import logger
import asyncio
import time
import os

def main():
    logger.info("Heavy Aggregator Starting...")
    
    # 1. Load Settings
    settings = get_settings()
    
    interactive_mode = len(sys.argv) == 1

    # 2. Site Selection
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

    # 3. Configuration
    if interactive_mode:
        print("\n--- Configuration (Press Enter to accept default) ---")
        
        # Proxy
        default = settings.get('proxy') or 'NONE'
        val = input(f"Proxy [{default}]: ").strip()
        if val:
            settings['proxy'] = val if val.lower() != 'none' else ''

        # User Agent
        default = settings.get('user_agent')
        display_ua = (default[:50] + '..') if len(default) > 50 else default
        val = input(f"User Agent [{display_ua}]: ").strip()
        if val: settings['user_agent'] = val

        # Retry Count
        default = settings.get('retry_count')
        val = input(f"Retry Count [{default}]: ").strip()
        if val:
            try: settings['retry_count'] = int(val)
            except ValueError: print("Invalid integer, keeping default.")

        # Throttle
        default = settings.get('throttle')
        val = input(f"Throttle (ms) [{default}]: ").strip()
        if val:
            try: settings['throttle'] = int(val)
            except ValueError: print("Invalid integer, keeping default.")

        # Concurrency
        default = settings.get('concurrency', 5)
        val = input(f"Concurrency [{default}]: ").strip()
        if val:
            try: settings['concurrency'] = int(val)
            except ValueError: print("Invalid integer, keeping default.")

        # Output Format
        default = settings.get('output_format')
        val = input(f"Output Format (json/csv) [{default}]: ").strip().lower()
        if val:
            if val in ['json', 'csv']: settings['output_format'] = val
            else: print("Invalid format, keeping default.")
            
        # Upload Prompt
        print("\n--- Remote Upload ---")
        print("Would you like to auto-upload results?")
        print("1. No")
        print("2. S3 Bucket")
        print("3. Webhook URL")
        up_choice = input("Enter choice [1]: ").strip()
        
        if up_choice == '2':
            settings['upload_provider'] = 'S3'
            settings['s3_bucket'] = input(f"S3 Bucket [{settings.get('s3_bucket') or ''}]: ").strip() or settings.get('s3_bucket')
            settings['s3_region'] = input(f"S3 Region [{settings.get('s3_region') or 'us-east-1'}]: ").strip() or settings.get('s3_region')
        elif up_choice == '3':
            settings['upload_provider'] = 'WEBHOOK'
            settings['webhook_url'] = input(f"Webhook URL [{settings.get('webhook_url') or ''}]: ").strip() or settings.get('webhook_url')

    logger.info(f"Configuration: Site={settings['site']}, Proxy={settings.get('proxy') or 'None'}, Throttle={settings.get('throttle')}, Concurrency={settings.get('concurrency', 5)}")
    if settings.get('upload_provider'):
        logger.info(f"Upload Provider: {settings.get('upload_provider')}")

    # 4. Run Scraper
    if settings['site'] == 'nasga':
        scraper = NasgaScraper(settings)
    elif settings['site'] == 'heavyathlete':
        scraper = HeavyAthleteScraper(settings)
    else:
        logger.error(f"Unknown site: {settings['site']}")
        sys.exit(1)
        
    try:
        start_time = time.time()
        
        # Async Execution
        asyncio.run(scraper.run())
        
        end_time = time.time()
        logger.info(f"Done in {end_time - start_time:.2f} seconds.")
        
        # 5. Upload Results
        from uploaders import get_uploader
        uploader = get_uploader(settings)
        
        if uploader:
            logger.info("Starting Remote Upload...")
            output_dir = 'output'
            files_to_upload = []
            if os.path.exists(output_dir):
                for f in os.listdir(output_dir):
                    fpath = os.path.join(output_dir, f)
                    if os.path.isfile(fpath):
                        if os.path.getmtime(fpath) > start_time:
                            files_to_upload.append(fpath)
            
            if not files_to_upload:
                logger.warning("No new files found to upload.")
            else:
                for fpath in files_to_upload:
                    uploader.upload(fpath)

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user.")
    except Exception as e:
        logger.exception(f"Fatal error during scraping: {e}")

if __name__ == "__main__":
    main()
