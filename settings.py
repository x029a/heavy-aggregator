import argparse
import os

DEFAULT_SETTINGS = {
    'proxy': '',
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'retry_count': 3,
    'throttle': 0,
    'concurrency': 5,
    'output_format': 'json',
    'max_output_line_count': 0
}

def load_settings_file(filepath='settings.txt'):
    settings = DEFAULT_SETTINGS.copy()
    if not os.path.exists(filepath):
        return settings

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip().lower()
                value = value.strip()

                if key in settings:
                    if key in ['retry_count', 'throttle', 'max_output_line_count', 'concurrency']:
                        try:
                            settings[key] = int(value)
                        except ValueError:
                            pass # Keep default
                    else:
                        settings[key] = value

    return settings

def get_settings():
    settings = load_settings_file()
    
    parser = argparse.ArgumentParser(description="Heavy Aggregator Scraper CLI")
    parser.add_argument('--site', choices=['nasga', 'heavyathlete', 'scottishscores', 'all'], help="Site to scrape")
    parser.add_argument('--proxy', help="Proxy URL")
    parser.add_argument('--user-agent', help="User Agent String")
    parser.add_argument('--retry-count', type=int, help="Number of retries")
    parser.add_argument('--throttle', type=int, help="Throttle delay in ms")
    parser.add_argument('--concurrency', type=int, default=5, help="Number of concurrent requests (default: 5)")
    parser.add_argument('--output-format', choices=['json', 'csv'], help="Output format")
    parser.add_argument('--max-output-line-count', type=int, help="Split output file after N lines")
    
    # Upload Arguments
    parser.add_argument('--upload', dest='upload_provider',  choices=['s3', 'webhook', 'none'], help="Upload provider")
    parser.add_argument('--s3-bucket', help="S3 Bucket Name")
    parser.add_argument('--s3-region', help="S3 Region")
    parser.add_argument('--webhook-url', help="Webhook URL for upload")

    args, unknown = parser.parse_known_args()

    # Override defaults with CLI args
    if args.site: settings['site'] = args.site
    if args.proxy: settings['proxy'] = args.proxy
    if args.user_agent: settings['user_agent'] = args.user_agent
    if args.throttle is not None: settings['throttle'] = args.throttle
    if args.retry_count is not None: settings['retry_count'] = args.retry_count
    if args.concurrency is not None: settings['concurrency'] = args.concurrency
    if args.output_format: settings['output_format'] = args.output_format
    if args.max_output_line_count is not None: settings['max_output_line_count'] = args.max_output_line_count
    
    # Upload Overrides
    if args.upload_provider: 
        settings['upload_provider'] = args.upload_provider.upper() if args.upload_provider != 'none' else ''
    if args.s3_bucket: settings['s3_bucket'] = args.s3_bucket
    if args.s3_region: settings['s3_region'] = args.s3_region
    if args.webhook_url: settings['webhook_url'] = args.webhook_url
    
    return settings
