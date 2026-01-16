import argparse
import os

DEFAULT_SETTINGS = {
    'proxy': '',
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'retry_count': 3,
    'throttle': 0,
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
                    if key in ['retry_count', 'throttle', 'max_output_line_count']:
                        try:
                            settings[key] = int(value)
                        except ValueError:
                            pass # Keep default
                    else:
                        settings[key] = value
    return settings

def get_settings():
    # 1. Load from file
    settings = load_settings_file()

    # 2. Override from CLI
    parser = argparse.ArgumentParser(description="Heavy Aggregator Scraper")
    parser.add_argument('--site', choices=['nasga', 'heavyathlete'], help="Site to scrape")
    parser.add_argument('--proxy', help="Proxy address")
    parser.add_argument('--user-agent', help="User Agent string")
    parser.add_argument('--retry-count', type=int, help="Number of retries for failed connections")
    parser.add_argument('--throttle', type=int, help="Throttle in ms between requests")
    parser.add_argument('--output-format', choices=['json', 'csv'], help="Output format")
    parser.add_argument('--max-output-line-count', type=int, help="Split output file after N lines")

    args, unknown = parser.parse_known_args()

    if args.proxy:
        settings['proxy'] = args.proxy
    if args.user_agent:
        settings['user_agent'] = args.user_agent
    if args.retry_count is not None:
        settings['retry_count'] = args.retry_count
    if args.throttle is not None:
        settings['throttle'] = args.throttle
    if args.output_format:
        settings['output_format'] = args.output_format
    if args.max_output_line_count is not None:
        settings['max_output_line_count'] = args.max_output_line_count
        
    settings['site'] = args.site

    return settings
