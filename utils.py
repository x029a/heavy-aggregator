import requests
import time
import logging
import sys
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import os

class StreamingJSONWriter:
    def __init__(self, output_dir, base_name, max_lines=0):
        self.output_dir = output_dir
        self.base_name = base_name
        self.max_lines = max_lines
        self.current_part = 0
        self.file_handle = None
        self.current_lines = 0
        self.first_item = True
        
        # Open first file immediately
        self._open_next_file()

    def _get_filename(self):
        # Insert part number before extension if simple base_name
        # base_name ex: "nasga_games_2026-01-15.json"
        
        if self.max_lines <= 0:
            return os.path.join(self.output_dir, self.base_name)
            
        root, ext = os.path.splitext(self.base_name)
        # If part 0 (first file), keeps original name? Or use part_1?
        # User said: "defaults to one full file, however if a max... set, then... separated into '_part_x' file multiples."
        # Interpretation: If max_lines set, maybe start with part_1? Or keep main file then spillover?
        # "separated into '_part_x' file multiples" implies all files have parts or subsequent ones do.
        # Let's simple: If splitting enabled, ALL files get _part_X suffix.
        if self.max_lines > 0:
            return os.path.join(self.output_dir, f"{root}_part_{self.current_part + 1}{ext}")
        return os.path.join(self.output_dir, self.base_name)

    def _open_next_file(self):
        if self.file_handle:
            self.file_handle.write('\n]')
            self.file_handle.close()
            self.current_part += 1
        
        filename = self._get_filename()
        self.file_handle = open(filename, 'w')
        self.file_handle.write('[\n')
        self.current_lines = 1 # '[' line
        self.first_item = True

    def write_item(self, item):
        # Convert to string to count lines
        json_str = json.dumps(item, indent=2)
        item_lines = json_str.count('\n') + 1
        
        # Check limit (plus 1 line for comma or potential closing bracket)
        if self.max_lines > 0 and (self.current_lines + item_lines + 2 > self.max_lines):
             # Rotate
             self._open_next_file()

        if not self.first_item:
            self.file_handle.write(',\n')
            self.current_lines += 1
        
        self.file_handle.write(json_str)
        self.file_handle.flush()
        self.current_lines += item_lines
        self.first_item = False

    def close(self):
        if self.file_handle:
            self.file_handle.write('\n]')
            self.file_handle.close()
            self.file_handle = None

# ... existing code ...
class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors and line breaks."""
    
    grey = "\x1b[38;20m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # Format: Colored Level/Time + Reset + Message + Newline
    format_str = "%(asctime)s - %(levelname)s" 
    # We will append " - " + reset + "%(message)s\n" in the format dict

    FORMATS = {
        logging.DEBUG: grey + format_str + reset + " - %(message)s\n",
        logging.INFO: blue + format_str + reset + " - %(message)s\n",
        logging.WARNING: yellow + format_str + reset + " - %(message)s\n",
        logging.ERROR: red + format_str + reset + " - %(message)s\n",
        logging.CRITICAL: bold_red + format_str + reset + " - %(message)s\n"
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging():
    logger = logging.getLogger("HeavyAggregator")
    logger.setLevel(logging.INFO)
    
    # Check if handlers already exist to avoid duplicates
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ColoredFormatter())
        logger.addHandler(handler)
    
    return logger

logger = setup_logging()

def get_session(settings):
    session = requests.Session()
    
    # Retry strategy
    retries = settings.get('retry_count', 3)
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Proxy
    proxy = settings.get('proxy')
    if proxy and proxy.lower() != 'none':
        session.proxies = {
            "http": proxy,
            "https": proxy
        }

    # User Agent and Headers
    session.headers.update({
        "User-Agent": settings.get('user_agent', 'HeavyAggregator/1.0'),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "http://www.nasgaweb.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    })
    
    return session

def fetch_url(session, url, method='GET', data=None, settings=None):
    if settings and settings.get('throttle', 0) > 0:
        time.sleep(settings['throttle'] / 1000.0)

    try:
        if method == 'GET':
            response = session.get(url, timeout=30)
        elif method == 'POST':
            response = session.post(url, data=data, timeout=30)
        else:
            return None
        
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None
