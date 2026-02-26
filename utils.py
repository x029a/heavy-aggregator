import requests
import time
import logging
import sys
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import os
import aiohttp
import asyncio
from aiohttp import ClientTimeout

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

async def get_async_session(settings):
    headers = {
        "User-Agent": settings.get('user_agent', 'HeavyAggregator/1.0'),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "http://www.nasgaweb.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    timeout = ClientTimeout(total=45)
    return aiohttp.ClientSession(headers=headers, timeout=timeout)

class FailedItemWriter:
    def __init__(self, output_dir, base_name):
        self.filename = os.path.join(output_dir, base_name)
        # Ensure dir exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Initialize list if file doesn't exist
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as f:
                json.dump([], f)
                
    def log_failure(self, url, item_id, reason):
        entry = {
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'url': url,
            'id': item_id,
            'reason': str(reason)
        }
        
        # Read, Append, Write (Inefficient for massive scale but safe for robust logging)
        # For concurrent access, we might need a lock if multiple processes used, but 
        # asyncio is single-threaded event loop, so file I/O blocking is the main concern.
        # Ideally, we append to a list in memory and dump periodically, or append to file?
        # JSON standard doesn't support append easily. 
        # Making this Append-Only JSON Lines (JSONL) is better for logging.
        # User requested ".json", but ".jsonl" is safer for crashes.
        # Let's stick to appending to a list is risky if crash.
        # Let's use JSONL logic but call it .json if user insists, or proper JSONL.
        # Actually user said "save that info ... to a 'nasga_failed_retrievals.json'"
        # Let's do simple read/write for now, assuming low failure rate.
        
        try:
            current = []
            if os.path.exists(self.filename) and os.path.getsize(self.filename) > 0:
                with open(self.filename, 'r') as f:
                    try:
                        current = json.load(f)
                    except json.JSONDecodeError:
                        current = []
            
            current.append(entry)
            
            with open(self.filename, 'w') as f:
                json.dump(current, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to log failure for {url}: {e}")


async def async_fetch_url(session, url, method='GET', data=None, settings=None):
    throttle = settings.get('throttle', 0) if settings else 0
    if throttle > 0:
        await asyncio.sleep(throttle / 1000.0)

    proxy = settings.get('proxy') if settings and settings.get('proxy') != 'NONE' else None
    retries = settings.get('retry_count', 3) if settings else 3
    
    for attempt in range(retries + 1):
        try:
            async with session.request(method, url, data=data, proxy=proxy) as response:
                if response.status in [429, 500, 502, 503, 504]:
                    # Check for unrecoverable errors in 500 response
                    if response.status == 500:
                        try:
                            text = await response.text()
                            if "Microsoft JET Database Engine error" in text or "Syntax error in date" in text or "error '80020009'" in text:
                                logger.error(f"Unrecoverable Database Error for {url}. Skipping retry.")
                                return None # Do not raise, return None to signal missing data
                        except Exception:
                            pass # Failed to read text, proceed with normal retry logic

                    if attempt < retries:
                        wait = (attempt + 1) * 2
                        logger.warning(f"Got {response.status} for {url}. Retrying in {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                    else:
                        response.raise_for_status()
                
                if response.status == 200:
                    try:
                        text = await response.text()
                        return text
                    except Exception as e:
                         logger.error(f"Error reading text from {url}: {e}")
                         return None
                else:
                    logger.warning(f"Failed {url} with status {response.status}")
                    return None
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < retries:
                logger.warning(f"Connection error for {url}: {e}. Retrying...")
                await asyncio.sleep(2)
            else:
                logger.error(f"Max retries reached for {url}: {e}")
                return None
    return None

def parse_athlete_name(name_raw):
    """
    Cleans up athlete name and returns {firstName, lastName}.
    Handles \u00a0 and other whitespace issues.
    Splits by first whitespace.
    """
    if not name_raw:
        return {'firstName': '', 'lastName': ''}
    
    # Normalize unicode characters
    clean = name_raw.replace('\u00a0', ' ').strip()
    parts = clean.split(None, 1)
    
    if len(parts) == 1:
        return {'firstName': parts[0], 'lastName': ''}
    else:
        return {'firstName': parts[0], 'lastName': parts[1]}

def parse_distance(text):
    """
    Unified distance parser for all scrapers.
    Handles:
    - 57' 4"
    - 57'
    - 57-4
    - 44 - 9
    - 574 (Heuristic for 57' 4")
    """
    import re
    if not text: return None
    text = str(text).strip()
    if text.upper() in ['NT', 'DNS', '-', '', 'F', 'FOUL', 'ND']: return None
    
    if ':' in text: return text # Time format often used in Caber or Track
    
    # 1. Standard Feet/Inches: 57' 4", 57'4, 57'-4"
    match = re.match(r"(\d+)'\s*-?\s*(\d*\.?\d*)\"?", text)
    if match:
        feet = float(match.group(1))
        inches_str = match.group(2)
        inches = float(inches_str) if inches_str else 0
        return round(feet + (inches / 12.0), 3)

    # 2. Scottish Style: "44 - 9" or "44-9" (No quotes)
    if '-' in text and "'" not in text:
        parts = text.split('-')
        if len(parts) == 2:
            try:
                ft = float(parts[0].strip())
                inch = float(parts[1].strip())
                return round(ft + (inch / 12.0), 3)
            except ValueError:
                pass
                
    # 3. Feet Only: 57'
    match_ft = re.match(r"(\d+)'$", text)
    if match_ft: return float(match_ft.group(1))
    
    # 4. Floating Point / Raw Number Fallback
    try:
        val = float(text)
        
        # HEURISTIC: Check for missing separators in large numbers (3-digit only)
        # e.g., 574 -> 57' 4"
        if val > 100 and val < 1000 and '.' not in text:
            s_val = str(int(val))
            if len(s_val) == 3:
                ft = float(s_val[:2])
                inch = float(s_val[2:])
                if inch < 12:
                    return round(ft + (inch / 12.0), 3)
                    
        return val
    except ValueError:
        pass
        
    return text
