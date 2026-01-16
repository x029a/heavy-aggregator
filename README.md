# Heavy Aggregator
<img width="1624" height="965" alt="image" src="https://github.com/user-attachments/assets/f94ffb26-3786-4751-aae1-d75a0dcc6763" />



**Heavy Aggregator** is a configurable scraping tool designed to collect comprehensive data on Scottish Heavy Athletics. Ideally suited for archiving and analysis.

The program currently supports automated (and automatable) scanning and scraping of:

*   `nasgaweb.com` (NASGA)
*   `Heavy Athlete`
*   `Scottish Scores`

It is built in a manner that additional scrapers can easily be added and inherit configuration. 

## Features

*   **Comprehensive Data Backup**:
    *   **Games**: Iterates through all available years to scrape full results for every Game event.
    *   **Athletes**: Builds a master list of athletes and scrapes their detailed event history.
*   **Data Quality**:
    *   **Hierarchical Schema**: `Class -> Athlete -> Events` structure.
    *   **Strict Types**: Integers for Places, Floats for Points/Distances (`20' 4"` -> `20.333`).
    *   **Cleaned Data**: Handles nulls (`NT`, `DNS`) and removes scraping artifacts.
*   **Streaming Output**: Writes data to disk in real-time to prevent data loss.
*   **Highly Configurable**: Customize behavior via `settings.txt` or CLI arguments.
*   **Resilient**: built-in retry logic, error handling, and ModSecurity evasion.
*   **Timestamped Output**: Saves data to JSON files with execution timestamps.

## Performance & Reliability

### Async & Concurrency
The scraper now uses `asyncio` and `aiohttp` to fetch data in parallel, drastically reducing scrape time. 
*   **Concurrency**: Controls how many simultaneous requests are made. Default is 5.
*   **Throttle**: Adds a delay (in ms) *per worker*.

Adjust concurrency via CLI (`--concurrency 10`) or `settings.txt`.

### Checkpoint & Resume
The scraper automatically saves its progress to `checkpoint.json`. If you stop the script (Ctrl+C) or it crashes, run it again to resume exactly where you left off (Year/Month/Game).
*   To reset progress, simply delete `checkpoint.json`.

### Docker Support
Run without installing Python dependencies using Docker.
1.  **Build**: `docker-compose build`
2.  **Run**: `docker-compose run scraper`
    *   Example: `docker-compose run scraper --site nasga --concurrency 10`
*   Output files are saved to the local `output/` directory.
*   Edit `settings.txt` locally and it will be reflected in the container.

## Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/heavy-aggregator.git
    cd heavy-aggregator
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Interactive Mode
Simply run the script to start the interactive wizard:
```bash
python main.py
```

### Command Line Interface
Run with arguments for automated or headless execution:

```bash
python main.py --site nasga --throttle 1000 --max-output-line-count 10000
```

**Available Arguments:**
*   `--site`: Target site (`nasga` or `heavyathlete`).
*   `--proxy`: HTTP/HTTPS proxy URL (e.g., `http://user:pass@host:port`).
*   `--user-agent`: Custom User-Agent string.
*   `--throttle`: Delay between requests in milliseconds (default: 0).
*   `--retry-count`: Number of retries for failed requests (default: 3).
*   `--concurrency`: Number of parallel requests (default: 5). Increase for speed, decrease for stability.
*   `--throttle`: Delay in milliseconds between requests.
*   `--output-format`: Output format (`json` or `csv`). *Note: CSV support is experimental.*
*   `--upload`: Upload provider (`s3` or `webhook`).
*   `--s3-bucket`: AWS S3 Bucket Name.
*   `--s3-region`: AWS Region (e.g. `us-east-1`).
*   `--webhook-url`: URL to POST output files to.

## Configuration

You can also configure the tool using `settings.txt`. This file allows you to set defaults so you don't have to pass arguments every time.

Example `settings.txt`:
```ini
proxy=http://127.0.0.1:8080 
(allows you to tunnel your request through a proxy server.)

user_agent=MyScraper/1.0 
(Specifies the user agent the site youre scraping will see.)

retry_count=5 
(Is the number of retries to attempt after a failed connection.)

throttle=2000 
(Is the time in MS to wait between requests.)

max_output_line_count=20000 
(Is the number of lines to write to a file before creating a new file (the default is 0, which means that it will write all data to a single file))

concurrency=5
(Number of parallel requests. Higher is faster but may trigger blocking. Default is 5.)

# --- Remote Upload ---
# upload_provider=S3
# s3_bucket=my-archive
# s3_region=us-east-1
# webhook_url=https://api.myapp.com/upload
```

## Output

Scraped data is saved to the `output/` directory with timestamped filenames:

*   `nasga_games_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `nasga_athletes_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `heavyathlete_games_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `scottishscores_games_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `scottishscores_athletes_YYYY-MM-DD_HH-MM-SS(_part_X).json`

## Disclaimer

This tool is for educational and archival purposes. Please respect the terms of service of the websites you scrape and use the `--throttle` option to avoid overwhelming their servers.

In order to cut down on the likelyhood that you get blocked by the site, I suggest a proxy. Additionally, we want to be good stewards of their bandwidth, and you should use the `--throttle` option to avoid overwhelming their servers.

Additionally, the data from NASGA is not perfect (or good really). There may be issues with the schema. This is meant to be a starting point for you.

## Troubleshooting
If you have issues running this, try doing so in a virtual environment. 

```
> python3 -m venv .venv
> source .venv/bin/activate
> pip install -r requirements.txt
```
If you are on a mac, you may see a `NotOpenSSLWarning`. This is normal and can be ignored. 
