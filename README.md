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
*   **Year-Based Output**: Game results are organized into per-year directories (e.g., `output/nasga/2024/nasga_games.json`).
*   **Data Quality**:
    *   **Hierarchical Schema**: `Class -> Athlete -> Events` structure.
    *   **Strict Types**: Integers for Places, Floats for Points/Distances (`20' 4"` -> `20.333`).
    *   **Cleaned Data**: Handles nulls (`NT`, `DNS`) and removes scraping artifacts.
*   **Streaming Output**: Writes data to disk in real-time to prevent data loss.
*   **Highly Configurable**: Customize behavior via `settings.txt` or CLI arguments.
*   **Resilient**: Built-in retry logic, error handling, and ModSecurity evasion.
*   **Search Index Generator**: `build_search_index.py` produces a static search index for the companion website.

## Performance & Reliability

### Async & Concurrency
The scraper uses `asyncio` and `aiohttp` to fetch data in parallel, drastically reducing scrape time. 
*   **Concurrency**: Controls how many simultaneous requests are made. Default is 5.
*   **Throttle**: Adds a delay (in ms) *per worker*.

Adjust concurrency via CLI (`--concurrency 10`) or `settings.txt`.

### Checkpoint & Resume
The scraper automatically saves its progress to `checkpoint.json`. If you stop the script (Ctrl+C) or it crashes, run it again to resume exactly where you left off (Year/Month/Game).
*   To reset progress, simply delete `checkpoint.json`.

## Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/x029a/heavy-aggregator.git
    cd heavy-aggregator
    ```

2.  **Create a virtual environment** (recommended):
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Interactive Mode
Simply run the script to start the interactive wizard:
```bash
python main.py
```

You'll be prompted to select a site (NASGA, Heavy Athlete, Scottish Scores, or All) and configure options like proxy, throttle, concurrency, and output format.

### Command Line Interface
Run with arguments for automated or headless execution:

```bash
# Scrape a specific site
python main.py --site nasga --concurrency 10

# Scrape all sites
python main.py --site all --output-format json

# Scrape with throttling (be polite)
python main.py --site heavyathlete --throttle 1000 --concurrency 5
```

**Available Arguments:**
*   `--site`: Target site (`nasga`, `heavyathlete`, `scottishscores`, or `all`).
*   `--proxy`: HTTP/HTTPS proxy URL (e.g., `http://user:pass@host:port`).
*   `--user-agent`: Custom User-Agent string.
*   `--retry-count`: Number of retries for failed requests (default: 3).
*   `--concurrency`: Number of parallel requests (default: 5). Increase for speed, decrease for stability.
*   `--throttle`: Delay in milliseconds between requests (default: 0).
*   `--output-format`: Output format (`json` or `csv`). *Note: CSV support is experimental.*
*   `--upload`: Upload provider (`s3` or `webhook`).
*   `--s3-bucket`: AWS S3 Bucket Name.
*   `--s3-region`: AWS Region (e.g. `us-east-1`).
*   `--webhook-url`: URL to POST output files to.

### Docker

Run without installing Python dependencies:

```bash
# Build
docker-compose build

# Run interactively
docker-compose run scraper

# Run with arguments
docker-compose run scraper --site nasga --concurrency 10
```

Output files are saved to the local `output/` directory via volume mount. Edit `settings.txt` locally and it will be reflected in the container.

## Configuration

You can also configure the tool using `settings.txt`. This file allows you to set defaults so you don't have to pass arguments every time.

Example `settings.txt`:
```ini
proxy=http://127.0.0.1:8080 
user_agent=MyScraper/1.0 
retry_count=5 
throttle=2000 
concurrency=5
# --- Remote Upload ---
# upload_provider=S3
# s3_bucket=my-archive
# s3_region=us-east-1
# webhook_url=https://api.myapp.com/upload
```

## Output

Scraped data is organized into **year-based directories** under `output/`:

```
output/
├── nasga/
│   ├── 2024/
│   │   └── nasga_games.json
│   ├── 2025/
│   │   └── nasga_games.json
│   ├── nasga_athletes.json
│   └── nasga_failed_retrievals.json
├── heavyathlete/
│   ├── 2024/
│   │   └── heavyathlete_games.json
│   ├── heavyathlete_athletes.json
│   └── heavyathlete_failed_retrievals.json
└── scottishscores/
    ├── 2024/
    │   └── scottishscores_games.json
    ├── scottishscores_athletes.json
    └── scottishscores_failed_retrievals.json
```

Each game file contains an array of game objects with results organized by class:

```json
{
  "id": "8588",
  "name": "Highland Games 2024",
  "year": "2024",
  "date": "06/15/2024",
  "results": {
    "Amateur": [
      {
        "Athlete": { "firstName": "John", "lastName": "Smith" },
        "Place": "1st",
        "GamesPoints": "12",
        "Braemar Stone": 32.5,
        "Open Stone": 42.167,
        "Heavy WFD": 35.0
      }
    ]
  }
}
```

### Search Index

To generate a static search index for the companion website:

```bash
python build_search_index.py [output_directory]
```

This produces:
- `athletes.json` — lightweight name index (~1.8 MB for ~25,000 athletes)
- `athletes/<id>.json` — individual detail files with full game + event data

### Nightly Automation

Use `cron_scrape.sh` for automated nightly scraping with backup rotation:

```bash
# Add to crontab
crontab -e
0 2 * * * /path/to/cron_scrape.sh >> /path/to/cron.log 2>&1
```

The script:
1. Backs up existing `output/` as a timestamped `.tar.gz`
2. Cleans old backups (keeps last 7)
3. Runs all scrapers
4. Rebuilds the search index

## Disclaimers

This tool is for educational and archival purposes. Please respect the terms of service of the websites you scrape and use the `--throttle` option to avoid overwhelming their servers.

In order to cut down on the likelihood that you get blocked by the site, I suggest a proxy. Additionally, we want to be good stewards of their bandwidth, and you should use the `--throttle` option to avoid overwhelming their servers.

Additionally, the data from NASGA is not perfect (or good really). There may be issues with the schema. This is meant to be a starting point for you.

NASGA Web has inherent issues both with data, and the queries used to access that data. For that reason `500`s that occur in the following format (or are in general unrecoverable) - those entries are skipped and won't be retried.

```
Microsoft JET Database Engine error '80040e07'

Syntax error in date in query expression '(Games.Gamesstart >= ## AND Games.Gamesstart <= ##) AND Athletes.Firstname='XXXX' AND Athletes.Lastname='XXXX''.

/dbase/resultsathlete3.asp, line 74
```

### Failure Logging
If a game or athlete fails to download (due to 500 errors, timeouts, or parsing issues), the scraper will skip it and log the failure to a JSON file in the output directory:
- `nasga_failed_retrievals.json`
- `heavyathlete_failed_retrievals.json`
- `scottishscores_failed_retrievals.json`

Check these files to see which items were missed and why.

## Troubleshooting
If you have issues running this, try doing so in a virtual environment. 

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
If you are on a mac, you may see a `NotOpenSSLWarning`. This is normal and can be ignored. 
