# Heavy Aggregator
<img width="1102" height="744" alt="image" src="https://github.com/user-attachments/assets/512ab356-d13f-4584-a05e-cf9cd15130fc" />


**Heavy Aggregator** is a configurable scraping tool designed to collect comprehensive data on Scottish Heavy Athletics. Ideally suited for archiving and analysis, it currently supports scraping `nasgaweb.com` (NASGA), and `Heavy Athlete`. It is built in a manner that additional scrapers can easily be added and inherit configuration.

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
*   `--max-output-line-count`: Split output files after N lines (e.g., 10000). Useful for large datasets.
*   `--output-format`: Output format (`json` or `csv`). *Note: CSV support is experimental.*

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
```

## Output

Scraped data is saved to the `output/` directory with timestamped filenames:

*   `nasga_games_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `nasga_athletes_YYYY-MM-DD_HH-MM-SS(_part_X).json`
*   `heavyathlete_games_YYYY-MM-DD_HH-MM-SS(_part_X).json`

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
