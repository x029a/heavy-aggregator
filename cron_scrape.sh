#!/bin/bash
# ──────────────────────────────────────────────────────
# Heavy Aggregator — Nightly Scrape + Backup
# 
# Add to crontab:
#   crontab -e
#   0 2 * * * /home/youruser/heavy-aggregator/cron_scrape.sh >> /home/youruser/heavy-aggregator/cron.log 2>&1
# ──────────────────────────────────────────────────────

set -e

# Config — update these paths for your VPS
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$APP_DIR/.venv/bin/python"
OUTPUT_DIR="$APP_DIR/output"
BACKUP_DIR="$APP_DIR/backups"
KEEP_BACKUPS=7

echo ""
echo "=== Heavy Aggregator Nightly Scrape — $(date) ==="

# 1. Create backup of current data
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "$BACKUP_DIR"

if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A $OUTPUT_DIR 2>/dev/null)" ]; then
    BACKUP_FILE="$BACKUP_DIR/output_$TIMESTAMP.tar.gz"
    tar -czf "$BACKUP_FILE" -C "$APP_DIR" output/
    echo "[Backup] Created: $BACKUP_FILE"
else
    echo "[Backup] No existing output to back up"
fi

# 2. Clean old backups (keep last N)
BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/output_*.tar.gz 2>/dev/null | wc -l)
if [ "$BACKUP_COUNT" -gt "$KEEP_BACKUPS" ]; then
    DELETE_COUNT=$((BACKUP_COUNT - KEEP_BACKUPS))
    ls -1t "$BACKUP_DIR"/output_*.tar.gz | tail -n "$DELETE_COUNT" | xargs rm -f
    echo "[Backup] Cleaned $DELETE_COUNT old backup(s), keeping $KEEP_BACKUPS"
fi

# 3. Run all scrapers
echo "[Scrape] Starting all scrapers..."
cd "$APP_DIR"
$VENV main.py --site all --output-format json 2>&1

# 4. Rebuild search index for the static website
echo "[Index] Rebuilding search index..."
$VENV build_search_index.py "$APP_DIR/data"

echo "[Scrape] Complete!"
echo "=== Done — $(date) ==="
