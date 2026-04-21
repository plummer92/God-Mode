#!/usr/bin/env bash
# install_crons.sh — Install cron jobs for daily_report.py and morning_brief.py
# Run once on the server after deployment.
# Assumes the server timezone is set to America/New_York (ET).
# Check with: timedatectl

HOME_DIR="${HOME:-/home/theplummer92}"
VENV="${GOD_MODE_VENV_PYTHON:-$HOME_DIR/venv/bin/python3}"
SCRIPTS_DIR="${GOD_MODE_REPO_DIR:-$HOME_DIR/god-mode-sync}"
LOG_DIR="${GOD_MODE_DATA_DIR:-$HOME_DIR}"

MORNING_CMD="30 9 * * 1-5 $VENV $SCRIPTS_DIR/morning_brief.py >> $LOG_DIR/morning_brief.log 2>&1"
DAILY_CMD="0 16 * * 1-5 $VENV $SCRIPTS_DIR/daily_report.py >> $LOG_DIR/daily_report.log 2>&1"

# Remove any existing entries for these scripts, then add fresh ones
(crontab -l 2>/dev/null | grep -v "morning_brief.py" | grep -v "daily_report.py"; \
 echo "$MORNING_CMD"; \
 echo "$DAILY_CMD") | crontab -

echo "Cron jobs installed:"
crontab -l | grep -E "morning_brief|daily_report"
