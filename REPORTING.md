# Reporting

Phase 4 reporting uses existing data files:

- `/home/theplummer92/trade_log.db`
- `/home/theplummer92/approved_symbols.json`
- `/home/theplummer92/regime_snapshot.json`
- `/home/theplummer92/.env`

Discord delivery uses `DISCORD_WEBHOOK` from `.env`. The shared helper also accepts `DISCORD_WEBHOOK_URL` as a fallback for compatibility with the current environment.

## Scripts

- Daily 4:00pm ET report: `python3 /home/theplummer92/daily_report.py`
- Morning 9:30am ET briefing: `python3 /home/theplummer92/morning_brief.py`
- On-demand summary: `python3 /home/theplummer92/trade_summary.py`

Useful flags:

- `python3 /home/theplummer92/daily_report.py --stdout-only`
- `python3 /home/theplummer92/daily_report.py --date 2026-04-15 --stdout-only`
- `python3 /home/theplummer92/morning_brief.py --stdout-only`
- `python3 /home/theplummer92/trade_summary.py --post-discord`

## Scheduling

Scheduled delivery is handled by `/home/theplummer92/scheduled_reports.py`.

- It runs continuously as a simple systemd service.
- It evaluates the current time in `America/New_York`.
- At `09:30` ET it sends the morning briefing once per trade date.
- At `16:00` ET it sends the daily report once per trade date.
- Delivery state is stored in `/home/theplummer92/report_schedule_state.json` to avoid duplicate sends after restarts.

Install or refresh the service:

```bash
sudo cp /home/theplummer92/systemd/scheduled-reports.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now scheduled-reports.service
sudo systemctl status scheduled-reports.service
```
