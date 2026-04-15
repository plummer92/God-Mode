#!/usr/bin/env python3
"""Small ET-aware scheduler for the morning brief and daily report."""

from __future__ import annotations

import json
import time
from pathlib import Path

from reporting import build_daily_report, build_morning_brief, now_et, post_to_discord


STATE_PATH = Path("/home/theplummer92/report_schedule_state.json")
SLEEP_SECONDS = 30


def _load_state() -> dict[str, str]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict[str, str]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _maybe_send(job_name: str, trade_date: str, build_message) -> None:
    state = _load_state()
    if state.get(job_name) == trade_date:
        return
    message = build_message()
    post_to_discord(message)
    state[job_name] = trade_date
    _save_state(state)
    print(f"sent {job_name} for {trade_date}", flush=True)


def run_forever() -> None:
    print("scheduled_reports.py running", flush=True)
    while True:
        now = now_et()
        trade_date = now.strftime("%Y-%m-%d")
        hour_minute = now.strftime("%H:%M")
        if hour_minute == "09:30":
            _maybe_send("morning_brief", trade_date, build_morning_brief)
        elif hour_minute == "16:00":
            _maybe_send("daily_report", trade_date, lambda: build_daily_report(trade_date))
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    run_forever()
