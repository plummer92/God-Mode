#!/usr/bin/env python3
"""Small ET-aware scheduler for Discord market audit reports."""

from __future__ import annotations

import json
import os
import time

from app_paths import DATA_DIR
from audit_report import DEFAULT_SINCE as DEFAULT_AUDIT_SINCE
from audit_report import build_report as build_audit_report
from backtest_signals import build_report as build_backtest_report
from reporting import build_daily_report, build_morning_brief, now_et, post_to_discord


STATE_PATH = DATA_DIR / "report_schedule_state.json"
SLEEP_SECONDS = 30
DISCORD_CHUNK_SIZE = 1800
AUDIT_REPORT_TIME = os.getenv("AUDIT_REPORT_TIME_ET", "16:10")
AUDIT_REPORT_MIN_SAMPLE = int(os.getenv("AUDIT_REPORT_MIN_SAMPLE", "50"))
AUDIT_REPORT_ENABLED = os.getenv("AUDIT_REPORT_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
AUDIT_REPORT_SINCE = os.getenv("AUDIT_REPORT_SINCE", DEFAULT_AUDIT_SINCE).strip() or None
BACKTEST_REPORT_TIME = os.getenv("BACKTEST_REPORT_TIME_ET", "16:20")
BACKTEST_REPORT_ENABLED = os.getenv("BACKTEST_REPORT_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
BACKTEST_REPORT_SINCE = os.getenv("BACKTEST_REPORT_SINCE", "2026-05-24 14:52:00")
BACKTEST_REPORT_MIN_SAMPLE = int(os.getenv("BACKTEST_REPORT_MIN_SAMPLE", "10"))
BACKTEST_SLIPPAGE_BPS = float(os.getenv("BACKTEST_SLIPPAGE_BPS", "2"))
BACKTEST_SPREAD_BPS = float(os.getenv("BACKTEST_SPREAD_BPS", "3"))


def _load_state() -> dict[str, str]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict[str, str]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _message_chunks(message: str, size: int = DISCORD_CHUNK_SIZE) -> list[str]:
    if len(message) <= size:
        return [message]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in message.splitlines():
        next_len = len(line) + 1
        if current and current_len + next_len > size:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        if next_len > size:
            chunks.append(line[:size])
            continue
        current.append(line)
        current_len += next_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def _post_message(message: str) -> None:
    chunks = _message_chunks(message)
    if len(chunks) == 1:
        post_to_discord(chunks[0])
        return
    for idx, chunk in enumerate(chunks, start=1):
        post_to_discord(f"{chunk}\n\n_part {idx}/{len(chunks)}_")


def _maybe_send(job_name: str, trade_date: str, build_message) -> None:
    state = _load_state()
    if state.get(job_name) == trade_date:
        return
    message = build_message()
    _post_message(message)
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
        elif AUDIT_REPORT_ENABLED and hour_minute == AUDIT_REPORT_TIME:
            _maybe_send(
                "audit_report",
                trade_date,
                lambda: build_audit_report(AUDIT_REPORT_MIN_SAMPLE, since=AUDIT_REPORT_SINCE),
            )
        elif BACKTEST_REPORT_ENABLED and hour_minute == BACKTEST_REPORT_TIME:
            _maybe_send(
                "backtest_report",
                trade_date,
                lambda: build_backtest_report(
                    since=BACKTEST_REPORT_SINCE,
                    min_sample=BACKTEST_REPORT_MIN_SAMPLE,
                    slippage_bps=BACKTEST_SLIPPAGE_BPS,
                    spread_bps=BACKTEST_SPREAD_BPS,
                ),
            )
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    run_forever()
