#!/usr/bin/env python3
"""Shared reporting helpers for daily reports, morning briefs, and summaries."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

import requests
from app_paths import DATA_DIR, ENV_FILE

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - fallback only used on minimal environments
    load_dotenv = None


BASE_DIR = DATA_DIR
ENV_PATH = Path(ENV_FILE)
DB_PATH = DATA_DIR / "trade_log.db"
APPROVED_PATH = DATA_DIR / "approved_symbols.json"
REGIME_PATH = DATA_DIR / "regime_snapshot.json"
ET = ZoneInfo("America/New_York")


def load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(ENV_PATH)
        return
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_env()


def get_discord_webhook() -> str:
    """Use DISCORD_WEBHOOK first; fall back to DISCORD_WEBHOOK_URL for compatibility."""
    return os.getenv("DISCORD_WEBHOOK", "").strip() or os.getenv("DISCORD_WEBHOOK_URL", "").strip()


def post_to_discord(message: str) -> None:
    webhook = get_discord_webhook()
    if not webhook:
        raise RuntimeError(f"DISCORD_WEBHOOK is not set in {ENV_PATH}")
    response = requests.post(webhook, json={"content": message}, timeout=10)
    response.raise_for_status()


def now_et() -> datetime:
    return datetime.now(ET)


def today_et_str() -> str:
    return now_et().strftime("%Y-%m-%d")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_closed_trades(trade_date: Optional[str] = None) -> list[sqlite3.Row]:
    conn = _connect()
    try:
        cur = conn.cursor()
        if trade_date:
            cur.execute(
                """
                SELECT *
                FROM trades
                WHERE outcome != 'open'
                  AND date(exit_time) = ?
                ORDER BY exit_time ASC, id ASC
                """,
                (trade_date,),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM trades
                WHERE outcome != 'open'
                ORDER BY COALESCE(exit_time, entry_time) ASC, id ASC
                """
            )
        return cur.fetchall()
    finally:
        conn.close()


def fetch_open_positions() -> list[sqlite3.Row]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM trades
            WHERE outcome = 'open'
            ORDER BY entry_time ASC, id ASC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def load_approved_symbols() -> tuple[list[str], list[str], list[str]]:
    try:
        data = json.loads(APPROVED_PATH.read_text(encoding="utf-8"))
    except Exception:
        return [], [], []
    buy = list(data.get("buy", data.get("approved", [])))
    sell = list(data.get("sell", data.get("short", [])))
    cooling = list(data.get("cooling_off", []))
    return buy, sell, cooling


def load_regime_snapshot() -> dict[str, Any]:
    try:
        return json.loads(REGIME_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _sum_realized_pnl(rows: Iterable[sqlite3.Row]) -> float:
    return sum(float(row["pnl_usd"]) for row in rows if row["pnl_usd"] is not None)


def _realized_rows(rows: Iterable[sqlite3.Row]) -> list[sqlite3.Row]:
    return [row for row in rows if row["pnl_usd"] is not None]


def _win_rate(rows: Iterable[sqlite3.Row]) -> tuple[int, int, float]:
    realized = _realized_rows(rows)
    wins = sum(1 for row in realized if float(row["pnl_usd"]) > 0)
    total = len(realized)
    rate = (wins / total * 100.0) if total else 0.0
    return wins, total, rate


def _fmt_money(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):+.2f}"


def _fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{float(value) * 100:+.2f}%"


def _fmt_timestamp(value: Optional[str]) -> str:
    if not value:
        return "N/A"
    return value[:16]


def build_daily_report(trade_date: Optional[str] = None) -> str:
    trade_date = trade_date or today_et_str()
    closed_rows = fetch_closed_trades(trade_date)
    open_rows = fetch_open_positions()
    realized_pnl = _sum_realized_pnl(closed_rows)
    wins, realized_count, win_rate = _win_rate(closed_rows)

    lines = [f"**Daily Performance Report — {trade_date}**"]
    lines.append(f"Closed trades today: **{len(closed_rows)}**")
    lines.append(f"Realized P&L: **{_fmt_money(realized_pnl)}**")
    lines.append(f"Win rate: **{win_rate:.0f}%** ({wins}/{realized_count} realized trades)")

    lines.append("")
    lines.append(f"**Today's Closed Trades ({len(closed_rows)})**")
    if not closed_rows:
        lines.append("None")
    else:
        for row in closed_rows:
            lines.append(
                f"`{row['symbol']}` {row['direction']} | {_fmt_money(row['pnl_usd'])} "
                f"({_fmt_pct(row['pnl_pct'])}) | {row['outcome']} | {_fmt_timestamp(row['exit_time'])}"
            )

    lines.append("")
    lines.append(f"**Open Positions ({len(open_rows)})**")
    if not open_rows:
        lines.append("None")
    else:
        for row in open_rows:
            lines.append(
                f"`{row['symbol']}` {row['direction']} @ ${float(row['entry_price']):.2f} "
                f"| {row['signal_type']} | since {_fmt_timestamp(row['entry_time'])}"
            )
    return "\n".join(lines)


def _regime_emoji(regime: str) -> str:
    return {
        "OPEN": "🟢",
        "SELL_ONLY": "🟡",
        "BLOCKED": "🔴",
    }.get(regime, "⚪")


def build_morning_brief() -> str:
    buy_list, sell_list, cooling = load_approved_symbols()
    open_rows = fetch_open_positions()
    regime_data = load_regime_snapshot()

    regime = regime_data.get("regime", "UNKNOWN")
    vix = regime_data.get("vix")
    snapshot_ts = regime_data.get("timestamp", "N/A")

    lines = [f"**Morning Briefing — {today_et_str()} 09:30 ET**"]
    lines.append(f"Regime: {_regime_emoji(regime)} `{regime}` | VIX: **{vix:.2f}**" if isinstance(vix, (int, float)) else f"Regime: {_regime_emoji(regime)} `{regime}` | VIX: **N/A**")
    lines.append(f"Snapshot: `{snapshot_ts}`")

    lines.append("")
    lines.append(f"**Watchlist Buy ({len(buy_list)})**")
    lines.append("`" + "  ".join(buy_list) + "`" if buy_list else "None")
    lines.append("")
    lines.append(f"**Watchlist Sell ({len(sell_list)})**")
    lines.append("`" + "  ".join(sell_list) + "`" if sell_list else "None")
    if cooling:
        lines.append("")
        lines.append(f"Cooling Off: `{ '  '.join(cooling) }`")

    lines.append("")
    lines.append(f"**Open Positions ({len(open_rows)})**")
    if not open_rows:
        lines.append("None")
    else:
        for row in open_rows:
            lines.append(
                f"`{row['symbol']}` {row['direction']} @ ${float(row['entry_price']):.2f} "
                f"| since {_fmt_timestamp(row['entry_time'])}"
            )
    return "\n".join(lines)


def build_trade_summary() -> str:
    closed_rows = fetch_closed_trades(None)
    realized = _realized_rows(closed_rows)
    realized_pnl = _sum_realized_pnl(closed_rows)
    wins, realized_count, win_rate = _win_rate(closed_rows)
    best = max(realized, key=lambda row: float(row["pnl_usd"]), default=None)
    worst = min(realized, key=lambda row: float(row["pnl_usd"]), default=None)

    lines = ["**Trade Summary**"]
    lines.append(f"All closed trades: **{len(closed_rows)}**")
    lines.append(f"Overall win rate: **{win_rate:.0f}%** ({wins}/{realized_count} realized trades)")
    lines.append(f"Total P&L: **{_fmt_money(realized_pnl)}**")
    lines.append("")
    lines.append("Best trade:")
    if best is None:
        lines.append("None")
    else:
        lines.append(
            f"`{best['symbol']}` {best['direction']} | {_fmt_money(best['pnl_usd'])} "
            f"({_fmt_pct(best['pnl_pct'])}) | {best['outcome']} | {_fmt_timestamp(best['exit_time'])}"
        )
    lines.append("")
    lines.append("Worst trade:")
    if worst is None:
        lines.append("None")
    else:
        lines.append(
            f"`{worst['symbol']}` {worst['direction']} | {_fmt_money(worst['pnl_usd'])} "
            f"({_fmt_pct(worst['pnl_pct'])}) | {worst['outcome']} | {_fmt_timestamp(worst['exit_time'])}"
        )
    return "\n".join(lines)
