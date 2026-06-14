#!/usr/bin/env python3
"""Shared reporting helpers for daily reports, morning briefs, and summaries."""

from __future__ import annotations

import json
import os
import sqlite3
import time
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
PAPER_STATE_DB_PATH = DATA_DIR / "paper_sniper_state.db"
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
    attempts = int(os.getenv("DISCORD_POST_ATTEMPTS", "5"))
    base_delay = float(os.getenv("DISCORD_POST_RETRY_SECONDS", "2"))
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            response = requests.post(webhook, json={"content": message}, timeout=15)
            response.raise_for_status()
            return
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                break
            delay = min(base_delay * (2 ** (attempt - 1)), 30.0)
            print(
                f"Discord post failed attempt {attempt}/{attempts}: {exc}; retrying in {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"Discord post failed after {attempts} attempts: {last_error}")


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


def _paper_state_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(PAPER_STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _paper_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def build_paper_guardrail_summary(trade_date: str) -> list[str]:
    lines = ["", f"**Paper Guardrail Summary ({trade_date})**"]
    if not PAPER_STATE_DB_PATH.exists():
        lines.append("No paper state DB yet")
        return lines

    conn = _paper_state_connect()
    try:
        if not _paper_table_exists(conn, "paper_signal_events"):
            lines.append("No guardrail event table yet")
            return lines

        counts = {
            str(row["action"]): int(row["n"])
            for row in conn.execute(
                """
                SELECT action, COUNT(*) AS n
                FROM paper_signal_events
                WHERE substr(created_at, 1, 10) = ?
                GROUP BY action
                """,
                (trade_date,),
            )
        }
        entered = counts.get("ENTERED", 0)
        blocked = counts.get("BLOCKED", 0)
        total = entered + blocked
        lines.append(f"Signals acted on: **{total}** | entered: **{entered}** | blocked: **{blocked}**")

        if blocked:
            lines.append("Top blocked reasons:")
            for row in conn.execute(
                """
                SELECT COALESCE(reason, 'unknown') AS reason, COUNT(*) AS n
                FROM paper_signal_events
                WHERE substr(created_at, 1, 10) = ? AND action = 'BLOCKED'
                GROUP BY COALESCE(reason, 'unknown')
                ORDER BY n DESC, reason ASC
                LIMIT 5
                """,
                (trade_date,),
            ):
                lines.append(f"  {row['reason']} - {int(row['n'])}")
        else:
            lines.append("Top blocked reasons: none")

        if entered:
            lines.append("Entered buckets:")
            for row in conn.execute(
                """
                SELECT signal_type, direction, COUNT(*) AS n
                FROM paper_signal_events
                WHERE substr(created_at, 1, 10) = ? AND action = 'ENTERED'
                GROUP BY signal_type, direction
                ORDER BY n DESC, signal_type ASC, direction ASC
                LIMIT 6
                """,
                (trade_date,),
            ):
                lines.append(f"  {row['signal_type']} | {row['direction']} - {int(row['n'])}")
        else:
            lines.append("Entered buckets: none")

        exit_rows = []
        if _paper_table_exists(conn, "exit_events"):
            exit_rows = conn.execute(
                """
                SELECT symbol, exit_reason, verification_result, pnl_usd, created_at
                FROM exit_events
                WHERE substr(created_at, 1, 10) = ?
                ORDER BY created_at ASC, id ASC
                """,
                (trade_date,),
            ).fetchall()
        closed = [row for row in exit_rows if str(row["verification_result"] or "").startswith("closed:")]
        realized = [row for row in closed if row["pnl_usd"] is not None]
        pnl = sum(float(row["pnl_usd"]) for row in realized)
        wins = sum(1 for row in realized if float(row["pnl_usd"]) > 0)
        lines.append(f"Paper exits: **{len(closed)}** | realized P&L: **{_fmt_money(pnl)}** ({wins}/{len(realized)} wins)")
        if closed:
            for row in closed[:6]:
                lines.append(
                    f"  `{row['symbol']}` {row['exit_reason']} | "
                    f"{_fmt_money(row['pnl_usd'])} | {_fmt_timestamp(row['created_at'])}"
                )
    finally:
        conn.close()
    return lines


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
    lines.extend(build_paper_guardrail_summary(trade_date))
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
