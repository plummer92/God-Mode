#!/usr/bin/env python3
"""daily_report.py — Post 4pm daily performance report to Discord."""
import sqlite3
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv("/home/theplummer92/.env")

DB_PATH      = "/home/theplummer92/trade_log.db"
WEBHOOK_URL  = os.getenv("DISCORD_WEBHOOK_URL")
APPROVED_PATH = "/home/theplummer92/approved_symbols.json"


def fetch_todays_closed():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, direction, entry_price, exit_price,
               pnl_usd, pnl_pct, outcome, signal_type, exit_time
        FROM trades
        WHERE outcome != 'open'
          AND date(exit_time) = ?
        ORDER BY exit_time ASC
    """, (today,))
    rows = cur.fetchall()
    conn.close()
    return rows


def fetch_open_positions():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, direction, entry_price, entry_time, signal_type
        FROM trades
        WHERE outcome = 'open'
        ORDER BY entry_time ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def build_message(closed, open_pos):
    today_str = datetime.now().strftime("%a %b %d, %Y")
    lines = [f"**Daily Performance Report — {today_str}**"]

    if not closed:
        lines.append("\nNo closed trades today.")
    else:
        total_pnl = sum(r[4] for r in closed if r[4] is not None)
        wins      = sum(1 for r in closed if r[4] and r[4] > 0)
        win_rate  = (wins / len(closed) * 100) if closed else 0

        lines.append(f"\n**Closed Trades ({len(closed)})**")
        lines.append(f"Total P&L: **${total_pnl:+.2f}** | Win Rate: **{win_rate:.0f}%** ({wins}/{len(closed)})")
        lines.append("")
        for r in closed:
            sym, direction, entry, exit_p, pnl_usd, pnl_pct, outcome, sig, exit_time = r
            pnl_str  = f"${pnl_usd:+.2f}" if pnl_usd is not None else "N/A"
            pct_str  = f"{pnl_pct*100:+.2f}%" if pnl_pct is not None else ""
            emoji    = "+" if pnl_usd and pnl_usd > 0 else "-"
            outcome_short = outcome.replace("_", " ")
            lines.append(
                f"`{emoji}` **{sym}** {direction} | {pnl_str} ({pct_str}) | {outcome_short} | {sig}"
            )

    lines.append(f"\n**Open Positions ({len(open_pos)})**")
    if not open_pos:
        lines.append("None")
    else:
        for sym, direction, entry, entry_time, sig in open_pos:
            lines.append(f"  **{sym}** {direction} @ ${entry:.2f} | {sig} | since {entry_time[:16]}")

    return "\n".join(lines)


def post_to_discord(message):
    if not WEBHOOK_URL:
        print("ERROR: DISCORD_WEBHOOK_URL not set")
        return
    resp = requests.post(WEBHOOK_URL, json={"content": message}, timeout=10)
    resp.raise_for_status()
    print(f"Posted daily report ({len(message)} chars)")


if __name__ == "__main__":
    closed   = fetch_todays_closed()
    open_pos = fetch_open_positions()
    msg      = build_message(closed, open_pos)
    print(msg)
    post_to_discord(msg)
