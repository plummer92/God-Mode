#!/usr/bin/env python3
"""morning_brief.py — Post 9:30am morning briefing to Discord."""
import sqlite3
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv("/home/theplummer92/.env")

DB_PATH       = "/home/theplummer92/trade_log.db"
APPROVED_PATH = "/home/theplummer92/approved_symbols.json"
REGIME_PATH   = "/home/theplummer92/regime_snapshot.json"
WEBHOOK_URL   = os.getenv("DISCORD_WEBHOOK_URL")


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


def load_approved():
    try:
        with open(APPROVED_PATH) as f:
            data = json.load(f)
        buy  = data.get("buy", data.get("approved", []))
        sell = data.get("sell", data.get("short", []))
        return buy, sell
    except Exception:
        return [], []


def load_regime():
    try:
        with open(REGIME_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def regime_emoji(regime):
    mapping = {
        "OPEN":               "GREEN",
        "SELL_ONLY":          "YELLOW",
        "BLOCKED":            "RED",
        "NEUTRAL":            "WHITE",
        "RISK_OFF_VOLATILITY":"RED",
        "RISK_ON":            "GREEN",
    }
    color = mapping.get(regime, "")
    return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "WHITE": "⚪"}.get(color, "")


def build_message(buy_list, sell_list, open_pos, regime_data):
    today_str = datetime.now().strftime("%a %b %d, %Y")
    lines = [f"**Morning Briefing — {today_str}  |  Market Open**"]

    # Regime
    regime = regime_data.get("regime", "N/A")
    vix    = regime_data.get("vix")
    tnx    = regime_data.get("tnx")
    dxy    = regime_data.get("dxy")
    snap_ts = regime_data.get("timestamp", "")
    emoji  = regime_emoji(regime)
    lines.append(f"\n**Macro Regime:** {emoji} `{regime}`")
    if vix:
        lines.append(f"VIX: **{vix:.2f}** | 10Y: **{tnx:.2f}%** | DXY: **{dxy:.2f}**")
    if snap_ts:
        lines.append(f"_(as of {snap_ts})_")

    # Watchlist
    lines.append(f"\n**Watchlist — Buy ({len(buy_list)})**")
    lines.append("`" + "  ".join(buy_list) + "`" if buy_list else "_none_")

    lines.append(f"\n**Watchlist — Short ({len(sell_list)})**")
    lines.append("`" + "  ".join(sell_list) + "`" if sell_list else "_none_")

    # Open positions
    lines.append(f"\n**Open Positions ({len(open_pos)})**")
    if not open_pos:
        lines.append("None — starting fresh today")
    else:
        for sym, direction, entry, entry_time, sig in open_pos:
            lines.append(f"  **{sym}** {direction} @ ${entry:.2f} | since {entry_time[:16]}")

    return "\n".join(lines)


def post_to_discord(message):
    if not WEBHOOK_URL:
        print("ERROR: DISCORD_WEBHOOK_URL not set")
        return
    resp = requests.post(WEBHOOK_URL, json={"content": message}, timeout=10)
    resp.raise_for_status()
    print(f"Posted morning brief ({len(message)} chars)")


if __name__ == "__main__":
    buy_list, sell_list = load_approved()
    open_pos            = fetch_open_positions()
    regime_data         = load_regime()
    msg                 = build_message(buy_list, sell_list, open_pos, regime_data)
    print(msg)
    post_to_discord(msg)
