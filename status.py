#!/usr/bin/env python3
"""status.py — Quick health check for all God-Mode services."""
import sqlite3
import json
import subprocess
from datetime import datetime, timezone, date

TRADE_LOG_DB  = "/home/theplummer92/trade_log.db"
SIGNALS_DB    = "/home/theplummer92/wolfe_signals.db"
REGIME_PATH   = "/home/theplummer92/regime_snapshot.json"
APPROVED_PATH = "/home/theplummer92/approved_symbols.json"
SERVICES      = ["sniper", "paper-sniper", "strategy-lab", "dashboard"]

W = 60


def hr(char="─", width=W):
    print(char * width)


def service_status(name):
    try:
        out = subprocess.check_output(
            ["systemctl", "is-active", f"{name}.service"],
            stderr=subprocess.DEVNULL, text=True
        ).strip()
        return out
    except Exception:
        return "unknown"


def load_regime():
    try:
        with open(REGIME_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def load_approved():
    try:
        with open(APPROVED_PATH) as f:
            d = json.load(f)
        return d.get("buy", []), d.get("sell", [])
    except Exception:
        return [], []


def fetch_sniper_status():
    try:
        conn = sqlite3.connect(SIGNALS_DB)
        row = conn.execute(
            "SELECT ts_utc, status, note FROM sniper_status ORDER BY ts_utc DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row
    except Exception:
        return None


def fetch_open_positions():
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        rows = conn.execute(
            "SELECT symbol, direction, entry_price, entry_time, signal_type "
            "FROM trades WHERE outcome='open' ORDER BY entry_time ASC"
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def fetch_today_pnl():
    today = str(date.today())
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        rows = conn.execute(
            "SELECT pnl_usd, outcome FROM trades "
            "WHERE outcome != 'open' AND date(exit_time) = ?",
            (today,)
        ).fetchall()
        conn.close()
        if not rows:
            return 0.0, 0, 0
        pnls  = [r[0] for r in rows if r[0] is not None]
        wins  = sum(1 for r in rows if r[0] and r[0] > 0)
        total = sum(pnls)
        return total, wins, len(rows)
    except Exception:
        return 0.0, 0, 0


def fetch_alltime_pnl():
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        rows = conn.execute(
            "SELECT pnl_usd FROM trades WHERE outcome != 'open' AND pnl_usd IS NOT NULL"
        ).fetchall()
        conn.close()
        if not rows:
            return 0.0, 0
        pnls = [r[0] for r in rows]
        return sum(pnls), len(pnls)
    except Exception:
        return 0.0, 0


def fetch_signal_count():
    try:
        conn = sqlite3.connect(SIGNALS_DB)
        row = conn.execute("SELECT COUNT(*) FROM signals").fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception:
        return 0


def main():
    hr("═")
    print("  WOLFE GOD-MODE — SYSTEM STATUS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    hr("═")

    # Services
    print("\n  SERVICES")
    hr()
    for svc in SERVICES:
        status = service_status(svc)
        indicator = "OK  " if status == "active" else "DEAD"
        print(f"  [{indicator}]  {svc}.service  ({status})")

    # Sniper heartbeat
    hb = fetch_sniper_status()
    if hb:
        ts_utc, status, note = hb
        try:
            hb_dt = datetime.strptime(ts_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_s  = int((datetime.now(timezone.utc) - hb_dt).total_seconds())
            age    = f"{age_s}s ago" if age_s < 120 else f"{age_s//60}m ago"
        except Exception:
            age = "?"
        print(f"\n  Last heartbeat : {ts_utc} UTC ({age})  |  {note}")
    else:
        print("\n  Last heartbeat : no data")

    # Regime
    print("\n  REGIME")
    hr()
    r = load_regime()
    if r:
        regime = r.get("regime", "N/A")
        vix    = r.get("vix", "?")
        tnx    = r.get("tnx", "?")
        dxy    = r.get("dxy", "?")
        snap   = r.get("timestamp", "")
        indicator = "OK  " if regime in ("OPEN", "RISK_ON") else "WARN" if regime == "SELL_ONLY" else "BLKD"
        print(f"  [{indicator}]  {regime}  |  VIX={vix}  10Y={tnx}  DXY={dxy}")
        print(f"          as of {snap}")
    else:
        print("  [WARN]  regime_snapshot.json not found")

    # Watchlist
    buy_list, sell_list = load_approved()
    print(f"\n  WATCHLIST  (buy: {len(buy_list)}  sell: {len(sell_list)})")
    hr()
    print(f"  Buy  : {', '.join(buy_list) or 'none'}")
    print(f"  Sell : {', '.join(sell_list) or 'none'}")

    # Open positions
    open_pos = fetch_open_positions()
    print(f"\n  OPEN POSITIONS ({len(open_pos)})")
    hr()
    if open_pos:
        for sym, direction, entry, entry_time, sig in open_pos:
            print(f"  {sym:<6} {direction:<6} @ ${entry:.2f}  since {entry_time[:16]}  |  {sig}")
    else:
        print("  None")

    # Today's P&L
    total_pnl, wins, n_trades = fetch_today_pnl()
    sign = "+" if total_pnl >= 0 else ""
    print(f"\n  TODAY'S P&L")
    hr()
    if n_trades:
        wr = wins / n_trades * 100
        print(f"  {sign}${total_pnl:.2f}  |  {wins}W/{n_trades - wins}L  ({wr:.0f}% WR)  |  {n_trades} trades closed")
    else:
        print("  No closed trades today")

    # All-time
    alltime, total_trades = fetch_alltime_pnl()
    sign = "+" if alltime >= 0 else ""
    sig_count = fetch_signal_count()
    print(f"\n  ALL-TIME")
    hr()
    print(f"  P&L: {sign}${alltime:.2f}  |  {total_trades} closed trades  |  {sig_count:,} signals in DB")

    hr("═")


if __name__ == "__main__":
    main()
