#!/usr/bin/env python3
"""
roster_manager.py — Wolfe Trading Auto Roster Manager
Runs after each strategy lab iteration and updates approved symbol lists.
Long bot top-10 STRONG BUY -> approved_symbols.json
Short bot top-10 STRONG SELL -> approved_symbols.json (short key)
"""
import sqlite3, json, os
from datetime import datetime
import pytz

LAB_DB        = "/home/theplummer92/strategy_lab.db"
APPROVED_PATH = "/home/theplummer92/approved_symbols.json"
LOG           = "/home/theplummer92/roster_manager.log"
CST           = pytz.timezone("America/Chicago")
MIN_TRADES    = 20
MIN_WIN_RATE  = 0.70
MIN_SCORE     = 100.0
TOP_N         = 10
ALWAYS_ELIGIBLE = ["SPY", "IWM", "QQQ"]

def log(msg):
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")

def get_best_per_symbol(direction):
    if direction == "BUY":
        clause = "signal_filter LIKE '%STRONG BUY%' AND signal_filter NOT LIKE '%STRONG SELL%'"
    else:
        clause = "signal_filter LIKE '%STRONG SELL%' AND signal_filter NOT LIKE '%STRONG BUY%'"
    conn = sqlite3.connect(LAB_DB)
    cur = conn.cursor()
    cur.execute(f"""SELECT symbol, signal_filter, win_rate, n_trades, avg_return, score
        FROM results WHERE {clause} AND n_trades>=? AND win_rate>=? AND score>=?
        GROUP BY symbol HAVING score=MAX(score) ORDER BY score DESC""",
        (MIN_TRADES, MIN_WIN_RATE, MIN_SCORE))
    rows = cur.fetchall()
    conn.close()
    return rows

def build_roster(direction):
    rows = get_best_per_symbol(direction)
    symbols = [r[0] for r in rows]
    for sym in ALWAYS_ELIGIBLE:
        if sym not in symbols:
            symbols.append(sym)
    return symbols[:TOP_N], symbols[TOP_N:], rows

def main():
    log("="*50)
    log("Roster Manager running...")
    if not os.path.exists(LAB_DB):
        log("ERROR: strategy_lab.db not found"); return

    long_roster, long_bench, long_rows = build_roster("BUY")
    short_roster, short_bench, short_rows = build_roster("SELL")

    log(f"LONG  roster: {', '.join(long_roster)}")
    log(f"SHORT roster: {', '.join(short_roster)}")
    if long_bench:  log(f"LONG  bench:  {', '.join(long_bench)}")
    if short_bench: log(f"SHORT bench:  {', '.join(short_bench)}")

    log("--- LONG leaderboard ---")
    for r in long_rows[:10]:
        log(f"  {r[0]:6s} WR={r[2]*100:.1f}% trades={r[3]} score={r[5]:.1f}")
    log("--- SHORT leaderboard ---")
    for r in short_rows[:10]:
        log(f"  {r[0]:6s} WR={r[2]*100:.1f}% trades={r[3]} score={r[5]:.1f}")

    data = {
        "approved": long_roster,
        "buy": long_roster,
        "sell": short_roster,
        "short": short_roster,
        "updated": datetime.now(CST).isoformat(),
        "roster_managed": True
    }
    with open(APPROVED_PATH, "w") as f:
        json.dump(data, f, indent=2)
    log("approved_symbols.json updated")
    log("="*50)

if __name__ == "__main__":
    main()
