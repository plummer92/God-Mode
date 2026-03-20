#!/usr/bin/env python3
"""strategy_lab.py — runs every 6 hours testing strategy combinations"""
import sqlite3, json, os, time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

DB_PATH = "/home/theplummer92/wolfe_signals.db"
LAB_DB  = "/home/theplummer92/strategy_lab.db"
LOG     = "/home/theplummer92/strategy_lab.log"
CST     = pytz.timezone("America/Chicago")
LOOKBACK = 30
MIN_TRADES = 10

RVOL_THRESHOLDS = [2.0, 3.0]
TP_VALUES       = [0.02, 0.04, 0.08]
SL_VALUES       = [0.02, 0.03]
HOLD_HOURS      = [4, 24]
SIGNAL_FILTERS  = [
    ["STRONG BUY","STRONG SELL"],
    ["STRONG BUY"],
    ["STRONG SELL"],
    ["STRONG BUY","STRONG SELL","ABSORPTION"],
]
SYMBOLS = ["NFLX","META","AAPL","AMZN","TSLA","AMD","COIN","GME","SPY","IWM",
           "NVDA","IBM","UNH","NET","SNOW","PINS","WYNN","OXY","HOOD","ZS","QQQ","MSFT"]

def log(msg):
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")

def init_db():
    conn = sqlite3.connect(LAB_DB)
    cur  = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT, tested_at TEXT,
        symbol TEXT, signal_filter TEXT, rvol REAL, tp REAL, sl REAL,
        hold_hours INTEGER, n_trades INTEGER, win_rate REAL,
        avg_return REAL, profit_factor REAL, score REAL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS leaderboard (
        rank INTEGER, symbol TEXT, signal_filter TEXT, rvol REAL,
        tp REAL, sl REAL, hold_hours INTEGER, n_trades INTEGER,
        win_rate REAL, avg_return REAL, score REAL, updated_at TEXT)""")
    conn.commit(); conn.close()

def get_signals(symbol, sig_filter, rvol):
    try:
        conn = sqlite3.connect(DB_PATH)
        ph = " OR ".join([f"signal_type LIKE ?" for _ in sig_filter])
        params = [symbol] + [f"%{s}%" for s in sig_filter] + [rvol]
        cur = conn.cursor()
        cur.execute(f"""SELECT timestamp, signal_type, price, rvol FROM signals
            WHERE symbol=? AND ({ph}) AND rvol>=?
            AND timestamp > datetime('now', '-{LOOKBACK} days')
            ORDER BY timestamp ASC""", params)
        rows = cur.fetchall(); conn.close(); return rows
    except: return []

def simulate(symbol, signals, tp, sl, hold_hours):
    if not signals: return []
    # Deduplicate - one signal per hour per symbol direction
    seen = set()
    deduped = []
    for s in signals:
        key = (s[0][:13], "BUY" if "BUY" in s[1].upper() else "SELL")  # hour + direction
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    signals = deduped
    try:
        end = datetime.today(); start = end - timedelta(days=LOOKBACK+5)
        df = yf.download(symbol, start=start, end=end, interval="1h", progress=False, auto_adjust=False)
        if df is None or len(df) < 10: return []
        df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=["Close","Volume"])
    except: return []
    results = []
    hold_bars = max(1, round(hold_hours / 6.5))
    for sig in signals:
        try:
            ts_str, sig_type, entry_price, _ = sig
            entry_price = float(entry_price)
            ts = pd.Timestamp(ts_str, tz="UTC")
            future = df[df.index >= ts]
            if len(future) < 2: continue
            is_long = "BUY" in sig_type.upper()
            exit_idx = min(hold_bars, len(future)-1)
            exit_price = float(future["Close"].iloc[exit_idx])
            if is_long:
                ret = (exit_price - entry_price) / entry_price
                for j in range(1, exit_idx+1):
                    if (float(future["High"].iloc[j]) - entry_price) / entry_price >= tp: ret=tp; break
                    if (entry_price - float(future["Low"].iloc[j])) / entry_price >= sl: ret=-sl; break
            else:
                ret = (entry_price - exit_price) / entry_price
                for j in range(1, exit_idx+1):
                    if (entry_price - float(future["Low"].iloc[j])) / entry_price >= tp: ret=tp; break
                    if (float(future["High"].iloc[j]) - entry_price) / entry_price >= sl: ret=-sl; break
            results.append({"ret": ret, "win": ret > 0})
        except: continue
    return results

def score(wr, avg_ret, n, pf):
    return round((wr*0.4 + avg_ret*10*0.3 + min(pf,10)*0.2 + min(n/50,1)*0.1)*100, 2)

def run():
    log("Starting iteration...")
    init_db()
    all_results = []
    conn = sqlite3.connect(LAB_DB)
    cur  = conn.cursor()
    for symbol in SYMBOLS:
        for sf in SIGNAL_FILTERS:
            fs = "+".join(sf)
            for rvol in RVOL_THRESHOLDS:
                sigs = get_signals(symbol, sf, rvol)
                if not sigs: continue
                for tp in TP_VALUES:
                    for sl in SL_VALUES:
                        if tp <= sl: continue
                        for hold in HOLD_HOURS:
                            trades = simulate(symbol, sigs, tp, sl, hold)
                            if len(trades) < MIN_TRADES: continue
                            wins = sum(1 for t in trades if t["win"])
                            wr   = wins/len(trades)
                            ar   = np.mean([t["ret"] for t in trades])
                            gw   = sum(t["ret"] for t in trades if t["win"])
                            gl   = abs(sum(t["ret"] for t in trades if not t["win"]))
                            pf   = gw/gl if gl > 0 else 99
                            sc   = score(wr, ar, len(trades), pf)
                            cur.execute("INSERT INTO results VALUES (null,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (datetime.now().isoformat(),symbol,fs,rvol,tp,sl,hold,
                                 len(trades),round(wr,4),round(ar,4),round(pf,4),sc))
                            all_results.append({"s":symbol,"f":fs,"rvol":rvol,"tp":tp,"sl":sl,
                                                "hold":hold,"n":len(trades),"wr":wr,"ar":ar,"sc":sc})
        conn.commit()
        log(f"  {symbol} done")
    cur.execute("DELETE FROM leaderboard")
    all_results.sort(key=lambda x: x["sc"], reverse=True)
    for i,r in enumerate(all_results[:20],1):
        cur.execute("INSERT INTO leaderboard VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (i,r["s"],r["f"],r["rvol"],r["tp"],r["sl"],r["hold"],r["n"],
             round(r["wr"],4),round(r["ar"],4),r["sc"],datetime.now().isoformat()))
    conn.commit(); conn.close()
    log(f"Done. {len(all_results)} strategies found.")
    if all_results:
        b = all_results[0]
        log(f"BEST: {b['s']} WR={b['wr']:.0%} TP={b['tp']:.0%} SL={b['sl']:.0%} Hold={b['hold']}h Score={b['sc']}")

def main():
    log("Strategy Lab starting...")
    while True:
        try:
            run()
            # Update rosters after each iteration
            try:
                import subprocess
                subprocess.run(["/home/theplummer92/venv/bin/python3", "/home/theplummer92/roster_manager.py"])
            except Exception as re:
                log(f"Roster manager error: {re}")
            log("Sleeping 6 hours...")
            time.sleep(6*3600)
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(300)

if __name__ == "__main__":
    main()
