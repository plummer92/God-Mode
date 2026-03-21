#!/usr/bin/env python3
"""
backtest_shorts.py — Simulate last week's signals with shorts alongside longs.

Reads wolfe_signals.db, replays every STRONG BUY + STRONG SELL signal from the
past 7 days, applies the same TP/SL rules as the live sniper, and prints a
side-by-side comparison of longs-only vs longs+shorts.

Usage:
    python3 backtest_shorts.py            # last 7 days
    python3 backtest_shorts.py --days 14  # last 14 days
"""

import sqlite3
import argparse
from datetime import datetime, timedelta, timezone

DB_PATH          = "/home/theplummer92/wolfe_signals.db"
TAKE_PROFIT_PCT  = 0.04   # +4%
STOP_LOSS_PCT    = 0.02   # -2%
NOTIONAL_USD     = 10.0   # per trade

# Same approved lists as sniper_bot.py defaults
BUY_APPROVED  = {"NFLX","META","AAPL","AMZN","BTC/USD","ETH/USD","SOL/USD",
                 "TSLA","AMD","COIN","GME","SPY","IWM","QQQ"}
SELL_APPROVED = {"AMD","COIN","IWM","TSLA","NVDA","SPY","AMZN","QQQ","META","NFLX"}

MAX_HOLD_BARS = 12   # max 5-min bars to hold before forced exit (~1 hour)


def fetch_signals(days: int):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT timestamp, symbol, signal_type, price
        FROM signals
        WHERE timestamp > ?
          AND (signal_type LIKE '%STRONG BUY%'
            OR signal_type LIKE '%STRONG SELL%'
            OR signal_type LIKE '%ABSORPTION BUY%'
            OR signal_type LIKE '%ABSORPTION SELL%')
        ORDER BY timestamp ASC
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()
    return rows


def fetch_price_after(symbol: str, after_ts: str, bars: int = MAX_HOLD_BARS):
    """
    Pull up to `bars` subsequent price rows for the symbol after the signal.
    Returns list of (timestamp, price) tuples sorted ascending.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT timestamp, price FROM signals
        WHERE symbol = ?
          AND timestamp > ?
        ORDER BY timestamp ASC
        LIMIT ?
    """, (symbol, after_ts, bars))
    rows = cur.fetchall()
    conn.close()
    return rows


def simulate_trade(entry_price: float, direction: str, future_prices: list):
    """
    Walk future_prices and apply TP/SL.
    Returns (exit_price, pnl_pct, outcome).
    """
    for _, p in future_prices:
        p = float(p)
        if direction == "LONG":
            chg = (p - entry_price) / entry_price
        else:  # SHORT
            chg = (entry_price - p) / entry_price

        if chg >= TAKE_PROFIT_PCT:
            return p, chg, "take_profit"
        if chg <= -STOP_LOSS_PCT:
            return p, chg, "stop_loss"

    # No exit triggered — use last known price
    if future_prices:
        last_p = float(future_prices[-1][1])
        if direction == "LONG":
            chg = (last_p - entry_price) / entry_price
        else:
            chg = (entry_price - last_p) / entry_price
        return last_p, chg, "expired"

    return entry_price, 0.0, "no_data"


def run(days: int):
    signals = fetch_signals(days)
    if not signals:
        print(f"No signals found in the last {days} days in {DB_PATH}")
        return

    # Deduplicate: one trade per hour per symbol per direction
    seen = set()
    trades_long_only   = []
    trades_long_short  = []

    for ts, symbol, sig_type, price in signals:
        sym = symbol.replace("-", "/").upper()
        is_buy  = "BUY"  in sig_type.upper()
        is_sell = "SELL" in sig_type.upper()
        direction = "LONG" if is_buy else "SHORT"

        hour_key = (ts[:13], sym, direction)
        if hour_key in seen:
            continue
        seen.add(hour_key)

        entry = float(price)
        future = fetch_price_after(sym, ts)
        if not future:
            # Also try original Yahoo format
            future = fetch_price_after(symbol, ts)

        exit_p, pnl_pct, outcome = simulate_trade(entry, direction, future)
        pnl_usd = NOTIONAL_USD * pnl_pct

        trade = {
            "ts": ts, "symbol": sym, "direction": direction,
            "signal": sig_type, "entry": entry, "exit": exit_p,
            "pnl_pct": pnl_pct, "pnl_usd": pnl_usd, "outcome": outcome,
        }

        # Longs-only scenario: only BUY signals on buy-approved
        if is_buy and sym in BUY_APPROVED:
            trades_long_only.append(trade)

        # Longs+shorts scenario: BUY on buy-approved + SELL on sell-approved
        if is_buy and sym in BUY_APPROVED:
            trades_long_short.append(trade)
        elif is_sell and sym in SELL_APPROVED:
            trades_long_short.append(trade)

    def summarise(trades, label):
        if not trades:
            print(f"\n  {label}: no trades")
            return
        pnls   = [t["pnl_usd"] for t in trades]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total  = sum(pnls)
        wr     = len(wins) / len(pnls) * 100 if pnls else 0
        avg_w  = sum(wins)   / len(wins)   if wins   else 0
        avg_l  = sum(losses) / len(losses) if losses else 0
        print(f"\n  {label}")
        print(f"  {'─'*50}")
        print(f"  Trades    : {len(trades)}  ({len(wins)}W / {len(losses)}L)")
        print(f"  Win Rate  : {wr:.1f}%")
        print(f"  Total P&L : ${total:+.2f}")
        print(f"  Avg Win   : ${avg_w:+.2f}    Avg Loss: ${avg_l:+.2f}")
        if wins and losses and avg_l != 0:
            print(f"  R/R Ratio : {abs(avg_w/avg_l):.2f}x")

        # Breakdown by direction
        longs  = [t for t in trades if t["direction"] == "LONG"]
        shorts = [t for t in trades if t["direction"] == "SHORT"]
        if longs:
            l_pnl = sum(t["pnl_usd"] for t in longs)
            l_wr  = sum(1 for t in longs if t["pnl_usd"] > 0) / len(longs) * 100
            print(f"  Longs     : {len(longs)} trades | WR {l_wr:.0f}% | P&L ${l_pnl:+.2f}")
        if shorts:
            s_pnl = sum(t["pnl_usd"] for t in shorts)
            s_wr  = sum(1 for t in shorts if t["pnl_usd"] > 0) / len(shorts) * 100
            print(f"  Shorts    : {len(shorts)} trades | WR {s_wr:.0f}% | P&L ${s_pnl:+.2f}")

    print("=" * 60)
    print(f"  BACKTEST — LAST {days} DAYS  (${NOTIONAL_USD}/trade)")
    print(f"  TP: +{TAKE_PROFIT_PCT*100:.0f}%  |  SL: -{STOP_LOSS_PCT*100:.0f}%  |  Max hold: {MAX_HOLD_BARS} bars")
    print("=" * 60)

    summarise(trades_long_only,  "SCENARIO A — Longs only")
    summarise(trades_long_short, "SCENARIO B — Longs + Shorts")

    # Extra detail: all short trades in scenario B
    shorts_only = [t for t in trades_long_short if t["direction"] == "SHORT"]
    if shorts_only:
        print(f"\n  Short trade detail:")
        print(f"  {'Symbol':<8} {'Entry':>8} {'Exit':>8} {'P&L':>8} {'Outcome':<12} {'Signal'}")
        print(f"  {'─'*70}")
        for t in shorts_only:
            print(f"  {t['symbol']:<8} ${t['entry']:>7.2f} ${t['exit']:>7.2f} "
                  f"${t['pnl_usd']:>+7.2f}  {t['outcome']:<12} {t['signal']}")

    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    run(args.days)
