#!/usr/bin/env python3
"""
backtest_shorts.py — Simulate last week's signals with shorts alongside longs.

Reads wolfe_signals.db for signals, fetches real 5-min OHLCV from yfinance
for exit simulation, and prints a side-by-side comparison of
longs-only vs longs+shorts.

Usage:
    python3 backtest_shorts.py            # last 7 days
    python3 backtest_shorts.py --days 14  # last 14 days
"""

import sqlite3
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import yfinance as yf

DB_PATH          = "/home/theplummer92/wolfe_signals.db"
TAKE_PROFIT_PCT  = 0.04   # +4%
STOP_LOSS_PCT    = 0.02   # -2%
NOTIONAL_USD     = 10.0   # per trade
MAX_HOLD_MINS    = 60     # max hold time in minutes (12 x 5m bars)

# Same approved lists as sniper_bot.py defaults
BUY_APPROVED  = {"NFLX","META","AAPL","AMZN","BTC-USD","ETH-USD","SOL-USD",
                 "TSLA","AMD","COIN","GME","SPY","IWM","QQQ"}
SELL_APPROVED = {"AMD","COIN","IWM","TSLA","NVDA","SPY","AMZN","QQQ","META","NFLX"}

# Cache yfinance data per symbol to avoid repeated downloads
_price_cache = {}


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


def get_price_bars(symbol: str, start: datetime, days: int):
    """Download 5m bars for symbol covering the backtest window. Cached."""
    yf_sym = symbol.replace("/", "-")
    if yf_sym not in _price_cache:
        try:
            df = yf.download(
                yf_sym,
                start=(start - timedelta(days=1)).strftime("%Y-%m-%d"),
                period=f"{days + 2}d",
                interval="5m",
                progress=False,
                auto_adjust=True,
            )
            _price_cache[yf_sym] = df
        except Exception:
            _price_cache[yf_sym] = None
    return _price_cache[yf_sym]


def simulate_trade(symbol: str, entry_price: float, entry_ts: str,
                   direction: str, bars_df):
    """
    Walk 5m bars after entry_ts and apply TP/SL.
    Returns (exit_price, pnl_pct, outcome).
    """
    if bars_df is None or bars_df.empty:
        return entry_price, 0.0, "no_data"

    try:
        entry_dt = datetime.strptime(entry_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return entry_price, 0.0, "no_data"

    deadline = entry_dt + timedelta(minutes=MAX_HOLD_MINS)

    for idx in bars_df.index:
        # Normalise index to UTC
        try:
            bar_ts = idx.to_pydatetime()
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if bar_ts <= entry_dt:
            continue
        if bar_ts > deadline:
            break

        try:
            close = float(bars_df.loc[idx, "Close"])
        except Exception:
            continue

        if direction == "LONG":
            chg = (close - entry_price) / entry_price
        else:
            chg = (entry_price - close) / entry_price

        if chg >= TAKE_PROFIT_PCT:
            return close, chg, "take_profit"
        if chg <= -STOP_LOSS_PCT:
            return close, chg, "stop_loss"

    # Time expired — use last bar price within window
    window = bars_df[(bars_df.index > entry_dt) & (bars_df.index <= deadline)]
    if not window.empty:
        try:
            last_p = float(window.iloc[-1]["Close"])
            if direction == "LONG":
                chg = (last_p - entry_price) / entry_price
            else:
                chg = (entry_price - last_p) / entry_price
            return last_p, chg, "expired"
        except Exception:
            pass

    return entry_price, 0.0, "no_data"


def run(days: int):
    signals = fetch_signals(days)
    if not signals:
        print(f"No signals found in the last {days} days in {DB_PATH}")
        return

    start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    # Pre-download price data for all symbols we'll need
    all_symbols = set()
    for _, symbol, sig_type, _ in signals:
        sym = symbol.upper()
        is_buy  = "BUY"  in sig_type.upper()
        is_sell = "SELL" in sig_type.upper()
        if is_buy and sym in BUY_APPROVED:
            all_symbols.add(sym)
        elif is_sell and sym in SELL_APPROVED:
            all_symbols.add(sym)

    print(f"  Downloading price data for {len(all_symbols)} symbols...", flush=True)
    for sym in sorted(all_symbols):
        get_price_bars(sym, start_dt, days)
        print(f"    {sym} OK", flush=True)

    # Deduplicate: one trade per calendar date per symbol per direction
    # (stricter than hourly — prevents re-entering the same trend all day)
    seen = set()
    trades_long_only  = []
    trades_long_short = []

    for ts, symbol, sig_type, price in signals:
        sym = symbol.upper()
        is_buy  = "BUY"  in sig_type.upper()
        is_sell = "SELL" in sig_type.upper()
        direction = "LONG" if is_buy else "SHORT"

        # Dedup: one per day per symbol per direction
        day_key = (ts[:10], sym, direction)
        if day_key in seen:
            continue

        entry = float(price)
        bars  = get_price_bars(sym, start_dt, days)
        exit_p, pnl_pct, outcome = simulate_trade(sym, entry, ts, direction, bars)

        # Skip no_data trades — they add noise without signal
        if outcome == "no_data":
            continue

        seen.add(day_key)
        pnl_usd = NOTIONAL_USD * pnl_pct

        trade = {
            "ts": ts, "symbol": sym, "direction": direction,
            "signal": sig_type, "entry": entry, "exit": exit_p,
            "pnl_pct": pnl_pct, "pnl_usd": pnl_usd, "outcome": outcome,
        }

        if is_buy and sym in BUY_APPROVED:
            trades_long_only.append(trade)

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
        pf     = (sum(wins) / abs(sum(losses))) if losses else 99.0

        # Max drawdown (peak-to-trough on cumulative P&L)
        cum, peak, max_dd = 0.0, 0.0, 0.0
        for p in pnls:
            cum += p
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd

        # Consecutive wins/losses
        best_streak = worst_streak = cur_streak = 0
        for p in pnls:
            if p > 0:
                cur_streak = max(cur_streak + 1, 1)
            else:
                cur_streak = min(cur_streak - 1, -1)
            best_streak  = max(best_streak,  cur_streak)
            worst_streak = min(worst_streak, cur_streak)

        best_trade  = max(trades, key=lambda t: t["pnl_usd"])
        worst_trade = min(trades, key=lambda t: t["pnl_usd"])

        print(f"\n  {label}")
        print(f"  {'─'*55}")
        print(f"  Trades      : {len(trades)}  ({len(wins)}W / {len(losses)}L)")
        print(f"  Win Rate    : {wr:.1f}%")
        print(f"  Total P&L   : ${total:+.2f}")
        print(f"  Avg Win     : ${avg_w:+.2f}    Avg Loss : ${avg_l:+.2f}")
        if wins and losses and avg_l != 0:
            print(f"  R/R Ratio   : {abs(avg_w/avg_l):.2f}x     Profit Factor: {pf:.2f}x")
        print(f"  Max Drawdown: ${max_dd:.2f}")
        print(f"  Best Streak : {best_streak}W    Worst Streak: {abs(worst_streak)}L")
        print(f"  Best Trade  : {best_trade['symbol']} ${best_trade['pnl_usd']:+.2f} ({best_trade['ts'][:10]})")
        print(f"  Worst Trade : {worst_trade['symbol']} ${worst_trade['pnl_usd']:+.2f} ({worst_trade['ts'][:10]})")

        # Outcome breakdown
        outcomes = {}
        for t in trades:
            outcomes[t["outcome"]] = outcomes.get(t["outcome"], 0) + 1
        print(f"  Outcomes    : " + "  ".join(f"{k}={v}" for k, v in sorted(outcomes.items())))

        # Long vs short split
        longs  = [t for t in trades if t["direction"] == "LONG"]
        shorts = [t for t in trades if t["direction"] == "SHORT"]
        if longs:
            l_pnl = sum(t["pnl_usd"] for t in longs)
            l_wr  = sum(1 for t in longs if t["pnl_usd"] > 0) / len(longs) * 100
            print(f"  Longs       : {len(longs)} trades | WR {l_wr:.0f}% | P&L ${l_pnl:+.2f}")
        if shorts:
            s_pnl = sum(t["pnl_usd"] for t in shorts)
            s_wr  = sum(1 for t in shorts if t["pnl_usd"] > 0) / len(shorts) * 100
            print(f"  Shorts      : {len(shorts)} trades | WR {s_wr:.0f}% | P&L ${s_pnl:+.2f}")

        # Per-symbol breakdown
        syms = {}
        for t in trades:
            s = t["symbol"]
            if s not in syms:
                syms[s] = []
            syms[s].append(t["pnl_usd"])
        print(f"\n  Per-symbol breakdown:")
        print(f"  {'Symbol':<7} {'Trades':>6} {'WR':>6} {'P&L':>8}  {'Avg':>7}")
        print(f"  {'─'*40}")
        for sym, sym_pnls in sorted(syms.items(), key=lambda x: -sum(x[1])):
            sym_wr  = sum(1 for p in sym_pnls if p > 0) / len(sym_pnls) * 100
            sym_tot = sum(sym_pnls)
            sym_avg = sym_tot / len(sym_pnls)
            print(f"  {sym:<7} {len(sym_pnls):>6} {sym_wr:>5.0f}% {sym_tot:>+8.2f}  {sym_avg:>+7.2f}")

    print("\n" + "=" * 60)
    print(f"  BACKTEST — LAST {days} DAYS  (${NOTIONAL_USD}/trade, 1 trade/day/symbol)")
    print(f"  TP: +{TAKE_PROFIT_PCT*100:.0f}%  |  SL: -{STOP_LOSS_PCT*100:.0f}%  |  Max hold: {MAX_HOLD_MINS}min")
    print("=" * 60)

    summarise(trades_long_only,  "SCENARIO A — Longs only")
    summarise(trades_long_short, "SCENARIO B — Longs + Shorts")

    shorts_only = [t for t in trades_long_short if t["direction"] == "SHORT"]
    if shorts_only:
        print(f"\n  Short trade detail:")
        print(f"  {'Date':<11} {'Symbol':<6} {'Entry':>8} {'Exit':>8} {'P&L':>8} {'Outcome':<12} {'Signal'}")
        print(f"  {'─'*75}")
        for t in sorted(shorts_only, key=lambda x: x["ts"]):
            print(f"  {t['ts'][:10]}  {t['symbol']:<6} ${t['entry']:>7.2f} ${t['exit']:>7.2f} "
                  f"${t['pnl_usd']:>+7.2f}  {t['outcome']:<12} {t['signal']}")

    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    run(args.days)
