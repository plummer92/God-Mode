#!/home/theplummer92/venv/bin/python3
import sqlite3
import json
from datetime import datetime, timedelta

DB_PATH        = "/home/theplummer92/strategy_lab.db"
TRADE_DB_PATH  = "/home/theplummer92/trade_log.db"
OUT_PATH       = "/home/theplummer92/approved_symbols.json"

MIN_TRADES    = 25
MIN_WIN_RATE  = 0.60
MIN_SCORE     = 200

MAX_LONGS     = 5
MAX_SHORTS    = 5

CRYPTO_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]

# Demotion thresholds
DEMOTION_NO_WIN_DAYS    = 14   # closed trades in this window but 0 wins → demote
DEMOTION_MIN_WIN_RATE   = 0.60 # rolling avg over last N strategy_lab results
DEMOTION_RESULTS_WINDOW = 20   # how many recent results to average


def fetch_best_per_symbol():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = """
    WITH ranked AS (
        SELECT
            symbol,
            signal_filter,
            rvol,
            win_rate,
            n_trades,
            avg_return,
            score,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, signal_filter
                ORDER BY score DESC, win_rate DESC, n_trades DESC
            ) AS rn
        FROM results
        WHERE n_trades >= ?
          AND win_rate >= ?
          AND score >= ?
    )
    SELECT symbol, signal_filter, rvol, win_rate, n_trades, avg_return, score
    FROM ranked
    WHERE rn = 1
    ORDER BY score DESC, win_rate DESC, n_trades DESC
    """
    rows = cur.execute(query, (MIN_TRADES, MIN_WIN_RATE, MIN_SCORE)).fetchall()
    conn.close()
    return rows


def check_demotion(symbols):
    """
    Returns {symbol: reason} for any rostered symbol that should be demoted.

    Criteria (either triggers demotion):
      1. Symbol has closed trades in the last DEMOTION_NO_WIN_DAYS days but
         none of them are wins (outcome = 'take_profit').
      2. Average win_rate across the last DEMOTION_RESULTS_WINDOW strategy_lab
         results is below DEMOTION_MIN_WIN_RATE.
    """
    demote = {}
    cutoff = (datetime.utcnow() - timedelta(days=DEMOTION_NO_WIN_DAYS)).isoformat()

    # Check 1: recent closed trades with no wins
    try:
        conn = sqlite3.connect(TRADE_DB_PATH)
        cur  = conn.cursor()
        for sym in symbols:
            cur.execute(
                "SELECT COUNT(*) FROM trades "
                "WHERE symbol=? AND exit_time IS NOT NULL AND exit_time >= ?",
                (sym, cutoff)
            )
            closed = cur.fetchone()[0]
            if closed > 0:
                cur.execute(
                    "SELECT COUNT(*) FROM trades "
                    "WHERE symbol=? AND exit_time IS NOT NULL AND exit_time >= ? "
                    "AND outcome='take_profit'",
                    (sym, cutoff)
                )
                wins = cur.fetchone()[0]
                if wins == 0:
                    demote[sym] = f"no_wins_in_{DEMOTION_NO_WIN_DAYS}d"
        conn.close()
    except Exception as e:
        print(f"Warning: trade_log demotion check failed: {e}")

    # Check 2: strategy_lab win-rate decay
    try:
        conn2 = sqlite3.connect(DB_PATH)
        cur2  = conn2.cursor()
        for sym in symbols:
            cur2.execute(
                "SELECT win_rate FROM results WHERE symbol=? "
                "ORDER BY tested_at DESC LIMIT ?",
                (sym, DEMOTION_RESULTS_WINDOW)
            )
            rows = cur2.fetchall()
            if len(rows) >= 5:  # need enough recent data to judge
                avg_wr = sum(r[0] for r in rows) / len(rows)
                if avg_wr < DEMOTION_MIN_WIN_RATE:
                    tag = f"win_rate_decay_{avg_wr:.0%}"
                    demote[sym] = f"{demote[sym]}|{tag}" if sym in demote else tag
        conn2.close()
    except Exception as e:
        print(f"Warning: strategy_lab demotion check failed: {e}")

    return demote


def build_roster(rows, current_data):
    current_cooling = set(current_data.get("cooling_off", []))

    buy_candidates  = []
    sell_candidates = []
    seen_buy  = set()
    seen_sell = set()

    for row in rows:
        symbol = row["symbol"].upper()
        signal = (row["signal_filter"] or "").upper()

        # Don't re-promote cooling_off symbols through the normal path;
        # they need explicit re-evaluation via Symbol_hunter first.
        if symbol in current_cooling:
            continue

        if "BUY" in signal and symbol not in seen_buy:
            buy_candidates.append(symbol)
            seen_buy.add(symbol)

        if "SELL" in signal and symbol not in seen_sell:
            sell_candidates.append(symbol)
            seen_sell.add(symbol)

    buy_list  = buy_candidates[:MAX_LONGS]
    sell_list = sell_candidates[:MAX_SHORTS]
    active    = set(buy_list + sell_list)

    # Check active roster for demotion criteria
    demote = check_demotion(active)
    if demote:
        print(f"Demoting to cooling_off: {demote}")
        buy_list  = [s for s in buy_list  if s not in demote]
        sell_list = [s for s in sell_list if s not in demote]
        current_cooling |= set(demote.keys())

    # Symbols that made it back onto the roster are cleared from cooling_off
    promoted = set(buy_list + sell_list)
    current_cooling -= promoted

    approved = sorted(set(buy_list + sell_list + CRYPTO_SYMBOLS))

    return {
        "buy":         buy_list,
        "sell":        sell_list,
        "approved":    approved,
        "cooling_off": sorted(current_cooling),
        "updated":     datetime.utcnow().isoformat(),
    }


def main():
    try:
        with open(OUT_PATH) as f:
            current_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        current_data = {}

    rows    = fetch_best_per_symbol()
    payload = build_roster(rows, current_data)

    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print("Updated approved_symbols.json")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
