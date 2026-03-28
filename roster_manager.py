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
DEMOTION_RESULTS_WINDOW = 20

# Freshness decay score
FRESHNESS_INITIAL     = 100.0  # score assigned to newly rostered symbols
FRESHNESS_DECAY_RATE  = 0.95   # multiplied per day (half-life ~13.5 days)
FRESHNESS_WIN_BOOST   = 8.0    # added per winning trade
FRESHNESS_LOSS_HIT    = 3.0    # subtracted per losing trade
FRESHNESS_MIN_ACTIVE  = 40.0   # below this → demote to cooling_off
FRESHNESS_PROMOTE_THR = 80.0   # above this → eligible for 1.5x trade size multiplier   # how many recent results to average


def compute_freshness_scores(symbols, current_data):
    """
    Returns {symbol: score} where score reflects recent performance health.

    Decay: each day without activity multiplies the score by FRESHNESS_DECAY_RATE.
    Boost: each winning closed trade adds FRESHNESS_WIN_BOOST.
    Hit:   each losing closed trade subtracts FRESHNESS_LOSS_HIT.
    New symbols start at FRESHNESS_INITIAL.
    Score is clamped to [0, 100].

    Also returns a size_multiplier dict:
      score >= FRESHNESS_PROMOTE_THR → 1.5x trade size
      score >= FRESHNESS_MIN_ACTIVE  → 1.0x
      score <  FRESHNESS_MIN_ACTIVE  → symbol should be demoted
    """
    now           = datetime.utcnow()
    stored_scores = current_data.get("freshness_scores", {})
    scores        = {}

    try:
        conn = sqlite3.connect(TRADE_DB_PATH)
        cur  = conn.cursor()

        for sym in symbols:
            entry     = stored_scores.get(sym, {})
            score     = float(entry.get("score", FRESHNESS_INITIAL))
            last_calc = entry.get("updated", now.isoformat())

            # Apply time decay since last calculation
            try:
                last_dt   = datetime.fromisoformat(last_calc)
                days_gone = max(0.0, (now - last_dt).total_seconds() / 86400)
            except ValueError:
                days_gone = 0.0
            score *= FRESHNESS_DECAY_RATE ** days_gone

            # Apply boosts/hits from closed trades since last calculation
            cur.execute(
                "SELECT outcome FROM trades "
                "WHERE symbol=? AND exit_time IS NOT NULL AND exit_time > ?",
                (sym, last_calc)
            )
            for (outcome,) in cur.fetchall():
                if outcome == "take_profit":
                    score += FRESHNESS_WIN_BOOST
                elif outcome == "stop_loss":
                    score -= FRESHNESS_LOSS_HIT

            score = max(0.0, min(100.0, score))
            scores[sym] = {"score": round(score, 2), "updated": now.isoformat()}

        conn.close()
    except Exception as e:
        print(f"Warning: freshness score computation failed: {e}")
        for sym in symbols:
            if sym not in scores:
                scores[sym] = {"score": FRESHNESS_INITIAL, "updated": now.isoformat()}

    return scores


def freshness_size_multiplier(score):
    if score >= FRESHNESS_PROMOTE_THR:
        return 1.5
    return 1.0


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

    # Compute freshness scores for all candidates
    all_candidates = set(buy_list + sell_list)
    fresh = compute_freshness_scores(all_candidates, current_data)

    # Freshness-based demotion (score too low)
    stale = {sym for sym, entry in fresh.items()
             if entry["score"] < FRESHNESS_MIN_ACTIVE}
    if stale:
        print(f"Demoting (low freshness): { {s: round(fresh[s]['score'],1) for s in stale} }")
        buy_list  = [s for s in buy_list  if s not in stale]
        sell_list = [s for s in sell_list if s not in stale]
        current_cooling |= stale

    # Check active roster for win/win-rate demotion criteria
    active = set(buy_list + sell_list)
    demote = check_demotion(active)
    if demote:
        print(f"Demoting to cooling_off: {demote}")
        buy_list  = [s for s in buy_list  if s not in demote]
        sell_list = [s for s in sell_list if s not in demote]
        current_cooling |= set(demote.keys())

    # Symbols that made it back onto the roster are cleared from cooling_off
    promoted = set(buy_list + sell_list)
    current_cooling -= promoted

    # Build size multipliers for active symbols
    size_multipliers = {
        sym: freshness_size_multiplier(fresh[sym]["score"])
        for sym in promoted
        if sym in fresh
    }

    # Persist freshness scores for all tracked symbols (roster + cooling_off)
    all_tracked = promoted | current_cooling
    stored_fresh = compute_freshness_scores(all_tracked, current_data)

    approved = sorted(set(buy_list + sell_list + CRYPTO_SYMBOLS))

    return {
        "buy":              buy_list,
        "sell":             sell_list,
        "approved":         approved,
        "cooling_off":      sorted(current_cooling),
        "freshness_scores": {s: stored_fresh[s] for s in stored_fresh},
        "size_multipliers": size_multipliers,
        "updated":          datetime.utcnow().isoformat(),
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
