#!/usr/bin/env python3
import sqlite3
import json
import os
from datetime import datetime, timedelta
from app_paths import DATA_DIR, ENV_FILE

DB_PATH        = os.environ.get("STRATEGY_LAB_DB_PATH", str(DATA_DIR / "strategy_lab.db"))
TRADE_DB_PATH  = os.environ.get("TRADE_DB_PATH", str(DATA_DIR / "trade_log.db"))
OUT_PATH       = os.environ.get("APPROVED_SYMBOLS_PATH", str(DATA_DIR / "approved_symbols.json"))

MIN_TRADES    = 20
MIN_WIN_RATE  = 0.60
MIN_SCORE     = 200

MAX_LONGS     = 5
MAX_SHORTS    = 5

CRYPTO_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]

# Demotion thresholds
DEMOTION_NO_WIN_DAYS    = 21   # closed trades in this window but 0 wins → demote
DEMOTION_MIN_WIN_RATE   = 0.60 # rolling avg over last N strategy_lab results
DEMOTION_RESULTS_WINDOW = 20
MIN_TRADES_FOR_DEMOTION = 15   # require enough recent live closes before any demotion can fire

# Freshness decay score
FRESHNESS_INITIAL     = 100.0  # score assigned to newly rostered symbols
FRESHNESS_DECAY_RATE  = 0.95   # multiplied per day (half-life ~13.5 days)
FRESHNESS_WIN_BOOST   = 8.0    # added per winning trade
FRESHNESS_LOSS_HIT    = 3.0    # subtracted per losing trade
FRESHNESS_MIN_ACTIVE  = 40.0   # below this → demote to cooling_off
FRESHNESS_PROMOTE_THR = 80.0   # above this → eligible for 1.5x trade size multiplier   # how many recent results to average


def log_roster_decision(action, symbol, reason, **fields):
    extras = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    suffix = f" {extras}" if extras else ""
    print(f"[roster-decision] action={action} symbol={symbol} reason={reason}{suffix}")


def append_reason(reason_map, symbol, reason):
    if not reason:
        return
    if symbol in reason_map and reason_map[symbol]:
        reason_map[symbol] = f"{reason_map[symbol]}|{reason}"
    else:
        reason_map[symbol] = reason


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


def get_overfit_symbols():
    """Return set of symbols marked OVERFIT in strategy_lab leaderboard."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM leaderboard WHERE overfit=1")
        result = {row[0].upper() for row in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        print(f"Warning: could not fetch overfit symbols: {e}")
        return set()


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


def fetch_latest_rows_for_symbols(symbols):
    if not symbols:
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    placeholders = ",".join("?" for _ in symbols)
    query = f"""
    WITH ranked AS (
        SELECT
            symbol,
            signal_filter,
            rvol,
            win_rate,
            n_trades,
            avg_return,
            score,
            tested_at,
            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY tested_at DESC, score DESC, win_rate DESC, n_trades DESC
            ) AS rn
        FROM results
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, signal_filter, rvol, win_rate, n_trades, avg_return, score, tested_at
    FROM ranked
    WHERE rn = 1
    """
    rows = cur.execute(query, tuple(symbols)).fetchall()
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
    Both checks require at least MIN_TRADES_FOR_DEMOTION recent live closed
    trades before a demotion is allowed, to avoid reacting to tiny samples.
    """
    demote = {}
    cutoff = (datetime.utcnow() - timedelta(days=DEMOTION_NO_WIN_DAYS)).isoformat()
    recent_closed = {}

    # Recent live sample gate + Check 1: recent closed trades with no wins
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
            recent_closed[sym] = closed
            if closed < MIN_TRADES_FOR_DEMOTION:
                log_roster_decision(
                    "skipped_demotion_low_sample",
                    sym,
                    "insufficient_live_trades_for_demotion",
                    closed_trades=closed,
                    min_required=MIN_TRADES_FOR_DEMOTION,
                )
                continue
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
            closed = recent_closed.get(sym, 0)
            if closed < MIN_TRADES_FOR_DEMOTION:
                log_roster_decision(
                    "skipped_demotion_low_sample",
                    sym,
                    "insufficient_live_trades_for_win_rate_decay",
                    closed_trades=closed,
                    min_required=MIN_TRADES_FOR_DEMOTION,
                )
                continue
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


def build_roster(rows, current_data, cooling_rows=None):
    previous_buy = current_data.get("buy", [])
    previous_sell = current_data.get("sell", [])
    previous_active = set(previous_buy + previous_sell)
    current_cooling = set(current_data.get("cooling_off", []))
    original_cooling = set(current_cooling)
    pending_retest = set(current_data.get("pending_retest", []))

    buy_candidates  = []
    sell_candidates = []
    seen_buy  = set()
    seen_sell = set()
    eligible_symbols = set()
    evaluated_cooling = {
        row["symbol"].upper()
        for row in (cooling_rows or [])
    }

    for row in rows:
        symbol = row["symbol"].upper()
        signal = (row["signal_filter"] or "").upper()
        eligible_symbols.add(symbol)

        if "BUY" in signal and symbol not in seen_buy:
            buy_candidates.append(symbol)
            seen_buy.add(symbol)

        if "SELL" in signal and symbol not in seen_sell:
            sell_candidates.append(symbol)
            seen_sell.add(symbol)

    buy_list  = buy_candidates[:MAX_LONGS]
    sell_list = sell_candidates[:MAX_SHORTS]
    selected = set(buy_list + sell_list)
    reviewed_symbols = selected | previous_active | current_cooling

    fresh = compute_freshness_scores(reviewed_symbols, current_data)
    demote = {}

    for sym in sorted(previous_active - eligible_symbols):
        append_reason(demote, sym, "failed_strategy_thresholds")

    for sym, entry in fresh.items():
        if entry["score"] < FRESHNESS_MIN_ACTIVE:
            append_reason(demote, sym, f"low_freshness_{entry['score']:.2f}")

    for sym, reason in check_demotion(reviewed_symbols).items():
        append_reason(demote, sym, reason)

    demoted_symbols = set(demote)
    if demoted_symbols:
        buy_list = [s for s in buy_list if s not in demoted_symbols]
        sell_list = [s for s in sell_list if s not in demoted_symbols]
        current_cooling |= demoted_symbols
        pending_retest -= demoted_symbols
        for sym in sorted(demoted_symbols):
            fields = {}
            if sym in fresh:
                fields["freshness"] = fresh[sym]["score"]
            action = "demoted_to_cooling_off"
            if sym in original_cooling:
                action = "skipped_from_approval"
            log_roster_decision(action, sym, demote[sym], **fields)

    # Symbols that made it back onto the roster are cleared from cooling_off
    promoted = set(buy_list + sell_list)
    restored = promoted & current_cooling
    current_cooling -= promoted
    pending_retest -= promoted

    for sym in sorted(restored):
        fields = {}
        if sym in fresh:
            fields["freshness"] = fresh[sym]["score"]
        log_roster_decision("restored_from_cooling_off", sym, "eligible_again", **fields)

    for sym in sorted(promoted - restored - previous_active):
        fields = {}
        if sym in fresh:
            fields["freshness"] = fresh[sym]["score"]
        log_roster_decision("newly_promoted", sym, "qualified_strategy_results", **fields)

    for sym in sorted((promoted - restored) & previous_active):
        fields = {}
        if sym in fresh:
            fields["freshness"] = fresh[sym]["score"]
        log_roster_decision("kept_approved", sym, "eligible_active", **fields)

    skipped_symbols = (eligible_symbols | previous_active | current_cooling) - promoted - demoted_symbols
    for sym in sorted(skipped_symbols):
        if sym in current_cooling:
            reason = "cooling_off_ineligible"
            if sym in eligible_symbols:
                reason = "cooling_off_not_restored"
            elif sym not in evaluated_cooling:
                reason = "cooling_off_no_strategy_data"
        elif sym in eligible_symbols:
            reason = "roster_capacity"
        else:
            reason = "not_selected"
        log_roster_decision("skipped_from_approval", sym, reason)

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
    payload = {
        "buy":              buy_list,
        "sell":             sell_list,
        "approved":         approved,
        "cooling_off":      sorted(current_cooling),
        "freshness_scores": {s: stored_fresh[s] for s in stored_fresh},
        "size_multipliers": size_multipliers,
        "updated":          datetime.utcnow().isoformat(),
    }
    if "cooling_off_history" in current_data:
        payload["cooling_off_history"] = current_data["cooling_off_history"]
    if pending_retest or "pending_retest" in current_data:
        payload["pending_retest"] = sorted(pending_retest)

    print(
        "[roster-summary] "
        f"buy_count={len(buy_list)} sell_count={len(sell_list)} "
        f"approved_count={len(approved)} cooling_off_count={len(current_cooling)} "
        f"pending_retest_count={len(pending_retest)}"
    )
    return payload


def check_wild_paper_performance(payload):
    """
    Query Alpaca paper account closed trades and promote/demote symbols
    based on wild paper performance.

    Promotion rules (applied AFTER build_roster):
      - >= 3 closed round-trip trades AND win_rate >= 60% -> add to buy or sell list
        (direction decided by which side produced more wins)
      - >= 3 closed round-trip trades AND win_rate < 30%  -> add to cooling_off
      - Never promote a symbol already in cooling_off on live roster
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_FILE)
    except Exception:
        pass

    paper_key    = os.environ.get("APCA_PAPER_KEY_ID")
    paper_secret = os.environ.get("APCA_PAPER_SECRET_KEY")

    if not paper_key or not paper_secret:
        print("[roster-wild] Skipping: APCA_PAPER_KEY_ID or APCA_PAPER_SECRET_KEY not set")
        return

    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
    except ImportError as e:
        print(f"[roster-wild] Skipping: alpaca SDK not available: {e}")
        return

    try:
        client = TradingClient(paper_key, paper_secret, paper=True)
        req    = GetOrdersRequest(status=QueryOrderStatus.CLOSED, limit=500)
        orders = client.get_orders(filter=req)
    except Exception as e:
        print(f"[roster-wild] Failed to fetch paper orders: {e}")
        return

    filled = [o for o in orders if str(o.status.value) == "filled"]
    if not filled:
        print("[roster-wild] No filled paper orders found — nothing to evaluate")
        return

    filled.sort(key=lambda o: o.filled_at)

    # Pair orders into round-trip trades via FIFO lot matching
    # open_lots: {symbol: [{"side": "buy"|"sell", "qty": float, "price": float}, ...]}
    open_lots          = {}
    trades_by_symbol   = {}   # {symbol: [{"side": "long"|"short", "win": bool}, ...]}

    for order in filled:
        sym   = order.symbol
        side  = str(order.side.value).lower()   # "buy" or "sell"
        qty   = float(order.filled_qty)
        price = float(order.filled_avg_price)

        lots = open_lots.setdefault(sym, [])
        trades_by_symbol.setdefault(sym, [])

        if not lots or lots[0]["side"] == side:
            # Opening or adding to an existing position
            lots.append({"side": side, "qty": qty, "price": price})
        else:
            # Closing / reducing the existing position
            existing_side      = lots[0]["side"]
            remaining_close    = qty

            while lots and remaining_close > 0:
                lot       = lots[0]
                close_qty = min(lot["qty"], remaining_close)

                if existing_side == "buy":
                    trade_side = "long"
                    pnl        = (price - lot["price"]) * close_qty
                else:
                    trade_side = "short"
                    pnl        = (lot["price"] - price) * close_qty

                trades_by_symbol[sym].append({"side": trade_side, "win": pnl > 0})

                remaining_close -= close_qty
                lot["qty"]      -= close_qty
                if lot["qty"] <= 0:
                    lots.pop(0)

            # Any leftover qty opens a new position in the opposite direction
            if remaining_close > 0:
                lots.append({"side": side, "qty": remaining_close, "price": price})

    WILD_MIN_TRADES = 3
    WILD_PROMOTE_WR = 0.60
    WILD_DEMOTE_WR  = 0.30

    current_cooling = set(payload.get("cooling_off", []))
    buy_list        = list(payload.get("buy", []))
    sell_list       = list(payload.get("sell", []))

    for sym, trade_list in trades_by_symbol.items():
        if len(trade_list) < WILD_MIN_TRADES:
            continue

        total = len(trade_list)
        wins  = sum(1 for t in trade_list if t["win"])
        wr    = wins / total

        if wr >= WILD_PROMOTE_WR:
            if sym in current_cooling:
                print(
                    f"[roster-wild] SKIP PROMOTE {sym} — already in cooling_off (live) "
                    f"— paper WR {wr:.0%} on {total} trades"
                )
                continue

            long_wins  = sum(1 for t in trade_list if t["win"] and t["side"] == "long")
            short_wins = sum(1 for t in trade_list if t["win"] and t["side"] == "short")

            if long_wins >= short_wins:
                if sym not in buy_list:
                    buy_list.append(sym)
                    print(
                        f"[roster-wild] PROMOTED {sym} to buy list "
                        f"— paper WR {wr:.0%} on {total} trades"
                    )
                else:
                    print(
                        f"[roster-wild] ALREADY IN buy list {sym} "
                        f"— paper WR {wr:.0%} on {total} trades"
                    )
            else:
                if sym not in sell_list:
                    sell_list.append(sym)
                    print(
                        f"[roster-wild] PROMOTED {sym} to sell list "
                        f"— paper WR {wr:.0%} on {total} trades"
                    )
                else:
                    print(
                        f"[roster-wild] ALREADY IN sell list {sym} "
                        f"— paper WR {wr:.0%} on {total} trades"
                    )

        elif wr < WILD_DEMOTE_WR:
            if sym not in current_cooling:
                current_cooling.add(sym)
                buy_list  = [s for s in buy_list  if s != sym]
                sell_list = [s for s in sell_list if s != sym]
                print(
                    f"[roster-wild] MOVED {sym} to cooling_off "
                    f"— paper WR {wr:.0%} on {total} trades"
                )
            else:
                print(
                    f"[roster-wild] ALREADY IN cooling_off {sym} "
                    f"— paper WR {wr:.0%} on {total} trades"
                )

        else:
            print(
                f"[roster-wild] WATCHING {sym} "
                f"— paper WR {wr:.0%} on {total} trades (inconclusive, need more data)"
            )

    payload["buy"]        = buy_list
    payload["sell"]       = sell_list
    payload["cooling_off"] = sorted(current_cooling)
    payload["approved"]   = sorted(set(buy_list + sell_list + CRYPTO_SYMBOLS))


def main():
    try:
        with open(OUT_PATH) as f:
            current_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        current_data = {}

    overfit_symbols = get_overfit_symbols()
    if overfit_symbols:
        print(f"[roster] Excluding {len(overfit_symbols)} OVERFIT symbols: {sorted(overfit_symbols)}")

    rows = fetch_best_per_symbol()
    rows = [r for r in rows if r["symbol"].upper() not in overfit_symbols]

    cooling_off_list = [s for s in current_data.get("cooling_off", []) if s.upper() not in overfit_symbols]
    cooling_rows = fetch_latest_rows_for_symbols(cooling_off_list)
    payload = build_roster(rows, current_data, cooling_rows)

    check_wild_paper_performance(payload)

    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print("Updated approved_symbols.json")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
