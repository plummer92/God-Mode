#!/usr/bin/env python3
import csv
import json
import os
import sqlite3
import importlib.util
from datetime import datetime
from app_paths import DATA_DIR

TRADING_DEV_DIR = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP_PATH = os.path.join(TRADING_DEV_DIR, "bootstrap_path.py")
_bootstrap_spec = importlib.util.spec_from_file_location("bootstrap_path", BOOTSTRAP_PATH)
_bootstrap_module = importlib.util.module_from_spec(_bootstrap_spec)
assert _bootstrap_spec is not None and _bootstrap_spec.loader is not None
_bootstrap_spec.loader.exec_module(_bootstrap_module)
_bootstrap_module.ensure_trading_dev_first(TRADING_DEV_DIR)

DB_PATH = str(DATA_DIR / "wolfe_signals.db")
APPROVED_PATH = str(DATA_DIR / "approved_symbols.json")
HUNTER_TOP_PATH = str(DATA_DIR / "symbol_hunt_top20.json")
HUNTER_RESULTS_PATH = str(DATA_DIR / "symbol_hunt_results.csv")
OUTPUT_PATH = str(DATA_DIR / "candidate_symbols.json")

LOOKBACK_DAYS = 14
TOP_N = int(os.getenv("CANDIDATE_SYMBOLS_TOP_N", "40"))
MIN_RECENT_HUNTER_SYMBOLS = int(os.getenv("CANDIDATE_MIN_RECENT_HUNTER", "10"))


def normalize_symbol(symbol):
    text = str(symbol or "").strip().upper()
    return text


def is_supported_equity(symbol):
    text = normalize_symbol(symbol)
    return bool(text) and "/" not in text and "-USD" not in text and "=" not in text and not text.startswith("^") and "." not in text


def load_recent_signal_scores():
    scores = {}
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        rows = cur.execute(
            f"""
            SELECT
                symbol,
                COUNT(*) AS signal_count,
                SUM(CASE WHEN signal_type LIKE '%STRONG%' THEN 1 ELSE 0 END) AS strong_count
            FROM signals
            WHERE timestamp > datetime('now', '-{LOOKBACK_DAYS} days')
              AND signal_type != 'Neutral'
            GROUP BY symbol
            """
        ).fetchall()
        for symbol, signal_count, strong_count in rows:
            symbol = normalize_symbol(symbol)
            if not is_supported_equity(symbol):
                continue
            scores[symbol] = {
                "signal_count": int(signal_count or 0),
                "strong_count": int(strong_count or 0),
            }
    except Exception:
        return {}
    finally:
        if conn:
            conn.close()
    return scores


def load_hunter_top():
    try:
        with open(HUNTER_TOP_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return []
    symbols = payload.get("top_sell", []) if isinstance(payload, dict) else []
    return [normalize_symbol(symbol) for symbol in symbols if is_supported_equity(symbol)]


def load_hunter_results():
    results = {}
    try:
        with open(HUNTER_RESULTS_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = normalize_symbol(row.get("symbol"))
                if not is_supported_equity(symbol):
                    continue
                results[symbol] = {
                    "n_signals": int(float(row.get("n_signals") or 0)),
                    "win_rate": float(row.get("win_rate") or 0),
                    "avg_return": float(row.get("avg_return") or 0),
                }
    except Exception:
        return {}
    return results


def load_approved_lists():
    try:
        with open(APPROVED_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {"buy": [], "sell": [], "cooling_off": []}
    return {
        "buy": [normalize_symbol(symbol) for symbol in payload.get("buy", []) if is_supported_equity(symbol)],
        "sell": [normalize_symbol(symbol) for symbol in payload.get("sell", []) if is_supported_equity(symbol)],
        "cooling_off": [normalize_symbol(symbol) for symbol in payload.get("cooling_off", []) if is_supported_equity(symbol)],
    }


def build_ranked_candidates():
    signal_scores = load_recent_signal_scores()
    hunter_top = load_hunter_top()
    hunter_results = load_hunter_results()
    approved = load_approved_lists()

    universe = set(signal_scores) | set(hunter_top) | set(hunter_results)
    universe.update(approved["buy"])
    universe.update(approved["sell"])
    universe.update(approved["cooling_off"])

    ranked = []
    hunter_top_rank = {symbol: idx for idx, symbol in enumerate(hunter_top)}

    for symbol in sorted(universe):
        signal_count = signal_scores.get(symbol, {}).get("signal_count", 0)
        strong_count = signal_scores.get(symbol, {}).get("strong_count", 0)
        hunter = hunter_results.get(symbol, {})
        in_hunter_top = symbol in hunter_top_rank
        is_active = symbol in approved["buy"] or symbol in approved["sell"]
        in_cooling = symbol in approved["cooling_off"]

        score = 0.0
        score += signal_count
        score += strong_count * 2.0
        if in_hunter_top:
            score += max(0, 20 - hunter_top_rank[symbol])
        if hunter:
            score += hunter.get("win_rate", 0.0) * 10.0
            score += hunter.get("avg_return", 0.0) * 100.0
            score += min(hunter.get("n_signals", 0), 20) * 0.25
        if is_active:
            score += 15.0
        elif in_cooling:
            score += 5.0

        ranked.append(
            {
                "symbol": symbol,
                "score": round(score, 4),
                "signal_count": signal_count,
                "strong_count": strong_count,
                "in_hunter_top": in_hunter_top,
                "hunter_n_signals": hunter.get("n_signals", 0),
                "hunter_win_rate": hunter.get("win_rate", 0.0),
                "hunter_avg_return": hunter.get("avg_return", 0.0),
                "is_active": is_active,
                "in_cooling_off": in_cooling,
            }
        )

    ranked.sort(
        key=lambda row: (
            -row["score"],
            -row["strong_count"],
            -row["signal_count"],
            row["symbol"],
        )
    )
    return ranked, {
        "signal_db": bool(signal_scores),
        "hunter_top20": bool(hunter_top),
        "hunter_results_csv": bool(hunter_results),
        "approved_symbols": bool(approved["buy"] or approved["sell"] or approved["cooling_off"]),
    }, hunter_top


def main():
    ranked, sources, hunter_top = build_ranked_candidates()
    symbols = [row["symbol"] for row in ranked[:TOP_N]]
    if MIN_RECENT_HUNTER_SYMBOLS > 0:
        retained_hunter = [symbol for symbol in hunter_top[:MIN_RECENT_HUNTER_SYMBOLS] if symbol not in symbols]
        if retained_hunter:
            for symbol in reversed(retained_hunter):
                if len(symbols) >= TOP_N:
                    symbols.pop()
                symbols.insert(0, symbol)
            symbols = list(dict.fromkeys(symbols))[:TOP_N]
    payload = {
        "symbols": symbols,
        "generated_at": datetime.utcnow().isoformat(),
        "sources": sources,
        "candidate_count": len(symbols),
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
