#!/usr/bin/env python3
"""strategy_lab.py — runs every 6 hours testing strategy combinations"""
import argparse
import json
import sqlite3, time, os, sys
import subprocess
import importlib.util
import pandas as pd
import numpy as np
import urllib.request
import yfinance as yf
from datetime import datetime, timedelta, timezone
import pytz
from app_paths import DATA_DIR, REPO_DIR, VENV_PYTHON

TRADING_DEV_DIR = str(REPO_DIR)
BOOTSTRAP_PATH = os.path.join(TRADING_DEV_DIR, "bootstrap_path.py")
IMPORT_ROOT = TRADING_DEV_DIR
_bootstrap_spec = importlib.util.spec_from_file_location("bootstrap_path", BOOTSTRAP_PATH)
_bootstrap_module = importlib.util.module_from_spec(_bootstrap_spec)
assert _bootstrap_spec is not None and _bootstrap_spec.loader is not None
_bootstrap_spec.loader.exec_module(_bootstrap_module)
_bootstrap_module.ensure_trading_dev_first(IMPORT_ROOT)

from alpaca_data import get_stock_hourly_bars

DB_PATH  = str(DATA_DIR / "wolfe_signals.db")
LAB_DB   = str(DATA_DIR / "strategy_lab.db")
LOG      = str(DATA_DIR / "strategy_lab.log")
LOCKFILE = "/tmp/strategy_lab.lock"
CANDIDATE_SYMBOLS_PATH = str(DATA_DIR / "candidate_symbols.json")
CANDIDATE_GENERATOR_PATH = os.getenv(
    "CANDIDATE_GENERATOR_PATH",
    os.path.join(str(REPO_DIR), "generate_candidate_symbols.py"),
)
ROSTER_MANAGER_PATH = os.getenv("ROSTER_MANAGER_PATH", str(REPO_DIR / "roster_manager.py"))
CST     = pytz.timezone("America/Chicago")
LOOKBACK = 30
MIN_TRADES = 10
TRAIN_FRACTION = float(os.getenv("STRATEGY_LAB_TRAIN_FRACTION", "0.6"))
VALIDATION_FRACTION = float(os.getenv("STRATEGY_LAB_VALIDATION_FRACTION", "0.2"))
OUT_OF_SAMPLE_FRACTION = max(0.0, 1.0 - TRAIN_FRACTION - VALIDATION_FRACTION)
LIVE_MIN_TOTAL_TRADES = int(os.getenv("STRATEGY_LAB_LIVE_MIN_TOTAL_TRADES", "30"))
LIVE_MIN_OOS_TRADES = int(os.getenv("STRATEGY_LAB_LIVE_MIN_OOS_TRADES", "8"))
OVERFIT_SCORE_GAP_THRESHOLD = float(os.getenv("STRATEGY_LAB_OVERFIT_SCORE_GAP_THRESHOLD", "20.0"))
OVERFIT_RETURN_GAP_THRESHOLD = float(os.getenv("STRATEGY_LAB_OVERFIT_RETURN_GAP_THRESHOLD", "0.015"))
try:
    LOOP_SLEEP_SECONDS = int(os.getenv("STRATEGY_LAB_LOOP_SLEEP_SECONDS", "14400"))
except Exception:
    LOOP_SLEEP_SECONDS = 14400

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
DEFAULT_SYMBOLS = ["NFLX","META","AAPL","AMZN","TSLA","AMD","COIN","GME","SPY","IWM",
                   "NVDA","QQQ","MSFT"]
VALIDATION_SYMBOLS = ["SPY", "QQQ", "META", "AMD", "NVDA", "COIN", "GME", "AMZN", "IWM"]
DATA_PROVIDER = os.getenv("STRATEGY_LAB_DATA_PROVIDER", "alpaca").strip().lower()
STOCK_DATA_FEED = os.getenv("ALPACA_STOCK_DATA_FEED", "iex").strip().lower()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
_price_cache = {}
_logged_bar_skips = set()

DISCOVERY_IDEA_TYPE = "symbol_edge_existing_family"
DISCOVERY_STATUS = "queued_for_validation"
# Conservative discovery thresholds to surface only symbol/filter combos
# that stay strong across multiple parameter variants in a single run.
DISCOVERY_MIN_SUPPORT_VARIANTS = 4
DISCOVERY_MIN_BEST_SCORE = 55.0
DISCOVERY_MIN_MEDIAN_SCORE = 50.0
DISCOVERY_MIN_REPRESENTATIVE_TRADES = 20
DISCOVERY_MIN_REPRESENTATIVE_WIN_RATE = 0.52
DISCOVERY_MIN_REPRESENTATIVE_AVG_RETURN = 0.002
VALIDATION_MIN_SUPPORT_VARIANTS = 8
VALIDATION_MIN_MEDIAN_SCORE = 60.0
VALIDATION_MIN_REPRESENTATIVE_TRADES = 30
VALIDATION_MIN_REPRESENTATIVE_WIN_RATE = 0.54
VALIDATION_MIN_REPRESENTATIVE_AVG_RETURN = 0.003
VALIDATION_MAX_DUPLICATE_SHARE = 0.75


def load_strategy_symbols():
    try:
        with open(CANDIDATE_SYMBOLS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            symbols = data.get("symbols", [])
        elif isinstance(data, list):
            symbols = data
        else:
            symbols = []
        cleaned = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
        if cleaned:
            return list(dict.fromkeys(cleaned))
    except Exception:
        pass
    return list(DEFAULT_SYMBOLS)

def log(msg):
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def refresh_candidate_symbols():
    try:
        result = subprocess.run(
            [sys.executable, CANDIDATE_GENERATOR_PATH],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        log(f"Candidate symbol refresh failed: {exc}")
        return

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or f"exit={result.returncode}"
        log(f"Candidate symbol refresh failed: {detail}")
        return

    symbols = load_strategy_symbols()
    log(f"Candidate symbol refresh complete: loaded {len(symbols)} symbols")

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
    cur.execute("""CREATE TABLE IF NOT EXISTS discovery_ideas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discovered_at TEXT,
        source TEXT,
        idea_type TEXT,
        symbol TEXT,
        signal_filter TEXT,
        summary TEXT,
        support_variants INTEGER,
        best_score REAL,
        median_score REAL,
        representative_n_trades INTEGER,
        representative_win_rate REAL,
        representative_avg_return REAL,
        status TEXT,
        metadata_json TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS validation_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        discovery_idea_id INTEGER,
        status TEXT,
        notes TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS strategy_registry (
        strategy_id TEXT PRIMARY KEY,
        name TEXT,
        family TEXT,
        source TEXT,
        status TEXT,
        account_mode TEXT,
        created_at TEXT,
        updated_at TEXT,
        notes TEXT
    )""")
    now_iso = datetime.now(timezone.utc).isoformat()
    cur.execute(
        """INSERT OR IGNORE INTO strategy_registry (
            strategy_id, name, family, source, status, account_mode,
            created_at, updated_at, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "line_1",
            "Line 1 Production Baseline",
            "wolfe_sniper",
            "existing_live_system",
            "approved_live",
            "live",
            now_iso,
            now_iso,
            "current production baseline; discovery must not affect it directly",
        ),
    )
    _ensure_column(cur, "discovery_ideas", "validation_summary", "TEXT")
    _ensure_column(cur, "discovery_ideas", "validated_at", "TEXT")
    _ensure_column(cur, "validation_queue", "validated_at", "TEXT")
    for table_name in ("results", "leaderboard"):
        _ensure_column(cur, table_name, "config_key", "TEXT")
        _ensure_column(cur, table_name, "in_sample_n_trades", "INTEGER")
        _ensure_column(cur, table_name, "in_sample_win_rate", "REAL")
        _ensure_column(cur, table_name, "in_sample_avg_return", "REAL")
        _ensure_column(cur, table_name, "in_sample_profit_factor", "REAL")
        _ensure_column(cur, table_name, "validation_n_trades", "INTEGER")
        _ensure_column(cur, table_name, "validation_win_rate", "REAL")
        _ensure_column(cur, table_name, "validation_avg_return", "REAL")
        _ensure_column(cur, table_name, "validation_profit_factor", "REAL")
        _ensure_column(cur, table_name, "out_of_sample_n_trades", "INTEGER")
        _ensure_column(cur, table_name, "out_of_sample_win_rate", "REAL")
        _ensure_column(cur, table_name, "out_of_sample_avg_return", "REAL")
        _ensure_column(cur, table_name, "out_of_sample_profit_factor", "REAL")
        _ensure_column(cur, table_name, "overfit_flag", "INTEGER")
        _ensure_column(cur, table_name, "min_trade_threshold_pass", "INTEGER")
        _ensure_column(cur, table_name, "final_live_eligible", "INTEGER")
        _ensure_column(cur, table_name, "consistency_score", "REAL")
        _ensure_column(cur, table_name, "research_candidate", "INTEGER")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_results_symbol_filter_score "
        "ON results(symbol, signal_filter, score DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_results_live_eligible_score "
        "ON results(final_live_eligible, score DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_validation_queue_status_id "
        "ON validation_queue(status, id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_discovery_ideas_symbol_filter "
        "ON discovery_ideas(symbol, signal_filter)"
    )
    conn.commit(); conn.close()


def _ensure_column(cur, table_name, column_name, column_type):
    cur.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cur.fetchall()}
    if column_name not in existing_columns:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _median_score(rows):
    return float(np.median([row["sc"] for row in rows]))


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _split_signals(signals):
    total = len(signals)
    if total <= 0:
        return [], [], []
    train_end = max(1, int(total * TRAIN_FRACTION))
    validation_end = max(train_end + 1, int(total * (TRAIN_FRACTION + VALIDATION_FRACTION)))
    validation_end = min(validation_end, total)
    if validation_end >= total and total >= 3:
        validation_end = total - 1
    train = signals[:train_end]
    validation = signals[train_end:validation_end]
    oos = signals[validation_end:]
    return train, validation, oos


def _trade_metrics(trades):
    if not trades:
        return {
            "n": 0,
            "wr": 0.0,
            "ar": 0.0,
            "pf": 0.0,
            "total_return": 0.0,
        }
    rets = [float(t["ret"]) for t in trades]
    wins = [ret for ret in rets if ret > 0]
    losses = [ret for ret in rets if ret <= 0]
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    return {
        "n": len(rets),
        "wr": len(wins) / len(rets),
        "ar": float(np.mean(rets)),
        "pf": float(gross_wins / gross_losses) if gross_losses > 0 else float(99.0 if gross_wins > 0 else 0.0),
        "total_return": float(sum(rets)),
    }


def _consistency_score(metric_list):
    populated = [m for m in metric_list if int(m["n"]) > 0]
    if len(populated) <= 1:
        return 0.0
    returns = [float(m["ar"]) for m in populated]
    spread = float(np.std(returns))
    return round(_clamp(1.0 - (spread / 0.03), 0.0, 1.0), 4)


def _robust_score(in_sample, validation, out_of_sample, consistency_score, overfit_flag, min_trade_threshold_pass):
    sample_score = _clamp((out_of_sample["n"] / max(LIVE_MIN_OOS_TRADES, 1)) * 0.6 + (in_sample["n"] / max(LIVE_MIN_TOTAL_TRADES, 1)) * 0.4, 0.0, 1.0)
    oos_wr = _clamp(out_of_sample["wr"], 0.0, 1.0)
    oos_return_component = _clamp(out_of_sample["ar"] / 0.03, -1.0, 1.0)
    oos_pf_component = _clamp(out_of_sample["pf"] / 2.5, 0.0, 1.0)
    validation_return_component = _clamp(validation["ar"] / 0.03, -1.0, 1.0)
    score_value = (
        (oos_wr * 0.25)
        + (oos_return_component * 0.25)
        + (oos_pf_component * 0.15)
        + (sample_score * 0.15)
        + (float(consistency_score) * 0.15)
        + (validation_return_component * 0.05)
    ) * 100.0
    if overfit_flag:
        score_value -= 15.0
    if not min_trade_threshold_pass:
        score_value -= 20.0
    return round(score_value, 2)


def summarize_discovery_ideas(all_results):
    grouped = {}
    for row in all_results:
        key = (row["s"], row["f"])
        grouped.setdefault(key, []).append(row)

    ideas = []
    for (symbol, signal_filter), rows in grouped.items():
        support_variants = len(rows)
        if support_variants < DISCOVERY_MIN_SUPPORT_VARIANTS:
            continue
        best_row = max(rows, key=lambda x: x["sc"])
        best_score = float(best_row["sc"])
        median_score = _median_score(rows)
        representative_n_trades = int(best_row.get("oos_n") or best_row["n"])
        representative_win_rate = float(best_row.get("oos_wr") or best_row["wr"])
        representative_avg_return = float(best_row.get("oos_ar") or best_row["ar"])

        if best_score < DISCOVERY_MIN_BEST_SCORE:
            continue
        if median_score < DISCOVERY_MIN_MEDIAN_SCORE:
            continue
        if representative_n_trades < DISCOVERY_MIN_REPRESENTATIVE_TRADES:
            continue
        if representative_win_rate < DISCOVERY_MIN_REPRESENTATIVE_WIN_RATE:
            continue
        if representative_avg_return < DISCOVERY_MIN_REPRESENTATIVE_AVG_RETURN:
            continue

        summary = (
            f"{symbol} {signal_filter} stayed strong across {support_variants} parameter variants; "
            f"best_score={best_score:.2f} median_score={median_score:.2f} "
            f"oos_trades={representative_n_trades}"
        )
        metadata = {
            "thresholds": {
                "min_support_variants": DISCOVERY_MIN_SUPPORT_VARIANTS,
                "min_best_score": DISCOVERY_MIN_BEST_SCORE,
                "min_median_score": DISCOVERY_MIN_MEDIAN_SCORE,
                "min_representative_n_trades": DISCOVERY_MIN_REPRESENTATIVE_TRADES,
                "min_representative_win_rate": DISCOVERY_MIN_REPRESENTATIVE_WIN_RATE,
                "min_representative_avg_return": DISCOVERY_MIN_REPRESENTATIVE_AVG_RETURN,
            },
            "representative_best_row": {
                "symbol": best_row["s"],
                "signal_filter": best_row["f"],
                "rvol": best_row["rvol"],
                "tp": best_row["tp"],
                "sl": best_row["sl"],
                "hold_hours": best_row["hold"],
                "n_trades": representative_n_trades,
                "win_rate": round(representative_win_rate, 4),
                "avg_return": round(representative_avg_return, 4),
                "score": round(best_score, 2),
                "overfit_flag": int(best_row.get("overfit_flag", 0) or 0),
                "final_live_eligible": int(best_row.get("final_live_eligible", 0) or 0),
            },
        }
        ideas.append(
            {
                "source": "strategy_lab",
                "idea_type": DISCOVERY_IDEA_TYPE,
                "symbol": symbol,
                "signal_filter": signal_filter,
                "summary": summary,
                "support_variants": support_variants,
                "best_score": round(best_score, 2),
                "median_score": round(median_score, 2),
                "representative_n_trades": representative_n_trades,
                "representative_win_rate": round(representative_win_rate, 4),
                "representative_avg_return": round(representative_avg_return, 4),
                "status": DISCOVERY_STATUS,
                "metadata_json": json.dumps(metadata, sort_keys=True),
            }
        )
    ideas.sort(
        key=lambda row: (
            -row["best_score"],
            -row["median_score"],
            -row["support_variants"],
            row["symbol"],
            row["signal_filter"],
        )
    )
    return ideas


def post_discovery_alert(idea):
    if not DISCORD_WEBHOOK_URL:
        return False
    message = (
        f"**DISCOVERY IDEA** | {idea['idea_type']} | {idea['symbol']} | {idea['signal_filter']}\n"
        f"support_variants={idea['support_variants']} | best_score={idea['best_score']:.2f} | "
        f"median_score={idea['median_score']:.2f}\n"
        f"n_trades={idea['representative_n_trades']} | "
        f"win_rate={idea['representative_win_rate']:.2%} | "
        f"avg_return={idea['representative_avg_return']:.2%}\n"
        f"status={idea['status']}"
    )
    try:
        payload = json.dumps({"content": message}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            return True
    except Exception as exc:
        log(f"Discovery Discord alert failed for {idea['symbol']} {idea['signal_filter']}: {exc}")
        return False


def persist_discovery_ideas(all_results):
    ideas = summarize_discovery_ideas(all_results)
    if not ideas:
        log("Discovery summary: no notable ideas found for validation queue.")
        return 0

    conn = sqlite3.connect(LAB_DB)
    cur = conn.cursor()
    created = 0
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        for idea in ideas:
            cur.execute(
                """INSERT INTO discovery_ideas (
                    discovered_at, source, idea_type, symbol, signal_filter, summary,
                    support_variants, best_score, median_score,
                    representative_n_trades, representative_win_rate,
                    representative_avg_return, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now_iso,
                    idea["source"],
                    idea["idea_type"],
                    idea["symbol"],
                    idea["signal_filter"],
                    idea["summary"],
                    idea["support_variants"],
                    idea["best_score"],
                    idea["median_score"],
                    idea["representative_n_trades"],
                    idea["representative_win_rate"],
                    idea["representative_avg_return"],
                    idea["status"],
                    idea["metadata_json"],
                ),
            )
            discovery_idea_id = cur.lastrowid
            cur.execute(
                """INSERT INTO validation_queue (
                    created_at, discovery_idea_id, status, notes
                ) VALUES (?, ?, ?, ?)""",
                (
                    now_iso,
                    discovery_idea_id,
                    "queued",
                    "queued by strategy_lab discovery summary",
                ),
            )
            created += 1
            post_discovery_alert(idea)
        conn.commit()
    finally:
        conn.close()
    log(f"Discovery summary: created {created} queued validation idea(s).")
    return created


def post_validated_idea_alert(idea, verdict, summary):
    if verdict != "validated" or not DISCORD_WEBHOOK_URL:
        return False
    message = (
        f"**VALIDATED IDEA** | {idea['symbol']} | {idea['signal_filter']}\n"
        f"support_variants={idea['support_variants']} | best_score={idea['best_score']:.2f} | "
        f"median_score={idea['median_score']:.2f}\n"
        f"{summary}"
    )
    try:
        payload = json.dumps({"content": message}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            return True
    except Exception as exc:
        log(f"Validation Discord alert failed for {idea['symbol']} {idea['signal_filter']}: {exc}")
        return False


def _fetch_discovery_validation_rows(cur, symbol, signal_filter):
    cur.execute(
        """SELECT rvol, tp, sl, hold_hours, n_trades, win_rate, avg_return, profit_factor, score,
                  out_of_sample_n_trades, out_of_sample_win_rate, out_of_sample_avg_return,
                  out_of_sample_profit_factor, overfit_flag, min_trade_threshold_pass,
                  final_live_eligible, consistency_score
           FROM results
           WHERE symbol=? AND signal_filter=?
           ORDER BY score DESC""",
        (symbol, signal_filter),
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "rvol": float(row[0]),
                "tp": float(row[1]),
                "sl": float(row[2]),
                "hold_hours": int(row[3]),
                "n_trades": int(row[4]),
                "win_rate": float(row[5]),
                "avg_return": float(row[6]),
                "profit_factor": float(row[7]),
                "score": float(row[8]),
                "out_of_sample_n_trades": int(row[9] or 0),
                "out_of_sample_win_rate": float(row[10] or 0.0),
                "out_of_sample_avg_return": float(row[11] or 0.0),
                "out_of_sample_profit_factor": float(row[12] or 0.0),
                "overfit_flag": int(row[13] or 0),
                "min_trade_threshold_pass": int(row[14] or 0),
                "final_live_eligible": int(row[15] or 0),
                "consistency_score": float(row[16] or 0.0),
            }
        )
    return rows


def _evaluate_validation_rows(rows):
    if not rows:
        return {
            "verdict": "rejected",
            "support_variants": 0,
            "best_score": 0.0,
            "median_score": 0.0,
            "representative_n_trades": 0,
            "representative_win_rate": 0.0,
            "representative_avg_return": 0.0,
            "duplicate_share": 0.0,
            "overfit_share": 0.0,
            "live_eligible_share": 0.0,
            "robustness_note": "no supporting result rows found in strategy_lab.db",
        }

    support_variants = len(rows)
    best_row = rows[0]
    best_score = float(best_row["score"])
    median_score = float(np.median([row["score"] for row in rows]))
    representative_n_trades = int(np.median([row["out_of_sample_n_trades"] or row["n_trades"] for row in rows]))
    representative_win_rate = float(np.median([row["out_of_sample_win_rate"] or row["win_rate"] for row in rows]))
    representative_avg_return = float(np.median([row["out_of_sample_avg_return"] or row["avg_return"] for row in rows]))

    fingerprint_counts = {}
    for row in rows:
        fingerprint = (
            round(row["out_of_sample_n_trades"] or row["n_trades"], 0),
            round(row["out_of_sample_win_rate"] or row["win_rate"], 4),
            round(row["out_of_sample_avg_return"] or row["avg_return"], 4),
            round(row["score"], 2),
        )
        fingerprint_counts[fingerprint] = fingerprint_counts.get(fingerprint, 0) + 1
    duplicate_share = max(fingerprint_counts.values()) / support_variants
    overfit_share = sum(1 for row in rows if int(row.get("overfit_flag", 0) or 0) == 1) / support_variants
    live_eligible_share = sum(1 for row in rows if int(row.get("final_live_eligible", 0) or 0) == 1) / support_variants

    validated = (
        support_variants >= VALIDATION_MIN_SUPPORT_VARIANTS
        and median_score >= VALIDATION_MIN_MEDIAN_SCORE
        and representative_n_trades >= VALIDATION_MIN_REPRESENTATIVE_TRADES
        and representative_win_rate >= VALIDATION_MIN_REPRESENTATIVE_WIN_RATE
        and representative_avg_return >= VALIDATION_MIN_REPRESENTATIVE_AVG_RETURN
        and duplicate_share <= VALIDATION_MAX_DUPLICATE_SHARE
        and overfit_share <= 0.5
        and live_eligible_share >= 0.5
    )

    watchlist = (
        not validated
        and support_variants >= DISCOVERY_MIN_SUPPORT_VARIANTS
        and median_score >= DISCOVERY_MIN_MEDIAN_SCORE
        and (
            representative_n_trades >= DISCOVERY_MIN_REPRESENTATIVE_TRADES
            or representative_win_rate >= DISCOVERY_MIN_REPRESENTATIVE_WIN_RATE
            or representative_avg_return >= DISCOVERY_MIN_REPRESENTATIVE_AVG_RETURN
        )
    )

    if validated:
        verdict = "validated"
        robustness_note = "broad support with strong median performance"
    elif watchlist:
        verdict = "watchlist"
        robustness_note = "promising but below validation strength thresholds"
    else:
        verdict = "rejected"
        robustness_note = "not robust enough after the stricter second pass"

    if duplicate_share > VALIDATION_MAX_DUPLICATE_SHARE:
        robustness_note += "; duplicate-performance clutter is too concentrated"
    if overfit_share > 0.5:
        robustness_note += "; too many variants are flagged as overfit"

    return {
        "verdict": verdict,
        "support_variants": support_variants,
        "best_score": best_score,
        "median_score": median_score,
        "representative_n_trades": representative_n_trades,
        "representative_win_rate": representative_win_rate,
        "representative_avg_return": representative_avg_return,
        "duplicate_share": duplicate_share,
        "overfit_share": overfit_share,
        "live_eligible_share": live_eligible_share,
        "robustness_note": robustness_note,
    }


def _build_validation_summary(metrics):
    return (
        f"{metrics['verdict']}: support_variants={metrics['support_variants']} | "
        f"best_score={metrics['best_score']:.2f} | median_score={metrics['median_score']:.2f} | "
        f"representative_n_trades={metrics['representative_n_trades']} | "
        f"representative_win_rate={metrics['representative_win_rate']:.2%} | "
        f"representative_avg_return={metrics['representative_avg_return']:.2%} | "
        f"duplicate_share={metrics['duplicate_share']:.0%} | "
        f"overfit_share={metrics['overfit_share']:.0%} | "
        f"live_eligible_share={metrics['live_eligible_share']:.0%} | {metrics['robustness_note']}"
    )


def validate_queued_discovery_ideas():
    conn = sqlite3.connect(LAB_DB)
    cur = conn.cursor()
    processed = {"validated": 0, "watchlist": 0, "rejected": 0}
    try:
        cur.execute(
            """SELECT
                   v.id,
                   d.id,
                   d.symbol,
                   d.signal_filter,
                   d.support_variants,
                   d.best_score,
                   d.median_score
               FROM validation_queue v
               JOIN discovery_ideas d ON d.id = v.discovery_idea_id
               WHERE v.status='queued'
               ORDER BY v.id ASC"""
        )
        queued_rows = cur.fetchall()
        if not queued_rows:
            return processed

        now_iso = datetime.now(timezone.utc).isoformat()
        for (
            queue_id,
            discovery_idea_id,
            symbol,
            signal_filter,
            support_variants,
            best_score,
            median_score,
        ) in queued_rows:
            results_rows = _fetch_discovery_validation_rows(cur, symbol, signal_filter)
            metrics = _evaluate_validation_rows(results_rows)
            summary = _build_validation_summary(metrics)
            processed[metrics["verdict"]] += 1

            cur.execute(
                """UPDATE discovery_ideas
                   SET status=?, validation_summary=?, validated_at=?
                   WHERE id=?""",
                (metrics["verdict"], summary, now_iso, discovery_idea_id),
            )
            cur.execute(
                """UPDATE validation_queue
                   SET status=?, notes=?, validated_at=?
                   WHERE id=?""",
                (metrics["verdict"], summary, now_iso, queue_id),
            )
            post_validated_idea_alert(
                {
                    "symbol": symbol,
                    "signal_filter": signal_filter,
                    "support_variants": support_variants,
                    "best_score": best_score,
                    "median_score": median_score,
                },
                metrics["verdict"],
                summary,
            )
        conn.commit()
        return processed
    finally:
        conn.close()

def is_stock_etf_symbol(symbol: str) -> bool:
    text = str(symbol).strip().upper()
    return "/" not in text and "-USD" not in text and "=" not in text and "." not in text and not text.startswith("^")


def _alpaca_skip_reason(symbol: str) -> str | None:
    text = str(symbol).strip().upper()
    if text.startswith("^"):
        return "caret-prefixed Yahoo index symbol"
    if "=" in text:
        return "equals-sign Yahoo macro/FX/futures symbol"
    if "." in text:
        return "dot-suffixed unsupported symbol format"
    if "/" in text:
        return "slash-formatted non-stock symbol"
    if "-USD" in text:
        return "crypto Yahoo symbol"
    return None


def _log_bar_skip_once(symbol: str, reason: str) -> None:
    key = (str(symbol).strip().upper(), reason)
    if key in _logged_bar_skips:
        return
    _logged_bar_skips.add(key)
    log(f"Skipping {key[0]} Alpaca bars: {reason}")


def _normalize_price_frame(df):
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.get_level_values(0)
    required_columns = ["Open", "High", "Low", "Close", "Volume"]
    if not set(required_columns).issubset(out.columns):
        return pd.DataFrame()
    out = out[required_columns].dropna(subset=["Close", "Volume"])
    out = out[~out.index.duplicated(keep="last")].sort_index()
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    return out


def _fetch_yahoo_hourly_bars(symbol, start, end):
    raw = yf.download(
        str(symbol).strip().upper(),
        start=(start - timedelta(days=2)).strftime("%Y-%m-%d"),
        end=(end + timedelta(days=2)).strftime("%Y-%m-%d"),
        interval="60m",
        progress=False,
        group_by="column",
        threads=False,
        auto_adjust=False,
    )
    return _normalize_price_frame(raw)


def _dedupe_signals(signals):
    seen = set()
    deduped = []
    for signal in signals:
        ts_text = str(signal[0])
        direction = "BUY" if "BUY" in str(signal[1]).upper() else "SELL"
        key = (
            ts_text[:13],
            (ts_text[14:16] if len(ts_text) >= 16 else "00"),
            direction,
            round(float(signal[2]), 2),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(signal)
    return deduped


def get_signals(symbol, sig_filter, rvol, lookback_days=LOOKBACK):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        ph = " OR ".join(["signal_type LIKE ?" for _ in sig_filter])
        params = [symbol] + [f"%{s}%" for s in sig_filter] + [rvol]
        cur = conn.cursor()
        cur.execute(f"""SELECT timestamp, signal_type, price, rvol FROM signals
            WHERE symbol=? AND ({ph}) AND rvol>=?
            AND timestamp > datetime('now', '-{lookback_days} days')
            ORDER BY timestamp ASC""", params)
        return cur.fetchall()
    except:
        return []
    finally:
        if conn:
            conn.close()

def get_price_bars(symbol, start, end, provider=DATA_PROVIDER, allow_fallback=True):
    cache_key = (
        str(symbol).upper(),
        provider,
        start.isoformat(),
        end.isoformat(),
        bool(allow_fallback),
    )
    if cache_key in _price_cache:
        return _price_cache[cache_key]

    provider_name = str(provider).strip().lower()
    df = None

    if provider_name == "yahoo":
        try:
            df = _fetch_yahoo_hourly_bars(symbol, start, end)
        except Exception as exc:
            log(f"Skipping {str(symbol).strip().upper()} Yahoo bars: fetch failed: {exc}")
            df = None
    else:
        if not is_stock_etf_symbol(symbol):
            reason = _alpaca_skip_reason(symbol)
            if reason is not None:
                _log_bar_skip_once(symbol, reason)
        else:
            try:
                frames = get_stock_hourly_bars([symbol], start=start, end=end, feed=STOCK_DATA_FEED)
                df = _normalize_price_frame(frames.get(str(symbol).strip().upper()))
                if df.empty:
                    _log_bar_skip_once(symbol, "empty Alpaca bars")
                    df = None
            except Exception as exc:
                _log_bar_skip_once(symbol, f"Alpaca fetch failed: {exc}")
                df = None

    if df is None:
        df = pd.DataFrame()
    _price_cache[cache_key] = df
    return df


def simulate(symbol, signals, tp, sl, hold_hours, provider=DATA_PROVIDER, lookback_days=LOOKBACK, allow_fallback=True, run_end=None):
    if not signals:
        return []
    end = run_end or datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days + 5)
    df = get_price_bars(symbol, start, end, provider=provider, allow_fallback=allow_fallback)
    if df is None or len(df) < 10:
        return []
    results = []
    hold_bars = max(1, round(hold_hours / 6.5))
    bar_index = df.index
    for sig in signals:
        try:
            ts_str, sig_type, entry_price, _ = sig
            ts = pd.Timestamp(ts_str, tz="UTC")
            entry_idx = int(bar_index.searchsorted(ts, side="right"))
            if entry_idx >= len(df):
                continue
            future = df.iloc[entry_idx:]
            if len(future) < 2:
                continue
            entry_price = float(future["Open"].iloc[0])
            if entry_price <= 0:
                continue
            is_long = "BUY" in sig_type.upper()
            exit_idx = min(hold_bars, len(future) - 1)
            exit_price = float(future["Close"].iloc[exit_idx])
            if is_long:
                ret = (exit_price - entry_price) / entry_price
                for j in range(0, exit_idx + 1):
                    high_ret = (float(future["High"].iloc[j]) - entry_price) / entry_price
                    low_ret = (entry_price - float(future["Low"].iloc[j])) / entry_price
                    if low_ret >= sl and high_ret >= tp:
                        ret = -sl
                        break
                    if low_ret >= sl:
                        ret = -sl
                        break
                    if high_ret >= tp:
                        ret = tp
                        break
            else:
                ret = (entry_price - exit_price) / entry_price
                for j in range(0, exit_idx + 1):
                    tp_ret = (entry_price - float(future["Low"].iloc[j])) / entry_price
                    sl_ret = (float(future["High"].iloc[j]) - entry_price) / entry_price
                    if sl_ret >= sl and tp_ret >= tp:
                        ret = -sl
                        break
                    if sl_ret >= sl:
                        ret = -sl
                        break
                    if tp_ret >= tp:
                        ret = tp
                        break
            results.append({
                "ret": ret,
                "win": ret > 0,
                "signal_ts": ts_str,
                "entry_bar_ts": str(future.index[0]),
            })
        except Exception:
            continue
    return results

def score(wr, avg_ret, n, pf):
    return round(((wr * 0.25) + (_clamp(avg_ret / 0.03, -1.0, 1.0) * 0.35) + (_clamp(pf / 2.5, 0.0, 1.0) * 0.2) + (_clamp(n / 40.0, 0.0, 1.0) * 0.2)) * 100.0, 2)


def _rounded(value, digits):
    if value is None:
        return None
    return float(round(float(value), digits))

def compute_results(
    symbols=None,
    provider=DATA_PROVIDER,
    lookback_days=LOOKBACK,
    allow_fallback=True,
    signal_filters=None,
    rvol_thresholds=None,
    tp_values=None,
    sl_values=None,
    hold_hours_values=None,
    run_end=None,
):
    if symbols is None:
        symbols = load_strategy_symbols()
    signal_filters = signal_filters or SIGNAL_FILTERS
    rvol_thresholds = rvol_thresholds or RVOL_THRESHOLDS
    tp_values = tp_values or TP_VALUES
    sl_values = sl_values or SL_VALUES
    hold_hours_values = hold_hours_values or HOLD_HOURS
    all_results = []
    evaluated_symbols = set()
    run_end = run_end or datetime.now(timezone.utc)
    seen_configs = set()
    for symbol in symbols:
        symbol_rows = 0
        for sf in signal_filters:
            fs = "+".join(sf)
            for rvol in rvol_thresholds:
                sigs = get_signals(symbol, sf, rvol, lookback_days=lookback_days)
                if not sigs:
                    continue
                sigs = _dedupe_signals(sigs)
                train_sigs, validation_sigs, oos_sigs = _split_signals(sigs)
                for tp in tp_values:
                    for sl in sl_values:
                        if tp <= sl:
                            continue
                        for hold in hold_hours_values:
                            config_key = f"{symbol}|{fs}|{rvol:.2f}|{tp:.4f}|{sl:.4f}|{int(hold)}"
                            if config_key in seen_configs:
                                continue
                            seen_configs.add(config_key)

                            train_trades = simulate(
                                symbol,
                                train_sigs,
                                tp,
                                sl,
                                hold,
                                provider=provider,
                                lookback_days=lookback_days,
                                allow_fallback=allow_fallback,
                                run_end=run_end,
                            )
                            validation_trades = simulate(
                                symbol,
                                validation_sigs,
                                tp,
                                sl,
                                hold,
                                provider=provider,
                                lookback_days=lookback_days,
                                allow_fallback=allow_fallback,
                                run_end=run_end,
                            )
                            oos_trades = simulate(
                                symbol,
                                oos_sigs,
                                tp,
                                sl,
                                hold,
                                provider=provider,
                                lookback_days=lookback_days,
                                allow_fallback=allow_fallback,
                                run_end=run_end,
                            )
                            all_trades = train_trades + validation_trades + oos_trades
                            if len(all_trades) < MIN_TRADES:
                                continue
                            in_sample = _trade_metrics(train_trades)
                            validation = _trade_metrics(validation_trades)
                            out_of_sample = _trade_metrics(oos_trades)
                            overall = _trade_metrics(all_trades)
                            min_trade_threshold_pass = (
                                overall["n"] >= LIVE_MIN_TOTAL_TRADES
                                and out_of_sample["n"] >= LIVE_MIN_OOS_TRADES
                            )
                            overfit_flag = int(
                                (in_sample["n"] > 0 and out_of_sample["n"] > 0)
                                and (
                                    (score(in_sample["wr"], in_sample["ar"], in_sample["n"], in_sample["pf"]) -
                                     score(out_of_sample["wr"], out_of_sample["ar"], out_of_sample["n"], out_of_sample["pf"]))
                                    >= OVERFIT_SCORE_GAP_THRESHOLD
                                    or (in_sample["ar"] - out_of_sample["ar"]) >= OVERFIT_RETURN_GAP_THRESHOLD
                                )
                            )
                            consistency_score = _consistency_score([in_sample, validation, out_of_sample])
                            final_live_eligible = int(
                                min_trade_threshold_pass
                                and not overfit_flag
                                and out_of_sample["n"] > 0
                                and out_of_sample["ar"] > 0
                                and out_of_sample["pf"] >= 1.05
                                and out_of_sample["wr"] >= 0.5
                                and consistency_score >= 0.35
                            )
                            sc = _robust_score(
                                in_sample,
                                validation,
                                out_of_sample,
                                consistency_score,
                                bool(overfit_flag),
                                bool(min_trade_threshold_pass),
                            )
                            all_results.append({
                                "config_key": config_key,
                                "s": symbol,
                                "f": fs,
                                "rvol": rvol,
                                "tp": tp,
                                "sl": sl,
                                "hold": hold,
                                "n": overall["n"],
                                "wr": overall["wr"],
                                "ar": overall["ar"],
                                "pf": overall["pf"],
                                "sc": sc,
                                "in_n": in_sample["n"],
                                "in_wr": in_sample["wr"],
                                "in_ar": in_sample["ar"],
                                "in_pf": in_sample["pf"],
                                "val_n": validation["n"],
                                "val_wr": validation["wr"],
                                "val_ar": validation["ar"],
                                "val_pf": validation["pf"],
                                "oos_n": out_of_sample["n"],
                                "oos_wr": out_of_sample["wr"],
                                "oos_ar": out_of_sample["ar"],
                                "oos_pf": out_of_sample["pf"],
                                "overfit_flag": overfit_flag,
                                "min_trade_threshold_pass": 1 if min_trade_threshold_pass else 0,
                                "final_live_eligible": final_live_eligible,
                                "consistency_score": consistency_score,
                                "research_candidate": 1,
                            })
                            symbol_rows += 1
        if symbol_rows:
            evaluated_symbols.add(symbol)
    return all_results, evaluated_symbols


def run():
    log("Starting iteration...")
    init_db()
    _price_cache.clear()
    _logged_bar_skips.clear()
    refresh_candidate_symbols()
    symbols = load_strategy_symbols()
    all_results, _ = compute_results(symbols=symbols)
    conn = sqlite3.connect(LAB_DB)
    cur  = conn.cursor()
    try:
        tested_at = datetime.now().isoformat()
        cur.executemany(
            """
            INSERT INTO results (
                tested_at, symbol, signal_filter, rvol, tp, sl, hold_hours,
                n_trades, win_rate, avg_return, profit_factor, score,
                config_key, in_sample_n_trades, in_sample_win_rate, in_sample_avg_return,
                in_sample_profit_factor, validation_n_trades, validation_win_rate,
                validation_avg_return, validation_profit_factor, out_of_sample_n_trades,
                out_of_sample_win_rate, out_of_sample_avg_return, out_of_sample_profit_factor,
                overfit_flag, min_trade_threshold_pass, final_live_eligible,
                consistency_score, research_candidate
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            [
                (
                    tested_at,
                    r["s"],
                    r["f"],
                    r["rvol"],
                    r["tp"],
                    r["sl"],
                    r["hold"],
                    r["n"],
                    round(r["wr"], 4),
                    round(r["ar"], 4),
                    round(r["pf"], 4),
                    r["sc"],
                    r["config_key"],
                    r["in_n"],
                    round(r["in_wr"], 4),
                    round(r["in_ar"], 4),
                    round(r["in_pf"], 4),
                    r["val_n"],
                    round(r["val_wr"], 4),
                    round(r["val_ar"], 4),
                    round(r["val_pf"], 4),
                    r["oos_n"],
                    round(r["oos_wr"], 4),
                    round(r["oos_ar"], 4),
                    round(r["oos_pf"], 4),
                    r["overfit_flag"],
                    r["min_trade_threshold_pass"],
                    r["final_live_eligible"],
                    round(r["consistency_score"], 4),
                    r["research_candidate"],
                )
                for r in all_results
            ],
        )
        cur.execute("DELETE FROM leaderboard")
        best_per_symbol_filter = {}
        for row in all_results:
            key = (row["s"], row["f"])
            incumbent = best_per_symbol_filter.get(key)
            if incumbent is None or (
                row["sc"],
                row["final_live_eligible"],
                row["oos_n"],
                row["oos_ar"],
            ) > (
                incumbent["sc"],
                incumbent["final_live_eligible"],
                incumbent["oos_n"],
                incumbent["oos_ar"],
            ):
                best_per_symbol_filter[key] = row
        leaderboard_rows = sorted(
            best_per_symbol_filter.values(),
            key=lambda x: (
                -x["final_live_eligible"],
                -x["sc"],
                -x["oos_n"],
                -x["oos_ar"],
                x["s"],
                x["f"],
            ),
        )
        updated_at = datetime.now().isoformat()
        cur.executemany(
            """
            INSERT INTO leaderboard (
                rank, symbol, signal_filter, rvol, tp, sl, hold_hours,
                n_trades, win_rate, avg_return, score, updated_at,
                config_key, in_sample_n_trades, in_sample_win_rate, in_sample_avg_return,
                in_sample_profit_factor, validation_n_trades, validation_win_rate,
                validation_avg_return, validation_profit_factor, out_of_sample_n_trades,
                out_of_sample_win_rate, out_of_sample_avg_return, out_of_sample_profit_factor,
                overfit_flag, min_trade_threshold_pass, final_live_eligible,
                consistency_score, research_candidate
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            [
                (
                    i,
                    r["s"],
                    r["f"],
                    r["rvol"],
                    r["tp"],
                    r["sl"],
                    r["hold"],
                    r["n"],
                    round(r["wr"], 4),
                    round(r["ar"], 4),
                    r["sc"],
                    updated_at,
                    r["config_key"],
                    r["in_n"],
                    round(r["in_wr"], 4),
                    round(r["in_ar"], 4),
                    round(r["in_pf"], 4),
                    r["val_n"],
                    round(r["val_wr"], 4),
                    round(r["val_ar"], 4),
                    round(r["val_pf"], 4),
                    r["oos_n"],
                    round(r["oos_wr"], 4),
                    round(r["oos_ar"], 4),
                    round(r["oos_pf"], 4),
                    r["overfit_flag"],
                    r["min_trade_threshold_pass"],
                    r["final_live_eligible"],
                    round(r["consistency_score"], 4),
                    r["research_candidate"],
                )
                for i, r in enumerate(leaderboard_rows[:20], 1)
            ],
        )
        conn.commit()
    finally:
        conn.close()
    for symbol in symbols:
        log(f"  {symbol} done")
    log(f"Done. {len(all_results)} strategies found.")
    persist_discovery_ideas(all_results)
    validation_results = validate_queued_discovery_ideas()
    log(
        "Validation summary: "
        f"validated={validation_results['validated']} "
        f"watchlist={validation_results['watchlist']} "
        f"rejected={validation_results['rejected']}"
    )
    if all_results:
        b = all_results[0]
        log(f"BEST: {b['s']} WR={b['wr']:.0%} TP={b['tp']:.0%} SL={b['sl']:.0%} Hold={b['hold']}h Score={b['sc']}")


def run_validation():
    validation_symbols = VALIDATION_SYMBOLS
    lookback_days = 14
    _price_cache.clear()
    _logged_bar_skips.clear()
    yahoo_results, yahoo_symbols = compute_results(validation_symbols, provider="yahoo", lookback_days=lookback_days, allow_fallback=False)
    _price_cache.clear()
    _logged_bar_skips.clear()
    alpaca_results, alpaca_symbols = compute_results(validation_symbols, provider="alpaca", lookback_days=lookback_days, allow_fallback=False)

    def summarize(results):
        by_symbol = {}
        for r in results:
            by_symbol.setdefault(r["s"], []).append(r)
        top_rank = {
            (r["s"], r["f"], r["rvol"], r["tp"], r["sl"], r["hold"]): idx + 1
            for idx, r in enumerate(sorted(results, key=lambda x: x["sc"], reverse=True))
        }
        return by_symbol, top_rank

    yahoo_by_symbol, yahoo_rank = summarize(yahoo_results)
    alpaca_by_symbol, alpaca_rank = summarize(alpaca_results)

    per_symbol = []
    for symbol in validation_symbols:
        y_rows = yahoo_by_symbol.get(symbol, [])
        a_rows = alpaca_by_symbol.get(symbol, [])
        y_best = max(y_rows, key=lambda x: x["sc"]) if y_rows else None
        a_best = max(a_rows, key=lambda x: x["sc"]) if a_rows else None
        per_symbol.append({
            "symbol": symbol,
            "yahoo_rows": len(y_rows),
            "alpaca_rows": len(a_rows),
            "row_delta": len(a_rows) - len(y_rows),
            "yahoo_best_score": None if y_best is None else _rounded(y_best["sc"], 2),
            "alpaca_best_score": None if a_best is None else _rounded(a_best["sc"], 2),
            "score_delta": None if y_best is None or a_best is None else _rounded(a_best["sc"] - y_best["sc"], 2),
            "yahoo_best_wr": None if y_best is None else _rounded(y_best["wr"], 4),
            "alpaca_best_wr": None if a_best is None else _rounded(a_best["wr"], 4),
            "wr_delta": None if y_best is None or a_best is None else _rounded(a_best["wr"] - y_best["wr"], 4),
            "yahoo_best_ar": None if y_best is None else _rounded(y_best["ar"], 4),
            "alpaca_best_ar": None if a_best is None else _rounded(a_best["ar"], 4),
            "ar_delta": None if y_best is None or a_best is None else _rounded(a_best["ar"] - y_best["ar"], 4),
        })

    combined_keys = set(yahoo_rank) | set(alpaca_rank)
    ranking_changes = []
    for key in combined_keys:
        ranking_changes.append({
            "symbol": key[0],
            "config": {
                "filter": key[1], "rvol": key[2], "tp": key[3], "sl": key[4], "hold": key[5],
            },
            "yahoo_rank": yahoo_rank.get(key),
            "alpaca_rank": alpaca_rank.get(key),
            "rank_delta": None if yahoo_rank.get(key) is None or alpaca_rank.get(key) is None else alpaca_rank[key] - yahoo_rank[key],
        })

    ranking_changes.sort(
        key=lambda x: 10**6 if x["rank_delta"] is None else abs(x["rank_delta"]),
        reverse=True,
    )

    report = {
        "lookback_days": lookback_days,
        "symbols_compared": validation_symbols,
        "yahoo_total_symbols_evaluated": len(yahoo_symbols),
        "alpaca_total_symbols_evaluated": len(alpaca_symbols),
        "yahoo_total_strategy_rows": len(yahoo_results),
        "alpaca_total_strategy_rows": len(alpaca_results),
        "per_symbol_deltas": per_symbol,
        "largest_ranking_changes": ranking_changes[:10],
    }
    print(report)


def run_validation_fast():
    validation_symbols = VALIDATION_SYMBOLS
    lookback_days = 14
    fast_signal_filters = [
        ["STRONG BUY", "STRONG SELL"],
        ["STRONG BUY", "STRONG SELL", "ABSORPTION"],
    ]
    fast_rvol_thresholds = [2.0, 3.0]
    fast_tp_values = [0.04]
    fast_sl_values = [0.02]
    fast_hold_hours = [4, 24]
    run_end = datetime.now(timezone.utc)
    window_start = run_end - timedelta(days=lookback_days + 5)

    provider_status = {}
    provider_results = {}

    for provider in ["yahoo", "alpaca"]:
        _price_cache.clear()
        _logged_bar_skips.clear()
        status_rows = []
        ok_symbols = []
        skipped_symbols = []
        for symbol in validation_symbols:
            bars = get_price_bars(
                symbol,
                window_start,
                run_end,
                provider=provider,
                allow_fallback=False,
            )
            if bars is None or bars.empty or len(bars) < 10:
                skipped_symbols.append(symbol)
                status_rows.append(
                    {
                        "symbol": symbol,
                        "bars": 0 if bars is None else len(bars),
                        "start": None,
                        "end": None,
                    }
                )
                print(f"[validate-fast] provider={provider} symbol={symbol} bars=0 window=n/a..n/a")
                continue

            ok_symbols.append(symbol)
            status_rows.append(
                {
                    "symbol": symbol,
                    "bars": len(bars),
                    "start": str(bars.index[0]),
                    "end": str(bars.index[-1]),
                }
            )
            print(
                f"[validate-fast] provider={provider} symbol={symbol} "
                f"bars={len(bars)} window={bars.index[0]}..{bars.index[-1]}",
                flush=True,
            )

        results, evaluated = compute_results(
            ok_symbols,
            provider=provider,
            lookback_days=lookback_days,
            allow_fallback=False,
            signal_filters=fast_signal_filters,
            rvol_thresholds=fast_rvol_thresholds,
            tp_values=fast_tp_values,
            sl_values=fast_sl_values,
            hold_hours_values=fast_hold_hours,
            run_end=run_end,
        )
        provider_status[provider] = {
            "requested": list(validation_symbols),
            "successful": ok_symbols,
            "skipped": skipped_symbols,
            "bar_status": status_rows,
            "evaluated_symbols": sorted(evaluated),
            "rows": len(results),
        }
        provider_results[provider] = results

    def summarize(results):
        by_symbol = {}
        for r in results:
            by_symbol.setdefault(r["s"], []).append(r)
        top_rank = {
            (r["s"], r["f"], r["rvol"], r["tp"], r["sl"], r["hold"]): idx + 1
            for idx, r in enumerate(sorted(results, key=lambda x: x["sc"], reverse=True))
        }
        return by_symbol, top_rank

    yahoo_by_symbol, yahoo_rank = summarize(provider_results["yahoo"])
    alpaca_by_symbol, alpaca_rank = summarize(provider_results["alpaca"])

    compared_symbols = []
    score_deltas = []
    wr_deltas = []
    ar_deltas = []
    per_symbol = []
    for symbol in validation_symbols:
        y_rows = yahoo_by_symbol.get(symbol, [])
        a_rows = alpaca_by_symbol.get(symbol, [])
        if y_rows and a_rows:
            compared_symbols.append(symbol)
        y_best = max(y_rows, key=lambda x: x["sc"]) if y_rows else None
        a_best = max(a_rows, key=lambda x: x["sc"]) if a_rows else None
        score_delta = None if y_best is None or a_best is None else _rounded(a_best["sc"] - y_best["sc"], 2)
        wr_delta = None if y_best is None or a_best is None else _rounded(a_best["wr"] - y_best["wr"], 4)
        ar_delta = None if y_best is None or a_best is None else _rounded(a_best["ar"] - y_best["ar"], 4)
        if score_delta is not None:
            score_deltas.append(score_delta)
            wr_deltas.append(wr_delta)
            ar_deltas.append(ar_delta)
        per_symbol.append(
            {
                "symbol": symbol,
                "yahoo_rows": len(y_rows),
                "alpaca_rows": len(a_rows),
                "score_delta": score_delta,
                "wr_delta": wr_delta,
                "ar_delta": ar_delta,
            }
        )

    combined_keys = set(yahoo_rank) | set(alpaca_rank)
    ranking_changes = []
    for key in combined_keys:
        ranking_changes.append(
            {
                "symbol": key[0],
                "config": {
                    "filter": key[1],
                    "rvol": key[2],
                    "tp": key[3],
                    "sl": key[4],
                    "hold": key[5],
                },
                "yahoo_rank": yahoo_rank.get(key),
                "alpaca_rank": alpaca_rank.get(key),
                "rank_delta": None if yahoo_rank.get(key) is None or alpaca_rank.get(key) is None else alpaca_rank[key] - yahoo_rank[key],
            }
        )
    ranking_changes.sort(
        key=lambda x: 10**6 if x["rank_delta"] is None else abs(x["rank_delta"]),
        reverse=True,
    )

    report = {
        "symbols_requested": validation_symbols,
        "symbols_successfully_compared": compared_symbols,
        "skipped_symbols_by_provider": {
            "yahoo": provider_status["yahoo"]["skipped"],
            "alpaca": provider_status["alpaca"]["skipped"],
        },
        "rows_generated_by_provider": {
            "yahoo": provider_status["yahoo"]["rows"],
            "alpaca": provider_status["alpaca"]["rows"],
        },
        "avg_score_delta": None if not score_deltas else _rounded(sum(score_deltas) / len(score_deltas), 4),
        "avg_win_rate_delta": None if not wr_deltas else _rounded(sum(wr_deltas) / len(wr_deltas), 4),
        "avg_return_delta": None if not ar_deltas else _rounded(sum(ar_deltas) / len(ar_deltas), 4),
        "largest_ranking_changes": ranking_changes[:10],
        "per_symbol_deltas": per_symbol,
    }
    print(report)

def main_loop():
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log(f"⛔ Already running (PID {pid}). Exiting.")
            sys.exit(0)
        except (ProcessLookupError, ValueError, OSError):
            log("⚠️ Stale lockfile found. Taking over.")
    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))

    log("Strategy Lab starting...")
    try:
        _main_loop()
    finally:
        try:
            os.remove(LOCKFILE)
        except Exception:
            pass


def _main_loop():
    while True:
        try:
            run()
            # Update rosters after each iteration
            try:
                import subprocess
                subprocess.run([str(VENV_PYTHON), ROSTER_MANAGER_PATH])
            except Exception as re:
                log(f"Roster manager error: {re}")
            sleep_hours = LOOP_SLEEP_SECONDS / 3600
            log(f"Sleeping {sleep_hours:.1f} hours...")
            time.sleep(LOOP_SLEEP_SECONDS)
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(300)

def parse_args():
    parser = argparse.ArgumentParser(description="Run strategy lab or provider validation.")
    parser.add_argument("--validate-stock-window", action="store_true")
    parser.add_argument("--validate-fast", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.validate_stock_window and args.validate_fast:
        run_validation_fast()
    elif args.validate_stock_window:
        run_validation()
    else:
        main_loop()
