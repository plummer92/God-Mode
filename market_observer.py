#!/usr/bin/env python3
"""
market_observer.py — Passive daily signal outcome labeler.

Triggered by systemd timer at 4:30pm ET (21:30 UTC).
  1. Pulls today's signals from wolfe_signals.db
  2. Fetches 1-minute bars via yfinance
  3. Simulates 2% TP / 2% SL outcomes at 30m / 60m / 120m windows
  4. Writes results to market_intel.db → signal_outcomes
  5. Posts daily win-rate summary to Discord
  6. Exposes weekly_patterns() for 30-day trend analysis

Pass --backfill to label all historical unlabeled signals in one shot.
"""

import argparse
import os
import sqlite3
import logging
import logging.handlers
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import requests
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────────────────────
HOME         = Path("/home/theplummer92")
ENV_FILE     = HOME / ".env"
SIGNALS_DB   = HOME / "wolfe_signals.db"
INTEL_DB     = HOME / "market_intel.db"
LOG_FILE     = HOME / "market_observer.log"
WEIGHTS_FILE = HOME / "signal_weights.json"

# ── Env ───────────────────────────────────────────────────────────────────────
load_dotenv()
load_dotenv(ENV_FILE)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

# ── Constants ─────────────────────────────────────────────────────────────────
TAKE_PROFIT_PCT  = 0.02
STOP_LOSS_PCT    = 0.02
WINDOWS_MINUTES  = [30, 60, 120]
ET               = ZoneInfo("America/New_York")
UTC              = timezone.utc

# Signal type → direction mapping.
# None means ambiguous / not tradeable — these signals are skipped.
DIRECTION_MAP: dict[str, Optional[str]] = {
    "STRONG BUY FLOW":    "LONG",
    "STRONG SELL FLOW":   "SHORT",
    "ABSORPTION BUY":     "LONG",
    "ABSORPTION SELL":    "SHORT",
    "ABSORPTION WALL":    None,   # ambiguous reversal — skip
    "BULL TRAP":          "SHORT",
    "BEAR TRAP":          "LONG",
    "CLIMAX":             None,
    "FAKE-OUT (Low Vol)": None,
}

# ── Logging ───────────────────────────────────────────────────────────────────
_log = logging.getLogger("market_observer")
_log.setLevel(logging.INFO)
_fh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=7)
_fh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_log.addHandler(_fh)
_log.addHandler(logging.StreamHandler())


def log(msg: str) -> None:
    _log.info(msg)


# ── Direction inference ────────────────────────────────────────────────────────
def infer_direction(signal_type: str) -> Optional[str]:
    """Return LONG, SHORT, or None (skip) based on signal_type string."""
    s = signal_type.upper()
    for key, direction in DIRECTION_MAP.items():
        if key.upper() in s:
            return direction
    return None


# ── DB: schema setup ──────────────────────────────────────────────────────────
def ensure_outcomes_table() -> None:
    conn = sqlite3.connect(INTEL_DB)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                date          TEXT    NOT NULL,
                symbol        TEXT    NOT NULL,
                signal_type   TEXT,
                regime        TEXT,
                vix           REAL,
                direction     TEXT,
                signal_price  REAL,
                outcome_30m   TEXT,
                outcome_60m   TEXT,
                outcome_120m  TEXT,
                best_outcome  TEXT,
                labeled_at    TEXT    NOT NULL,
                UNIQUE(date, symbol, signal_type, signal_price)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_so_date   ON signal_outcomes(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_so_symbol ON signal_outcomes(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_so_regime ON signal_outcomes(regime)")
        conn.commit()
    finally:
        conn.close()


# ── DB: reads ─────────────────────────────────────────────────────────────────
def query_todays_signals(date_str: str) -> list[dict]:
    """
    Pull all signals for date_str (YYYY-MM-DD) from wolfe_signals.db.
    Left-joins macro_features to get the closest preceding regime/vix snapshot.
    """
    conn = sqlite3.connect(SIGNALS_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT
                s.timestamp,
                s.symbol,
                s.signal_type,
                s.price,
                s.confidence,
                mf.risk_regime  AS regime,
                mf.vix          AS vix
            FROM signals s
            LEFT JOIN macro_features mf
                ON mf.timestamp = (
                    SELECT m2.timestamp
                    FROM   macro_features m2
                    WHERE  m2.timestamp <= s.timestamp
                    ORDER  BY m2.timestamp DESC
                    LIMIT  1
                )
            WHERE s.timestamp LIKE ?
            ORDER BY s.timestamp ASC
        """, (f"{date_str}%",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── DB: writes ────────────────────────────────────────────────────────────────
def write_outcome(row: dict) -> None:
    conn = sqlite3.connect(INTEL_DB)
    try:
        conn.execute("""
            INSERT OR IGNORE INTO signal_outcomes
                (date, symbol, signal_type, regime, vix, direction,
                 signal_price, outcome_30m, outcome_60m, outcome_120m,
                 best_outcome, labeled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["date"], row["symbol"], row["signal_type"],
            row["regime"], row["vix"], row["direction"],
            row["signal_price"],
            row["outcome_30m"], row["outcome_60m"], row["outcome_120m"],
            row["best_outcome"], row["labeled_at"],
        ))
        conn.commit()
    finally:
        conn.close()


# ── Price fetching ────────────────────────────────────────────────────────────
_bar_cache:  dict[str, pd.DataFrame] = {}   # keyed f"{symbol}|{date_str}|{interval}"
_bulk_cache: dict[str, pd.DataFrame] = {}   # keyed f"{symbol}|{interval}" — full multi-day


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns and convert index to UTC."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df.sort_index()


def _is_unsupported(symbol: str) -> bool:
    """Futures (=), indices (^), and crypto (-USD / /USD) — skip simulation."""
    s = symbol.upper()
    return "=" in s or s.startswith("^") or s.endswith("-USD") or s.endswith("/USD")


def get_intraday_bars(symbol: str, date_str: str) -> pd.DataFrame:
    """
    Download 1-minute OHLC bars for the given symbol on date_str.
    Returns a UTC-indexed DataFrame.  Cached per symbol+date.
    Only available for the last ~7 calendar days from yfinance.
    """
    if _is_unsupported(symbol):
        return pd.DataFrame()

    cache_key = f"{symbol}|{date_str}|1m"
    if cache_key in _bar_cache:
        return _bar_cache[cache_key]

    ticker = symbol.replace("/", "-")
    try:
        target = pd.Timestamp(date_str)
        df = yf.download(
            ticker,
            start=target.strftime("%Y-%m-%d"),
            end=(target + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1m",
            progress=False,
            auto_adjust=True,
        )
        df = _normalise_df(df) if not df.empty else df
    except Exception as e:
        log(f"  yfinance 1m error [{symbol}]: {e}")
        df = pd.DataFrame()

    _bar_cache[cache_key] = df
    return df


def fetch_bulk_bars(symbol: str, start_date: str, interval: str) -> None:
    """
    Download all bars for symbol from start_date → today in one API call.
    Slices results into _bar_cache per date so get_bars_backfill() is O(1).
    Safe to call multiple times — skips if already fetched.
    """
    if _is_unsupported(symbol):
        return

    bulk_key = f"{symbol}|{interval}"
    if bulk_key in _bulk_cache:
        return  # already fetched

    ticker      = symbol.replace("/", "-")
    end_date    = (datetime.now(tz=UTC) + timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
        df = _normalise_df(df) if not df.empty else df
    except Exception as e:
        log(f"  bulk prefetch error [{symbol} {interval}]: {e}")
        df = pd.DataFrame()

    _bulk_cache[bulk_key] = df

    if df.empty:
        return

    # Pre-slice into per-date _bar_cache entries
    for ts in pd.date_range(start_date, end_date, freq="D"):
        d     = ts.strftime("%Y-%m-%d")
        slice_ = df[df.index.date == ts.date()].copy()
        _bar_cache[f"{symbol}|{d}|{interval}"] = slice_


def get_bars_backfill(symbol: str, date_str: str) -> tuple[pd.DataFrame, str]:
    """
    Backfill bar lookup: reads from pre-sliced _bar_cache.
    Prefers 1m (last 7 days) over 5m (last 60 days).
    Returns (DataFrame, interval_used).
    """
    for interval in ("1m", "5m"):
        key = f"{symbol}|{date_str}|{interval}"
        df  = _bar_cache.get(key, pd.DataFrame())
        if not df.empty:
            return df, interval
    return pd.DataFrame(), "none"


def get_bars_daily(symbol: str, date_str: str) -> tuple[pd.DataFrame, str]:
    """Daily-mode bar lookup: live 1m download, no fallback needed."""
    return get_intraday_bars(symbol, date_str), "1m"


# ── Simulation ────────────────────────────────────────────────────────────────
def simulate_outcome(
    signal_price: float,
    direction: str,
    bars: pd.DataFrame,
    signal_ts: datetime,
    window_minutes: int,
) -> str:
    """
    Walk 1-minute bars from signal_ts to signal_ts + window_minutes.

    LONG:  TP when High  >= signal_price * 1.02
           SL when Low   <= signal_price * 0.98
    SHORT: TP when Low   <= signal_price * 0.98
           SL when High  >= signal_price * 1.02

    Returns first hit: 'TP', 'SL', 'FLAT', or 'UNKNOWN'.
    """
    if bars.empty or signal_price <= 0:
        return "UNKNOWN"

    tp_price = signal_price * (1.0 + TAKE_PROFIT_PCT)
    sl_price = signal_price * (1.0 - STOP_LOSS_PCT)
    end_ts   = signal_ts + timedelta(minutes=window_minutes)

    # Ensure signal_ts is UTC-aware
    if signal_ts.tzinfo is None:
        signal_ts = signal_ts.replace(tzinfo=UTC)
    end_ts_aware = end_ts if end_ts.tzinfo else end_ts.replace(tzinfo=UTC)

    window = bars[(bars.index >= signal_ts) & (bars.index <= end_ts_aware)]
    if window.empty:
        return "UNKNOWN"

    for _, bar in window.iterrows():
        high = float(bar.get("High") or 0)
        low  = float(bar.get("Low")  or 0)
        if direction == "LONG":
            if high >= tp_price:
                return "TP"
            if low  <= sl_price:
                return "SL"
        else:  # SHORT
            if low  <= sl_price:
                return "TP"
            if high >= tp_price:
                return "SL"

    return "FLAT"


def best_of(outcomes: list[str]) -> str:
    """TP > FLAT > SL > UNKNOWN (best case across time windows)."""
    if "TP"      in outcomes: return "TP"
    if "FLAT"    in outcomes: return "FLAT"
    if "SL"      in outcomes: return "SL"
    return "UNKNOWN"


# ── Discord ────────────────────────────────────────────────────────────────────
def post_discord(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        log("Discord webhook not configured — skipping")
        return
    # Discord cap: 2000 chars per message
    for chunk in [message[i:i+1900] for i in range(0, len(message), 1900)]:
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
            r.raise_for_status()
        except Exception as e:
            log(f"Discord post failed: {e}")


# ── Daily summary ─────────────────────────────────────────────────────────────
def generate_daily_summary(date_str: str) -> str:
    conn = sqlite3.connect(INTEL_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM signal_outcomes WHERE date = ?", (date_str,)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return f"📊 **Market Observer — {date_str}**\nNo labeled signals for today."

    total     = len(rows)
    tp_count  = sum(1 for r in rows if r["best_outcome"] == "TP")
    sl_count  = sum(1 for r in rows if r["best_outcome"] == "SL")
    flat_count= sum(1 for r in rows if r["best_outcome"] == "FLAT")
    unk_count = sum(1 for r in rows if r["best_outcome"] == "UNKNOWN")
    decided   = tp_count + sl_count
    overall_wr = tp_count / decided * 100 if decided > 0 else 0.0

    # ── By signal_type ──────────────────────────────────────────────────────
    by_type: dict[str, dict] = {}
    for r in rows:
        k = (r["signal_type"] or "UNKNOWN").strip()
        d = by_type.setdefault(k, {"tp": 0, "sl": 0, "flat": 0, "unk": 0})
        o = r["best_outcome"]
        if   o == "TP":      d["tp"]   += 1
        elif o == "SL":      d["sl"]   += 1
        elif o == "FLAT":    d["flat"] += 1
        else:                d["unk"]  += 1

    # ── By regime ───────────────────────────────────────────────────────────
    by_regime: dict[str, dict] = {}
    for r in rows:
        k = (r["regime"] or "UNKNOWN").strip()
        d = by_regime.setdefault(k, {"tp": 0, "sl": 0})
        if   r["best_outcome"] == "TP": d["tp"] += 1
        elif r["best_outcome"] == "SL": d["sl"] += 1

    # ── By symbol (top 8 by signal count) ──────────────────────────────────
    by_symbol: dict[str, dict] = {}
    for r in rows:
        k = r["symbol"]
        if _is_unsupported(k):
            continue  # skip crypto/futures — no DB entries, 0% WR is misleading
        d = by_symbol.setdefault(k, {"tp": 0, "sl": 0, "total": 0})
        d["total"] += 1
        if   r["best_outcome"] == "TP": d["tp"] += 1
        elif r["best_outcome"] == "SL": d["sl"] += 1

    lines = [
        f"📊 **Market Observer — {date_str}**",
        f"Labeled: {total} | TP: {tp_count} | SL: {sl_count} | Flat: {flat_count} | Unknown: {unk_count}",
        f"Win rate (TP vs SL): **{overall_wr:.1f}%**",
        "",
        "**By Signal Type:**",
    ]
    for sig_type, c in sorted(by_type.items(), key=lambda x: -(x[1]["tp"] + x[1]["sl"])):
        n = c["tp"] + c["sl"]
        wr = c["tp"] / n * 100 if n > 0 else 0.0
        lines.append(f"  `{sig_type}`: {c['tp']}W / {c['sl']}L ({wr:.0f}% WR)")

    lines += ["", "**By Regime:**"]
    for regime, c in sorted(by_regime.items(), key=lambda x: -(x[1]["tp"] + x[1]["sl"])):
        n = c["tp"] + c["sl"]
        wr = c["tp"] / n * 100 if n > 0 else 0.0
        lines.append(f"  `{regime}`: {c['tp']}W / {c['sl']}L ({wr:.0f}% WR)")

    lines += ["", "**By Symbol (top 8):**"]
    top_syms = sorted(by_symbol.items(), key=lambda x: -x[1]["total"])[:8]
    for sym, c in top_syms:
        n = c["tp"] + c["sl"]
        wr = c["tp"] / n * 100 if n > 0 else 0.0
        lines.append(f"  {sym}: {c['tp']}W / {c['sl']}L ({wr:.0f}% WR, n={c['total']})")

    return "\n".join(lines)


# ── Weekly patterns ───────────────────────────────────────────────────────────
def weekly_patterns() -> str:
    """
    Query the last 30 days of signal_outcomes.
    Surface top 3 signal configs (signal_type × regime × direction)
    by win rate, requiring at least 10 decided outcomes.
    """
    conn = sqlite3.connect(INTEL_DB)
    conn.row_factory = sqlite3.Row
    try:
        cutoff = (datetime.now(tz=UTC) - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT
                signal_type,
                regime,
                direction,
                COUNT(*)                                                AS total,
                SUM(CASE WHEN best_outcome = 'TP' THEN 1 ELSE 0 END)  AS wins,
                SUM(CASE WHEN best_outcome = 'SL' THEN 1 ELSE 0 END)  AS losses
            FROM signal_outcomes
            WHERE date >= ?
              AND best_outcome IN ('TP', 'SL')
            GROUP BY signal_type, regime, direction
            HAVING total >= 10
            ORDER BY CAST(wins AS REAL) / (wins + losses) DESC
            LIMIT 3
        """, (cutoff,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return "🔬 **Weekly Patterns:** not enough data yet (need ≥10 outcomes per config)"

    lines = ["🔬 **Top Signal Configs — last 30 days (min 10 occurrences):**"]
    for i, r in enumerate(rows, 1):
        wr = r["wins"] / (r["wins"] + r["losses"]) * 100
        lines.append(
            f"{i}. `{r['signal_type']}` | {r['regime']} | {r['direction']}"
            f" — {r['wins']}W/{r['losses']}L ({wr:.1f}% WR, n={r['total']})"
        )
    return "\n".join(lines)


# ── Signal weights ────────────────────────────────────────────────────────────
WEIGHTS_MIN_OUTCOMES = 15
WEIGHTS_LOOKBACK_DAYS = 21


def compute_signal_weights() -> dict:
    """
    Query the last 21 days of signal_outcomes and return a per (signal_type|regime)
    weight dict.  Keys match the exact signal_type strings stored in the DB.

    Weight rules (requires >= WEIGHTS_MIN_OUTCOMES decided outcomes):
      win_rate >= 0.80  →  1.3   (strong boost)
      win_rate >= 0.65  →  1.0   (neutral)
      win_rate >= 0.50  →  0.8   (slight penalty)
      win_rate <  0.50  →  0.6   (significant penalty)
      fewer than 15     →  1.0   (not enough data)
    """
    cutoff = (datetime.now(tz=UTC) - timedelta(days=WEIGHTS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(INTEL_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT
                signal_type,
                regime,
                SUM(CASE WHEN best_outcome = 'TP' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN best_outcome = 'SL' THEN 1 ELSE 0 END) AS losses
            FROM signal_outcomes
            WHERE date >= ?
              AND best_outcome IN ('TP', 'SL')
            GROUP BY signal_type, regime
        """, (cutoff,)).fetchall()
    finally:
        conn.close()

    weights: dict = {}
    for r in rows:
        sig_type = (r["signal_type"] or "UNKNOWN").strip()
        regime   = (r["regime"]      or "UNKNOWN").strip()
        wins     = int(r["wins"])
        losses   = int(r["losses"])
        n        = wins + losses
        key      = f"{sig_type}|{regime}"

        if n < WEIGHTS_MIN_OUTCOMES:
            weight   = 1.0
            win_rate = wins / n if n > 0 else 0.0
        else:
            win_rate = wins / n
            if   win_rate >= 0.80: weight = 1.3
            elif win_rate >= 0.65: weight = 1.0
            elif win_rate >= 0.50: weight = 0.8
            else:                  weight = 0.6

        weights[key] = {
            "win_rate": round(win_rate, 4),
            "n":        n,
            "weight":   weight,
        }

    return weights


def write_signal_weights(weights: dict) -> None:
    """Persist the weights dict to WEIGHTS_FILE (signal_weights.json)."""
    payload = {
        "generated_at":   datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "lookback_days":  WEIGHTS_LOOKBACK_DAYS,
        "min_outcomes":   WEIGHTS_MIN_OUTCOMES,
        "weights":        weights,
    }
    tmp = str(WEIGHTS_FILE) + ".tmp"
    with open(tmp, "w") as f:
        import json as _json
        _json.dump(payload, f, indent=2)
    import os as _os
    _os.replace(tmp, WEIGHTS_FILE)
    log(f"signal_weights.json written — {len(weights)} entries")


def print_weights_table() -> None:
    """Pretty-print the current weights to stdout (used by --weights flag)."""
    import json as _json
    try:
        with open(WEIGHTS_FILE) as f:
            data = _json.load(f)
        weights  = data.get("weights", {})
        gen_at   = data.get("generated_at", "unknown")
        lookback = data.get("lookback_days", "?")
        print(f"\n📊 Signal Weights  (generated {gen_at}, last {lookback}d)\n")
        print(f"  {'KEY':<55} {'WR':>6}  {'N':>5}  WEIGHT")
        print(f"  {'-'*55}  {'------'}  {'-----'}  ------")
        rows = sorted(weights.items(), key=lambda x: -x[1]["weight"])
        for key, v in rows:
            wr  = v["win_rate"]
            n   = v["n"]
            w   = v["weight"]
            tag = "🔼" if w > 1.0 else ("🔽" if w < 1.0 else "  ")
            print(f"  {key:<55}  {wr:>5.0%}  {n:>5}  {tag} {w:.1f}x")
        print()
    except FileNotFoundError:
        print(f"⚠️  {WEIGHTS_FILE} not found — run market_observer.py first to generate it.")
    except Exception as e:
        print(f"Error reading weights: {e}")


# ── Backfill helpers ──────────────────────────────────────────────────────────
def get_all_signal_dates() -> list[str]:
    """All distinct dates in wolfe_signals.db, oldest first."""
    conn = sqlite3.connect(SIGNALS_DB)
    try:
        rows = conn.execute("""
            SELECT DISTINCT substr(timestamp, 1, 10) AS date
            FROM signals ORDER BY date ASC
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_all_signal_symbols() -> list[str]:
    """All distinct tradeable symbols (no futures/indices) in wolfe_signals.db."""
    conn = sqlite3.connect(SIGNALS_DB)
    try:
        rows = conn.execute("SELECT DISTINCT symbol FROM signals").fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows if not _is_unsupported(r[0])]


# ── Shared per-date processing core ───────────────────────────────────────────
def process_date(
    date_str: str,
    now_utc:  datetime,
    bars_fn:  Callable[[str, str], tuple[pd.DataFrame, str]],
    verbose:  bool = True,
) -> tuple[int, int, int]:
    """
    Label all unlabeled signals for date_str.

    bars_fn(symbol, date_str) → (DataFrame, interval_str)
    Returns (labeled, skipped, errors).
    """
    signals = query_todays_signals(date_str)

    # Deduplicate: keep the earliest occurrence of each (symbol, signal_type, price).
    # INSERT OR IGNORE handles DB-level dedup; this avoids redundant simulations.
    seen:    set[tuple]  = set()
    unique:  list[dict]  = []
    for sig in signals:
        key = (sig["symbol"], sig["signal_type"], sig["price"])
        if key not in seen:
            seen.add(key)
            unique.append(sig)

    labeled = skipped = errors = 0

    for sig in unique:
        symbol       = str(sig.get("symbol") or "").strip().upper()
        signal_type  = str(sig.get("signal_type") or "").strip()
        signal_price = float(sig.get("price") or 0)
        raw_ts       = str(sig.get("timestamp") or "")
        regime       = str(sig.get("regime") or "UNKNOWN").strip()
        vix          = sig.get("vix")

        direction = infer_direction(signal_type)
        if direction is None or signal_price <= 0 or not symbol:
            skipped += 1
            continue

        try:
            signal_ts = datetime.fromisoformat(raw_ts)
            if signal_ts.tzinfo is None:
                signal_ts = signal_ts.replace(tzinfo=UTC)
        except Exception:
            log(f"  bad timestamp [{symbol}]: {raw_ts!r}")
            errors += 1
            continue

        if signal_ts.astimezone(ET).strftime("%Y-%m-%d") != date_str:
            skipped += 1
            continue

        bars, interval = bars_fn(symbol, date_str)
        outcomes = [
            simulate_outcome(signal_price, direction, bars, signal_ts, w)
            for w in WINDOWS_MINUTES
        ]
        best = best_of(outcomes)

        write_outcome({
            "date":         date_str,
            "symbol":       symbol,
            "signal_type":  signal_type,
            "regime":       regime,
            "vix":          vix,
            "direction":    direction,
            "signal_price": signal_price,
            "outcome_30m":  outcomes[0],
            "outcome_60m":  outcomes[1],
            "outcome_120m": outcomes[2],
            "best_outcome": best,
            "labeled_at":   now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        })
        labeled += 1
        if verbose:
            log(
                f"  {symbol:<6} {direction:<5} {signal_type:<30} [{interval}]"
                f" 30m:{outcomes[0]:<7} 60m:{outcomes[1]:<7} 120m:{outcomes[2]:<7} best:{best}"
            )

    return labeled, skipped, errors


# ── Backfill ──────────────────────────────────────────────────────────────────
def run_backfill() -> None:
    now_utc = datetime.now(tz=UTC)
    log("=== Market Observer BACKFILL starting ===")
    ensure_outcomes_table()

    dates   = get_all_signal_dates()
    symbols = get_all_signal_symbols()
    log(f"Signal dates: {len(dates)}  ({dates[0]} → {dates[-1]})")
    log(f"Unique tradeable symbols: {len(symbols)}")

    # ── Bulk prefetch 5m bars (≤60 day history) — one API call per symbol ──
    log("Prefetching 5m bars (covers all dates)…")
    for sym in symbols:
        fetch_bulk_bars(sym, dates[0], "5m")

    # ── Bulk prefetch 1m bars (≤7 day history) — higher resolution for recent dates ──
    recent_start = (datetime.now(tz=UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
    log(f"Prefetching 1m bars (last 7 days, from {recent_start})…")
    for sym in symbols:
        fetch_bulk_bars(sym, recent_start, "1m")

    log("Prefetch complete — processing dates…")

    total_labeled = total_skipped = total_errors = 0

    for date_str in dates:
        labeled, skipped, errors = process_date(
            date_str, now_utc, get_bars_backfill, verbose=False
        )
        total_labeled += labeled
        total_skipped += skipped
        total_errors  += errors
        log(
            f"  {date_str}  labeled={labeled:<4} skipped={skipped:<4} errors={errors}"
        )

    log(
        f"BACKFILL complete — "
        f"total labeled:{total_labeled}  skipped:{total_skipped}  errors:{total_errors}"
    )

    # Final outcome distribution
    conn = sqlite3.connect(INTEL_DB)
    try:
        rows = conn.execute("""
            SELECT best_outcome, COUNT(*) AS n
            FROM signal_outcomes
            GROUP BY best_outcome ORDER BY n DESC
        """).fetchall()
    finally:
        conn.close()
    log("signal_outcomes distribution:")
    for r in rows:
        log(f"  {r[0]}: {r[1]}")

    weights = compute_signal_weights()
    write_signal_weights(weights)

    log("=== Market Observer BACKFILL done ===")


# ── Main (daily timer mode) ───────────────────────────────────────────────────
def main() -> None:
    now_utc  = datetime.now(tz=UTC)
    now_ny   = now_utc.astimezone(ET)
    date_str = now_ny.strftime("%Y-%m-%d")

    log(f"=== Market Observer starting | date={date_str} | utc={now_utc.strftime('%H:%M:%S')} ===")
    ensure_outcomes_table()

    log(f"Signals found for {date_str}: {len(query_todays_signals(date_str))}")
    labeled, skipped, errors = process_date(date_str, now_utc, get_bars_daily, verbose=True)
    log(f"Done — labeled:{labeled}  skipped:{skipped}  errors:{errors}")

    summary  = generate_daily_summary(date_str)
    patterns = weekly_patterns()
    post_discord(f"{summary}\n\n{patterns}")
    log("Discord summary posted")

    weights = compute_signal_weights()
    write_signal_weights(weights)

    log("=== Market Observer complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Observer ��� signal outcome labeler")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Label all unlabeled historical signals (runs once, no Discord post)",
    )
    parser.add_argument(
        "--weights",
        action="store_true",
        help="Print current signal weights table from signal_weights.json and exit",
    )
    args = parser.parse_args()

    if args.weights:
        print_weights_table()
    elif args.backfill:
        run_backfill()
    else:
        main()
