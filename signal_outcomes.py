#!/usr/bin/env python3
"""Label signal forward returns at fixed horizons."""

from __future__ import annotations

import argparse
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd
import yfinance as yf

from app_paths import DATA_DIR


DB_PATH = str(DATA_DIR / "wolfe_signals.db")
LOG_PATH = str(DATA_DIR / "signal_outcomes.log")
LOCKFILE = os.getenv(
    "SIGNAL_OUTCOME_LOCKFILE",
    os.path.join(tempfile.gettempdir(), "signal_outcomes.lock"),
)

HORIZONS = (
    ("5m", timedelta(minutes=5), timedelta(minutes=12)),
    ("15m", timedelta(minutes=15), timedelta(minutes=15)),
    ("30m", timedelta(minutes=30), timedelta(minutes=20)),
    ("1h", timedelta(hours=1), timedelta(minutes=30)),
    ("1d", timedelta(days=1), timedelta(hours=3)),
)

DEFAULT_BATCH_SIZE = int(os.getenv("SIGNAL_OUTCOME_BATCH_SIZE", "500"))
DEFAULT_SLEEP_SECONDS = int(os.getenv("SIGNAL_OUTCOME_SLEEP_SECONDS", "300"))
FLAT_BAND_PCT = float(os.getenv("SIGNAL_OUTCOME_FLAT_BAND_PCT", "0.05"))


def log(message: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] {message}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:
        pass


def acquire_lock() -> None:
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE, "r", encoding="utf-8") as handle:
                old_pid = int((handle.read() or "0").strip())
            os.kill(old_pid, 0)
            raise SystemExit(f"signal_outcomes already running with pid {old_pid}")
        except ProcessLookupError:
            pass
        except ValueError:
            pass
    with open(LOCKFILE, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))


def release_lock() -> None:
    try:
        os.remove(LOCKFILE)
    except FileNotFoundError:
        pass


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_rowid INTEGER NOT NULL,
                horizon TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_ts TEXT NOT NULL,
                target_ts TEXT NOT NULL,
                signal_type TEXT,
                direction TEXT,
                signal_price REAL,
                target_price REAL,
                return_pct REAL,
                outcome TEXT,
                reviewed_at TEXT NOT NULL,
                source TEXT,
                UNIQUE(signal_rowid, horizon)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_outcomes_horizon "
            "ON signal_outcomes(horizon, signal_ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_outcomes_symbol "
            "ON signal_outcomes(symbol, horizon, signal_ts)"
        )


def parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def direction_for(signal_type: str | None, flow_m: float | None) -> str:
    label = str(signal_type or "").upper()
    if "BUY" in label:
        return "LONG"
    if "SELL" in label:
        return "SHORT"
    try:
        return "SHORT" if float(flow_m or 0.0) < 0 else "LONG"
    except Exception:
        return "LONG"


def yfinance_symbol(symbol: str) -> str:
    return str(symbol).strip().upper().replace("/", "-")


def load_due_signals(horizon: str, delta: timedelta, batch_size: int) -> list[sqlite3.Row]:
    cutoff = datetime.now(timezone.utc) - delta
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            """
            SELECT rowid AS signal_rowid, timestamp, symbol, signal_type, price, flow_m
            FROM signals
            WHERE timestamp <= ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM signal_outcomes o
                  WHERE o.signal_rowid = signals.rowid
                    AND o.horizon = ?
              )
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (cutoff.strftime("%Y-%m-%d %H:%M:%S"), horizon, batch_size),
        ).fetchall()


def fetch_prices(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    try:
        frame = yf.download(
            yfinance_symbol(symbol),
            start=start - timedelta(days=1),
            end=end + timedelta(days=1),
            interval="5m",
            progress=False,
            auto_adjust=False,
            prepost=True,
            threads=False,
        )
    except Exception:
        return pd.DataFrame()
    if frame.empty:
        return pd.DataFrame()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    if "Close" not in frame.columns:
        return pd.DataFrame()
    if frame.index.tz is None:
        frame.index = frame.index.tz_localize("UTC")
    else:
        frame.index = frame.index.tz_convert("UTC")
    return frame.sort_index()


def price_near(frame: pd.DataFrame, target_ts: datetime, tolerance: timedelta) -> float | None:
    if frame.empty:
        return None
    window = frame.loc[
        (frame.index >= target_ts - tolerance)
        & (frame.index <= target_ts + tolerance)
    ]
    if window.empty:
        return None
    closest_idx = min(window.index, key=lambda ts: abs((ts - target_ts).total_seconds()))
    value = window.loc[closest_idx, "Close"]
    if isinstance(value, pd.Series):
        value = value.iloc[-1]
    try:
        return float(value)
    except Exception:
        return None


def classify_return(return_pct: float | None) -> str:
    if return_pct is None:
        return "NO_DATA"
    if abs(return_pct) < FLAT_BAND_PCT:
        return "FLAT"
    return "WIN" if return_pct > 0 else "LOSS"


def build_outcome_rows(
    rows: Iterable[sqlite3.Row],
    horizon: str,
    delta: timedelta,
    tolerance: timedelta,
) -> list[tuple]:
    rows = list(rows)
    if not rows:
        return []

    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    outcome_rows: list[tuple] = []
    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]).upper(), []).append(row)

    for symbol, symbol_rows in by_symbol.items():
        parsed = [(row, parse_ts(row["timestamp"])) for row in symbol_rows]
        parsed = [(row, ts) for row, ts in parsed if ts is not None]
        if not parsed:
            continue
        start = min(ts for _, ts in parsed)
        end = max(ts + delta for _, ts in parsed)
        prices = fetch_prices(symbol, start, end)

        for row, signal_dt in parsed:
            target_dt = signal_dt + delta
            signal_price = None if row["price"] is None else float(row["price"])
            target_price = price_near(prices, target_dt, tolerance)
            direction = direction_for(row["signal_type"], row["flow_m"])
            return_pct = None
            if signal_price and signal_price > 0 and target_price is not None:
                if direction == "SHORT":
                    return_pct = ((signal_price - target_price) / signal_price) * 100.0
                else:
                    return_pct = ((target_price - signal_price) / signal_price) * 100.0

            outcome_rows.append(
                (
                    int(row["signal_rowid"]),
                    horizon,
                    str(row["symbol"]).upper(),
                    str(row["timestamp"]),
                    target_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    row["signal_type"],
                    direction,
                    signal_price,
                    target_price,
                    return_pct,
                    classify_return(return_pct),
                    now_text,
                    "yfinance_5m",
                )
            )
    return outcome_rows


def save_outcomes(rows: list[tuple]) -> int:
    if not rows:
        return 0
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO signal_outcomes (
                signal_rowid, horizon, symbol, signal_ts, target_ts, signal_type,
                direction, signal_price, target_price, return_pct, outcome,
                reviewed_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return conn.total_changes


def run_once(batch_size: int) -> int:
    init_db()
    total = 0
    for horizon, delta, tolerance in HORIZONS:
        due = load_due_signals(horizon, delta, batch_size)
        outcome_rows = build_outcome_rows(due, horizon, delta, tolerance)
        saved = save_outcomes(outcome_rows)
        total += saved
        log(f"{horizon}: due={len(due)} saved={saved}")
    return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one backfill pass and exit")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--sleep-seconds", type=int, default=DEFAULT_SLEEP_SECONDS)
    args = parser.parse_args()

    acquire_lock()
    try:
        while True:
            saved = run_once(max(1, args.batch_size))
            if args.once:
                break
            time.sleep(5 if saved else max(30, args.sleep_seconds))
    finally:
        release_lock()


if __name__ == "__main__":
    main()
