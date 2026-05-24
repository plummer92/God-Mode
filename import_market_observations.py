#!/usr/bin/env python3
"""Backfill the observations table from market_log.csv."""

from __future__ import annotations

import argparse
import csv
import itertools
import sqlite3
from pathlib import Path

from app_paths import DATA_DIR


DB_PATH = DATA_DIR / "wolfe_signals.db"
DEFAULT_MARKET_LOG = DATA_DIR / "market_log.csv"
MARKET_HEADER = [
    "Timestamp",
    "Sector",
    "Ticker",
    "Price",
    "Change_Pct",
    "RVOL",
    "Money_Flow_M",
    "Signal",
]


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def ensure_observations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
            timestamp_utc TEXT NOT NULL,
            symbol TEXT NOT NULL,
            sector TEXT,
            price REAL,
            open_price REAL,
            volume REAL,
            avg_vol REAL,
            rvol REAL,
            change_pct REAL,
            flow_m REAL,
            signal_type TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_symbol_ts "
        "ON observations(symbol, timestamp_utc)"
    )


def import_rows(path: Path, batch_size: int = 1000) -> int:
    if not path.exists():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(DB_PATH, timeout=45)
    conn.execute("PRAGMA busy_timeout=45000")
    ensure_observations(conn)
    total = 0
    batch = []

    def flush_batch() -> None:
        nonlocal total, batch
        if not batch:
            return
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO observations (
                timestamp_utc, symbol, price, rvol, flow_m, change_pct, signal_type, sector
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM observations
                WHERE timestamp_utc = ?
                  AND symbol = ?
            )
            """,
            batch,
        )
        total += conn.total_changes - before
        conn.commit()
        batch.clear()

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        first_row = next(reader, None)
        if first_row is None:
            conn.close()
            return total
        has_header = first_row == MARKET_HEADER
        rows = reader if has_header else itertools.chain([first_row], reader)
        for raw_row in rows:
            if len(raw_row) < len(MARKET_HEADER):
                continue
            row = dict(zip(MARKET_HEADER, raw_row))
            timestamp = (row.get("Timestamp") or "").strip()
            symbol = (row.get("Ticker") or "").strip().upper()
            if not timestamp or not symbol:
                continue
            batch.append(
                (
                    timestamp,
                    symbol,
                    to_float(row.get("Price")),
                    to_float(row.get("RVOL")),
                    to_float(row.get("Money_Flow_M")),
                    to_float(row.get("Change_Pct")),
                    row.get("Signal"),
                    row.get("Sector"),
                    timestamp,
                    symbol,
                )
            )
            if len(batch) >= batch_size:
                flush_batch()
    flush_batch()
    conn.close()
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=DEFAULT_MARKET_LOG)
    args = parser.parse_args()
    saved = import_rows(args.path)
    print(f"observations_imported={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
