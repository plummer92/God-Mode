#!/usr/bin/env python3
"""Backfill earnings context onto existing signal rows."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone

from app_paths import DATA_DIR
from earnings_context import get_earnings_context


DB_PATH = DATA_DIR / "wolfe_signals.db"
DEFAULT_SINCE = "2026-05-24 14:52:00"


def parse_ts(value: str) -> datetime:
    text = str(value).strip().replace("T", " ")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def ensure_columns(conn: sqlite3.Connection) -> None:
    for column_name, column_type in (
        ("next_earnings_date", "TEXT"),
        ("days_to_earnings", "INTEGER"),
        ("earnings_window", "TEXT DEFAULT 'UNKNOWN'"),
        ("earnings_source", "TEXT"),
    ):
        try:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def enrich_signals(since: str, batch_size: int, force: bool = False) -> int:
    conn = sqlite3.connect(DB_PATH, timeout=45)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=45000")
    ensure_columns(conn)
    earnings_filter = "" if force else "AND COALESCE(earnings_window, 'UNKNOWN') = 'UNKNOWN'"
    rows = conn.execute(
        f"""
        SELECT rowid, timestamp, symbol
        FROM signals
        WHERE timestamp >= ?
          {earnings_filter}
        ORDER BY timestamp ASC
        LIMIT ?
        """,
        (since, batch_size),
    ).fetchall()
    saved = 0
    for row in rows:
        ctx = get_earnings_context(row["symbol"], as_of=parse_ts(row["timestamp"]))
        conn.execute(
            """
            UPDATE signals
            SET next_earnings_date = ?,
                days_to_earnings = ?,
                earnings_window = ?,
                earnings_source = ?
            WHERE rowid = ?
            """,
            (
                ctx.get("next_earnings_date"),
                ctx.get("days_to_earnings"),
                ctx.get("earnings_window", "UNKNOWN"),
                ctx.get("earnings_source"),
                int(row["rowid"]),
            ),
        )
        saved += 1
    conn.commit()
    conn.close()
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--force", action="store_true", help="Refresh rows even if earnings_window is already set")
    args = parser.parse_args()
    saved = enrich_signals(args.since, max(1, args.batch_size), force=args.force)
    print(f"signals_enriched={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
