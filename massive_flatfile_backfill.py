#!/usr/bin/env python3
"""Backfill observations from Massive/Polygon flat-file minute aggregates.

This is intentionally a batch utility. Flat-file daily stock aggregates are large
files, so the live signal labeler should not download them on demand.
"""

from __future__ import annotations

import argparse
import gzip
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from app_paths import DATA_DIR, ENV_FILE, REPO_DIR
from market_data_sources import is_stock_symbol

try:
    import boto3
    from botocore.config import Config
except Exception:
    boto3 = None
    Config = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


DB_PATH = DATA_DIR / "wolfe_signals.db"


def cache_dir() -> Path:
    return Path(os.getenv("MASSIVE_FLATFILES_CACHE_DIR", DATA_DIR / "massive_flatfiles")).expanduser()


def endpoint_url() -> str:
    return os.getenv("MASSIVE_S3_ENDPOINT_URL", "https://files.massive.com")


def bucket_name() -> str:
    return os.getenv("MASSIVE_S3_BUCKET", "flatfiles")


def dataset_prefix() -> str:
    return os.getenv("MASSIVE_STOCK_MINUTE_AGGS_PREFIX", "us_stocks_sip/minute_aggs_v1").strip("/")


def load_env_files() -> None:
    env_files = [ENV_FILE, REPO_DIR / ".env"]
    if load_dotenv is not None:
        for env_file in env_files:
            if env_file.exists():
                load_dotenv(env_file, override=False)
        return
    for env_file in env_files:
        if not env_file.exists():
            continue
        for line in env_file.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#") or "=" not in text:
                continue
            key, value = text.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def flatfile_key(day: date) -> str:
    return f"{dataset_prefix()}/{day:%Y/%m/%Y-%m-%d}.csv.gz"


def local_path_for_key(key: str) -> Path:
    return cache_dir() / key


def s3_client():
    if boto3 is None or Config is None:
        raise RuntimeError("boto3 is required for Massive flat files; install it in the venv first")
    access_key = env_first("MASSIVE_S3_ACCESS_KEY_ID", "MASSIVE_FLATFILES_ACCESS_KEY_ID")
    secret_key = env_first("MASSIVE_S3_SECRET_ACCESS_KEY", "MASSIVE_FLATFILES_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Missing Massive S3 credentials. Set MASSIVE_S3_ACCESS_KEY_ID and "
            "MASSIVE_S3_SECRET_ACCESS_KEY in .env."
        )
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return session.client(
        "s3",
        endpoint_url=endpoint_url(),
        config=Config(signature_version="s3v4"),
    )


def download_day(client, day: date, force: bool = False) -> Path | None:
    key = flatfile_key(day)
    local_path = local_path_for_key(key)
    if local_path.exists() and not force:
        return local_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(bucket_name(), key, str(local_path))
    except Exception as exc:
        print(f"missing_or_unavailable {key}: {exc}")
        return None
    return local_path


def symbols_from_arg(raw: str) -> set[str]:
    return {
        item.strip().upper()
        for item in raw.split(",")
        if item.strip() and is_stock_symbol(item.strip())
    }


def symbols_from_signals(start: date, end: date) -> set[str]:
    start_text = start.strftime("%Y-%m-%d 00:00:00")
    end_text = (end + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM signals
            WHERE timestamp >= ?
              AND timestamp < ?
            """,
            (start_text, end_text),
        ).fetchall()
    return {str(row[0]).strip().upper() for row in rows if row and is_stock_symbol(str(row[0]))}


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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_symbol_ts ON observations(symbol, timestamp_utc)")


def read_filtered_rows(path: Path, symbols: set[str], chunksize: int) -> Iterable[tuple]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for chunk in pd.read_csv(handle, chunksize=chunksize):
            if chunk.empty or "ticker" not in chunk.columns:
                continue
            filtered = chunk[chunk["ticker"].isin(symbols)]
            if filtered.empty:
                continue
            for row in filtered.itertuples(index=False):
                window_start = getattr(row, "window_start", None)
                if window_start is None:
                    continue
                ts = datetime.fromtimestamp(int(window_start) / 1_000_000_000, tz=timezone.utc)
                yield (
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    str(getattr(row, "ticker")).upper(),
                    float(getattr(row, "close")),
                    float(getattr(row, "open")),
                    float(getattr(row, "volume", 0.0)),
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    str(getattr(row, "ticker")).upper(),
                )


def insert_observations(rows: Iterable[tuple], batch_size: int) -> int:
    conn = sqlite3.connect(DB_PATH, timeout=45)
    conn.execute("PRAGMA busy_timeout=45000")
    ensure_observations(conn)
    total = 0
    batch: list[tuple] = []

    def flush() -> None:
        nonlocal total, batch
        if not batch:
            return
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO observations (timestamp_utc, symbol, price, open_price, volume)
            SELECT ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM observations
                WHERE timestamp_utc = ?
                  AND symbol = ?
            )
            """,
            batch,
        )
        conn.commit()
        total += conn.total_changes - before
        batch.clear()

    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            flush()
    flush()
    conn.close()
    return total


def main() -> int:
    load_env_files()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, type=parse_date, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=parse_date, help="End date, YYYY-MM-DD")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols; defaults to symbols in signals table")
    parser.add_argument("--force-download", action="store_true", help="Redownload cached files")
    parser.add_argument("--download-only", action="store_true", help="Download files without importing observations")
    parser.add_argument("--chunksize", type=int, default=250_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()

    symbols = symbols_from_arg(args.symbols) if args.symbols else symbols_from_signals(args.start, args.end)
    if not symbols:
        raise SystemExit("No stock symbols found to import")
    print(f"symbols={len(symbols)} dates={args.start}..{args.end}")

    client = s3_client()
    saved = 0
    for day in date_range(args.start, args.end):
        path = download_day(client, day, force=args.force_download)
        if path is None:
            continue
        print(f"file={path}")
        if args.download_only:
            continue
        saved += insert_observations(read_filtered_rows(path, symbols, args.chunksize), args.batch_size)
        print(f"observations_imported_so_far={saved}")
    print(f"observations_imported={saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
