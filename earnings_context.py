"""Earnings calendar enrichment for signal research."""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import yfinance as yf

from app_paths import ENV_FILE, REPO_DIR

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


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


load_env_files()


EARNINGS_CONTEXT_ENABLED = os.getenv("EARNINGS_CONTEXT_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
EARNINGS_CACHE_TTL_SECONDS = int(os.getenv("EARNINGS_CACHE_TTL_SECONDS", "21600"))
EARNINGS_NEAR_DAYS = int(os.getenv("EARNINGS_NEAR_DAYS", "7"))
NO_EARNINGS_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv(
        "NO_EARNINGS_SYMBOLS",
        "SPY,QQQ,IWM,DIA,XLK,XLF,XLE,XLV,XLI,XLY,XLP,XLU,XLC,XLRE,SOXS,SOXL,TQQQ,SQQQ",
    ).split(",")
    if item.strip()
}

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def is_stock_symbol(symbol: str) -> bool:
    text = str(symbol).strip().upper()
    return bool(text) and "/" not in text and "-" not in text and "=" not in text and not text.startswith("^")


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return None
        for candidate in value.index:
            parsed = _coerce_date(candidate)
            if parsed is not None:
                return parsed
        return None
    if isinstance(value, (list, tuple, set)):
        dates = [parsed for item in value if (parsed := _coerce_date(item)) is not None]
        return min(dates) if dates else None
    if isinstance(value, dict):
        for key in ("Earnings Date", "earningsDate", "nextEarningsDate"):
            parsed = _coerce_date(value.get(key))
            if parsed is not None:
                return parsed
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.date()
        return value.tz_convert("UTC").date()
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None
    try:
        return pd.to_datetime(text, utc=True).date()
    except Exception:
        return None


def _fetch_finnhub_next_earnings_date(symbol: str, as_of: date) -> date | None:
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        return None
    end = as_of + timedelta(days=370)
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/calendar/earnings",
            params={
                "from": as_of.isoformat(),
                "to": end.isoformat(),
                "symbol": symbol,
                "token": api_key,
            },
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json().get("earningsCalendar") or []
    except Exception:
        return None
    dates = [_coerce_date(row.get("date")) for row in rows if isinstance(row, dict)]
    dates = [item for item in dates if item is not None and item >= as_of]
    return min(dates) if dates else None


def _fetch_yfinance_next_earnings_date(symbol: str) -> date | None:
    ticker = yf.Ticker(str(symbol).strip().upper())
    try:
        calendar = ticker.calendar
    except Exception:
        calendar = None
    parsed = _coerce_date(calendar)
    if parsed is not None:
        return parsed
    try:
        dates = ticker.get_earnings_dates(limit=4)
    except Exception:
        dates = None
    return _coerce_date(dates)


def get_earnings_context(symbol: str, as_of: datetime | None = None) -> dict[str, Any]:
    symbol = str(symbol).strip().upper()
    if symbol in NO_EARNINGS_SYMBOLS:
        return {
            "next_earnings_date": None,
            "days_to_earnings": None,
            "earnings_window": "NO_EARNINGS",
            "earnings_source": "symbol_exclusion",
        }
    if not EARNINGS_CONTEXT_ENABLED or not is_stock_symbol(symbol):
        return {
            "next_earnings_date": None,
            "days_to_earnings": None,
            "earnings_window": "UNKNOWN",
            "earnings_source": "disabled_or_non_stock",
        }

    now_epoch = time.time()
    cached = _cache.get(symbol)
    if cached and now_epoch - cached[0] < EARNINGS_CACHE_TTL_SECONDS:
        return dict(cached[1])

    as_of_date = (as_of or datetime.now(timezone.utc)).date()
    source = "yfinance"
    next_date = _fetch_finnhub_next_earnings_date(symbol, as_of_date)
    if next_date is not None:
        source = "finnhub"
    else:
        try:
            next_date = _fetch_yfinance_next_earnings_date(symbol)
        except Exception:
            next_date = None

    days_to = (next_date - as_of_date).days if next_date is not None else None
    if days_to is None:
        window = "UNKNOWN"
    elif days_to < 0:
        window = "POST_EARNINGS"
    elif days_to == 0:
        window = "EARNINGS_TODAY"
    elif days_to <= EARNINGS_NEAR_DAYS:
        window = "PRE_EARNINGS"
    else:
        window = "CLEAR"

    result = {
        "next_earnings_date": next_date.isoformat() if next_date is not None else None,
        "days_to_earnings": days_to,
        "earnings_window": window,
        "earnings_source": source,
    }
    _cache[symbol] = (now_epoch, result)
    return dict(result)
