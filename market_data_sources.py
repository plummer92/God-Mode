"""Market data provider helpers for research labeling.

The collector already writes live observations. This module is for backfill and
outcome labeling when an observation is missing near a target timestamp.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
import requests
import yfinance as yf


STOCK_DATA_FEED = os.getenv("STOCK_DATA_FEED", "iex")
DEFAULT_PRICE_PROVIDERS = os.getenv(
    "SIGNAL_PRICE_PROVIDERS",
    "observations,alpaca,polygon,yfinance",
)


def provider_order(raw: str | None = None) -> list[str]:
    text = raw if raw is not None else DEFAULT_PRICE_PROVIDERS
    providers = []
    for item in str(text or "").split(","):
        provider = item.strip().lower()
        if provider and provider not in providers:
            providers.append(provider)
    return providers or ["observations", "alpaca", "polygon", "yfinance"]


def is_stock_symbol(symbol: str) -> bool:
    text = str(symbol).strip().upper()
    return bool(text) and "/" not in text and "-" not in text and "=" not in text and not text.startswith("^")


def yfinance_symbol(symbol: str) -> str:
    return str(symbol).strip().upper().replace("/", "-")


def normalize_bars(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    out = frame.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.get_level_values(0)
    out = out.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
            "v": "Volume",
            "o": "Open",
            "h": "High",
            "l": "Low",
            "c": "Close",
        }
    )
    if "Close" not in out.columns:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    keep = [col for col in ["Open", "High", "Low", "Close", "Volume"] if col in out.columns]
    return out[keep].sort_index()


def fetch_alpaca_bars(symbol: str, start: datetime, end: datetime, minutes: int = 5) -> pd.DataFrame:
    if not is_stock_symbol(symbol):
        return pd.DataFrame()
    try:
        from alpaca_data import get_stock_minute_bars

        frames = get_stock_minute_bars(
            [str(symbol).strip().upper()],
            start=start - timedelta(days=1),
            end=end + timedelta(days=1),
            minutes=minutes,
            feed=STOCK_DATA_FEED,
        )
        return normalize_bars(frames.get(str(symbol).strip().upper()))
    except Exception:
        return pd.DataFrame()


def fetch_polygon_bars(symbol: str, start: datetime, end: datetime, minutes: int = 5) -> pd.DataFrame:
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key or not is_stock_symbol(symbol):
        return pd.DataFrame()
    ticker = str(symbol).strip().upper()
    start_date = (start - timedelta(days=1)).date().isoformat()
    end_date = (end + timedelta(days=1)).date().isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/"
        f"{minutes}/minute/{start_date}/{end_date}"
    )
    try:
        resp = requests.get(
            url,
            params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key},
            timeout=20,
        )
        resp.raise_for_status()
        rows = resp.json().get("results") or []
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if "t" not in frame.columns:
        return pd.DataFrame()
    frame.index = pd.to_datetime(frame["t"], unit="ms", utc=True)
    return normalize_bars(frame)


def fetch_yfinance_bars(symbol: str, start: datetime, end: datetime, minutes: int = 5) -> pd.DataFrame:
    try:
        frame = yf.download(
            yfinance_symbol(symbol),
            start=start - timedelta(days=1),
            end=end + timedelta(days=1),
            interval=f"{minutes}m",
            progress=False,
            auto_adjust=False,
            prepost=True,
            threads=False,
        )
    except Exception:
        return pd.DataFrame()
    return normalize_bars(frame)


def fetch_price_bars(
    symbol: str,
    start: datetime,
    end: datetime,
    minutes: int = 5,
    providers: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, str]:
    for provider in providers or provider_order():
        provider = str(provider).strip().lower()
        if provider == "observations":
            continue
        if provider == "alpaca":
            frame = fetch_alpaca_bars(symbol, start, end, minutes=minutes)
        elif provider == "polygon":
            frame = fetch_polygon_bars(symbol, start, end, minutes=minutes)
        elif provider in {"yahoo", "yfinance"}:
            frame = fetch_yfinance_bars(symbol, start, end, minutes=minutes)
            provider = "yfinance"
        else:
            continue
        if not frame.empty:
            return frame, f"{provider}_{minutes}m"
    return pd.DataFrame(), "no_provider_data"
