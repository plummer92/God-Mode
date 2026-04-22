from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable

import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


DEBUG = os.getenv("ALPACA_DATA_DEBUG", "").strip().lower() in {"1", "true", "yes"}


def _debug(*parts: object) -> None:
    if DEBUG:
        print("[alpaca_data]", *parts, flush=True)


def _get_stock_client() -> StockHistoricalDataClient:
    return StockHistoricalDataClient(
        api_key=os.getenv("APCA_API_KEY_ID"),
        secret_key=os.getenv("APCA_API_SECRET_KEY"),
    )


def _resolve_feed(feed: str | DataFeed | None) -> DataFeed:
    if isinstance(feed, DataFeed):
        return feed
    if isinstance(feed, str) and feed.strip():
        return DataFeed(feed.strip().lower())
    # IEX avoids requiring SIP entitlements during initial migration.
    return DataFeed.IEX


def _coerce_symbols(symbols: str | Iterable[str]) -> list[str]:
    if isinstance(symbols, str):
        return [symbols]
    return [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    out = frame.copy()
    out = out.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    cols = ["Open", "High", "Low", "Close", "Volume"]
    out = out[[col for col in cols if col in out.columns]]
    out = out.sort_index()
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    return out


def _bars_by_symbol(
    symbols: str | Iterable[str],
    start: datetime,
    end: datetime,
    timeframe: TimeFrame,
    feed: str | DataFeed | None = None,
) -> dict[str, pd.DataFrame]:
    symbol_list = _coerce_symbols(symbols)
    _debug(
        "request",
        {
            "symbols": symbol_list,
            "start": start.isoformat() if hasattr(start, "isoformat") else str(start),
            "end": end.isoformat() if hasattr(end, "isoformat") else str(end),
            "timeframe": str(timeframe),
            "feed": str(_resolve_feed(feed).value),
        },
    )
    client = _get_stock_client()
    request = StockBarsRequest(
        symbol_or_symbols=symbol_list,
        start=start,
        end=end,
        timeframe=timeframe,
        feed=_resolve_feed(feed),
    )
    barset = client.get_stock_bars(request)
    _debug("raw_df_empty", barset.df.empty)
    if not barset.df.empty:
        try:
            counts = barset.df.groupby(level="symbol").size().to_dict()
        except Exception:
            counts = {"_unavailable": len(barset.df)}
        _debug("raw_row_counts", counts)
    result: dict[str, pd.DataFrame] = {}
    for symbol in symbol_list:
        try:
            frame = barset.df.xs(symbol, level="symbol")
        except Exception:
            frame = pd.DataFrame()
        result[symbol] = _normalize_frame(frame)
        _debug("normalized_rows", symbol, len(result[symbol]))
    return result


def get_stock_minute_bars(
    symbols: str | Iterable[str],
    start: datetime,
    end: datetime,
    minutes: int = 5,
    feed: str | DataFeed | None = None,
) -> dict[str, pd.DataFrame]:
    return _bars_by_symbol(
        symbols=symbols,
        start=start,
        end=end,
        timeframe=TimeFrame(minutes, TimeFrameUnit.Minute),
        feed=feed,
    )


def get_stock_hourly_bars(
    symbols: str | Iterable[str],
    start: datetime,
    end: datetime,
    feed: str | DataFeed | None = None,
) -> dict[str, pd.DataFrame]:
    return _bars_by_symbol(
        symbols=symbols,
        start=start,
        end=end,
        timeframe=TimeFrame.Hour,
        feed=feed,
    )


def get_latest_price(symbol: str, feed: str | DataFeed | None = None) -> float | None:
    client = _get_stock_client()
    request = StockLatestTradeRequest(
        symbol_or_symbols=[str(symbol).strip().upper()],
        feed=_resolve_feed(feed),
    )
    data = client.get_stock_latest_trade(request)
    trade = data.get(str(symbol).strip().upper())
    if trade is None:
        return None
    return float(trade.price)
