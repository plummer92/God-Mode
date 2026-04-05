#!/usr/bin/env python3
"""God Mode trading dashboard."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import pandas as pd
import pytz
import streamlit as st
import yfinance as yf
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv

from Symbol_hunter import classify_sector
from market_context import get_market_context


DB_PATH = "/home/theplummer92/wolfe_signals.db"
TRADE_LOG_DB = "/home/theplummer92/trade_log.db"
REGIME_PATH = "/home/theplummer92/regime_snapshot.json"
SNIPER_LOG = "/home/theplummer92/sniper.log"
APPROVED_PATH = "/home/theplummer92/approved_symbols.json"
MARKET_SNAPSHOT_JSON = "/home/theplummer92/market_snapshot.json"
MARKET_SNAPSHOT_CSV = "/home/theplummer92/market_snapshot.csv"
MARKET_SNAPSHOT_LOG = "/home/theplummer92/market_snapshot.log"
CST = pytz.timezone("America/Chicago")
FLOW_AUDIT_MAX_SYMBOLS = 24
FLOW_AUDIT_LOOKBACK_BARS = 12
FLOW_AUDIT_MIN_BARS = 24
FLOW_AUDIT_CHART_BARS = 36
FLOW_AUDIT_PERIOD = "10d"
FLOW_AUDIT_INTERVAL = "1h"
TICKER_SCROLL_SECONDS = 90
TICKER_MAX_ITEMS = 10
MARKET_SNAPSHOT_COLUMNS = {
    "symbol",
    "sector",
    "signal_type",
    "bias",
    "price",
    "rvol",
    "change_pct",
    "avg_volume_20",
    "avg_dollar_volume_20",
    "score",
}
SEMI_SYMBOLS = {
    "AMD", "NVDA", "AVGO", "QCOM", "INTC", "MU", "AMAT", "LRCX", "KLAC",
    "MRVL", "TXN", "TSM", "ASML", "ARM", "SMCI", "SOXL", "SOXS", "SMH",
}
SECTOR_ETF_MAP = {
    "TECH": "QQQ",
    "FINANCIALS": "XLF",
    "ENERGY": "XLE",
    "HEALTHCARE": "XLV",
    "INDUSTRIALS": "XLI",
    "CONSUMER": "SPY",
    "COMMUNICATION": "QQQ",
    "SPECULATIVE": "IWM",
    "CRYPTO_PROXY": "IWM",
    "ETF": "SPY",
    "UNKNOWN": "SPY",
}


st.set_page_config(
    page_title="GOD MODE",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    (
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700;900&display=swap');
* { box-sizing: border-box; }
html, body, [data-testid="stAppViewContainer"] {
  background: #020408 !important;
  color: #e0ffe0 !important;
  font-family: 'Share Tech Mono', monospace !important;
}
[data-testid="stAppViewContainer"] {
  background:
    repeating-linear-gradient(0deg, transparent, transparent 39px, #0a1a0a 39px, #0a1a0a 40px),
    repeating-linear-gradient(90deg, transparent, transparent 39px, #0a1a0a 39px, #0a1a0a 40px),
    #020408 !important;
}
h1, h2, h3, [data-testid="stTabs"] button {
  font-family: 'Orbitron', monospace !important;
}
.block-container { padding: 1.25rem 1.5rem !important; max-width: 100% !important; }
[data-testid="metric-container"] {
  background: #000d00 !important;
  border: 1px solid #1a4a1a !important;
  border-radius: 4px !important;
  padding: 0.8rem !important;
  box-shadow: 0 0 12px #00ff0022 inset !important;
}
[data-testid="stMetricLabel"] {
  color: #4aff4a !important;
  font-size: 0.65rem !important;
  letter-spacing: 0.15em !important;
  text-transform: uppercase !important;
}
[data-testid="stMetricValue"] {
  color: #00ff41 !important;
  font-family: 'Orbitron', monospace !important;
  font-size: 1.25rem !important;
}
.section-header {
  font-family: 'Orbitron', monospace;
  font-size: 0.72rem;
  letter-spacing: 0.28em;
  color: #4aff4a;
  text-transform: uppercase;
  border-bottom: 1px solid #1a4a1a;
  padding-bottom: 0.35rem;
  margin-bottom: 0.9rem;
  margin-top: 1rem;
}
.god-title {
  font-family: 'Orbitron', monospace;
  font-size: 2.2rem;
  font-weight: 900;
  color: #00ff41;
  letter-spacing: 0.18em;
  margin: 0;
}
.god-sub {
  font-size: 0.68rem;
  color: #2a7a2a;
  letter-spacing: 0.35em;
  text-transform: uppercase;
  margin-top: 0.1rem;
}
.regime-open { color: #00ff41; font-weight: bold; text-shadow: 0 0 10px #00ff41; }
.regime-sellonly { color: #ffaa00; font-weight: bold; text-shadow: 0 0 10px #ffaa00; }
.regime-blocked { color: #ff3333; font-weight: bold; text-shadow: 0 0 10px #ff3333; }
.health-good { color: #00ff41; }
.health-warn { color: #ffaa00; }
.health-bad { color: #ff5555; }
.panel {
  background: #000d00;
  border: 1px solid #1a4a1a;
  border-radius: 4px;
  padding: 0.9rem 1rem;
}
.banner {
  border-radius: 4px;
  padding: 0.8rem 1rem;
  margin-bottom: 0.8rem;
  border: 1px solid;
  font-size: 0.78rem;
}
.banner-good { background: #001400; border-color: #1f6f1f; color: #7aff7a; }
.banner-warn { background: #1a1200; border-color: #7a5400; color: #ffd36a; }
.banner-bad { background: #180000; border-color: #6f1f1f; color: #ff8a8a; }
.log-box {
  background: #000d00;
  border: 1px solid #1a4a1a;
  border-radius: 4px;
  padding: 1rem;
  font-size: 0.72rem;
  line-height: 1.55;
  max-height: 460px;
  overflow-y: auto;
  color: #7aff7a;
}
.snapshot-card {
  background: linear-gradient(180deg, #03110a 0%, #000d00 100%);
  border: 1px solid #1d5a34;
  border-radius: 6px;
  padding: 0.9rem 1rem;
  min-height: 132px;
  box-shadow: 0 0 18px #00ff4112 inset;
}
.snapshot-kicker {
  color: #67ff9a;
  font-size: 0.62rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  margin-bottom: 0.45rem;
}
.snapshot-value {
  color: #d8ffe7;
  font-family: 'Orbitron', monospace;
  font-size: 1.35rem;
  line-height: 1.2;
}
.snapshot-sub {
  color: #7bd49a;
  font-size: 0.72rem;
  margin-top: 0.45rem;
  line-height: 1.45;
}
.tone-chip {
  display: inline-block;
  border-radius: 999px;
  padding: 0.25rem 0.7rem;
  font-size: 0.68rem;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  border: 1px solid;
}
.tone-long { color: #71ff9c; background: #0b2314; border-color: #2a9f59; }
.tone-short { color: #ff8f8f; background: #200808; border-color: #8f2a2a; }
.tone-mixed { color: #ffe189; background: #221b08; border-color: #9c7b26; }
.tone-neutral { color: #9ad0ff; background: #071927; border-color: #255b88; }
.sector-pill {
  display: inline-block;
  padding: 0.25rem 0.55rem;
  margin: 0.15rem 0.25rem 0.15rem 0;
  border-radius: 999px;
  background: #0a1711;
  border: 1px solid #244a34;
  color: #8af8b3;
  font-size: 0.7rem;
}
.symbol-pill {
  display: inline-block;
  padding: 0.24rem 0.5rem;
  margin: 0.12rem 0.2rem 0.12rem 0;
  border-radius: 999px;
  background: #08140f;
  border: 1px solid #1f5034;
  color: #ddffea;
  font-size: 0.72rem;
}
.ticker-wrap {
  position: relative;
  overflow: hidden;
  border: 1px solid #17462f;
  border-radius: 6px;
  background: linear-gradient(90deg, #04110b 0%, #07170f 100%);
  padding: 0.65rem 0;
  margin: 0.4rem 0 1rem 0;
}
.ticker-track {
  display: inline-block;
  white-space: nowrap;
  padding-left: 100%;
  animation: ticker-scroll __TICKER_SCROLL_SECONDS__ linear infinite;
  will-change: transform;
}
.ticker-item {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  margin-right: 1.25rem;
  padding: 0.2rem 0.7rem;
  border-radius: 999px;
  border: 1px solid #1c5036;
  background: #07140e;
  color: #d9ffea;
  font-size: 0.72rem;
  box-shadow: 0 0 14px #00000033 inset;
}
.ticker-signal {
  letter-spacing: 0.08em;
  font-weight: 700;
}
.ticker-positive { color: #74f7a3; }
.ticker-negative { color: #ff9a9a; }
.ticker-long {
  background: linear-gradient(90deg, #081b12 0%, #0b2316 100%);
  border-color: #2a8f57;
}
.ticker-long .ticker-signal { color: #74f7a3; }
.ticker-short {
  background: linear-gradient(90deg, #1a0909 0%, #220c0c 100%);
  border-color: #8f3030;
}
.ticker-short .ticker-signal { color: #ff9d9d; }
.ticker-event {
  background: linear-gradient(90deg, #221907 0%, #2a1d09 100%);
  border-color: #a9842d;
}
.ticker-event .ticker-signal { color: #ffd87a; }
.ticker-caution {
  background: linear-gradient(90deg, #101018 0%, #141421 100%);
  border-color: #59648a;
}
.ticker-caution .ticker-signal { color: #a7bbff; }
.ticker-neutral {
  background: linear-gradient(90deg, #0b1012 0%, #10171a 100%);
  border-color: #35515d;
}
.ticker-neutral .ticker-signal { color: #9ad0df; }
.overview-discovery {
  background: linear-gradient(135deg, #06130d 0%, #0b1c13 55%, #07100c 100%);
  border: 1px solid #22563a;
  border-radius: 8px;
  padding: 1rem 1.1rem;
  margin-bottom: 1rem;
  box-shadow: 0 0 22px #00ff4110 inset;
}
.overview-discovery-title {
  color: #7fffb0;
  font-family: 'Orbitron', monospace;
  font-size: 0.75rem;
  letter-spacing: 0.28em;
  text-transform: uppercase;
}
.overview-discovery-main {
  margin-top: 0.55rem;
  color: #ecfff4;
  font-size: 1rem;
  line-height: 1.5;
}
.overview-discovery-side {
  color: #8ed4ab;
  font-size: 0.73rem;
  line-height: 1.55;
}
.decision-card {
  background: linear-gradient(180deg, #04130c 0%, #000d00 100%);
  border: 1px solid #20583a;
  border-radius: 8px;
  padding: 0.95rem 1rem;
  min-height: 240px;
  box-shadow: 0 0 18px #00ff4110 inset;
  margin-bottom: 0.8rem;
}
.decision-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 0.8rem;
}
.decision-symbol {
  color: #e8fff1;
  font-family: 'Orbitron', monospace;
  font-size: 1.1rem;
  letter-spacing: 0.08em;
}
.decision-setup {
  color: #89d9a9;
  font-size: 0.72rem;
  margin-top: 0.15rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
}
.decision-score {
  color: #73ff9c;
  font-family: 'Orbitron', monospace;
  font-size: 1rem;
  white-space: nowrap;
}
.decision-meta {
  color: #77c697;
  font-size: 0.7rem;
  margin-top: 0.5rem;
  line-height: 1.5;
}
.decision-reason {
  color: #d9ffea;
  font-size: 0.76rem;
  line-height: 1.55;
  margin-top: 0.75rem;
}
.decision-chip {
  display: inline-block;
  border-radius: 999px;
  padding: 0.22rem 0.62rem;
  margin: 0.18rem 0.28rem 0 0;
  font-size: 0.63rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  border: 1px solid;
}
.decision-chip-good { color: #7effae; background: #0b2315; border-color: #2f8f5c; }
.decision-chip-warn { color: #ffd78a; background: #241a08; border-color: #94722b; }
.decision-chip-bad { color: #ff9f9f; background: #240909; border-color: #8f3434; }
.decision-chip-neutral { color: #9ed7ff; background: #091823; border-color: #2c6288; }
@keyframes ticker-scroll {
  0% { transform: translateX(0); }
  100% { transform: translateX(-100%); }
}
</style>
"""
    ).replace("__TICKER_SCROLL_SECONDS__", f"{TICKER_SCROLL_SECONDS}s"),
    unsafe_allow_html=True,
)

load_dotenv("/home/theplummer92/.env")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def file_age_seconds(path: str) -> int | None:
    try:
        return max(0, int(utc_now().timestamp() - os.path.getmtime(path)))
    except OSError:
        return None


def age_label(seconds: int | None) -> str:
    if seconds is None:
        return "missing"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h"


def health_class(seconds: int | None, warn_after: int, bad_after: int) -> str:
    if seconds is None or seconds >= bad_after:
        return "health-bad"
    if seconds >= warn_after:
        return "health-warn"
    return "health-good"


def run_query(db_path: str, query: str, params: tuple = ()) -> tuple[pd.DataFrame, str | None]:
    try:
        conn = get_db_connection(db_path)
        return pd.read_sql_query(query, conn, params=params), None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


class RenderTimer:
    def __init__(self) -> None:
        self.started_at = time.perf_counter()
        self.totals = {
            "db_query": 0.0,
            "dataframe_transform": 0.0,
            "chart_build": 0.0,
            "table_render": 0.0,
        }

    def measure(self, bucket: str, fn, *args, **kwargs):
        started = time.perf_counter()
        result = fn(*args, **kwargs)
        self.totals[bucket] = self.totals.get(bucket, 0.0) + (time.perf_counter() - started)
        return result

    def add(self, bucket: str, duration: float) -> None:
        self.totals[bucket] = self.totals.get(bucket, 0.0) + max(0.0, float(duration))

    def total_page_render(self) -> float:
        return time.perf_counter() - self.started_at


@st.cache_resource(show_spinner=False)
def get_db_connection(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(
        f"file:{db_path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )


@st.cache_resource(show_spinner=False)
def get_trading_client() -> TradingClient:
    return TradingClient(
        os.getenv("APCA_API_KEY_ID"),
        os.getenv("APCA_API_SECRET_KEY"),
        paper=False,
    )


@st.cache_data(ttl=30)
def load_snapshot() -> tuple[dict, str | None]:
    try:
        with open(REGIME_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle), None
    except Exception as exc:
        return {}, str(exc)


@st.cache_data(ttl=30)
def load_market_context() -> tuple[dict, str | None]:
    try:
        return get_market_context(REGIME_PATH), None
    except Exception as exc:
        return {}, str(exc)


@st.cache_data(ttl=15)
def load_sniper_status() -> tuple[dict, str | None]:
    df, err = run_query(
        DB_PATH,
        """
        SELECT ts_utc, status, bot_version, note
        FROM sniper_status
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
    )
    if err or df.empty:
        return {}, err
    return df.iloc[0].to_dict(), None


@st.cache_data(ttl=15)
def load_daily_risk_state() -> tuple[dict, str | None]:
    df, err = run_query(
        DB_PATH,
        """
        SELECT singleton_id, start_balance, halt_mode, trading_day
        FROM daily_risk_state
        WHERE singleton_id = 1
        """,
    )
    if err or df.empty:
        return {}, err
    return df.iloc[0].to_dict(), None


@st.cache_data(ttl=20)
def load_recent_signals(limit: int = 30) -> tuple[pd.DataFrame, str | None]:
    return run_query(
        DB_PATH,
        f"""
        SELECT timestamp, symbol, signal_type, price, rvol, change_pct
        FROM signals
        ORDER BY timestamp DESC
        LIMIT {int(limit)}
        """,
    )


@st.cache_data(ttl=30)
def load_signal_counts() -> tuple[pd.DataFrame, str | None]:
    return run_query(
        DB_PATH,
        """
        SELECT signal_type, COUNT(*) AS count
        FROM signals
        WHERE timestamp > datetime('now', '-24 hours')
        GROUP BY signal_type
        ORDER BY count DESC
        """,
    )


@st.cache_data(ttl=30)
def load_top_movers() -> tuple[pd.DataFrame, str | None]:
    return run_query(
        DB_PATH,
        """
        SELECT symbol, signal_type, price, rvol, change_pct, timestamp
        FROM signals
        WHERE timestamp > datetime('now', '-4 hours')
          AND (signal_type LIKE '%STRONG%' OR signal_type LIKE '%ABSORPTION%')
        ORDER BY ABS(change_pct) DESC
        LIMIT 12
        """,
    )


@st.cache_data(ttl=20)
def load_recent_trades(limit: int = 20) -> tuple[pd.DataFrame, str | None]:
    return run_query(
        TRADE_LOG_DB,
        f"""
        SELECT id, symbol, direction, entry_price, exit_price, entry_time, exit_time,
               pnl_usd, outcome
        FROM trades
        ORDER BY id DESC
        LIMIT {int(limit)}
        """,
    )


@st.cache_data(ttl=20)
def load_trade_summary() -> tuple[pd.DataFrame, str | None]:
    return run_query(
        TRADE_LOG_DB,
        """
        SELECT outcome, COUNT(*) AS count
        FROM trades
        WHERE entry_time > datetime('now', '-7 days')
        GROUP BY outcome
        ORDER BY count DESC
        """,
    )


@st.cache_data(ttl=60)
def load_approved() -> tuple[dict, str | None]:
    try:
        with open(APPROVED_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle), None
    except Exception as exc:
        return {}, str(exc)


@st.cache_data(ttl=15)
def load_sniper_log(lines: int = 80) -> tuple[list[str], str | None]:
    try:
        with open(SNIPER_LOG, "r", encoding="utf-8") as handle:
            return handle.readlines()[-lines:], None
    except Exception as exc:
        return [], str(exc)


@st.cache_data(ttl=120)
def load_market_snapshot_file() -> tuple[dict, str | None]:
    try:
        with open(MARKET_SNAPSHOT_JSON, "r", encoding="utf-8") as handle:
            return json.load(handle), None
    except Exception as exc:
        return {}, str(exc)


@st.cache_data(ttl=120)
def load_market_snapshot_rows(limit: int = 25) -> tuple[pd.DataFrame, str | None]:
    try:
        df = pd.read_csv(
            MARKET_SNAPSHOT_CSV,
            usecols=lambda column: column in MARKET_SNAPSHOT_COLUMNS,
            nrows=int(limit),
        )
        return df, None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


@st.cache_data(ttl=20)
def load_open_trades() -> tuple[pd.DataFrame, str | None]:
    return run_query(
        TRADE_LOG_DB,
        """
        SELECT symbol, direction, entry_price, entry_time, outcome
        FROM trades
        WHERE exit_time IS NULL
        ORDER BY id DESC
        """,
    )


@st.cache_data(ttl=20)
def load_broker_snapshot() -> tuple[dict, str | None]:
    try:
        client = get_trading_client()
        account = client.get_account()
        positions = client.get_all_positions()
        rows = []
        for pos in positions:
            rows.append(
                {
                    "symbol": pos.symbol,
                    "side": "SHORT" if float(pos.qty) < 0 else "LONG",
                    "qty": float(pos.qty),
                    "market_value": float(pos.market_value),
                    "entry_price": float(pos.avg_entry_price),
                    "current_price": float(pos.current_price),
                    "unrealized_pl": float(pos.unrealized_pl),
                    "unrealized_plpc": float(pos.unrealized_plpc) * 100.0,
                }
            )
        return {
            "equity": float(account.equity),
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
            "portfolio_value": float(account.portfolio_value),
            "positions": rows,
        }, None
    except Exception as exc:
        return {}, str(exc)


def regime_badge(final_regime: str) -> tuple[str, str, str]:
    label = str(final_regime or "UNKNOWN").upper()
    if label.startswith("BLOCKED") or label.endswith("PANIC"):
        return label, "regime-blocked", "BLOCKED"
    if label.startswith("SELL_ONLY") or label.endswith("RISK_OFF"):
        return label, "regime-sellonly", "SELL-ONLY"
    return label, "regime-open", "OPEN"


def fmt_money(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "—"


def fmt_pct(value) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "—"


@st.cache_data(show_spinner=False)
def format_positions_table(positions: tuple[tuple, ...]) -> pd.DataFrame:
    if not positions:
        return pd.DataFrame()
    df = pd.DataFrame(
        positions,
        columns=[
            "symbol",
            "side",
            "qty",
            "market_value",
            "entry_price",
            "current_price",
            "unrealized_pl",
            "unrealized_plpc",
        ],
    )
    df["market_value"] = df["market_value"].map(fmt_money)
    df["entry_price"] = df["entry_price"].map(fmt_money)
    df["current_price"] = df["current_price"].map(fmt_money)
    df["unrealized_pl"] = df["unrealized_pl"].map(fmt_money)
    df["unrealized_plpc"] = df["unrealized_plpc"].map(fmt_pct)
    return df


@st.cache_data(show_spinner=False)
def format_trade_log_table(records: tuple[tuple, ...], columns: tuple[str, ...]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=list(columns))
    df = pd.DataFrame(records, columns=list(columns))
    for column in ("entry_price", "exit_price", "pnl_usd"):
        if column in df.columns:
            df[column] = df[column].map(lambda value: fmt_money(value) if pd.notna(value) else "—")
    return df


@st.cache_data(show_spinner=False)
def format_recent_signals_table(records: tuple[tuple, ...], columns: tuple[str, ...]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["TIME", "SYMBOL", "SIGNAL", "PRICE", "RVOL", "CHG%"])
    df = pd.DataFrame(records, columns=list(columns))
    df["timestamp"] = df["timestamp"].astype(str).str[11:19]
    df["price"] = df["price"].map(lambda value: fmt_money(value) if pd.notna(value) else "—")
    df["rvol"] = df["rvol"].map(lambda value: f"{float(value):.2f}x" if pd.notna(value) else "—")
    df["change_pct"] = df["change_pct"].map(
        lambda value: fmt_pct(float(value) * 100.0) if pd.notna(value) else "—"
    )
    df.columns = ["TIME", "SYMBOL", "SIGNAL", "PRICE", "RVOL", "CHG%"]
    return df


@st.cache_data(show_spinner=False)
def format_hot_signals_table(records: tuple[tuple, ...], columns: tuple[str, ...]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["SYMBOL", "SIGNAL", "PRICE", "RVOL", "CHG%", "TIME"])
    df = pd.DataFrame(records, columns=list(columns))
    df["timestamp"] = df["timestamp"].astype(str).str[11:19]
    df["price"] = df["price"].map(lambda value: fmt_money(value) if pd.notna(value) else "—")
    df["rvol"] = df["rvol"].map(lambda value: f"{float(value):.2f}x" if pd.notna(value) else "—")
    df["change_pct"] = df["change_pct"].map(
        lambda value: fmt_pct(float(value) * 100.0) if pd.notna(value) else "—"
    )
    df.columns = ["SYMBOL", "SIGNAL", "PRICE", "RVOL", "CHG%", "TIME"]
    return df


@st.cache_data(show_spinner=False)
def format_market_snapshot_table(records: tuple[tuple, ...], columns: tuple[str, ...]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records, columns=list(columns))
    for column in ("price", "avg_dollar_volume_20"):
        if column in df.columns:
            df[column] = df[column].map(lambda value: fmt_money(value) if pd.notna(value) else "—")
    if "avg_volume_20" in df.columns:
        df["avg_volume_20"] = df["avg_volume_20"].map(
            lambda value: f"{int(float(value)):,}" if pd.notna(value) else "—"
        )
    if "rvol" in df.columns:
        df["rvol"] = df["rvol"].map(lambda value: f"{float(value):.2f}x" if pd.notna(value) else "—")
    if "change_pct" in df.columns:
        df["change_pct"] = df["change_pct"].map(
            lambda value: fmt_pct(float(value) * 100.0) if pd.notna(value) else "—"
        )
    if "score" in df.columns:
        df["score"] = df["score"].map(lambda value: f"{float(value):.1f}" if pd.notna(value) else "—")
    display_cols = [
        column for column in [
            "symbol", "sector", "signal_type", "bias", "price", "rvol",
            "change_pct", "avg_volume_20", "avg_dollar_volume_20", "score",
        ] if column in df.columns
    ]
    df = df[display_cols]
    df.columns = [
        "SYMBOL", "SECTOR", "SIGNAL", "BIAS", "PRICE", "RVOL",
        "CHG%", "AVG VOL", "AVG $ VOL", "SCORE",
    ][:len(df.columns)]
    return df


@st.cache_data(show_spinner=False)
def enrich_flow_audit_table(records: tuple[tuple, ...], columns: tuple[str, ...], signal_lookup: tuple[tuple[str, str], ...]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records, columns=list(columns))
    latest_signal_lookup = dict(signal_lookup)
    df["observed_signal_family"] = df["symbol"].map(lambda symbol: latest_signal_lookup.get(symbol, "UNKNOWN"))
    df["observed_bias"] = df["observed_signal_family"].map(
        lambda signal: (
            "LONG" if "BUY" in str(signal).upper()
            else "SHORT" if "SELL" in str(signal).upper()
            else "EVENT" if "CLIMAX" in str(signal).upper()
            else "MIXED"
        )
    )
    return df


def safe_float(value) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def parse_signal_timestamp(value) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value, utc=True)
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def build_signal_recency_lookup(records: tuple[tuple, ...], columns: tuple[str, ...]) -> dict[str, dict]:
    if not records:
        return {}
    df = pd.DataFrame(records, columns=list(columns))
    if df.empty or "symbol" not in df.columns:
        return {}
    if "timestamp" in df.columns:
        df["parsed_timestamp"] = df["timestamp"].map(parse_signal_timestamp)
        df = df.sort_values(by="parsed_timestamp", ascending=False, na_position="last")
    lookup: dict[str, dict] = {}
    for _, row in df.iterrows():
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or symbol in lookup:
            continue
        signal_dt = row.get("parsed_timestamp")
        age_minutes = None
        if signal_dt is not None:
            age_minutes = max(0, int((utc_now() - signal_dt).total_seconds() // 60))
        lookup[symbol] = {
            "timestamp": row.get("timestamp"),
            "signal_type": row.get("signal_type"),
            "age_minutes": age_minutes,
            "rvol": safe_float(row.get("rvol")),
            "change_pct": safe_float(row.get("change_pct")),
        }
    return lookup


def derive_setup_type(row: pd.Series, signal_meta: dict | None) -> str:
    bias = str(row.get("bias", "") or "").upper()
    signal_type = str((signal_meta or {}).get("signal_type") or row.get("signal_type") or "SETUP").replace("_", " ")
    if bias in {"LONG", "SHORT", "EVENT", "CAUTION"}:
        return f"{bias} | {signal_type}"
    return signal_type


def derive_strategy_alignment_flag(bias: str, symbol: str, approved_map: dict) -> str | None:
    approved_buy = {str(item).upper() for item in approved_map.get("buy", [])}
    approved_sell = {str(item).upper() for item in approved_map.get("sell", [])}
    if bias == "LONG" and symbol in approved_buy:
        return "STRATEGY ALIGNED"
    if bias == "SHORT" and symbol in approved_sell:
        return "STRATEGY ALIGNED"
    return None


def derive_quality_label(opportunity: dict) -> str:
    score = safe_float(opportunity.get("score"))
    flow_score = safe_float(opportunity.get("flow_score"))
    freshness = str(opportunity.get("flow_freshness") or "")
    source_quality = str(opportunity.get("flow_source_quality") or "")
    signal_age_minutes = opportunity.get("signal_age_minutes")
    aligned = bool(opportunity.get("strategy_alignment_flag"))
    risky = bool(opportunity.get("risk_flag"))
    if (
        score is not None and score >= 8.0
        and flow_score is not None and flow_score >= 8.0
        and freshness == "FRESH"
        and source_quality == "HIGH"
        and aligned
        and not risky
    ):
        return "HIGH PROBABILITY"
    if score is not None and score >= 7.0 and not risky and (
        flow_score is None or flow_score >= 4.0 or signal_age_minutes is not None and signal_age_minutes <= 90
    ):
        return "GOOD SETUP"
    if risky or freshness == "STALE" or source_quality == "LOW":
        return "LOW QUALITY"
    return "WATCHLIST"


def derive_risk_flag(opportunity: dict) -> str | None:
    bias = str(opportunity.get("bias") or "")
    divergence = str(opportunity.get("flow_divergence") or "")
    verdict = str(opportunity.get("flow_verdict") or "")
    freshness = str(opportunity.get("flow_freshness") or "")
    source_quality = str(opportunity.get("flow_source_quality") or "")
    signal_age_minutes = opportunity.get("signal_age_minutes")
    rvol = safe_float(opportunity.get("rvol"))
    if "Distribution risk" in verdict or divergence not in {"", "—"}:
        return "FLOW DIVERGENCE"
    if bias == "CAUTION" or (rvol is not None and rvol >= 2.5):
        return "RISKY / VOLATILE"
    if freshness == "STALE" or source_quality == "LOW" or (signal_age_minutes is not None and signal_age_minutes >= 360):
        return "STALE / LOW INFO"
    return None


def derive_reason_text(opportunity: dict) -> str:
    fragments: list[str] = []
    score = safe_float(opportunity.get("score"))
    if score is not None:
        fragments.append(f"discovery score {score:.1f}")
    signal_type = opportunity.get("signal_type")
    signal_age_minutes = opportunity.get("signal_age_minutes")
    if signal_type and signal_age_minutes is not None:
        fragments.append(f"{str(signal_type).replace('_', ' ')} {signal_age_minutes}m ago")
    elif signal_type:
        fragments.append(str(signal_type).replace("_", " "))
    flow_verdict = str(opportunity.get("flow_verdict") or "")
    if flow_verdict and flow_verdict != "Mixed":
        fragments.append(flow_verdict.lower())
    elif safe_float(opportunity.get("flow_score")) is not None:
        fragments.append(f"flow score {float(opportunity['flow_score']):.1f}")
    if opportunity.get("strategy_alignment_flag"):
        fragments.append("approved by current strategy list")
    return ", ".join(fragments[:4]).capitalize() if fragments else "Supported by current discovery context."


def derive_opportunity_tags(opportunity: dict) -> list[tuple[str, str]]:
    tags: list[tuple[str, str]] = []
    quality_label = str(opportunity.get("quality_label") or "")
    if quality_label == "HIGH PROBABILITY":
        tags.append((quality_label, "good"))
    elif quality_label == "GOOD SETUP":
        tags.append((quality_label, "neutral"))
    elif quality_label == "LOW QUALITY":
        tags.append((quality_label, "bad"))

    if opportunity.get("strategy_alignment_flag"):
        tags.append((str(opportunity["strategy_alignment_flag"]), "good"))

    signal_age_minutes = opportunity.get("signal_age_minutes")
    if signal_age_minutes is not None and signal_age_minutes <= 90:
        tags.append(("FRESH SIGNAL", "good"))

    flow_score = safe_float(opportunity.get("flow_score"))
    if flow_score is not None and flow_score >= 8.0:
        tags.append(("MOMENTUM", "good"))

    risk_flag = opportunity.get("risk_flag")
    if risk_flag:
        tags.append((str(risk_flag), "bad" if "DIVERGENCE" in str(risk_flag) else "warn"))
    return tags[:4]


@st.cache_data(show_spinner=False)
def build_top_opportunities(
    snapshot_records: tuple[tuple, ...],
    snapshot_columns: tuple[str, ...],
    flow_records: tuple[tuple, ...],
    flow_columns: tuple[str, ...],
    signal_records: tuple[tuple, ...],
    signal_columns: tuple[str, ...],
    approved_buy: tuple[str, ...],
    approved_sell: tuple[str, ...],
    limit: int = 6,
) -> pd.DataFrame:
    if not snapshot_records:
        return pd.DataFrame()

    snapshot_df = pd.DataFrame(snapshot_records, columns=list(snapshot_columns))
    if snapshot_df.empty or "symbol" not in snapshot_df.columns:
        return pd.DataFrame()
    snapshot_df["symbol"] = snapshot_df["symbol"].astype(str).str.upper()

    flow_lookup: dict[str, dict] = {}
    if flow_records:
        flow_df = pd.DataFrame(flow_records, columns=list(flow_columns))
        if not flow_df.empty and "symbol" in flow_df.columns:
            flow_df["symbol"] = flow_df["symbol"].astype(str).str.upper()
            flow_lookup = flow_df.set_index("symbol").to_dict(orient="index")

    signal_lookup = build_signal_recency_lookup(signal_records, signal_columns)
    approved_map = {"buy": list(approved_buy), "sell": list(approved_sell)}
    opportunities: list[dict] = []

    for _, row in snapshot_df.iterrows():
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        flow_row = flow_lookup.get(symbol, {})
        signal_meta = signal_lookup.get(symbol, {})
        bias = str(row.get("bias", "") or "").upper()
        score = safe_float(row.get("score"))
        rvol = safe_float(row.get("rvol"))
        signal_age_minutes = signal_meta.get("age_minutes")
        strategy_alignment_flag = derive_strategy_alignment_flag(bias, symbol, approved_map)
        opportunity = {
            "symbol": symbol,
            "bias": bias,
            "setup_type": derive_setup_type(row, signal_meta),
            "score": score,
            "rvol": rvol,
            "signal_type": signal_meta.get("signal_type") or row.get("signal_type"),
            "signal_timestamp": signal_meta.get("timestamp"),
            "signal_age_minutes": signal_age_minutes,
            "snapshot_change_pct": safe_float(row.get("change_pct")),
            "flow_score": safe_float(flow_row.get("derived_flow_score")),
            "flow_verdict": flow_row.get("verdict_final_audit_verdict"),
            "flow_divergence": flow_row.get("verdict_divergence_flag"),
            "flow_etf_confirmation": flow_row.get("verdict_etf_confirmation_flag"),
            "flow_freshness": flow_row.get("observed_freshness_flag"),
            "flow_source_quality": flow_row.get("observed_source_quality_flag"),
            "strategy_alignment_flag": strategy_alignment_flag,
        }
        opportunity["risk_flag"] = derive_risk_flag(opportunity)
        opportunity["quality_label"] = derive_quality_label(opportunity)
        opportunity["reason_text"] = derive_reason_text(opportunity)
        opportunity["tags"] = derive_opportunity_tags(opportunity)

        ranking_score = (score or 0.0) * 1.6
        flow_score = opportunity["flow_score"]
        if flow_score is not None:
            ranking_score += flow_score * 0.9
        if rvol is not None:
            ranking_score += min(max(rvol - 1.0, 0.0), 2.0) * 2.5
        # Ranking stays explainable: reward strong discovery score, fresh signals,
        # flow confirmation, and approved-list alignment; penalize stale or risky rows.
        if signal_age_minutes is not None:
            if signal_age_minutes <= 30:
                ranking_score += 6.0
            elif signal_age_minutes <= 120:
                ranking_score += 3.0
            elif signal_age_minutes >= 360:
                ranking_score -= 4.0
        if opportunity["flow_freshness"] == "FRESH":
            ranking_score += 4.0
        elif opportunity["flow_freshness"] == "STALE":
            ranking_score -= 5.0
        if opportunity["flow_source_quality"] == "HIGH":
            ranking_score += 3.0
        elif opportunity["flow_source_quality"] == "LOW":
            ranking_score -= 3.0
        if opportunity["flow_etf_confirmation"] == "CONFIRMED":
            ranking_score += 2.0
        elif opportunity["flow_etf_confirmation"] == "CONFLICT":
            ranking_score -= 2.0
        if strategy_alignment_flag:
            ranking_score += 5.0
        if "STRONG" in str(opportunity["signal_type"]).upper() or "ABSORPTION" in str(opportunity["signal_type"]).upper():
            ranking_score += 2.0
        if opportunity["risk_flag"]:
            ranking_score -= 6.0 if "DIVERGENCE" in opportunity["risk_flag"] else 3.5
        if bias == "CAUTION":
            ranking_score -= 3.0

        opportunity["ranking_score"] = round(ranking_score, 2)
        opportunities.append(opportunity)

    if not opportunities:
        return pd.DataFrame()

    ranked = pd.DataFrame(opportunities)
    ranked = ranked.sort_values(
        by=["ranking_score", "score", "flow_score"],
        ascending=[False, False, False],
        na_position="last",
    ).head(int(limit))
    return ranked.reset_index(drop=True)


def render_decision_card(opportunity: dict, rank: int) -> None:
    score = "—" if opportunity.get("score") is None else f"{float(opportunity['score']):.1f}"
    recency = "No recent signal timestamp"
    _age = opportunity.get("signal_age_minutes")
    if _age is not None and pd.notna(_age):
        recency = f"Signal age {int(_age)}m"
    elif opportunity.get("flow_freshness"):
        recency = f"Flow {opportunity['flow_freshness']}"
    risk_flag = opportunity.get("risk_flag") or "CLEAR"
    strategy_flag = opportunity.get("strategy_alignment_flag") or "NOT FLAGGED"
    tags = "".join(
        f'<span class="decision-chip decision-chip-{tone}">{label}</span>'
        for label, tone in opportunity.get("tags", [])
    )
    st.markdown(
        f'<div class="decision-card">'
        f'<div class="decision-head">'
        f'<div><div class="decision-symbol">#{rank} {opportunity.get("symbol", "—")}</div>'
        f'<div class="decision-setup">{opportunity.get("setup_type", "—")}</div></div>'
        f'<div class="decision-score">SCORE {score}</div>'
        f'</div>'
        f'<div class="decision-meta">Recency: {recency}<br>'
        f'Quality: {opportunity.get("quality_label", "—")}<br>'
        f'Strategy: {strategy_flag}<br>'
        f'Risk: {risk_flag}</div>'
        f'<div class="decision-reason">{opportunity.get("reason_text", "—")}</div>'
        f'<div style="margin-top:0.7rem;">{tags}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_timed_dataframe(timer: RenderTimer, df: pd.DataFrame, **kwargs) -> None:
    started = time.perf_counter()
    st.dataframe(df, **kwargs)
    timer.add("table_render", time.perf_counter() - started)


def render_timed_line_chart(timer: RenderTimer, data, **kwargs) -> None:
    started = time.perf_counter()
    st.line_chart(data, **kwargs)
    timer.add("chart_build", time.perf_counter() - started)


@st.cache_data(ttl=120, show_spinner=False)
def build_ticker_html(records: tuple[tuple, ...], columns: tuple[str, ...]) -> str:
    if not records:
        return '<div class="ticker-wrap"><div class="ticker-track"><span class="ticker-item">No discovery snapshot yet</span></div></div>'

    rows = pd.DataFrame(records, columns=list(columns))
    items = []
    for _, row in rows.head(TICKER_MAX_ITEMS).iterrows():
        bias = str(row.get("bias", "neutral")).lower()
        ticker_class = "ticker-neutral"
        if bias == "long":
            ticker_class = "ticker-long"
        elif bias == "short":
            ticker_class = "ticker-short"
        elif bias == "event":
            ticker_class = "ticker-event"
        elif bias == "caution":
            ticker_class = "ticker-caution"
        change = row.get("change_pct")
        try:
            change_txt = f"{float(change) * 100:+.2f}%"
            change_class = "ticker-positive" if float(change) >= 0 else "ticker-negative"
        except Exception:
            change_txt = "—"
            change_class = ""
        score = row.get("score")
        try:
            score_txt = f"{float(score):.1f}"
        except Exception:
            score_txt = "—"
        signal_label = str(row.get("signal_type", "—")).replace("_", " ")
        items.append(
            f'<span class="ticker-item {ticker_class}"><strong>{row.get("symbol", "—")}</strong>'
            f'<span class="ticker-signal">{signal_label}</span>'
            f'<span class="{change_class}">{change_txt}</span>'
            f'<span>RVOL {row.get("rvol", "—")}</span>'
            f'<span>S {score_txt}</span></span>'
        )
    repeated = "".join(items + items)
    return f'<div class="ticker-wrap"><div class="ticker-track">{repeated}</div></div>'


def render_banner(text: str, level: str = "good") -> None:
    st.markdown(f'<div class="banner banner-{level}">{text}</div>', unsafe_allow_html=True)


def get_or_create_ticker_html(rows: pd.DataFrame) -> str:
    if rows.empty:
        return build_ticker_html((), ())
    signature = (
        tuple(rows.columns),
        tuple(rows.head(TICKER_MAX_ITEMS).itertuples(index=False, name=None)),
    )
    if st.session_state.get("ticker_signature") != signature:
        st.session_state["ticker_signature"] = signature
        st.session_state["ticker_html"] = build_ticker_html(signature[1], signature[0])
    return st.session_state.get("ticker_html", "")


def unique_symbols(*symbol_groups, limit: int = FLOW_AUDIT_MAX_SYMBOLS) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in symbol_groups:
        for raw_symbol in group or []:
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
            if len(ordered) >= int(limit):
                return ordered
    return ordered


def benchmark_for_symbol(symbol: str, sector: str) -> str:
    if symbol in SEMI_SYMBOLS:
        return "SMH"
    return SECTOR_ETF_MAP.get(str(sector or "UNKNOWN").upper(), "SPY")


def extract_latest_signal_lookup(symbols: list[str]) -> dict[str, str]:
    clean_symbols = [str(symbol).upper() for symbol in symbols if str(symbol).strip()]
    if not clean_symbols:
        return {}

    try:
        conn = get_db_connection(DB_PATH)
        placeholders = ",".join("?" for _ in clean_symbols)
        query = f"""
            SELECT s.symbol, s.signal_type
            FROM signals s
            INNER JOIN (
                SELECT symbol, MAX(timestamp) AS max_timestamp
                FROM signals
                WHERE symbol IN ({placeholders})
                GROUP BY symbol
            ) latest
              ON latest.symbol = s.symbol
             AND latest.max_timestamp = s.timestamp
        """
        df = pd.read_sql_query(query, conn, params=tuple(clean_symbols))
        if df.empty:
            return {}
        df["symbol"] = df["symbol"].astype(str).str.upper()
        df = df.drop_duplicates(subset=["symbol"], keep="first")
        return {
            str(row["symbol"]).upper(): str(row["signal_type"])
            for _, row in df.iterrows()
        }
    except Exception:
        return {}


def extract_download_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            level0 = {str(v) for v in raw.columns.get_level_values(0)}
            if symbol in level0:
                frame = raw[symbol].copy()
            else:
                return pd.DataFrame()
        else:
            frame = raw.copy()
        needed = [column for column in ("Open", "Close", "Volume") if column in frame.columns]
        if len(needed) < 3:
            return pd.DataFrame()
        frame = frame[["Open", "Close", "Volume"]].copy()
        frame = frame.dropna(subset=["Open", "Close", "Volume"])
        return frame
    except Exception:
        return pd.DataFrame()


def flow_data_freshness(index) -> tuple[str, str, float | None]:
    if index is None or len(index) == 0:
        return "STALE", "LOW", None
    try:
        latest = pd.Timestamp(index[-1])
        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")
        else:
            latest = latest.tz_convert("UTC")
        age_hours = max(0.0, (utc_now() - latest.to_pydatetime()).total_seconds() / 3600.0)
    except Exception:
        return "STALE", "LOW", None

    if age_hours <= 2.5:
        return "FRESH", "HIGH", age_hours
    if age_hours <= 8.0:
        return "OK", "MEDIUM", age_hours
    return "STALE", "LOW", age_hours


def build_exposure_summary(positions: list[dict]) -> dict:
    gross_long = 0.0
    gross_short = 0.0
    gross_by_sector: dict[str, float] = {}
    gross_by_symbol: dict[str, float] = {}
    for row in positions or []:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        exposure = abs(float(row.get("market_value") or 0.0))
        sector = classify_sector(symbol)
        if str(row.get("side", "")).upper() == "SHORT":
            gross_short += exposure
        else:
            gross_long += exposure
        gross_by_symbol[symbol] = gross_by_symbol.get(symbol, 0.0) + exposure
        gross_by_sector[sector] = gross_by_sector.get(sector, 0.0) + exposure
    return {
        "gross_long": gross_long,
        "gross_short": gross_short,
        "gross_total": gross_long + gross_short,
        "gross_by_sector": gross_by_sector,
        "gross_by_symbol": gross_by_symbol,
    }


def flow_divergence_flag(price_change_pct: float, flow_ratio: float) -> tuple[str, float]:
    flags: list[str] = []
    divergence_strength = 0.0
    if price_change_pct >= 1.0 and flow_ratio <= 0.0:
        flags.append("price up / flow weak")
        divergence_strength = max(divergence_strength, abs(price_change_pct) + abs(flow_ratio * 100.0))
    if abs(price_change_pct) <= 0.35 and flow_ratio >= 0.02:
        flags.append("price flat / flow positive")
        divergence_strength = max(divergence_strength, abs(flow_ratio * 100.0))
    if price_change_pct <= -1.0 and flow_ratio >= 0.02:
        flags.append("price down / flow positive")
        divergence_strength = max(divergence_strength, abs(price_change_pct) + abs(flow_ratio * 100.0))
    return (" | ".join(flags) if flags else "—", divergence_strength)


def etf_confirmation_flag(symbol_price_change_pct: float, symbol_flow_ratio: float,
                          benchmark_price_change_pct: float | None,
                          benchmark_flow_ratio: float | None) -> str:
    if benchmark_price_change_pct is None or benchmark_flow_ratio is None:
        return "NO DATA"

    symbol_bias = 0
    benchmark_bias = 0
    if symbol_price_change_pct >= 0.25 and symbol_flow_ratio > 0:
        symbol_bias = 1
    elif symbol_price_change_pct <= -0.25 and symbol_flow_ratio < 0:
        symbol_bias = -1

    if benchmark_price_change_pct >= 0.15 and benchmark_flow_ratio > 0:
        benchmark_bias = 1
    elif benchmark_price_change_pct <= -0.15 and benchmark_flow_ratio < 0:
        benchmark_bias = -1

    if symbol_bias == 0 or benchmark_bias == 0:
        return "MIXED"
    return "CONFIRMED" if symbol_bias == benchmark_bias else "CONFLICT"


def final_audit_verdict(flow_score: float, divergence_flag: str, etf_flag: str) -> str:
    if divergence_flag != "—":
        if "price up / flow weak" in divergence_flag:
            return "Distribution risk"
        return "Accumulation divergence"
    if flow_score >= 8.0:
        return "Positive flow + ETF support" if etf_flag == "CONFIRMED" else "Positive flow"
    if flow_score <= -8.0:
        return "Negative flow + ETF support" if etf_flag == "CONFIRMED" else "Negative flow"
    if flow_score >= 4.0:
        return "Positive flow watch"
    if flow_score <= -4.0:
        return "Negative flow watch"
    return "Mixed"


@st.cache_data(ttl=300, show_spinner=False)
def load_flow_audit_market(symbols: tuple[str, ...]) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], str | None]:
    tracked = tuple(str(symbol).upper() for symbol in symbols if str(symbol).strip())
    if not tracked:
        return pd.DataFrame(), {}, None

    sector_map = {symbol: classify_sector(symbol) for symbol in tracked}
    benchmark_map = {symbol: benchmark_for_symbol(symbol, sector_map[symbol]) for symbol in tracked}
    benchmarks = set(benchmark_map.values())
    download_symbols = sorted(set(tracked) | benchmarks)
    try:
        raw = yf.download(
            download_symbols if len(download_symbols) > 1 else download_symbols[0],
            period=FLOW_AUDIT_PERIOD,
            interval=FLOW_AUDIT_INTERVAL,
            progress=False,
            auto_adjust=False,
            group_by="ticker",
            threads=True,
        )
    except Exception as exc:
        return pd.DataFrame(), {}, str(exc)

    frame_map = {
        symbol: extract_download_frame(raw, symbol)
        for symbol in download_symbols
    }
    benchmark_stats: dict[str, dict] = {}
    benchmark_chart_map: dict[str, pd.Series] = {}
    for benchmark in benchmarks:
        benchmark_frame = frame_map.get(benchmark, pd.DataFrame())
        if len(benchmark_frame) < FLOW_AUDIT_MIN_BARS:
            continue
        benchmark_work = benchmark_frame.copy()
        benchmark_work["dollar_volume"] = benchmark_work["Close"] * benchmark_work["Volume"]
        benchmark_work["price_return"] = benchmark_work["Close"].pct_change().fillna(0.0)
        benchmark_work["signed_dollar_flow"] = benchmark_work["price_return"] * benchmark_work["dollar_volume"]
        benchmark_work = benchmark_work.dropna(subset=["dollar_volume", "signed_dollar_flow"])
        if len(benchmark_work) < FLOW_AUDIT_LOOKBACK_BARS:
            continue
        benchmark_recent = benchmark_work.tail(FLOW_AUDIT_LOOKBACK_BARS)
        benchmark_stats[benchmark] = {
            "price_change_pct": (
                (benchmark_recent["Close"].iloc[-1] / benchmark_recent["Close"].iloc[0]) - 1.0
            ) * 100.0,
            "flow_ratio": float(benchmark_recent["signed_dollar_flow"].sum()) / max(
                float(benchmark_recent["dollar_volume"].mean()), 1.0
            ),
        }
        benchmark_chart = benchmark_frame["Close"].tail(FLOW_AUDIT_CHART_BARS)
        if not benchmark_chart.empty and float(benchmark_chart.iloc[0]) != 0.0:
            benchmark_chart_map[benchmark] = benchmark_chart / float(benchmark_chart.iloc[0]) * 100.0

    rows: list[dict] = []
    charts: dict[str, pd.DataFrame] = {}
    for symbol in tracked:
        frame = frame_map.get(symbol, pd.DataFrame())
        if len(frame) < FLOW_AUDIT_MIN_BARS:
            continue

        sector = sector_map[symbol]
        benchmark = benchmark_map[symbol]

        work = frame.copy()
        work["dollar_volume"] = work["Close"] * work["Volume"]
        work["rvol"] = work["Volume"] / work["Volume"].rolling(20).mean()
        work["price_return"] = work["Close"].pct_change().fillna(0.0)
        work["signed_dollar_flow"] = work["price_return"] * work["dollar_volume"]
        work["cum_dollar_flow"] = work["signed_dollar_flow"].cumsum()
        work = work.dropna(subset=["rvol", "dollar_volume", "cum_dollar_flow"])
        if len(work) < FLOW_AUDIT_MIN_BARS:
            continue
        freshness_flag, source_quality_flag, data_age_hours = flow_data_freshness(work.index)

        recent = work.tail(FLOW_AUDIT_LOOKBACK_BARS)
        price_change_pct = ((recent["Close"].iloc[-1] / recent["Close"].iloc[0]) - 1.0) * 100.0
        cumulative_flow = float(recent["signed_dollar_flow"].sum())
        avg_recent_dollar_volume = max(float(recent["dollar_volume"].mean()), 1.0)
        flow_ratio = cumulative_flow / avg_recent_dollar_volume
        last_rvol = float(work["rvol"].iloc[-1])
        last_dollar_volume = float(work["dollar_volume"].iloc[-1])
        divergence_flag, divergence_strength = flow_divergence_flag(price_change_pct, flow_ratio)

        benchmark_price_change_pct = None
        benchmark_flow_ratio = None
        if benchmark in benchmark_stats:
            benchmark_price_change_pct = benchmark_stats[benchmark]["price_change_pct"]
            benchmark_flow_ratio = benchmark_stats[benchmark]["flow_ratio"]

        etf_flag = etf_confirmation_flag(
            price_change_pct,
            flow_ratio,
            benchmark_price_change_pct,
            benchmark_flow_ratio,
        )
        flow_score = (flow_ratio * 250.0) + ((last_rvol - 1.0) * 6.0) + price_change_pct
        verdict = final_audit_verdict(flow_score, divergence_flag, etf_flag)

        chart_source = work[["Close", "cum_dollar_flow", "rvol", "dollar_volume"]].tail(FLOW_AUDIT_CHART_BARS)
        chart_frame = pd.DataFrame(index=chart_source.index)
        if not chart_source.empty and float(chart_source["Close"].iloc[0]) != 0.0:
            chart_frame["price_rebased"] = chart_source["Close"] / float(chart_source["Close"].iloc[0]) * 100.0
        if benchmark in benchmark_chart_map:
            benchmark_chart = benchmark_chart_map[benchmark].tail(len(chart_source))
            benchmark_chart = benchmark_chart.reindex(chart_source.index).ffill().bfill()
            if not benchmark_chart.empty:
                chart_frame["benchmark_price_rebased"] = benchmark_chart
        chart_frame["cum_dollar_flow"] = chart_source["cum_dollar_flow"]
        chart_frame["rvol"] = chart_source["rvol"]
        chart_frame["dollar_volume"] = chart_source["dollar_volume"]
        charts[symbol] = chart_frame

        rows.append(
            {
                "symbol": symbol,
                "observed_sector": sector,
                "observed_price_change_pct": round(price_change_pct, 2),
                "observed_rvol": round(last_rvol, 2),
                "observed_dollar_volume": round(last_dollar_volume, 2),
                "observed_cumulative_dollar_flow": round(cumulative_flow, 2),
                "observed_benchmark": benchmark,
                "observed_data_source": "yfinance_1h",
                "observed_freshness_flag": freshness_flag,
                "observed_source_quality_flag": source_quality_flag,
                "observed_data_age_hours": (
                    round(float(data_age_hours), 2) if data_age_hours is not None else None
                ),
                "derived_flow_ratio": round(flow_ratio, 4),
                "derived_flow_score": round(flow_score, 2),
                "derived_divergence_strength": round(divergence_strength, 2),
                "derived_benchmark_price_change_pct": (
                    round(float(benchmark_price_change_pct), 2)
                    if benchmark_price_change_pct is not None else None
                ),
                "derived_benchmark_flow_ratio": (
                    round(float(benchmark_flow_ratio), 4)
                    if benchmark_flow_ratio is not None else None
                ),
                "verdict_divergence_flag": divergence_flag,
                "verdict_etf_confirmation_flag": etf_flag,
                "verdict_final_audit_verdict": verdict,
            }
        )

    if not rows:
        return pd.DataFrame(), charts, "No usable flow-audit rows."
    return pd.DataFrame(rows).sort_values(
        by=["derived_flow_score", "observed_cumulative_dollar_flow"],
        ascending=[False, False],
    ), charts, None


perf = RenderTimer()

snapshot, snapshot_err = load_snapshot()
market_context, market_err = load_market_context()
status, status_err = perf.measure("db_query", load_sniper_status)
risk_state, risk_err = perf.measure("db_query", load_daily_risk_state)
approved, approved_err = load_approved()
broker, broker_err = load_broker_snapshot()
recent_signals, signals_err = perf.measure("db_query", load_recent_signals, 30)
signal_counts, counts_err = perf.measure("db_query", load_signal_counts)
hot_signals, hot_err = perf.measure("db_query", load_top_movers)
market_snapshot, market_snapshot_err = load_market_snapshot_file()
market_snapshot_rows, market_snapshot_rows_err = load_market_snapshot_rows(25)
recent_trades, trades_err = perf.measure("db_query", load_recent_trades, 20)
trade_summary, summary_err = perf.measure("db_query", load_trade_summary)
open_trades, open_trades_err = perf.measure("db_query", load_open_trades)
log_lines, log_err = load_sniper_log(80)
exposure_summary = perf.measure("dataframe_transform", build_exposure_summary, broker.get("positions", []))
flow_audit_symbols = unique_symbols(
    broker.get("positions", []) and [row.get("symbol") for row in broker.get("positions", [])],
    open_trades["symbol"].tolist() if not open_trades.empty and "symbol" in open_trades.columns else [],
    recent_signals["symbol"].tolist() if not recent_signals.empty and "symbol" in recent_signals.columns else [],
    market_snapshot.get("top_symbols", []) if isinstance(market_snapshot, dict) else [],
    market_snapshot_rows["symbol"].tolist() if not market_snapshot_rows.empty and "symbol" in market_snapshot_rows.columns else [],
    approved.get("buy", []) if isinstance(approved, dict) else [],
    approved.get("sell", []) if isinstance(approved, dict) else [],
)
latest_signal_lookup = perf.measure("db_query", extract_latest_signal_lookup, flow_audit_symbols)
flow_audit_df, flow_audit_charts, flow_audit_err = load_flow_audit_market(tuple(flow_audit_symbols))
if not flow_audit_df.empty:
    flow_audit_df = perf.measure(
        "dataframe_transform",
        enrich_flow_audit_table,
        tuple(flow_audit_df.itertuples(index=False, name=None)),
        tuple(flow_audit_df.columns),
        tuple(sorted(latest_signal_lookup.items())),
    )
else:
    flow_audit_df = pd.DataFrame()
top_opportunities_df = perf.measure(
    "dataframe_transform",
    build_top_opportunities,
    tuple(market_snapshot_rows.itertuples(index=False, name=None)) if not market_snapshot_rows.empty else (),
    tuple(market_snapshot_rows.columns) if not market_snapshot_rows.empty else (),
    tuple(flow_audit_df.itertuples(index=False, name=None)) if not flow_audit_df.empty else (),
    tuple(flow_audit_df.columns) if not flow_audit_df.empty else (),
    tuple(recent_signals.itertuples(index=False, name=None)) if not recent_signals.empty else (),
    tuple(recent_signals.columns) if not recent_signals.empty else (),
    tuple(str(symbol).upper() for symbol in approved.get("buy", [])) if isinstance(approved, dict) else (),
    tuple(str(symbol).upper() for symbol in approved.get("sell", [])) if isinstance(approved, dict) else (),
    6,
)

now_cst = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S CST")
final_regime = market_context.get("final_regime") or snapshot.get("regime") or "UNKNOWN"
state_label, regime_css, mode_label = regime_badge(final_regime)
vix = market_context.get("vix", snapshot.get("vix", 0))
spy = market_context.get("spy_move_pct")
qqq = market_context.get("qqq_move_pct")
iwm = market_context.get("iwm_move_pct")
oil = market_context.get("oil_move_pct")
tnx = market_context.get("tnx", snapshot.get("tnx", 0))

title_col, meta_col = st.columns([3, 1])
with title_col:
    st.markdown('<p class="god-title">GOD MODE</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="god-sub">Autonomous Signal Intelligence · Live Ops Surface</p>',
        unsafe_allow_html=True,
    )
with meta_col:
    st.markdown(
        f'<div class="panel" style="text-align:right;font-size:0.72rem;">'
        f'<div>{now_cst}</div>'
        f'<div style="margin-top:0.35rem;color:#2a7a2a;">Caches refresh every 15-60s</div>'
        f"</div>",
        unsafe_allow_html=True,
    )

if not market_snapshot_rows.empty:
    ticker_started = time.perf_counter()
    ticker_html = get_or_create_ticker_html(market_snapshot_rows)
    perf.add("dataframe_transform", time.perf_counter() - ticker_started)
    st.markdown(ticker_html, unsafe_allow_html=True)

if market_err:
    render_banner(f"Market context degraded: {market_err}", "warn")
if broker_err:
    render_banner(f"Broker snapshot unavailable: {broker_err}", "warn")
if status_err:
    render_banner(f"Sniper status unavailable: {status_err}", "warn")
if risk_err:
    render_banner(f"Daily risk state unavailable: {risk_err}", "warn")

status_ts = status.get("ts_utc")
status_age = file_age_seconds(DB_PATH)
heartbeat_age = None
if status_ts:
    try:
        heartbeat_dt = datetime.strptime(status_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        heartbeat_age = int((utc_now() - heartbeat_dt).total_seconds())
    except ValueError:
        heartbeat_age = None

regime_age = file_age_seconds(REGIME_PATH)
approved_age = file_age_seconds(APPROVED_PATH)
log_age = file_age_seconds(SNIPER_LOG)
snapshot_discovery_age = file_age_seconds(MARKET_SNAPSHOT_JSON)

sidebar = st.sidebar
sidebar.markdown("### Data Health")
sidebar.markdown(
    f"- Snapshot: <span class='{health_class(regime_age, 180, 600)}'>{age_label(regime_age)}</span>",
    unsafe_allow_html=True,
)
sidebar.markdown(
    f"- Heartbeat: <span class='{health_class(heartbeat_age, 120, 300)}'>{age_label(heartbeat_age)}</span>",
    unsafe_allow_html=True,
)
sidebar.markdown(
    f"- Approved list: <span class='{health_class(approved_age, 1800, 14400)}'>{age_label(approved_age)}</span>",
    unsafe_allow_html=True,
)
sidebar.markdown(
    f"- Sniper log: <span class='{health_class(log_age, 180, 900)}'>{age_label(log_age)}</span>",
    unsafe_allow_html=True,
)
sidebar.markdown(
    f"- Discovery snapshot: <span class='{health_class(snapshot_discovery_age, 14400, 43200)}'>{age_label(snapshot_discovery_age)}</span>",
    unsafe_allow_html=True,
)

tabs = st.tabs(["Overview", "Execution", "Signals", "Discovery", "Flow Audit", "System"])
system_perf_placeholder = None

with tabs[0]:
    st.markdown('<p class="section-header">Market Regime</p>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(
            f'<div class="panel" style="text-align:center;">'
            f'<div style="font-size:0.65rem;color:#4aff4a;letter-spacing:0.2em;">TRADING MODE</div>'
            f'<div class="{regime_css}" style="margin-top:0.45rem;font-size:1.05rem;">{mode_label}</div>'
            f'<div style="margin-top:0.35rem;font-size:0.7rem;color:#7aff7a;">{state_label}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.metric("VIX", f"{float(vix or 0):.1f}")
    with c3:
        st.metric("SPY", fmt_pct(spy))
    with c4:
        st.metric("QQQ", fmt_pct(qqq))
    with c5:
        st.metric("IWM", fmt_pct(iwm))
    with c6:
        st.metric("OIL / TNX", f"{fmt_pct(oil)} / {float(tnx or 0):.3f}")

    st.markdown('<p class="section-header">Risk And Bot Status</p>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    live_equity = broker.get("equity")
    start_balance = risk_state.get("start_balance")
    daily_pnl = None
    if live_equity is not None and start_balance is not None:
        daily_pnl = float(live_equity) - float(start_balance)
    with c1:
        st.metric("Bot Status", status.get("status", "UNKNOWN"))
    with c2:
        st.metric("Bot Version", status.get("bot_version", "—"))
    with c3:
        st.metric("Halt Mode", "ON" if int(risk_state.get("halt_mode", 0) or 0) else "OFF")
    with c4:
        st.metric("Start Balance", fmt_money(start_balance))
    with c5:
        st.metric("Live Equity", fmt_money(live_equity))
    with c6:
        st.metric("Daily P&L", fmt_money(daily_pnl))

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Open Positions", len(broker.get("positions", [])))
    with c2:
        st.metric("Buying Power", fmt_money(broker.get("buying_power")))
    with c3:
        st.metric("Long Approved", len(approved.get("buy", [])))
    with c4:
        st.metric("Short Approved", len(approved.get("sell", [])))

    note = status.get("note") or "—"
    if int(risk_state.get("halt_mode", 0) or 0):
        render_banner(
            f"HALT MODE ACTIVE | trading_day={risk_state.get('trading_day', '—')} | note={note}",
            "bad",
        )
    else:
        render_banner(
            f"Trading enabled | trading_day={risk_state.get('trading_day', '—')} | note={note}",
            "good",
        )

    st.markdown('<p class="section-header">Discovery Pulse</p>', unsafe_allow_html=True)
    discovery_summary = market_snapshot.get("summary", {}) if isinstance(market_snapshot, dict) else {}
    discovery_top_symbols = market_snapshot.get("top_symbols", []) if isinstance(market_snapshot, dict) else []
    discovery_top_sector = (discovery_summary.get("sector_summary") or [{}])[0]
    discovery_tone = discovery_summary.get("market_tone", "unknown")
    overview_symbols = ", ".join(discovery_top_symbols[:3]) if discovery_top_symbols else "—"
    overview_signal_mix = discovery_summary.get("signal_counts") or []
    top_signal_label = overview_signal_mix[0][0] if overview_signal_mix else "—"
    top_signal_count = overview_signal_mix[0][1] if overview_signal_mix else "—"
    st.markdown(
        f'<div class="overview-discovery">'
        f'<div class="overview-discovery-title">Broader Discovery Engine</div>'
        f'<div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;margin-top:0.2rem;">'
        f'<div class="overview-discovery-main">'
        f'Tone is <span class="tone-chip {"tone-short" if "short" in str(discovery_tone) else "tone-long" if "long" in str(discovery_tone) else "tone-mixed"}">{discovery_tone}</span>. '
        f'Top names right now: <strong>{overview_symbols}</strong>. '
        f'Lead sector: <strong>{discovery_top_sector.get("sector", "—")}</strong>.'
        f'</div>'
        f'<div class="overview-discovery-side">'
        f'<div>Snapshot age: {age_label(snapshot_discovery_age)}</div>'
        f'<div>Top signal family: {top_signal_label} ({top_signal_count})</div>'
        f'<div>Filtered names: {market_snapshot.get("passed_filters", "—")} / {market_snapshot.get("universe_count", "—")}</div>'
        f'</div>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<p class="section-header">Roster Snapshot</p>', unsafe_allow_html=True)
    buy_col, sell_col = st.columns(2)
    with buy_col:
        st.markdown("**Long List**")
        st.write(", ".join(approved.get("buy", [])) or "—")
    with sell_col:
        st.markdown("**Short List**")
        st.write(", ".join(approved.get("sell", [])) or "—")

with tabs[1]:
    st.markdown('<p class="section-header">Live Positions</p>', unsafe_allow_html=True)
    positions = broker.get("positions", [])
    if positions:
        pos_df = perf.measure(
            "dataframe_transform",
            format_positions_table,
            tuple(
                (
                    row.get("symbol"),
                    row.get("side"),
                    row.get("qty"),
                    row.get("market_value"),
                    row.get("entry_price"),
                    row.get("current_price"),
                    row.get("unrealized_pl"),
                    row.get("unrealized_plpc"),
                )
                for row in positions
            ),
        )
        render_timed_dataframe(perf, pos_df, width="stretch", hide_index=True, height=260)
    else:
        st.info("No live broker positions returned.")

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown('<p class="section-header">Trade Log</p>', unsafe_allow_html=True)
        if not recent_trades.empty:
            trade_df = perf.measure(
                "dataframe_transform",
                format_trade_log_table,
                tuple(recent_trades.itertuples(index=False, name=None)),
                tuple(recent_trades.columns),
            )
            render_timed_dataframe(perf, trade_df, width="stretch", hide_index=True, height=320)
        else:
            st.info(trades_err or "No trade history available.")
    with c2:
        st.markdown('<p class="section-header">Trade Outcomes · 7d</p>', unsafe_allow_html=True)
        if not trade_summary.empty:
            for _, row in trade_summary.iterrows():
                outcome = str(row["outcome"] or "unknown")
                count = int(row["count"])
                st.metric(outcome.upper(), count)
        else:
            st.info(summary_err or "No recent trade summary.")

    st.markdown('<p class="section-header">Open Trade Records</p>', unsafe_allow_html=True)
    if not open_trades.empty:
        render_timed_dataframe(perf, open_trades, width="stretch", hide_index=True, height=200)
    else:
        st.info(open_trades_err or "No open trade rows in trade_log.db.")

with tabs[2]:
    left, right = st.columns([3, 2])
    with left:
        st.markdown('<p class="section-header">Recent Signals</p>', unsafe_allow_html=True)
        if not recent_signals.empty:
            df = perf.measure(
                "dataframe_transform",
                format_recent_signals_table,
                tuple(recent_signals.itertuples(index=False, name=None)),
                tuple(recent_signals.columns),
            )
            render_timed_dataframe(perf, df, width="stretch", hide_index=True, height=340)
        else:
            st.info(signals_err or "No recent signals.")

        st.markdown('<p class="section-header">Hot Signals · 4h</p>', unsafe_allow_html=True)
        if not hot_signals.empty:
            hot_df = perf.measure(
                "dataframe_transform",
                format_hot_signals_table,
                tuple(hot_signals.itertuples(index=False, name=None)),
                tuple(hot_signals.columns),
            )
            render_timed_dataframe(perf, hot_df, width="stretch", hide_index=True, height=260)
        else:
            st.info(hot_err or "No strong signals in the last 4 hours.")
    with right:
        st.markdown('<p class="section-header">Signal Mix · 24h</p>', unsafe_allow_html=True)
        if not signal_counts.empty:
            counts_df = signal_counts.copy()
            counts_df["count"] = counts_df["count"].astype(int)
            render_timed_dataframe(perf, counts_df, width="stretch", hide_index=True, height=300)
        else:
            st.info(counts_err or "No signal count data.")

with tabs[3]:
    st.markdown('<p class="section-header">Discovery Market Snapshot</p>', unsafe_allow_html=True)
    if market_snapshot_err:
        render_banner(f"Discovery snapshot unavailable: {market_snapshot_err}", "warn")

    summary = market_snapshot.get("summary", {}) if isinstance(market_snapshot, dict) else {}
    bias_counts = summary.get("bias_counts", {}) if isinstance(summary, dict) else {}
    sector_summary = summary.get("sector_summary", []) if isinstance(summary, dict) else []
    signal_mix = summary.get("signal_counts", []) if isinstance(summary, dict) else []
    top_symbols = market_snapshot.get("top_symbols", []) if isinstance(market_snapshot, dict) else []
    market_tone = str(summary.get("market_tone", "unknown")).lower()
    tone_class = "tone-mixed"
    if "long" in market_tone:
        tone_class = "tone-long"
    elif "short" in market_tone:
        tone_class = "tone-short"
    elif "neutral" in market_tone:
        tone_class = "tone-neutral"

    card1, card2, card3, card4 = st.columns(4)
    with card1:
        st.markdown(
            f'<div class="snapshot-card"><div class="snapshot-kicker">Market Tone</div>'
            f'<div class="snapshot-value"><span class="tone-chip {tone_class}">{market_tone or "unknown"}</span></div>'
            f'<div class="snapshot-sub">Universe: {market_snapshot.get("universe_mode", "—")} · '
            f'Passed filters: {market_snapshot.get("passed_filters", "—")}</div></div>',
            unsafe_allow_html=True,
        )
    with card2:
        st.markdown(
            f'<div class="snapshot-card"><div class="snapshot-kicker">Bias Mix</div>'
            f'<div class="snapshot-value">{bias_counts.get("long", 0)} / {bias_counts.get("short", 0)}</div>'
            f'<div class="snapshot-sub">Long candidates / short candidates<br>'
            f'Event names: {bias_counts.get("event", 0)} · Caution: {bias_counts.get("caution", 0)}</div></div>',
            unsafe_allow_html=True,
        )
    with card3:
        top_sector = sector_summary[0] if sector_summary else {}
        st.markdown(
            f'<div class="snapshot-card"><div class="snapshot-kicker">Leading Sector</div>'
            f'<div class="snapshot-value">{top_sector.get("sector", "—")}</div>'
            f'<div class="snapshot-sub">Avg score: {top_sector.get("avg_score", "—")} · '
            f'Names: {top_sector.get("count", "—")}</div></div>',
            unsafe_allow_html=True,
        )
    with card4:
        st.markdown(
            f'<div class="snapshot-card"><div class="snapshot-kicker">Snapshot Freshness</div>'
            f'<div class="snapshot-value">{age_label(snapshot_discovery_age)}</div>'
            f'<div class="snapshot-sub">Updated: {market_snapshot.get("generated", "—")}<br>'
            f'Log: {MARKET_SNAPSHOT_LOG}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown('<p class="section-header">Decision Layer</p>', unsafe_allow_html=True)
    if top_opportunities_df.empty:
        st.info("No ranked opportunities available from the current discovery snapshot.")
    else:
        lead = top_opportunities_df.iloc[0].to_dict()
        lead_strategy_flag = lead.get("strategy_alignment_flag") or "not strategy-flagged"
        lead_risk_flag = lead.get("risk_flag") or "no immediate risk flag"
        render_banner(
            f"Top opportunity now: {lead.get('symbol', '—')} | {lead.get('quality_label', '—')} | "
            f"{lead.get('reason_text', '—')} | {lead_strategy_flag} | {lead_risk_flag}",
            "good" if str(lead.get("quality_label")) != "LOW QUALITY" else "warn",
        )
        decision_cols = st.columns(2)
        for index, (_, row) in enumerate(top_opportunities_df.iterrows()):
            with decision_cols[index % 2]:
                render_decision_card(row.to_dict(), index + 1)

    left, right = st.columns([3, 2])
    with left:
        st.markdown('<p class="section-header">Top Discovery Names</p>', unsafe_allow_html=True)
        if not market_snapshot_rows.empty:
            df = perf.measure(
                "dataframe_transform",
                format_market_snapshot_table,
                tuple(market_snapshot_rows.itertuples(index=False, name=None)),
                tuple(market_snapshot_rows.columns),
            )
            render_timed_dataframe(perf, df, width="stretch", hide_index=True, height=380)
        else:
            st.info(market_snapshot_rows_err or "No discovery snapshot rows available.")

        st.markdown('<p class="section-header">Top Symbols</p>', unsafe_allow_html=True)
        if top_symbols:
            pills = " ".join(
                [f'<span class="symbol-pill">{symbol}</span>' for symbol in top_symbols[:20]]
            )
            st.markdown(pills, unsafe_allow_html=True)
        else:
            st.info("No top symbols in the current snapshot.")

    with right:
        st.markdown('<p class="section-header">Signal Mix</p>', unsafe_allow_html=True)
        if signal_mix:
            mix_rows = pd.DataFrame(signal_mix, columns=["signal_type", "count"])
            render_timed_dataframe(perf, mix_rows, width="stretch", hide_index=True, height=220)
        else:
            st.info("No discovery signal mix available.")

        st.markdown('<p class="section-header">Sector Leaders</p>', unsafe_allow_html=True)
        if sector_summary:
            for sector in sector_summary[:6]:
                leader_pills = " ".join(
                    [f'<span class="sector-pill">{sym}</span>' for sym in sector.get("leaders", [])]
                )
                st.markdown(
                    f'<div class="panel" style="margin-bottom:0.55rem;">'
                    f'<div style="display:flex;justify-content:space-between;gap:0.8rem;">'
                    f'<div style="color:#dfffea;font-family:Orbitron;">{sector.get("sector", "—")}</div>'
                    f'<div style="color:#79c995;">avg {sector.get("avg_score", "—")} · count {sector.get("count", "—")}</div>'
                    f'</div><div style="margin-top:0.45rem;">{leader_pills}</div></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No sector summary available.")

with tabs[4]:
    st.markdown('<p class="section-header">Flow Audit</p>', unsafe_allow_html=True)
    if flow_audit_err:
        render_banner(f"Flow audit degraded: {flow_audit_err}", "warn")

    if flow_audit_df.empty:
        st.info(flow_audit_err or "No flow audit data available.")
    else:
        symbol_options = flow_audit_df["symbol"].tolist()
        default_symbol = symbol_options[0] if symbol_options else None
        selected_symbol = st.selectbox(
            "Single-symbol Flow Audit",
            symbol_options,
            index=0 if default_symbol else None,
        )
        selected_row = flow_audit_df.loc[flow_audit_df["symbol"] == selected_symbol].iloc[0]
        selected_chart = flow_audit_charts.get(selected_symbol, pd.DataFrame())

        top_metrics = st.columns(6)
        with top_metrics[0]:
            st.metric("Sector", str(selected_row["observed_sector"]))
        with top_metrics[1]:
            st.metric("Signal Family", str(selected_row["observed_signal_family"]).replace("_", " "))
        with top_metrics[2]:
            st.metric("Price Change", fmt_pct(selected_row["observed_price_change_pct"]))
        with top_metrics[3]:
            st.metric("RVOL", f"{float(selected_row['observed_rvol']):.2f}x")
        with top_metrics[4]:
            st.metric("Dollar Volume", fmt_money(selected_row["observed_dollar_volume"]))
        with top_metrics[5]:
            st.metric("Cumulative Flow", fmt_money(selected_row["observed_cumulative_dollar_flow"]))

        quality_cols = st.columns(4)
        with quality_cols[0]:
            st.metric("Source", str(selected_row["observed_data_source"]))
        with quality_cols[1]:
            st.metric("Freshness", str(selected_row["observed_freshness_flag"]))
        with quality_cols[2]:
            st.metric("Source Quality", str(selected_row["observed_source_quality_flag"]))
        with quality_cols[3]:
            st.metric(
                "Data Age",
                "—" if pd.isna(selected_row["observed_data_age_hours"]) else f"{float(selected_row['observed_data_age_hours']):.1f}h",
            )

        left, right = st.columns(2)
        with left:
            st.markdown("**Price vs Benchmark**")
            if not selected_chart.empty and "price_rebased" in selected_chart.columns:
                price_cols = [column for column in ("price_rebased", "benchmark_price_rebased") if column in selected_chart.columns]
                render_timed_line_chart(perf, selected_chart[price_cols], height=260)
            else:
                st.info("No price chart available.")
        with right:
            st.markdown("**Cumulative Dollar Flow**")
            if not selected_chart.empty and "cum_dollar_flow" in selected_chart.columns:
                render_timed_line_chart(perf, selected_chart[["cum_dollar_flow"]], height=260)
            else:
                st.info("No flow chart available.")

        left, right = st.columns(2)
        with left:
            st.markdown("**RVOL**")
            if not selected_chart.empty and "rvol" in selected_chart.columns:
                render_timed_line_chart(perf, selected_chart[["rvol"]], height=220)
            else:
                st.info("No RVOL series available.")
        with right:
            st.markdown("**Dollar Volume**")
            if not selected_chart.empty and "dollar_volume" in selected_chart.columns:
                render_timed_line_chart(perf, selected_chart[["dollar_volume"]], height=220)
            else:
                st.info("No dollar-volume series available.")

        audit_banner = (
            f"{selected_symbol} | divergence={selected_row['verdict_divergence_flag']} | "
            f"ETF feature={selected_row['observed_benchmark']} {selected_row['verdict_etf_confirmation_flag']} | "
            f"verdict={selected_row['verdict_final_audit_verdict']}"
        )
        render_banner(
            audit_banner,
            "good" if "Positive flow" in str(selected_row["verdict_final_audit_verdict"]) else "warn",
        )

        obs_col, drv_col, vrd_col = st.columns(3)
        with obs_col:
            st.markdown("**Observed Metrics**")
            observed_df = pd.DataFrame(
                [
                    {"field": "sector", "value": selected_row["observed_sector"]},
                    {"field": "signal_family", "value": selected_row["observed_signal_family"]},
                    {"field": "bias", "value": selected_row["observed_bias"]},
                    {"field": "price_change_pct", "value": fmt_pct(selected_row["observed_price_change_pct"])},
                    {"field": "rvol", "value": f"{float(selected_row['observed_rvol']):.2f}x"},
                    {"field": "dollar_volume", "value": fmt_money(selected_row["observed_dollar_volume"])},
                    {"field": "cumulative_dollar_flow", "value": fmt_money(selected_row["observed_cumulative_dollar_flow"])},
                    {"field": "benchmark", "value": selected_row["observed_benchmark"]},
                ]
            )
            render_timed_dataframe(perf, observed_df, width="stretch", hide_index=True, height=260)
        with drv_col:
            st.markdown("**Derived Metrics**")
            derived_df = pd.DataFrame(
                [
                    {"field": "flow_ratio", "value": f"{float(selected_row['derived_flow_ratio']):.4f}"},
                    {"field": "flow_score", "value": f"{float(selected_row['derived_flow_score']):.2f}"},
                    {"field": "divergence_strength", "value": f"{float(selected_row['derived_divergence_strength']):.2f}"},
                    {
                        "field": "benchmark_price_change_pct",
                        "value": "—" if pd.isna(selected_row["derived_benchmark_price_change_pct"])
                        else fmt_pct(selected_row["derived_benchmark_price_change_pct"]),
                    },
                    {
                        "field": "benchmark_flow_ratio",
                        "value": "—" if pd.isna(selected_row["derived_benchmark_flow_ratio"])
                        else f"{float(selected_row['derived_benchmark_flow_ratio']):.4f}",
                    },
                ]
            )
            render_timed_dataframe(perf, derived_df, width="stretch", hide_index=True, height=260)
        with vrd_col:
            st.markdown("**Interpretive Verdicts**")
            verdict_df = pd.DataFrame(
                [
                    {"field": "divergence_flag", "value": selected_row["verdict_divergence_flag"]},
                    {"field": "etf_confirmation_feature", "value": selected_row["verdict_etf_confirmation_flag"]},
                    {"field": "final_audit_verdict", "value": selected_row["verdict_final_audit_verdict"]},
                    {"field": "freshness_flag", "value": selected_row["observed_freshness_flag"]},
                    {"field": "source_quality_flag", "value": selected_row["observed_source_quality_flag"]},
                ]
            )
            render_timed_dataframe(perf, verdict_df, width="stretch", hide_index=True, height=260)

        st.markdown('<p class="section-header">Exposure Summary</p>', unsafe_allow_html=True)
        exposure_cols = st.columns(3)
        with exposure_cols[0]:
            st.metric("Gross Long", fmt_money(exposure_summary["gross_long"]))
        with exposure_cols[1]:
            st.metric("Gross Short", fmt_money(exposure_summary["gross_short"]))
        with exposure_cols[2]:
            st.metric("Gross Total", fmt_money(exposure_summary["gross_total"]))

        exp_left, exp_right = st.columns(2)
        with exp_left:
            sector_exposure_df = pd.DataFrame(
                [
                    {"sector": key, "gross_exposure": value}
                    for key, value in sorted(
                        exposure_summary["gross_by_sector"].items(),
                        key=lambda item: (-item[1], item[0]),
                    )
                ]
            )
            if sector_exposure_df.empty:
                st.info("No sector exposure.")
            else:
                sector_exposure_df["gross_exposure"] = sector_exposure_df["gross_exposure"].map(fmt_money)
                sector_exposure_df.columns = ["SECTOR", "GROSS"]
                render_timed_dataframe(perf, sector_exposure_df, width="stretch", hide_index=True, height=220)
        with exp_right:
            symbol_exposure_df = pd.DataFrame(
                [
                    {"symbol": key, "gross_exposure": value}
                    for key, value in sorted(
                        exposure_summary["gross_by_symbol"].items(),
                        key=lambda item: (-item[1], item[0]),
                    )
                ]
            )
            if symbol_exposure_df.empty:
                st.info("No symbol exposure.")
            else:
                symbol_exposure_df["gross_exposure"] = symbol_exposure_df["gross_exposure"].map(fmt_money)
                symbol_exposure_df.columns = ["SYMBOL", "GROSS"]
                render_timed_dataframe(perf, symbol_exposure_df, width="stretch", hide_index=True, height=220)

        st.markdown('<p class="section-header">Where Money Is Going</p>', unsafe_allow_html=True)
        ranked_started = time.perf_counter()
        ranked_df = flow_audit_df.copy()
        ranked_df["observed_price_change_pct"] = ranked_df["observed_price_change_pct"].map(fmt_pct)
        ranked_df["observed_rvol"] = ranked_df["observed_rvol"].map(lambda value: f"{float(value):.2f}x")
        ranked_df["observed_dollar_volume"] = ranked_df["observed_dollar_volume"].map(fmt_money)
        ranked_df["observed_cumulative_dollar_flow"] = ranked_df["observed_cumulative_dollar_flow"].map(fmt_money)
        ranked_df["derived_flow_score"] = ranked_df["derived_flow_score"].map(lambda value: f"{float(value):.2f}")
        ranked_df = ranked_df[
            [
                "symbol", "observed_sector", "observed_signal_family", "observed_bias",
                "observed_price_change_pct", "observed_rvol", "observed_dollar_volume",
                "observed_cumulative_dollar_flow", "derived_flow_score", "verdict_divergence_flag",
                "verdict_etf_confirmation_flag", "verdict_final_audit_verdict",
            ]
        ]
        ranked_df.columns = [
            "SYMBOL", "SECTOR", "SIGNAL FAMILY", "BIAS", "PRICE CHG %", "RVOL", "$ VOLUME",
            "CUM $ FLOW", "FLOW SCORE", "DIVERGENCE", "ETF CONFIRM", "VERDICT",
        ]
        perf.add("dataframe_transform", time.perf_counter() - ranked_started)
        render_timed_dataframe(perf, ranked_df, width="stretch", hide_index=True, height=360)

        st.markdown('<p class="section-header">Sector Rotation Summary</p>', unsafe_allow_html=True)
        sector_started = time.perf_counter()
        sector_rotation = (
            flow_audit_df.groupby("observed_sector", dropna=False)
            .agg(
                names=("symbol", "count"),
                avg_flow_score=("derived_flow_score", "mean"),
                avg_rvol=("observed_rvol", "mean"),
                total_cum_flow=("observed_cumulative_dollar_flow", "sum"),
            )
            .reset_index()
            .sort_values(by=["avg_flow_score", "total_cum_flow"], ascending=[False, False])
        )
        sector_rotation["avg_flow_score"] = sector_rotation["avg_flow_score"].map(lambda value: f"{float(value):.2f}")
        sector_rotation["avg_rvol"] = sector_rotation["avg_rvol"].map(lambda value: f"{float(value):.2f}x")
        sector_rotation["total_cum_flow"] = sector_rotation["total_cum_flow"].map(fmt_money)
        sector_rotation.columns = ["SECTOR", "NAMES", "AVG FLOW SCORE", "AVG RVOL", "TOTAL CUM FLOW"]
        perf.add("dataframe_transform", time.perf_counter() - sector_started)
        render_timed_dataframe(perf, sector_rotation, width="stretch", hide_index=True, height=220)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**Top Positive Flow Names**")
            positive_df = flow_audit_df.nlargest(8, "derived_flow_score")[
                ["symbol", "observed_sector", "derived_flow_score", "verdict_final_audit_verdict"]
            ].copy()
            positive_df["derived_flow_score"] = positive_df["derived_flow_score"].map(lambda value: f"{float(value):.2f}")
            positive_df.columns = ["SYMBOL", "SECTOR", "FLOW SCORE", "VERDICT"]
            render_timed_dataframe(perf, positive_df, width="stretch", hide_index=True, height=260)
        with col2:
            st.markdown("**Top Negative Flow Names**")
            negative_df = flow_audit_df.nsmallest(8, "derived_flow_score")[
                ["symbol", "observed_sector", "derived_flow_score", "verdict_final_audit_verdict"]
            ].copy()
            negative_df["derived_flow_score"] = negative_df["derived_flow_score"].map(lambda value: f"{float(value):.2f}")
            negative_df.columns = ["SYMBOL", "SECTOR", "FLOW SCORE", "VERDICT"]
            render_timed_dataframe(perf, negative_df, width="stretch", hide_index=True, height=260)
        with col3:
            st.markdown("**Biggest Price / Flow Divergences**")
            divergence_df = flow_audit_df.loc[flow_audit_df["verdict_divergence_flag"] != "—"].copy()
            if divergence_df.empty:
                st.info("No major divergences detected.")
            else:
                divergence_df = divergence_df.nlargest(8, "derived_divergence_strength")[
                    ["symbol", "observed_sector", "verdict_divergence_flag", "verdict_final_audit_verdict"]
                ]
                divergence_df.columns = ["SYMBOL", "SECTOR", "DIVERGENCE", "VERDICT"]
                render_timed_dataframe(perf, divergence_df, width="stretch", hide_index=True, height=260)

with tabs[5]:
    system_perf_placeholder = st.empty()
    st.markdown('<p class="section-header">Operator Health</p>', unsafe_allow_html=True)
    health_rows = pd.DataFrame(
        [
            {"source": "regime_snapshot.json", "age": age_label(regime_age), "error": snapshot_err or ""},
            {"source": "sniper_status heartbeat", "age": age_label(heartbeat_age), "error": status_err or ""},
            {"source": "approved_symbols.json", "age": age_label(approved_age), "error": approved_err or ""},
            {"source": "sniper.log", "age": age_label(log_age), "error": log_err or ""},
            {"source": "market_snapshot.json", "age": age_label(snapshot_discovery_age), "error": market_snapshot_err or ""},
            {"source": "broker snapshot", "age": "cached", "error": broker_err or ""},
        ]
    )
    render_timed_dataframe(perf, health_rows, width="stretch", hide_index=True, height=220)

    st.markdown('<p class="section-header">Recent Sniper Log</p>', unsafe_allow_html=True)
    if log_lines:
        rendered = []
        for raw in log_lines:
            line = raw.strip()
            if "🚨" in line or "🛑" in line or "ERROR" in line:
                color = "#ff6666"
            elif "🚀" in line or "✅" in line:
                color = "#00ff41"
            elif "📉" in line or "⚠️" in line or "⛔" in line:
                color = "#ffaa00"
            elif "🫀" in line:
                color = "#2a7a2a"
            else:
                color = "#7aff7a"
            rendered.append(f'<span style="color:{color}">{line}</span>')
        st.markdown(f'<div class="log-box">{"<br>".join(rendered)}</div>', unsafe_allow_html=True)
    else:
        st.info(log_err or "No log lines available.")

st.markdown("---")
st.markdown(
    f'<div style="text-align:center;font-size:0.62rem;color:#1a4a1a;letter-spacing:0.16em;">'
    f'GOD MODE · {state_label} · VIX {float(vix or 0):.1f} · {now_cst}'
    f"</div>",
    unsafe_allow_html=True,
)

if system_perf_placeholder is not None:
    perf_rows = pd.DataFrame(
        [
            {"stage": "total_page_render", "seconds": round(perf.total_page_render(), 4)},
            {"stage": "db_query_time", "seconds": round(perf.totals["db_query"], 4)},
            {"stage": "dataframe_transform_time", "seconds": round(perf.totals["dataframe_transform"], 4)},
            {"stage": "chart_build_time", "seconds": round(perf.totals["chart_build"], 4)},
            {"stage": "table_render_time", "seconds": round(perf.totals["table_render"], 4)},
        ]
    )
    system_perf_placeholder.markdown('<p class="section-header">Render Performance</p>', unsafe_allow_html=True)
    render_timed_dataframe(perf, perf_rows, width="stretch", hide_index=True, height=220)
