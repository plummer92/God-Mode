import os
import sys
import json
import time
import sqlite3
import requests
import math
from datetime import datetime, timezone, date, timedelta
import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from Symbol_hunter import classify_sector
from market_context import (
    get_market_context,
    market_multiplier_for_direction,
    should_block_direction,
)

# -------------------- CONFIG --------------------
TRADE_NOTIONAL_USD = 25       # Trade size per signal
HARD_STOP_LOSS_PCT = 0.02     # -2.0% hard stop loss
TAKE_PROFIT_PCT = 0.02        # +2.0% take profit
BREAK_EVEN_ARM_PCT = 0.0075   # Arm break-even protection after +0.75% unrealized PnL
DAILY_LOSS_LIMIT_USD = 60.00  # Stop trading if down $60 in one day
MAX_OPEN_POSITIONS = 5        # Never hold more than 5 positions at once
MAX_SIGNAL_AGE_SECONDS = 900  # Skip entries if the scanner signal is older than 15 minutes
MAX_SIGNAL_DRIFT_BPS = 75     # Skip entries if live Alpaca price drifts more than 75 bps from signal
EXECUTION_TELEMETRY_LOOKBACK = 12
EXECUTION_TELEMETRY_CACHE_TTL_S = 300
EXECUTION_RISK_WARN_DRIFT_BPS = 60
EXECUTION_RISK_HAIRCUT_DRIFT_BPS = 65
EXECUTION_RISK_HAIRCUT_ENTRY_SLIP_BPS = 25
EXECUTION_RISK_HAIRCUT_MULTIPLIER = 0.85

ENABLE_CONFIDENCE_SIZING = True
MIN_TRADE_NOTIONAL_USD = 20.0
BASE_TRADE_NOTIONAL_USD = TRADE_NOTIONAL_USD
MAX_TRADE_NOTIONAL_USD = 35.0
MAX_GROSS_EXPOSURE_USD = 2000.0

# Paths
DB_PATH = "/home/theplummer92/wolfe_signals.db"
SNIPER_LOG = "/home/theplummer92/sniper.log"
REGIME_PATH = "/home/theplummer92/regime_snapshot.json"
LOCKFILE = "/tmp/sniper_bot.lock"
POLL_SECONDS = 10

BOT_VERSION = "SNIPER V8.0 (SHORTING ENABLED)"
STATUS_DB = DB_PATH
MARKET_CONTEXT_TRANSITION_ALERTS = True

# -------------------- SETUP --------------------
cst_tz = pytz.timezone("America/Chicago")
et_tz = pytz.timezone("America/New_York")
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")


def env_float(name: str, default=None):
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


MAX_RISK_PER_TRADE_USD = env_float("MAX_RISK_PER_TRADE_USD", None)
MAX_SINGLE_POSITION_GROSS_USD = env_float("MAX_SINGLE_POSITION_GROSS_USD", MAX_GROSS_EXPOSURE_USD)
MAX_SECTOR_GROSS_USD = env_float("MAX_SECTOR_GROSS_USD", MAX_GROSS_EXPOSURE_USD)
ENABLE_STRICT_GAP_BLOCK = False
ENABLE_STRICT_CORRELATION_BLOCK = False

TRADING_DEV_PATH = "/home/theplummer92/trading-dev"
if TRADING_DEV_PATH not in sys.path and os.path.isdir(TRADING_DEV_PATH):
    sys.path.append(TRADING_DEV_PATH)

try:
    from alpaca_data import get_latest_price as get_alpaca_latest_price
except Exception:
    get_alpaca_latest_price = None


# -------------------- LOGGING --------------------
_daily_start_balance = 0.0
_daily_start_date = None
_daily_loss_alerted = False
_daily_risk_state_loaded = False
_last_final_regime = None
_no_reentry_today: set = set()
_no_reentry_date: str = ""
_pending_closes: dict = {}
_halt_mode = False
_halt_mode_date = None
_last_halt_log_ts = 0.0
_skip_log_cache: dict = {}
_approved_symbols_cache: dict | None = None
_approved_symbols_mtime: float | None = None
_execution_telemetry_cache: dict = {}
_stop_model_cache: dict = {}

CLOSE_RETRY_COOLDOWN_S = 30
CLOSE_PENDING_LOG_INTERVAL_S = 30
CLOSE_STALE_TIMEOUT_S = 90
CLOSE_CANCEL_COOLDOWN_S = 30
SKIP_LOG_SUPPRESS_SECONDS = 300
HALT_MODE_LOG_INTERVAL_S = 300
STOP_MODEL_CACHE_TTL_S = 300
STOP_MODEL_ATR_BARS = 14
STOP_MODEL_LOOKBACK_BARS = 8
STOP_MODEL_ATR_MULTIPLIER = 1.5

PENDING_CLOSE_ORDER_STATUSES = (
    "new",
    "accepted",
    "pending_new",
    "partially_filled",
    "accepted_for_bidding",
    "pending_cancel",
)

def log_line(msg: str):
    ts = datetime.now(cst_tz).strftime("%Y-%m-%d %H:%M:%S")
    full = f"[{ts}] {msg}"
    print(full, flush=True)
    with open(SNIPER_LOG, "a", encoding="utf-8") as f:
        f.write(full + "\n")


def should_log_skip(key: str, interval_s: int = SKIP_LOG_SUPPRESS_SECONDS) -> bool:
    now = time.time()
    last_ts = float(_skip_log_cache.get(key, 0))
    if (now - last_ts) < interval_s:
        return False
    _skip_log_cache[key] = now
    return True


def post_discord(msg: str):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=5)
    except Exception as e:
        log_line(f"Discord alert failed: {e}")


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def current_trading_day() -> str:
    """
    Use the market/session date in America/Chicago for all daily reset logic.
    This avoids relying on the server's local timezone.
    """
    return datetime.now(cst_tz).date().isoformat()

TRADE_LOG_DB = "/home/theplummer92/trade_log.db"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def fmt_pct(value) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "n/a"


def fmt_num(value, digits: int = 2) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def calculate_slippage(decision_price, fill_price, direction: str, phase: str) -> dict:
    decision = float(decision_price)
    fill = float(fill_price)
    side = str(direction or "").upper()
    leg = str(phase or "").lower()

    if leg == "entry":
        adverse_dps = (fill - decision) if side == "LONG" else (decision - fill)
    elif leg == "exit":
        adverse_dps = (decision - fill) if side == "LONG" else (fill - decision)
    else:
        raise ValueError(f"unsupported slippage phase: {phase}")

    pct = (adverse_dps / decision) * 100.0 if decision else 0.0
    bps = pct * 100.0
    return {
        "dps": adverse_dps,
        "pct": pct,
        "bps": bps,
    }


def log_slippage(symbol: str, direction: str, phase: str, decision_price, fill_price, source: str):
    try:
        slip = calculate_slippage(decision_price, fill_price, direction, phase)
        log_line(
            f"↔️ SLIP {phase.upper()} {symbol} {direction} "
            f"decision=${float(decision_price):.4f} fill=${float(fill_price):.4f} "
            f"bps={slip['bps']:+.1f} source={source}"
        )
    except Exception as e:
        log_line(f"⚠️ Slippage log failed for {symbol} {phase}: {e}")

def init_trade_log():
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, side TEXT, direction TEXT,
            entry_price REAL, exit_price REAL,
            entry_time TEXT, exit_time TEXT,
            pnl_pct REAL, pnl_usd REAL,
            signal_type TEXT, notional REAL,
            outcome TEXT, vix REAL, regime TEXT,
            signal_price REAL, slippage_pct REAL,
            entry_order_id TEXT, exit_order_id TEXT,
            entry_price_source TEXT, entry_fill_confirmed INTEGER,
            entry_slippage_dps REAL, entry_slippage_bps REAL,
            signal_age_seconds REAL, entry_validation_price REAL,
            signal_to_live_drift_bps REAL,
            execution_quality_flag TEXT, execution_size_multiplier_used REAL,
            break_even_armed INTEGER, break_even_armed_at_utc TEXT,
            exit_decision_price REAL, exit_price_source TEXT, exit_fill_confirmed INTEGER,
            exit_slippage_dps REAL, exit_slippage_pct REAL, exit_slippage_bps REAL
        )""")
        # Add columns to existing tables that pre-date this schema change
        for col, coltype in (
            ("signal_price", "REAL"),
            ("slippage_pct", "REAL"),
            ("entry_order_id", "TEXT"),
            ("exit_order_id", "TEXT"),
            ("entry_price_source", "TEXT"),
            ("entry_fill_confirmed", "INTEGER"),
            ("entry_slippage_dps", "REAL"),
            ("entry_slippage_bps", "REAL"),
            ("signal_age_seconds", "REAL"),
            ("entry_validation_price", "REAL"),
            ("signal_to_live_drift_bps", "REAL"),
            ("execution_quality_flag", "TEXT"),
            ("execution_size_multiplier_used", "REAL"),
            ("break_even_armed", "INTEGER"),
            ("break_even_armed_at_utc", "TEXT"),
            ("exit_decision_price", "REAL"),
            ("exit_price_source", "TEXT"),
            ("exit_fill_confirmed", "INTEGER"),
            ("exit_slippage_dps", "REAL"),
            ("exit_slippage_pct", "REAL"),
            ("exit_slippage_bps", "REAL"),
        ):
            try:
                cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {coltype}")
            except Exception:
                pass  # column already exists
        cur.execute("""CREATE TABLE IF NOT EXISTS processed_signals (
            signal_key TEXT PRIMARY KEY,
            signal_ts TEXT,
            symbol TEXT,
            signal_type TEXT,
            direction TEXT,
            confidence INTEGER,
            entry_order_id TEXT,
            processed_at TEXT
        )""")
        conn.commit()
    finally:
        if conn:
            conn.close()


def load_daily_risk_state():
    global _daily_start_balance, _daily_start_date, _daily_loss_alerted
    global _halt_mode, _halt_mode_date, _daily_risk_state_loaded

    conn = None
    try:
        conn = sqlite3.connect(STATUS_DB)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS daily_risk_state (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            trading_day TEXT,
            start_balance REAL,
            halt_mode INTEGER,
            halt_mode_date TEXT,
            daily_loss_alerted INTEGER,
            updated_at_utc TEXT
        )""")
        cur.execute("""SELECT trading_day, start_balance, halt_mode, halt_mode_date,
                              daily_loss_alerted
                       FROM daily_risk_state
                       WHERE singleton_id = 1""")
        row = cur.fetchone()
        _daily_risk_state_loaded = True
        if row is None:
            return
        _daily_start_date = row[0]
        _daily_start_balance = float(row[1] or 0.0)
        _halt_mode = bool(row[2])
        _halt_mode_date = row[3]
        _daily_loss_alerted = bool(row[4])
    except Exception as e:
        log_line(f"⚠️ Could not load daily risk state: {e}")
    finally:
        if conn:
            conn.close()


def save_daily_risk_state():
    conn = None
    try:
        conn = sqlite3.connect(STATUS_DB)
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS daily_risk_state (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            trading_day TEXT,
            start_balance REAL,
            halt_mode INTEGER,
            halt_mode_date TEXT,
            daily_loss_alerted INTEGER,
            updated_at_utc TEXT
        )""")
        cur.execute("""INSERT OR REPLACE INTO daily_risk_state (
            singleton_id, trading_day, start_balance, halt_mode, halt_mode_date,
            daily_loss_alerted, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?)""", (
            1,
            _daily_start_date,
            float(_daily_start_balance or 0.0),
            1 if _halt_mode else 0,
            _halt_mode_date,
            1 if _daily_loss_alerted else 0,
            utc_now_str(),
        ))
        conn.commit()
    except Exception as e:
        log_line(f"⚠️ Could not save daily risk state: {e}")
    finally:
        if conn:
            conn.close()

def log_trade_open(symbol, direction, fill_price, signal_type, notional, signal_price=None,
                   entry_order_id=None, fill_confirmed=True, price_source="order_fill",
                   signal_age_seconds=None, entry_validation_price=None,
                   signal_to_live_drift_bps=None, execution_quality_flag=None,
                   execution_size_multiplier_used=None):
    """
    Log a trade open.  fill_price is the actual Alpaca fill (filled_avg_price).
    signal_price is what the scanner saw when the signal fired — stored separately
    so slippage can be computed as (fill - signal) / signal.
    """
    conn = None
    try:
        regime_data = get_regime()
        if signal_price is None:
            signal_price = fill_price
        entry_slip = calculate_slippage(signal_price, fill_price, direction, "entry")
        slippage_pct = entry_slip["pct"] / 100.0
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""INSERT INTO trades
            (symbol, side, direction, entry_price, entry_time, signal_type, notional,
             outcome, vix, regime, signal_price, slippage_pct, entry_order_id,
             entry_price_source, entry_fill_confirmed, entry_slippage_dps,
             entry_slippage_bps, signal_age_seconds, entry_validation_price,
             signal_to_live_drift_bps, execution_quality_flag,
             execution_size_multiplier_used, break_even_armed, break_even_armed_at_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol, "buy" if direction=="LONG" else "sell", direction,
             fill_price, utc_now_str(), signal_type, notional,
             "open", regime_data.get("vix",0), regime_data.get("regime","UNKNOWN"),
             signal_price, slippage_pct, entry_order_id,
             price_source, 1 if fill_confirmed else 0,
             entry_slip["dps"], entry_slip["bps"],
             signal_age_seconds, entry_validation_price, signal_to_live_drift_bps,
             execution_quality_flag, execution_size_multiplier_used, 0, None))
        conn.commit()
        log_slippage(symbol, direction, "entry", signal_price, fill_price, price_source)
        slip_str = f" | slip {entry_slip['bps']:+.1f}bps" if abs(entry_slip["bps"]) >= 0.1 else ""
        fill_note = "" if fill_confirmed else f" | {price_source}"
        post_discord(
            f"**TRADE OPEN** | {direction} {symbol} @ ${fill_price:.2f}"
            f" | signal ${signal_price:.2f}{slip_str}"
            f" | {signal_type} | ${notional:.2f} notional{fill_note}"
        )
    except Exception as e:
        log_line(f"⚠️ Trade log open error: {e}")
    finally:
        if conn:
            conn.close()

def get_open_trade(symbol):
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""SELECT id, symbol, direction, entry_price, notional,
                              entry_order_id, exit_order_id, break_even_armed,
                              break_even_armed_at_utc, exit_decision_price
                       FROM trades
                       WHERE symbol=? AND outcome='open'
                       ORDER BY id DESC LIMIT 1""", (symbol,))
        return cur.fetchone()
    except Exception as e:
        log_line(f"⚠️ Open trade lookup error for {symbol}: {e}")
        return None
    finally:
        if conn:
            conn.close()


def list_open_trades():
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows = cur.execute(
            """SELECT id, symbol, direction, exit_order_id, exit_decision_price
               FROM trades
               WHERE outcome='open'
               ORDER BY id DESC"""
        ).fetchall()
        latest_by_symbol = {}
        for row in rows:
            symbol = str(row["symbol"]).upper()
            if symbol not in latest_by_symbol:
                latest_by_symbol[symbol] = row
        return list(latest_by_symbol.values())
    except Exception as e:
        log_line(f"⚠️ Open trade list lookup error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def update_trade_exit_order_id(symbol, exit_order_id):
    if not exit_order_id:
        return
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""UPDATE trades
                       SET exit_order_id=?
                       WHERE symbol=? AND outcome='open'
                       ORDER BY id DESC LIMIT 1""",
                    (str(exit_order_id), symbol))
        conn.commit()
    except Exception as e:
        log_line(f"⚠️ Exit order id update failed for {symbol}: {e}")
    finally:
        if conn:
            conn.close()


def update_trade_exit_decision(symbol, outcome, decision_price):
    if decision_price is None:
        return
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""UPDATE trades
                       SET exit_decision_price=?
                       WHERE symbol=? AND outcome='open'
                       ORDER BY id DESC LIMIT 1""",
                    (float(decision_price), symbol))
        conn.commit()
    except Exception as e:
        log_line(f"⚠️ Exit decision price update failed for {symbol}: {e}")
    finally:
        if conn:
            conn.close()


def arm_trade_break_even(symbol):
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute(
            """UPDATE trades
               SET break_even_armed=1,
                   break_even_armed_at_utc=COALESCE(break_even_armed_at_utc, ?)
               WHERE symbol=? AND outcome='open' AND COALESCE(break_even_armed, 0)=0
               ORDER BY id DESC LIMIT 1""",
            (utc_now_str(), symbol),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        log_line(f"⚠️ Break-even arm update failed for {symbol}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def log_trade_close(symbol, exit_price, outcome, exit_order_id=None,
                    exit_decision_price=None, fill_confirmed=None, price_source=None):
    conn = None
    try:
        open_trade = get_open_trade(symbol)
        conn = sqlite3.connect(TRADE_LOG_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if open_trade is None:
            if exit_order_id:
                cur.execute("""SELECT id, outcome FROM trades
                               WHERE symbol=? AND exit_order_id=?
                               ORDER BY id DESC LIMIT 1""",
                            (symbol, str(exit_order_id)))
                existing_close = cur.fetchone()
                if existing_close is not None:
                    log_line(
                        f"ℹ️ CLOSE ALREADY RECONCILED {symbol}: "
                        f"trade row {existing_close['id']} already closed"
                    )
                    return
            cur.execute("""SELECT id, outcome FROM trades
                           WHERE symbol=? AND outcome!='open'
                           ORDER BY id DESC LIMIT 1""", (symbol,))
            existing_close = cur.fetchone()
            if existing_close is not None:
                log_line(
                    f"ℹ️ CLOSE RECONCILED WITHOUT OPEN ROW {symbol}: "
                    f"latest trade row {existing_close['id']} already closed"
                )
            else:
                log_line(f"ℹ️ CLOSE CONFIRMED {symbol}: no open trade row to finalize")
            return

        pnl_pct = None
        pnl_usd = None
        exit_slip = None
        direction = None
        stored_decision_price = None
        if exit_price is not None:
            entry_price = float(open_trade["entry_price"])
            direction = str(open_trade["direction"]).upper()
            notional = float(open_trade["notional"] or 0)
            try:
                stored_decision_price = float(exit_decision_price) if exit_decision_price is not None else None
            except Exception:
                stored_decision_price = None
            if entry_price > 0:
                if direction == "SHORT":
                    pnl_pct = (entry_price - float(exit_price)) / entry_price
                else:
                    pnl_pct = (float(exit_price) - entry_price) / entry_price
                pnl_usd = notional * pnl_pct
            if stored_decision_price is not None:
                exit_slip = calculate_slippage(stored_decision_price, exit_price, direction, "exit")

        cur.execute("""UPDATE trades SET
            exit_price=?, exit_time=?, pnl_pct=?, pnl_usd=?, outcome=?, exit_order_id=?,
            exit_decision_price=COALESCE(?, exit_decision_price),
            exit_price_source=?, exit_fill_confirmed=?,
            exit_slippage_dps=?, exit_slippage_pct=?, exit_slippage_bps=?
            WHERE symbol=? AND outcome='open'
            ORDER BY id DESC LIMIT 1""",
            (exit_price, utc_now_str(), pnl_pct, pnl_usd, outcome,
             str(exit_order_id) if exit_order_id else open_trade["exit_order_id"],
             stored_decision_price,
             price_source,
             None if fill_confirmed is None else (1 if fill_confirmed else 0),
             None if exit_slip is None else exit_slip["dps"],
             None if exit_slip is None else (exit_slip["pct"] / 100.0),
             None if exit_slip is None else exit_slip["bps"],
             symbol))
        conn.commit()

        if exit_slip is not None and direction is not None and stored_decision_price is not None:
            log_slippage(
                symbol,
                direction,
                "exit",
                stored_decision_price,
                exit_price,
                price_source or "unknown",
            )

        if outcome == "stop_loss":
            global _no_reentry_today, _no_reentry_date
            today = str(date.today())
            if _no_reentry_date != today:
                _no_reentry_today = set()
                _no_reentry_date = today
            _no_reentry_today.add(symbol)
            log_line(f"🚫 {symbol} blocked from re-entry for the rest of today")

        if exit_price is not None and pnl_usd is not None and pnl_pct is not None:
            sign = "+" if pnl_usd >= 0 else ""
            post_discord(
                f"**TRADE CLOSE** | {symbol} {outcome.upper().replace('_', ' ')}"
                f" | {sign}${pnl_usd:.2f} ({sign}{pnl_pct*100:.2f}%)"
                f" | exit ${exit_price:.2f}"
            )
        else:
            post_discord(
                f"**TRADE CLOSE** | {symbol} {outcome.upper().replace('_', ' ')}"
                f" | fill pending/unavailable"
            )
    except Exception as e:
        log_line(f"⚠️ Trade log close error: {e}")
    finally:
        if conn:
            conn.close()


def summarize_recent_slippage(limit: int = 100) -> dict:
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT
                COUNT(*) AS trade_count,
                AVG(entry_slippage_bps) AS avg_entry_slippage_bps,
                AVG(exit_slippage_bps) AS avg_exit_slippage_bps,
                MAX(entry_slippage_bps) AS worst_entry_slippage_bps,
                MAX(exit_slippage_bps) AS worst_exit_slippage_bps
            FROM (
                SELECT entry_slippage_bps, exit_slippage_bps
                FROM trades
                ORDER BY id DESC
                LIMIT ?
            )
            """,
            (int(limit),),
        ).fetchone()
        if row is None:
            return {}
        return dict(row)
    except Exception as e:
        log_line(f"⚠️ Slippage summary unavailable: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_recent_symbol_execution_telemetry(symbol: str, direction: str,
                                          limit: int = EXECUTION_TELEMETRY_LOOKBACK) -> dict:
    cache_key = (str(symbol).upper(), str(direction).upper(), int(limit))
    now_ts = time.time()
    cached = _execution_telemetry_cache.get(cache_key)
    if cached and (now_ts - float(cached.get("cached_at", 0.0))) < EXECUTION_TELEMETRY_CACHE_TTL_S:
        return dict(cached.get("data", {}))

    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT
                COUNT(*) AS trade_count,
                AVG(entry_slippage_bps) AS avg_entry_slippage_bps,
                AVG(exit_slippage_bps) AS avg_exit_slippage_bps,
                MAX(entry_slippage_bps) AS worst_entry_slippage_bps,
                MAX(exit_slippage_bps) AS worst_exit_slippage_bps
            FROM (
                SELECT entry_slippage_bps, exit_slippage_bps
                FROM trades
                WHERE symbol=? AND direction=?
                ORDER BY id DESC
                LIMIT ?
            )
            """,
            (str(symbol).upper(), str(direction).upper(), int(limit)),
        ).fetchone()
        data = dict(row) if row is not None else {}
        _execution_telemetry_cache[cache_key] = {
            "cached_at": now_ts,
            "data": data,
        }
        return data
    except Exception as e:
        log_line(f"⚠️ Execution telemetry unavailable for {symbol} {direction}: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def apply_execution_quality_adjustment(symbol: str, direction: str, trade_notional: float,
                                       validation: dict) -> tuple[float, float, str | None]:
    telemetry = get_recent_symbol_execution_telemetry(symbol, direction)
    reasons: list[str] = []
    size_multiplier = 1.0

    drift_bps = validation.get("absolute_move_bps")
    avg_entry_slip_bps = telemetry.get("avg_entry_slippage_bps")
    avg_exit_slip_bps = telemetry.get("avg_exit_slippage_bps")
    trade_count = int(telemetry.get("trade_count") or 0)

    if drift_bps is not None and float(drift_bps) >= EXECUTION_RISK_WARN_DRIFT_BPS:
        reasons.append("elevated_signal_drift")

    if trade_count >= 3 and avg_entry_slip_bps is not None and float(avg_entry_slip_bps) >= EXECUTION_RISK_HAIRCUT_ENTRY_SLIP_BPS:
        reasons.append("high_recent_entry_slippage")
        size_multiplier = min(size_multiplier, EXECUTION_RISK_HAIRCUT_MULTIPLIER)

    if drift_bps is not None and float(drift_bps) >= EXECUTION_RISK_HAIRCUT_DRIFT_BPS:
        if "elevated_signal_drift" not in reasons:
            reasons.append("elevated_signal_drift")
        size_multiplier = min(size_multiplier, EXECUTION_RISK_HAIRCUT_MULTIPLIER)

    if reasons:
        log_line(
            f"EXECUTION RISK HIGH {symbol} {direction} "
            f"avg_entry_slip={fmt_num(avg_entry_slip_bps, 1)}bps "
            f"avg_exit_slip={fmt_num(avg_exit_slip_bps, 1)}bps "
            f"drift={fmt_num(drift_bps, 1)}bps trades={trade_count}"
        )

    quality_flag = ",".join(reasons) if reasons else None
    adjusted_notional = float(trade_notional) * size_multiplier
    if size_multiplier < 1.0:
        adjusted_notional = max(MIN_TRADE_NOTIONAL_USD, adjusted_notional)
        log_line(
            f"EXECUTION HAIRCUT {symbol} {direction} "
            f"size_mult={size_multiplier:.2f} reason={quality_flag}"
        )

    return adjusted_notional, size_multiplier, quality_flag


def extract_order_id(order_response):
    if order_response is None:
        return None
    if isinstance(order_response, dict):
        value = order_response.get("id") or order_response.get("order_id")
        return str(value) if value else None
    value = getattr(order_response, "id", None)
    return str(value) if value else None


def parse_signal_direction(signal_text: str):
    text = str(signal_text or "").upper()
    short_terms = (
        "ABSORPTION SELL",
        "STRONG SELL",
        " SELL ",
        "SELL",
        "SHORT",
    )
    long_terms = (
        "ABSORPTION BUY",
        "STRONG BUY",
        " BUY ",
        "BUY",
        "LONG",
    )
    has_short = any(term in text for term in short_terms)
    has_long = any(term in text for term in long_terms)
    if has_short and has_long:
        return None
    if has_short:
        return "SHORT"
    if has_long:
        return "LONG"
    return None


def make_signal_key(signal_row):
    rowid = signal_row[0]
    ts = signal_row[1]
    symbol = str(signal_row[2]).upper()
    signal_type = str(signal_row[3])
    if rowid is not None:
        return f"rowid:{rowid}"
    return f"composite:{ts}|{symbol}|{signal_type}"


def is_signal_processed(signal_key: str) -> bool:
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM processed_signals WHERE signal_key=? LIMIT 1", (signal_key,))
        return cur.fetchone() is not None
    except Exception as e:
        log_line(f"⚠️ Processed-signal lookup failed for {signal_key}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def mark_signal_processed(signal_key: str, signal_ts: str, symbol: str, signal_type: str,
                          direction: str, confidence, entry_order_id: str | None):
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO processed_signals (
            signal_key, signal_ts, symbol, signal_type, direction, confidence,
            entry_order_id, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            signal_key,
            signal_ts,
            str(symbol).upper(),
            signal_type,
            direction,
            None if confidence is None else int(confidence),
            entry_order_id,
            utc_now_str(),
        ))
        conn.commit()
    except Exception as e:
        log_line(f"⚠️ Could not mark signal processed {signal_key}: {e}")
    finally:
        if conn:
            conn.close()


def compute_trade_notional(confidence, symbol_multiplier=1.0, market_multiplier=1.0):
    multiplier = 1.0
    confidence_value = None
    try:
        if confidence is not None:
            confidence_value = int(confidence)
    except Exception:
        confidence_value = None

    if ENABLE_CONFIDENCE_SIZING and confidence_value is not None:
        if confidence_value >= 80:
            multiplier = 1.2
        elif confidence_value >= 60:
            multiplier = 1.0
        elif confidence_value > 0:
            multiplier = 0.8

    base_notional = BASE_TRADE_NOTIONAL_USD * multiplier
    bounded_base = clamp(base_notional, MIN_TRADE_NOTIONAL_USD, MAX_TRADE_NOTIONAL_USD)
    final_notional = clamp(
        bounded_base * float(symbol_multiplier) * float(market_multiplier),
        MIN_TRADE_NOTIONAL_USD,
        MAX_TRADE_NOTIONAL_USD,
    )
    return final_notional, confidence_value, multiplier


def load_stop_model_frame(symbol: str) -> pd.DataFrame:
    cache_key = str(symbol).upper()
    cached = _stop_model_cache.get(cache_key)
    now = time.time()
    if cached and (now - float(cached.get("ts", 0))) < STOP_MODEL_CACHE_TTL_S:
        return cached.get("frame", pd.DataFrame()).copy()

    try:
        frame = yf.download(
            cache_key,
            period="20d",
            interval="1h",
            progress=False,
            auto_adjust=False,
        )
        if isinstance(frame.columns, pd.MultiIndex):
            frame.columns = frame.columns.get_level_values(0)
        frame = frame[[column for column in ("High", "Low", "Close") if column in frame.columns]].copy()
        frame = frame.dropna(subset=["High", "Low", "Close"])
    except Exception:
        frame = pd.DataFrame()

    _stop_model_cache[cache_key] = {"ts": now, "frame": frame.copy()}
    return frame


def estimate_stop_model(symbol: str, direction: str, entry_price: float) -> dict:
    price = float(entry_price or 0.0)
    fallback_distance_abs = max(price * float(HARD_STOP_LOSS_PCT), 0.01)
    fallback = {
        "stop_distance_abs": fallback_distance_abs,
        "stop_distance_pct": fallback_distance_abs / price if price > 0 else float(HARD_STOP_LOSS_PCT),
        "atr_abs": None,
        "structure_distance_abs": None,
        "source": "fallback_pct",
        "quality": "LOW",
    }
    if price <= 0.0:
        return fallback

    frame = load_stop_model_frame(symbol)
    if len(frame) < max(STOP_MODEL_ATR_BARS + 2, STOP_MODEL_LOOKBACK_BARS + 2):
        return fallback

    work = frame.copy()
    prev_close = work["Close"].shift(1)
    work["true_range"] = pd.concat(
        [
            work["High"] - work["Low"],
            (work["High"] - prev_close).abs(),
            (work["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    work["atr"] = work["true_range"].rolling(STOP_MODEL_ATR_BARS).mean()
    work = work.dropna(subset=["atr"])
    if work.empty:
        return fallback

    atr_abs = float(work["atr"].iloc[-1])
    recent = work.tail(STOP_MODEL_LOOKBACK_BARS)
    structure_distance_abs = None
    if direction == "LONG":
        structure_stop = float(recent["Low"].min())
        structure_distance_abs = max(0.0, price - structure_stop)
    else:
        structure_stop = float(recent["High"].max())
        structure_distance_abs = max(0.0, structure_stop - price)

    candidates = [max(atr_abs * STOP_MODEL_ATR_MULTIPLIER, 0.01)]
    source = "atr_only"
    quality = "MEDIUM"
    if structure_distance_abs is not None and structure_distance_abs > 0.0:
        candidates.append(structure_distance_abs)
        source = "atr+structure"
        quality = "HIGH"

    stop_distance_abs = max(candidates)
    return {
        "stop_distance_abs": stop_distance_abs,
        "stop_distance_pct": stop_distance_abs / price if price > 0 else float(HARD_STOP_LOSS_PCT),
        "atr_abs": atr_abs,
        "structure_distance_abs": structure_distance_abs,
        "source": source,
        "quality": quality,
    }


def calculate_position_gross_exposure(position) -> float:
    try:
        market_value = safe_float(getattr(position, "market_value", None), None)
        if market_value is not None:
            return abs(float(market_value))
    except Exception:
        pass

    qty = abs(position_qty(position))
    current_price = safe_float(getattr(position, "current_price", None), None)
    if current_price is None:
        current_price = safe_float(getattr(position, "avg_entry_price", None), 0.0)
    return abs(qty * float(current_price or 0.0))


def portfolio_exposure_from_trade_log() -> dict:
    conn = None
    try:
        conn = sqlite3.connect(TRADE_LOG_DB)
        df = pd.read_sql_query(
            """
            SELECT symbol, direction, notional, entry_price
            FROM trades
            WHERE exit_time IS NULL
            """,
            conn,
        )
        if df.empty:
            return {
                "gross_total": 0.0,
                "gross_long": 0.0,
                "gross_short": 0.0,
                "by_symbol": {},
                "by_sector": {},
                "source": "trade_log",
                "quality": "LOW",
            }
        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["direction"] = df["direction"].astype(str).str.upper()
        df["gross_exposure"] = df.apply(
            lambda row: abs(float(row["notional"] or row["entry_price"] or 0.0)),
            axis=1,
        )
        df["sector"] = df["symbol"].map(classify_sector)
        gross_long = float(df.loc[df["direction"] == "LONG", "gross_exposure"].sum())
        gross_short = float(df.loc[df["direction"] == "SHORT", "gross_exposure"].sum())
        by_symbol = df.groupby("symbol")["gross_exposure"].sum().to_dict()
        by_sector = df.groupby("sector")["gross_exposure"].sum().to_dict()
        return {
            "gross_total": float(df["gross_exposure"].sum()),
            "gross_long": gross_long,
            "gross_short": gross_short,
            "by_symbol": {str(key): float(value) for key, value in by_symbol.items()},
            "by_sector": {str(key): float(value) for key, value in by_sector.items()},
            "source": "trade_log",
            "quality": "LOW",
        }
    except Exception as e:
        log_line(f"⚠️ Could not compute local trade-log gross exposure: {e}")
        return {
            "gross_total": 0.0,
            "gross_long": 0.0,
            "gross_short": 0.0,
            "by_symbol": {},
            "by_sector": {},
            "source": "trade_log",
            "quality": "LOW",
        }
    finally:
        if conn:
            conn.close()


def portfolio_exposure_from_positions(positions) -> dict:
    gross_long = 0.0
    gross_short = 0.0
    by_symbol: dict[str, float] = {}
    by_sector: dict[str, float] = {}
    for position in positions:
        symbol = str(getattr(position, "symbol", "")).upper()
        if not symbol:
            continue
        exposure = calculate_position_gross_exposure(position)
        sector = classify_sector(symbol)
        qty = position_qty(position)
        if qty >= 0:
            gross_long += exposure
        else:
            gross_short += exposure
        by_symbol[symbol] = by_symbol.get(symbol, 0.0) + exposure
        by_sector[sector] = by_sector.get(sector, 0.0) + exposure
    return {
        "gross_total": float(gross_long + gross_short),
        "gross_long": float(gross_long),
        "gross_short": float(gross_short),
        "by_symbol": by_symbol,
        "by_sector": by_sector,
        "source": "broker",
        "quality": "HIGH",
    }


def get_portfolio_exposure_snapshot(client, positions=None) -> dict:
    try:
        position_rows = positions if positions is not None else trading_client_positions(client)
        return portfolio_exposure_from_positions(position_rows)
    except Exception as e:
        log_line(
            "⚠️ Gross exposure broker check unavailable; using local trade log fallback "
            f"({e})"
        )
        return portfolio_exposure_from_trade_log()


def build_position_size_plan(symbol: str, direction: str, is_crypto: bool,
                             entry_price: float, target_notional: float) -> dict:
    price = float(entry_price or 0.0)
    intended_notional = max(0.0, float(target_notional or 0.0))
    stop_model = estimate_stop_model(symbol, direction, price)
    stop_distance_abs = float(stop_model["stop_distance_abs"])
    stop_distance_pct = float(stop_model["stop_distance_pct"])
    risk_per_share = stop_distance_abs
    risk_candidate_shares = (
        float(MAX_RISK_PER_TRADE_USD) / risk_per_share
        if risk_per_share > 0 and MAX_RISK_PER_TRADE_USD is not None and MAX_RISK_PER_TRADE_USD > 0
        else float("inf")
    )
    target_candidate_shares = intended_notional / price if price > 0 else 0.0
    raw_candidate_shares = min(target_candidate_shares, risk_candidate_shares)
    risk_based_notional = (
        risk_candidate_shares * price if price > 0 and math.isfinite(risk_candidate_shares) else intended_notional
    )
    one_share_too_expensive = False
    short_rounding_changed = False
    final_shares = 0.0
    final_notional = 0.0
    skip_reason = None

    if price <= 0.0:
        skip_reason = "invalid entry price"
    elif intended_notional <= 0.0:
        skip_reason = "invalid target notional"
    elif stop_distance_abs <= 0.0:
        skip_reason = "invalid stop distance"
    elif direction == "SHORT" and not is_crypto:
        final_shares = math.floor(raw_candidate_shares)
        short_rounding_changed = final_shares < raw_candidate_shares
        final_notional = final_shares * price
        if final_shares < 1:
            one_share_too_expensive = price > intended_notional or risk_candidate_shares < 1.0
            skip_reason = (
                "1 share exceeds short sizing guardrails"
                if one_share_too_expensive
                else "short sizing resolved below 1 share"
            )
    else:
        final_shares = raw_candidate_shares
        final_notional = min(intended_notional, risk_based_notional)
        if final_notional <= 0.0:
            skip_reason = "final notional resolved to zero"

    intended_risk_dollars = final_notional * stop_distance_pct
    return {
        "symbol": symbol,
        "direction": direction,
        "entry_price": price,
        "intended_risk_dollars": intended_risk_dollars,
        "max_risk_dollars": (
            float(MAX_RISK_PER_TRADE_USD)
            if MAX_RISK_PER_TRADE_USD is not None and MAX_RISK_PER_TRADE_USD > 0 else None
        ),
        "intended_notional": intended_notional,
        "risk_based_notional": float(risk_based_notional),
        "stop_distance_pct": float(stop_distance_pct),
        "stop_distance_abs": float(stop_distance_abs),
        "risk_per_share": float(risk_per_share),
        "stop_model_source": stop_model["source"],
        "stop_model_quality": stop_model["quality"],
        "atr_abs": stop_model["atr_abs"],
        "structure_distance_abs": stop_model["structure_distance_abs"],
        "raw_candidate_shares": float(raw_candidate_shares),
        "final_shares": float(final_shares),
        "final_notional": float(final_notional),
        "short_rounding_changed": short_rounding_changed,
        "one_share_too_expensive": one_share_too_expensive,
        "valid": skip_reason is None,
        "skip_reason": skip_reason,
    }


def log_position_size_plan(plan: dict, prefix: str = "🧮 SIZE PLAN") -> None:
    max_risk_txt = "n/a" if plan["max_risk_dollars"] is None else f"${plan['max_risk_dollars']:.2f}"
    log_line(
        f"{prefix}: {plan['symbol']} {plan['direction']}"
        f" intended_risk=${plan['intended_risk_dollars']:.2f}"
        f" max_risk={max_risk_txt}"
        f" intended_notional=${plan['intended_notional']:.2f}"
        f" risk_notional=${plan['risk_based_notional']:.2f}"
        f" stop={plan['stop_distance_pct']:.2%}"
        f" stop_source={plan['stop_model_source']}"
        f" stop_quality={plan['stop_model_quality']}"
        f" risk_per_share=${plan['risk_per_share']:.4f}"
        f" final_shares={plan['final_shares']:.4f}"
        f" final_notional=${plan['final_notional']:.2f}"
        f" short_rounding_changed={'yes' if plan['short_rounding_changed'] else 'no'}"
        f" one_share_too_expensive={'yes' if plan['one_share_too_expensive'] else 'no'}"
    )


def get_order_snapshot(client, order_id: str):
    try:
        order = client.get_order_by_id(order_id)
        status = str(getattr(order, "status", "")).lower()
        fill_price = getattr(order, "filled_avg_price", None)
        return order, status, (float(fill_price) if fill_price is not None else None)
    except Exception as e:
        log_line(f"⚠️ Could not fetch order status for {order_id}: {e}")
        return None, None, None


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def order_submitted_ts(order) -> float:
    if order is None:
        return 0.0
    submitted_at = getattr(order, "submitted_at", None)
    if submitted_at is None:
        return 0.0
    if isinstance(submitted_at, datetime):
        return submitted_at.timestamp()
    text = str(submitted_at).strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def position_qty(position) -> float:
    if position is None:
        return 0.0
    return safe_float(getattr(position, "qty", 0), 0.0)


def position_close_side(position):
    qty = position_qty(position)
    if qty < 0:
        return OrderSide.BUY.value
    if qty > 0:
        return OrderSide.SELL.value
    return None


def build_pending_close_state(symbol: str, outcome: str, decision_price=None,
                              order_id: str | None = None, submitted_at: float | None = None,
                              retry_after: float | None = None,
                              cancel_submitted_at: float = 0.0,
                              last_log_ts: float | None = None):
    now = time.time()
    return {
        "order_id": order_id,
        "outcome": outcome,
        "decision_price": decision_price,
        "submitted_at": now if submitted_at is None else float(submitted_at),
        "retry_after": now if retry_after is None else float(retry_after),
        "cancel_submitted_at": float(cancel_submitted_at or 0.0),
        "last_log_ts": now if last_log_ts is None else float(last_log_ts),
    }


def find_active_close_order(trading_client, symbol: str, position=None, preferred_order_id: str | None = None):
    close_side = position_close_side(position)
    candidates = []

    if preferred_order_id:
        order, status, fill_price = get_order_snapshot(trading_client, preferred_order_id)
        if order is not None and str(getattr(order, "symbol", "")).upper() == str(symbol).upper():
            side = str(getattr(order, "side", "")).lower()
            if close_side is None or side == close_side or status not in PENDING_CLOSE_ORDER_STATUSES:
                return order, status, fill_price

    try:
        orders = trading_client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[str(symbol).upper()],
                limit=50,
            )
        )
    except Exception as e:
        log_line(f"⚠️ Could not list open orders for {symbol}: {e}")
        return None, None, None

    for order in orders or []:
        if str(getattr(order, "symbol", "")).upper() != str(symbol).upper():
            continue
        side = str(getattr(order, "side", "")).lower()
        if close_side and side != close_side:
            continue
        candidates.append(order)

    if not candidates:
        return None, None, None

    candidates.sort(key=order_submitted_ts, reverse=True)
    order = candidates[0]
    status = str(getattr(order, "status", "")).lower()
    fill_price = safe_float(getattr(order, "filled_avg_price", None))
    return order, status, fill_price


def is_close_pending_error(err_msg: str) -> bool:
    text = str(err_msg).lower()
    markers = (
        "held_for_orders",
        "insufficient qty available",
        "held for orders",
        "existing_qty",
        "open order",
        "pending",
    )
    return any(marker in text for marker in markers)


def pending_close_log(symbol: str, msg: str):
    state = _pending_closes.get(symbol, {})
    now = time.time()
    if now - float(state.get("last_log_ts", 0)) >= CLOSE_PENDING_LOG_INTERVAL_S:
        log_line(msg)
        state["last_log_ts"] = now
        _pending_closes[symbol] = state


def is_regular_market_hours(trading_client=None) -> bool:
    try:
        if trading_client is not None:
            return bool(trading_client.get_clock().is_open)
    except Exception:
        pass

    now_et = datetime.now(et_tz)
    if now_et.weekday() >= 5:
        return False
    session_start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    session_end = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return session_start <= now_et < session_end


def submit_close_attempt(trading_client, symbol: str, outcome: str, decision_price=None):
    now = time.time()
    state = _pending_closes.get(symbol)
    if state:
        order_id = state.get("order_id")
        retry_after = float(state.get("retry_after", 0))
        if order_id:
            pending_close_log(
                symbol,
                f"⏳ CLOSE SKIP {symbol}: close already pending (order_id={order_id}, outcome={state.get('outcome')})"
            )
            return
        if now < retry_after:
            pending_close_log(
                symbol,
                f"⏳ CLOSE SKIP {symbol}: cooling down after close failure until "
                f"{datetime.fromtimestamp(retry_after, cst_tz).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            return

    open_trade = get_open_trade(symbol)
    preferred_order_id = None if open_trade is None else open_trade["exit_order_id"]
    broker_position = None
    try:
        for pos in trading_client.get_all_positions():
            if pos.symbol == symbol:
                broker_position = pos
                break
    except Exception as e:
        log_line(f"⚠️ Could not refresh broker position for {symbol} before close submit: {e}")

    try:
        close_resp = trading_client.delete(
            f"/positions/{symbol}",
            {
                "time_in_force": TimeInForce.GTC.value,
                "extended_hours": True,
            },
        )
        exit_order_id = extract_order_id(close_resp)
        update_trade_exit_decision(symbol, outcome, decision_price)
        if not exit_order_id:
            active_order, _, _ = find_active_close_order(
                trading_client,
                symbol,
                position=broker_position,
                preferred_order_id=preferred_order_id,
            )
            exit_order_id = extract_order_id(active_order)
        _pending_closes[symbol] = build_pending_close_state(
            symbol,
            outcome,
            decision_price=decision_price,
            order_id=exit_order_id,
            submitted_at=now,
            retry_after=now + CLOSE_RETRY_COOLDOWN_S,
            cancel_submitted_at=0,
            last_log_ts=now,
        )
        if exit_order_id:
            update_trade_exit_order_id(symbol, exit_order_id)
            log_line(f"📤 CLOSE SUBMITTED {symbol} ({outcome}) order_id={exit_order_id}")
        else:
            log_line(f"📤 CLOSE SUBMITTED {symbol} ({outcome}) with no order id returned")
    except Exception as e:
        if is_close_pending_error(str(e)):
            active_order, status, _ = find_active_close_order(
                trading_client,
                symbol,
                position=broker_position,
                preferred_order_id=preferred_order_id,
            )
            active_order_id = extract_order_id(active_order)
            submitted_at = order_submitted_ts(active_order) or now
            _pending_closes[symbol] = build_pending_close_state(
                symbol,
                outcome,
                decision_price=decision_price,
                order_id=active_order_id,
                submitted_at=submitted_at,
                retry_after=now + CLOSE_RETRY_COOLDOWN_S,
                cancel_submitted_at=0,
                last_log_ts=0,
            )
            if active_order_id:
                update_trade_exit_decision(symbol, outcome, decision_price)
                update_trade_exit_order_id(symbol, active_order_id)
                log_line(
                    f"⏳ CLOSE ORDER WORKING {symbol}: order {active_order_id} "
                    f"status={status or 'unknown'}; broker held qty for existing close | {e}"
                )
            else:
                log_line(
                    f"⏳ CLOSE PENDING {symbol}: broker reports held/open-order state; "
                    f"retrying after cooldown | {e}"
                )
        else:
            _pending_closes[symbol] = build_pending_close_state(
                symbol,
                outcome,
                decision_price=decision_price,
                order_id=None,
                submitted_at=now,
                retry_after=now + CLOSE_RETRY_COOLDOWN_S,
                cancel_submitted_at=0,
                last_log_ts=now,
            )
            log_line(f"❌ CLOSE FAIL {symbol}: {e}")


def hydrate_pending_closes_from_trade_log(trading_client, open_positions):
    open_by_symbol = {p.symbol: p for p in open_positions}
    for trade in list_open_trades():
        symbol = str(trade["symbol"]).upper()
        if symbol in _pending_closes:
            continue

        exit_order_id = trade["exit_order_id"]
        decision_price = safe_float(trade["exit_decision_price"])
        broker_position = open_by_symbol.get(symbol)

        if not exit_order_id:
            continue

        if broker_position is None or abs(position_qty(broker_position)) <= 0.0:
            log_line(f"✅ POSITION GONE AT BROKER {symbol}: marking closed")
            log_trade_close(
                symbol,
                None,
                "closed",
                exit_order_id=exit_order_id,
                exit_decision_price=decision_price,
                fill_confirmed=False,
                price_source="reconciliation",
            )
            continue

        order, status, _ = find_active_close_order(
            trading_client,
            symbol,
            position=broker_position,
            preferred_order_id=exit_order_id,
        )
        active_order_id = extract_order_id(order) or str(exit_order_id)
        _pending_closes[symbol] = build_pending_close_state(
            symbol,
            "closed",
            decision_price=decision_price,
            order_id=active_order_id,
            submitted_at=order_submitted_ts(order) or time.time(),
            retry_after=time.time(),
            cancel_submitted_at=0,
            last_log_ts=0,
        )
        if status in PENDING_CLOSE_ORDER_STATUSES:
            log_line(
                f"⏳ CLOSE ORDER WORKING {symbol}: resumed tracking broker close order "
                f"{active_order_id} status={status}"
            )


def reconcile_pending_closes(trading_client, open_positions):
    hydrate_pending_closes_from_trade_log(trading_client, open_positions)
    open_by_symbol = {p.symbol: p for p in open_positions}
    for symbol, state in list(_pending_closes.items()):
        order_id = state.get("order_id")
        outcome = state.get("outcome", "closed")
        now = time.time()
        broker_position = open_by_symbol.get(symbol)

        if broker_position is None or abs(position_qty(broker_position)) <= 0.0:
            log_line(f"✅ POSITION GONE AT BROKER {symbol}: marking closed")
            log_trade_close(
                symbol,
                None,
                outcome,
                exit_order_id=order_id,
                exit_decision_price=state.get("decision_price"),
                fill_confirmed=False,
                price_source="reconciliation",
            )
            _pending_closes.pop(symbol, None)
            continue

        order = None
        status = None
        fill_price = None
        if order_id:
            order, status, fill_price = get_order_snapshot(trading_client, order_id)

        if order is None and broker_position is not None:
            order, status, fill_price = find_active_close_order(
                trading_client,
                symbol,
                position=broker_position,
                preferred_order_id=order_id,
            )
            discovered_order_id = extract_order_id(order)
            if discovered_order_id:
                state["order_id"] = discovered_order_id
                state["submitted_at"] = order_submitted_ts(order) or float(state.get("submitted_at", now))
                state["last_log_ts"] = 0
                _pending_closes[symbol] = state
                update_trade_exit_order_id(symbol, discovered_order_id)
                order_id = discovered_order_id

        if status in PENDING_CLOSE_ORDER_STATUSES:
            submitted_at = float(state.get("submitted_at", 0))
            cancel_submitted_at = float(state.get("cancel_submitted_at", 0))
            if (
                order_id
                and status != "pending_cancel"
                and submitted_at > 0
                and (now - submitted_at) >= CLOSE_STALE_TIMEOUT_S
                and (cancel_submitted_at <= 0 or (now - cancel_submitted_at) >= CLOSE_CANCEL_COOLDOWN_S)
            ):
                log_line(
                    f"⚠️ CLOSE ORDER STALE {symbol}: order {order_id} "
                    f"status={status} age={int(now - submitted_at)}s -> cancel/replace"
                )
                try:
                    trading_client.cancel_order_by_id(order_id)
                    state["cancel_submitted_at"] = now
                    state["last_log_ts"] = now
                    _pending_closes[symbol] = state
                    log_line(f"📤 CLOSE CANCEL SUBMITTED {symbol}: order {order_id}")
                except Exception as e:
                    pending_close_log(
                        symbol,
                        f"⚠️ CLOSE CANCEL FAIL {symbol}: order {order_id} | {e}"
                    )
                continue
            pending_close_log(
                symbol,
                f"⏳ CLOSE ORDER WORKING {symbol}: order {order_id} status={status}"
            )
            continue

        if status == "filled":
            if fill_price is not None:
                log_line(f"✅ CLOSE ORDER FILLED {symbol}: order {order_id} @ ${fill_price:.4f}")
            else:
                log_line(f"✅ CLOSE ORDER FILLED {symbol}: order {order_id}")
            log_trade_close(
                symbol,
                fill_price,
                outcome,
                exit_order_id=order_id,
                exit_decision_price=state.get("decision_price"),
                fill_confirmed=(fill_price is not None and status == "filled"),
                price_source="order_fill" if (fill_price is not None and status == "filled") else "reconciliation",
            )
            _pending_closes.pop(symbol, None)
            continue

        if status in ("canceled", "expired", "rejected"):
            log_line(f"⚠️ CLOSE ORDER {status.upper()} {symbol}: order {order_id}")
            _pending_closes.pop(symbol, None)
            submit_close_attempt(
                trading_client,
                symbol,
                outcome,
                decision_price=state.get("decision_price"),
            )
            continue

        state["order_id"] = None
        state["retry_after"] = now + CLOSE_RETRY_COOLDOWN_S
        state["cancel_submitted_at"] = 0
        state["last_log_ts"] = 0
        _pending_closes[symbol] = state
        pending_close_log(
            symbol,
            f"⚠️ CLOSE STATUS UNKNOWN {symbol}: order {order_id} status={status or 'unknown'}; "
            f"waiting before retry"
        )


# -------------------- REGIME FILTER --------------------
def get_regime() -> dict:
    """
    Read the current market regime from godmode.py's snapshot.
    Returns dict with regime string and vix value.
    Defaults to NEUTRAL if file missing or unreadable.
    """
    try:
        with open(REGIME_PATH, "r") as f:
            data = json.load(f)
        return {
            "regime": data.get("regime", "NEUTRAL"),
            "vix": float(data.get("vix") or 0),
            "timestamp": data.get("timestamp", ""),
        }
    except Exception:
        return {"regime": "NEUTRAL", "vix": 0, "timestamp": ""}


def get_regime_mode() -> str:
    """
    Returns trading mode from the unified market context source of truth.
    """
    context = get_market_context(REGIME_PATH, logger=log_line)
    return str(context.get("mode", "OPEN")).upper()


def log_market_regime_transition(context: dict):
    global _last_final_regime

    final_regime = str(context.get("final_regime", "OPEN_NEUTRAL")).upper()
    if _last_final_regime == final_regime:
        return

    if _last_final_regime is not None:
        log_line(
            "🌐 MARKET REGIME "
            f"{_last_final_regime} → {final_regime} "
            f"| SPY={fmt_pct(context.get('spy_move_pct'))} "
            f"QQQ={fmt_pct(context.get('qqq_move_pct'))} "
            f"IWM={fmt_pct(context.get('iwm_move_pct'))} "
            f"VIX={fmt_num(context.get('vix'), 1)} "
            f"VIXΔ={fmt_pct(context.get('vix_change_pct'))} "
            f"OIL={fmt_pct(context.get('oil_move_pct'))} "
            f"TNX={fmt_num(context.get('tnx'), 3)}"
        )
        if MARKET_CONTEXT_TRANSITION_ALERTS:
            post_discord(
                "🌐 **MARKET REGIME** "
                f"{_last_final_regime} → {final_regime}"
                f" | SPY {fmt_pct(context.get('spy_move_pct'))}"
                f" | QQQ {fmt_pct(context.get('qqq_move_pct'))}"
                f" | IWM {fmt_pct(context.get('iwm_move_pct'))}"
                f" | VIX {fmt_num(context.get('vix'), 1)}"
            )

    _last_final_regime = final_regime

def is_regime_safe() -> bool:
    return get_regime_mode() != "BLOCKED"

def check_daily_loss_limit(trading_client) -> bool:
    """
    Returns True if safe to trade (haven't hit daily loss limit).
    Returns False if daily loss >= DAILY_LOSS_LIMIT_USD.
    """
    global _daily_start_balance, _daily_start_date, _daily_loss_alerted
    global _halt_mode, _halt_mode_date, _last_halt_log_ts, _daily_risk_state_loaded

    today = current_trading_day()

    try:
        if not _daily_risk_state_loaded:
            load_daily_risk_state()

        if _daily_start_date != today:
            acct = trading_client.get_account()
            equity = float(acct.equity)
            previous_day = _daily_start_date
            had_halt_mode = _halt_mode
            _daily_start_balance = equity
            _daily_start_date = today
            _daily_loss_alerted = False
            if previous_day is not None and had_halt_mode:
                log_line("📅 New trading day — clearing HALT MODE")
            _halt_mode = False
            _halt_mode_date = None
            _last_halt_log_ts = 0.0
            _skip_log_cache.clear()
            save_daily_risk_state()
            if previous_day is not None:
                log_line(f"📅 New trading day — starting equity: ${equity:.2f}")

        if _halt_mode and _halt_mode_date == today:
            now = time.time()
            if now - _last_halt_log_ts >= HALT_MODE_LOG_INTERVAL_S:
                log_line("🛑 HALT MODE ACTIVE — managing existing positions only")
                _last_halt_log_ts = now
            return False

        acct = trading_client.get_account()
        equity = float(acct.equity)

        daily_pnl = equity - _daily_start_balance
        if daily_pnl <= -DAILY_LOSS_LIMIT_USD:
            if not _halt_mode:
                log_line(
                    f"🚨 DAILY LOSS LIMIT HIT: down ${abs(daily_pnl):.2f} today "
                    f"(limit: ${DAILY_LOSS_LIMIT_USD}) — entering HALT MODE"
                )
                log_line("🛑 HALT MODE ACTIVE — managing existing positions only")
                _last_halt_log_ts = time.time()
            _halt_mode = True
            _halt_mode_date = today
            if not _daily_loss_alerted:
                post_discord(
                    f"🚨 **DAILY LOSS LIMIT HIT** | down ${abs(daily_pnl):.2f} today"
                    f" (limit: ${DAILY_LOSS_LIMIT_USD}) — entering HALT MODE until tomorrow"
                )
                _daily_loss_alerted = True
            save_daily_risk_state()
            return False

    except Exception as e:
        log_line(f"⚠️ Could not check daily loss limit: {e}")

    return True


# -------------------- STATUS DB --------------------
def write_sniper_status(
    status: str = "OK",
    heartbeat_seq: int = 0,
    in_position: int = 0,
    symbol: str | None = None,
    side: str | None = None,
    position_qty: float | None = None,
    last_trade_ts_utc: str | None = None,
    last_trade_price: float | None = None,
    unrealized_pnl: float | None = None,
    realized_pnl: float | None = None,
    note: str | None = None,
):
    con = None
    try:
        ts = utc_now_str()
        con = sqlite3.connect(STATUS_DB)
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sniper_status (
                ts_utc TEXT PRIMARY KEY,
                status TEXT, bot_version TEXT, heartbeat_seq INTEGER,
                in_position INTEGER, symbol TEXT, side TEXT, position_qty REAL,
                last_trade_ts_utc TEXT, last_trade_price REAL,
                unrealized_pnl REAL, realized_pnl REAL, note TEXT
            )
        """)
        cur.execute("""
            INSERT OR REPLACE INTO sniper_status (
              ts_utc, status, bot_version, heartbeat_seq,
              in_position, symbol, side, position_qty,
              last_trade_ts_utc, last_trade_price,
              unrealized_pnl, realized_pnl, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ts, status, BOT_VERSION, int(heartbeat_seq),
            int(in_position), symbol, side, position_qty,
            last_trade_ts_utc, last_trade_price,
            unrealized_pnl, realized_pnl, note
        ))
        con.commit()
    except Exception as e:
        log_line(f"❌ STATUS WRITE FAIL: {e}")
    finally:
        if con:
            con.close()


# -------------------- CLIENT --------------------
def get_client():
    return TradingClient(API_KEY, SECRET_KEY, paper=False)


# -------------------- DB INIT --------------------
def init_db():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                timestamp TEXT, symbol TEXT, signal_type TEXT, price REAL,
                rvol REAL, flow_m REAL, confidence INTEGER, sector TEXT, change_pct REAL
            )
        ''')
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp ON signals(symbol, timestamp)")
        conn.commit()
    except Exception:
        pass
    finally:
        if conn:
            conn.close()


# -------------------- APPROVED SYMBOLS --------------------
def get_approved_symbols() -> dict:
    """
    Returns two lists:
      - buy_approved:  symbols we will go LONG on
      - sell_approved: symbols we will go SHORT on (margin account required)

    Based on backtest results:
      Best BUY signals:  NFLX, META, AAPL, AMZN, ETH/USD, BTC/USD
      Best SELL signals: AMD, COIN, IWM, TSLA, NVDA (95-100% win rate)
      Removed from buys: MSFT (31% WR), NVDA (17% WR) — losers as longs
    """
    global _approved_symbols_cache, _approved_symbols_mtime
    approved_path = "/home/theplummer92/approved_symbols.json"
    try:
        current_mtime = os.path.getmtime(approved_path)
        if _approved_symbols_cache is not None and _approved_symbols_mtime == current_mtime:
            return _approved_symbols_cache

        with open(approved_path, "r") as f:
            data = json.load(f)
        if "buy" in data and "sell" in data:
            _approved_symbols_cache = {
                "buy":              [s.upper() for s in data["buy"]],
                "sell":             [s.upper() for s in data["sell"]],
                "size_multipliers": {s.upper(): v for s, v in
                                     data.get("size_multipliers", {}).items()},
            }
            _approved_symbols_mtime = current_mtime
            return _approved_symbols_cache
    except Exception:
        pass

    # Backtest-optimised defaults
    _approved_symbols_cache = {
        "size_multipliers": {},
        "buy": [
            "NFLX", "META", "AAPL", "AMZN",
            "BTC/USD", "ETH/USD", "SOL/USD",
            "TSLA", "AMD", "COIN", "GME", "SPY", "IWM",
        ],
        "sell": [
            # Only short stocks with 90%+ backtest win rate on SELL signals
            "AMD", "COIN", "IWM", "TSLA", "NVDA", "SPY", "AMZN",
        ],
    }
    _approved_symbols_mtime = None
    return _approved_symbols_cache


# -------------------- POSITION MANAGEMENT --------------------
def manage_positions(trading_client):
    """
    Check all open positions every cycle.
    Handles both LONG and SHORT positions correctly.
    - Long:  profit when price goes UP   → close when pnl >= TP or <= SL
    - Short: profit when price goes DOWN → Alpaca's unrealized_plpc handles
              the sign correctly for shorts automatically
    """
    try:
        positions = trading_client.get_all_positions()
        reconcile_pending_closes(trading_client, positions)
        regular_hours = is_regular_market_hours(trading_client)
        for p in positions:
            symbol = p.symbol
            entry  = float(p.avg_entry_price)
            if entry == 0:
                continue

            # unrealized_plpc is already signed correctly for both long and short
            pnl_pct = float(p.unrealized_plpc)
            side    = "SHORT" if float(p.qty) < 0 else "LONG"
            current_price = None
            try:
                current_price = float(getattr(p, "current_price"))
            except Exception:
                current_price = None
            if current_price is None:
                try:
                    if side == "SHORT":
                        current_price = entry * (1.0 - pnl_pct)
                    else:
                        current_price = entry * (1.0 + pnl_pct)
                except Exception:
                    current_price = None

            open_trade = get_open_trade(symbol)
            break_even_armed = bool(open_trade["break_even_armed"]) if open_trade else False

            pending = _pending_closes.get(symbol)
            if pending:
                order_id = pending.get("order_id")
                if order_id:
                    pending_close_log(
                        symbol,
                        f"⏳ CLOSE SKIP {symbol}: awaiting prior close order {order_id}"
                    )
                    continue
                if time.time() < float(pending.get("retry_after", 0)):
                    pending_close_log(
                        symbol,
                        f"⏳ CLOSE SKIP {symbol}: retry cooldown active after prior close failure"
                    )
                    continue

            if (
                not break_even_armed
                and pnl_pct >= BREAK_EVEN_ARM_PCT
                and pnl_pct < TAKE_PROFIT_PCT
                and arm_trade_break_even(symbol)
            ):
                break_even_armed = True
                log_line(
                    f"🟰 BREAK-EVEN ARMED: {symbol} {side} at {pnl_pct:.2%} "
                    f"→ effective stop moved to 0.00%"
                )

            if break_even_armed and not regular_hours and pnl_pct < 0.0:
                log_line(
                    f"🌙 AFTER HOURS EXIT: {symbol} {side} break-even armed and "
                    f"PnL turned negative ({pnl_pct:.2%}) → closing"
                )
                submit_close_attempt(
                    trading_client,
                    symbol,
                    "stop_loss",
                    decision_price=current_price,
                )
                continue

            if pnl_pct >= TAKE_PROFIT_PCT:
                log_line(f"💰 TAKE PROFIT: {symbol} {side} +{pnl_pct:.2%} → closing")
                submit_close_attempt(
                    trading_client,
                    symbol,
                    "take_profit",
                    decision_price=current_price,
                )

            elif pnl_pct <= (0.0 if break_even_armed else -HARD_STOP_LOSS_PCT):
                stop_label = "BREAK-EVEN STOP" if break_even_armed else "HARD STOP"
                stop_floor = "0.00%" if break_even_armed else f"-{HARD_STOP_LOSS_PCT:.2%}"
                log_line(
                    f"🛑 {stop_label}: {symbol} {side} {pnl_pct:.2%} "
                    f"(stop {stop_floor}) → closing"
                )
                submit_close_attempt(
                    trading_client,
                    symbol,
                    "stop_loss",
                    decision_price=current_price,
                )

            elif pnl_pct < -0.01:
                log_line(f"📉 MONITOR: {symbol} {side} {pnl_pct:.2%}")

    except Exception as e:
        log_line(f"⚠️ Position management error: {e}")


# -------------------- SIGNALS --------------------
def get_new_signals(last_check_ts: str):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        query = """
            SELECT rowid, timestamp, symbol, signal_type, price, confidence
            FROM signals
            WHERE timestamp > ?
            AND (signal_type LIKE '%STRONG%' OR signal_type LIKE '%ABSORPTION%')
            ORDER BY timestamp ASC
        """
        cursor.execute(query, (last_check_ts,))
        return cursor.fetchall()
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


# -------------------- EARNINGS PROXIMITY CHECK --------------------
_earnings_cache: dict = {}  # symbol -> (next_earnings_date or None, checked_at timestamp)
EARNINGS_CACHE_TTL_SECONDS = 4 * 3600  # refresh every 4 hours
EARNINGS_BLOCK_DAYS = 7


def extract_next_earnings_date(cal):
    if cal is None:
        return None

    if isinstance(cal, dict):
        for key in ("Earnings Date", "earningsDate", "nextEarningsDate"):
            value = cal.get(key)
            if value:
                if isinstance(value, (list, tuple)) and value:
                    value = value[0]
                return pd_to_date(value)
        return None

    if hasattr(cal, "empty") and cal.empty:
        return None

    if hasattr(cal, "index") and "Earnings Date" in getattr(cal, "index", []):
        value = cal.loc["Earnings Date"]
        first = value.iloc[0] if hasattr(value, "iloc") else value
        return pd_to_date(first) if first is not None and str(first) != "NaT" else None

    if hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
        value = cal["Earnings Date"]
        first = value.iloc[0] if hasattr(value, "iloc") else value
        return pd_to_date(first) if first is not None and str(first) != "NaT" else None

    return None

def is_near_earnings(yahoo_symbol: str) -> bool:
    """
    Returns True if the symbol has an earnings date within the next 7 calendar days.
    Crypto symbols are skipped (always returns False).
    On any error, returns False so trading is not blocked unnecessarily.
    """
    # Crypto has no earnings
    if "-" in yahoo_symbol or "/" in yahoo_symbol:
        return False

    now_ts = time.time()
    cached = _earnings_cache.get(yahoo_symbol)
    if cached and (now_ts - cached[1]) < EARNINGS_CACHE_TTL_SECONDS:
        next_earnings = cached[0]
    else:
        try:
            ticker = yf.Ticker(yahoo_symbol)
            cal = ticker.calendar
            next_earnings = extract_next_earnings_date(cal)
        except Exception as e:
            if should_log_skip(f"earnings-error:{yahoo_symbol}", interval_s=3600):
                log_line(
                    f"⚠️ Earnings data unavailable for {yahoo_symbol}: {e} "
                    f"— continuing without earnings block"
                )
            next_earnings = None
        _earnings_cache[yahoo_symbol] = (next_earnings, now_ts)

    if next_earnings is None:
        return False

    today = date.today()
    days_until = (next_earnings - today).days
    return 0 <= days_until <= EARNINGS_BLOCK_DAYS


def pd_to_date(val) -> date:
    """Convert a pandas Timestamp or datetime-like to a Python date."""
    if val is None:
        raise ValueError("missing earnings date")
    if isinstance(val, (list, tuple)):
        if not val:
            raise ValueError("missing earnings date")
        val = val[0]
    try:
        return val.date()
    except AttributeError:
        return date.fromisoformat(str(val)[:10])


def parse_signal_timestamp(signal_ts: str | None) -> datetime | None:
    if not signal_ts:
        return None
    raw = str(signal_ts).strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def fetch_live_validation_price(symbol: str) -> tuple[float | None, str | None]:
    if get_alpaca_latest_price is None:
        return None, None
    try:
        live_price = get_alpaca_latest_price(symbol)
        if live_price is None:
            return None, None
        return float(live_price), "alpaca_latest_trade"
    except Exception as e:
        log_line(f"⚠️ Live price validation failed for {symbol}: {e}")
        return None, None


def validate_entry_signal(symbol: str, direction: str, signal_price: float,
                          signal_ts: str | None) -> dict:
    validation = {
        "decision": "skipped",
        "reason": "",
        "signal_age_seconds": None,
        "signal_to_live_move_pct": None,
        "absolute_move_bps": None,
        "live_price": None,
        "live_price_source": None,
    }

    parsed_signal_ts = parse_signal_timestamp(signal_ts)
    if parsed_signal_ts is None:
        validation["reason"] = "invalid_signal_timestamp"
        log_line(
            f"ENTRY SKIPPED {symbol} {direction} invalid signal timestamp"
        )
        return validation

    signal_age_seconds = max(0.0, (utc_now() - parsed_signal_ts).total_seconds())
    validation["signal_age_seconds"] = signal_age_seconds
    if signal_age_seconds > MAX_SIGNAL_AGE_SECONDS:
        validation["reason"] = "stale_signal"
        log_line(
            f"ENTRY SKIPPED {symbol} {direction} stale signal "
            f"age={signal_age_seconds:.1f}s signal=${signal_price:.4f}"
        )
        return validation

    live_price, live_price_source = fetch_live_validation_price(symbol)
    validation["live_price"] = live_price
    validation["live_price_source"] = live_price_source
    if live_price is None:
        validation["reason"] = "live_price_unavailable"
        log_line(
            f"ENTRY SKIPPED {symbol} {direction} live validation unavailable "
            f"signal=${signal_price:.4f} age={signal_age_seconds:.1f}s"
        )
        return validation

    signal_to_live_move_pct = ((float(live_price) - float(signal_price)) / float(signal_price))
    absolute_move_bps = abs(signal_to_live_move_pct) * 10000.0
    validation["signal_to_live_move_pct"] = signal_to_live_move_pct
    validation["absolute_move_bps"] = absolute_move_bps

    if absolute_move_bps > MAX_SIGNAL_DRIFT_BPS:
        validation["reason"] = "excessive_signal_drift"
        log_line(
            f"ENTRY SKIPPED {symbol} {direction} excessive signal drift "
            f"signal=${signal_price:.4f} live=${float(live_price):.4f} "
            f"age={signal_age_seconds:.1f}s drift={absolute_move_bps:.1f}bps"
        )
        return validation

    validation["decision"] = "proceed"
    validation["reason"] = "validated"
    log_line(
        f"ENTRY VALIDATED {symbol} {direction} signal=${signal_price:.4f} "
        f"live=${float(live_price):.4f} age={signal_age_seconds:.1f}s "
        f"drift={absolute_move_bps:.1f}bps source={live_price_source}"
    )
    return validation


# -------------------- FILL PRICE FETCH --------------------
FILL_POLL_ATTEMPTS = 12   # poll up to 12 times
FILL_POLL_SLEEP_S  = 1.0  # 1 second between polls → max 10s wait

def get_fill_price(client, order_id: str, signal_price: float, symbol: str):
    """
    Returns (price, fill_confirmed, price_source).
    price_source is one of: order_fill, position_avg, signal_estimate, or status:<terminal>.
    """
    last_status = None
    for attempt in range(1, FILL_POLL_ATTEMPTS + 1):
        try:
            order = client.get_order_by_id(order_id)
            status = str(order.status).lower()
            last_status = status
            if order.filled_avg_price is not None and status in ("filled", "partially_filled"):
                fill = float(order.filled_avg_price)
                log_line(f"✅ FILL CONFIRMED @ ${fill:.4f} "
                         f"(signal ${signal_price:.4f}, "
                         f"slip {(fill - signal_price) / signal_price * 100:+.3f}%)")
                return fill, True, "order_fill"
            if status in ("canceled", "expired", "rejected"):
                log_line(f"⚠️ Order {order_id} ended with status '{status}' before fill confirmation")
                return None, False, f"status:{status}"
        except Exception as e:
            log_line(f"⚠️ Fill poll attempt {attempt}/{FILL_POLL_ATTEMPTS} failed: {e}")
        time.sleep(FILL_POLL_SLEEP_S)

    try:
        for pos in client.get_all_positions():
            if pos.symbol == symbol:
                avg_entry = getattr(pos, "avg_entry_price", None)
                if avg_entry is not None:
                    fill = float(avg_entry)
                    log_line(
                        f"⚠️ Fill not confirmed from order after {FILL_POLL_ATTEMPTS}s — "
                        f"using estimated broker position avg entry ${fill:.4f} for {symbol}"
                    )
                    return fill, False, "position_avg"
                break
    except Exception as e:
        log_line(f"⚠️ Could not verify broker position entry for {symbol}: {e}")

    log_line(
        f"⚠️ Fill not confirmed after {FILL_POLL_ATTEMPTS}s "
        f"(last_status={last_status or 'unknown'}) — using estimated signal price "
        f"${signal_price:.4f} for {symbol}"
    )
    return signal_price, False, "signal_estimate"


# -------------------- TRADE EXECUTION --------------------
def execute_entry(client, symbol: str, signal: str, price: float, confidence=None,
                  signal_key: str | None = None, signal_ts: str | None = None,
                  market_context: dict | None = None):
    """
    Execute a trade with full shorting support.

    Parsed LONG signals  → go LONG
    Parsed SHORT signals → go SHORT

    Safety checks:
    1.  Skip futures (= in symbol)
    2.  Translate Yahoo → Alpaca symbol format
    3.  Check buy/sell approved lists separately
    4.  Skip if already in a position for this symbol (any direction)
    5.  Skip if max positions reached
    6.  Check market hours for stocks
    7.  Execute with correct OrderSide
    """

    # 1. Skip futures
    if "=" in symbol:
        return

    # 2. Translate BTC-USD → BTC/USD
    alpaca_symbol = symbol.replace("-", "/").upper()

    is_crypto = "/" in alpaca_symbol

    direction = parse_signal_direction(signal)
    if direction is None:
        log_line(f"⛔ SKIP {alpaca_symbol}: Unknown signal direction for '{signal}'")
        return
    is_buy_signal = direction == "LONG"
    is_sell_signal = direction == "SHORT"

    # 3. Check approved lists
    approved = get_approved_symbols()
    if is_buy_signal and alpaca_symbol not in approved["buy"]:
        if should_log_skip(f"buy-approved:{alpaca_symbol}"):
            log_line(f"⛔ SKIP LONG  {alpaca_symbol}: Not in buy-approved list")
        return
    if is_sell_signal and alpaca_symbol not in approved["sell"]:
        if should_log_skip(f"sell-approved:{alpaca_symbol}"):
            log_line(
                f"⛔ SKIP SHORT {alpaca_symbol}: Not in sell-approved list "
                f"(roster constraint)"
            )
        return

    # 3b. No re-entry after stop loss same day
    global _no_reentry_today, _no_reentry_date
    today = str(date.today())
    if _no_reentry_date != today:
        _no_reentry_today = set()
        _no_reentry_date = today
    if alpaca_symbol in _no_reentry_today:
        if should_log_skip(f"no-reentry:{alpaca_symbol}"):
            log_line(f"🚫 SKIP {alpaca_symbol}: stopped out earlier today, no re-entry until tomorrow")
        return

    # 3c. Earnings proximity block — skip if earnings within 7 days
    if is_near_earnings(symbol):
        if should_log_skip(f"earnings-near:{alpaca_symbol}"):
            log_line(f"📅 SKIP {alpaca_symbol}: Earnings within {EARNINGS_BLOCK_DAYS} days")
        return

    # 4. Get current positions
    try:
        positions = trading_client_positions(client)
    except Exception as e:
        log_line(f"⚠️ Could not fetch positions: {e}")
        return

    held_symbols = [p.symbol for p in positions]

    # Skip if already holding this symbol in any direction
    if alpaca_symbol in held_symbols:
        if should_log_skip(f"already-held:{alpaca_symbol}", interval_s=60):
            log_line(f"🛡️ SKIP {alpaca_symbol}: Already have an open position")
        return

    # 5. Max positions check
    if len(positions) >= MAX_OPEN_POSITIONS:
        if should_log_skip("max-open-positions", interval_s=60):
            log_line(f"🚦 SKIP {alpaca_symbol}: Max {MAX_OPEN_POSITIONS} positions reached")
        return

    # 6. Market hours check (stocks only — crypto trades 24/7)
    if not is_crypto:
        try:
            if not client.get_clock().is_open:
                if should_log_skip(f"market-closed:{alpaca_symbol}", interval_s=60):
                    log_line(f"💤 SKIP {alpaca_symbol}: Market closed")
                return
        except Exception:
            pass

    # 7. Execute
    side      = OrderSide.BUY  if is_buy_signal  else OrderSide.SELL
    tif       = TimeInForce.DAY

    symbol_multiplier = approved.get("size_multipliers", {}).get(alpaca_symbol, 1.0)
    context = market_context or get_market_context(REGIME_PATH, logger=log_line)
    market_multiplier = market_multiplier_for_direction(context, direction)
    trade_notional, confidence_value, confidence_multiplier = compute_trade_notional(
        confidence,
        symbol_multiplier=symbol_multiplier,
        market_multiplier=market_multiplier,
    )

    validation = validate_entry_signal(
        alpaca_symbol,
        direction,
        float(price),
        signal_ts,
    )
    if validation["decision"] != "proceed":
        return
    trade_notional, execution_size_multiplier, execution_quality_flag = apply_execution_quality_adjustment(
        alpaca_symbol,
        direction,
        trade_notional,
        validation,
    )
    size_plan = build_position_size_plan(
        alpaca_symbol,
        direction,
        is_crypto,
        float(price),
        trade_notional,
    )
    log_position_size_plan(size_plan)
    if not size_plan["valid"]:
        log_line(
            f"⛔ SIZE SKIP {alpaca_symbol}: {size_plan['skip_reason']}"
            f" | final_shares={size_plan['final_shares']:.4f}"
            f" final_notional=${size_plan['final_notional']:.2f}"
        )
        return

    exposure_snapshot = get_portfolio_exposure_snapshot(client, positions=positions)
    current_gross_exposure = float(exposure_snapshot["gross_total"])
    exposure_source = str(exposure_snapshot["source"])
    proposed_gross_exposure = current_gross_exposure + float(size_plan["final_notional"])
    current_symbol_gross = float(exposure_snapshot["by_symbol"].get(alpaca_symbol, 0.0))
    proposed_symbol_gross = current_symbol_gross + float(size_plan["final_notional"])
    sector = classify_sector(alpaca_symbol)
    current_sector_gross = float(exposure_snapshot["by_sector"].get(sector, 0.0))
    proposed_sector_gross = current_sector_gross + float(size_plan["final_notional"])

    if proposed_symbol_gross > float(MAX_SINGLE_POSITION_GROSS_USD):
        log_line(
            f"⛔ SYMBOL CAP SKIP: {alpaca_symbol} proposed gross ${proposed_symbol_gross:.2f} "
            f"exceeds symbol cap ${MAX_SINGLE_POSITION_GROSS_USD:.2f} "
            f"| current=${current_symbol_gross:.2f} new=${size_plan['final_notional']:.2f}"
        )
        return
    if proposed_sector_gross > float(MAX_SECTOR_GROSS_USD):
        log_line(
            f"⛔ SECTOR CAP SKIP: {sector} proposed gross ${proposed_sector_gross:.2f} "
            f"exceeds sector cap ${MAX_SECTOR_GROSS_USD:.2f} "
            f"| current=${current_sector_gross:.2f} new=${size_plan['final_notional']:.2f}"
        )
        return
    if proposed_gross_exposure > float(MAX_GROSS_EXPOSURE_USD):
        log_line(
            f"⛔ EXPOSURE CAP SKIP: proposed gross exposure ${proposed_gross_exposure:.2f} "
            f"exceeds cap ${MAX_GROSS_EXPOSURE_USD:.2f} | current=${current_gross_exposure:.2f} "
            f"new=${size_plan['final_notional']:.2f} source={exposure_source}"
        )
        return

    try:
        log_line(f"🚀 SNIPING {alpaca_symbol} {direction} @ ~${float(price):.2f} | {signal}"
                 + (
                     f" | conf={confidence_value if confidence_value is not None else 'n/a'}"
                     f" conf_mult={confidence_multiplier:.2f}x"
                     f" symbol_mult={float(symbol_multiplier):.2f}x"
                     f" market_state={context.get('state', 'NEUTRAL')}"
                     f" market_mult={float(market_multiplier):.2f}x"
                     f" exec_mult={execution_size_multiplier:.2f}x"
                     f" target=${trade_notional:.2f}"
                     f" final=${size_plan['final_notional']:.2f}"
                     f" sector={sector}"
                     f" sector_gross=${current_sector_gross:.2f}->{proposed_sector_gross:.2f}"
                     f" gross=${current_gross_exposure:.2f}->{proposed_gross_exposure:.2f}"
                 ))
        if side == OrderSide.SELL:
            qty = int(size_plan["final_shares"])
            order = client.submit_order(MarketOrderRequest(
                symbol=alpaca_symbol,
                qty=qty,
                side=side,
                time_in_force=tif,
            ))
        else:
            order = client.submit_order(MarketOrderRequest(
                symbol=alpaca_symbol,
                notional=float(size_plan["final_notional"]),
                side=side,
                time_in_force=tif,
            ))
        log_line(f"✅ ORDER SENT: {alpaca_symbol} ${size_plan['final_notional']:.2f} {direction} "
                 f"(order_id={order.id})")
        entry_order_id = extract_order_id(order)
        if signal_key:
            mark_signal_processed(signal_key, signal_ts or "", alpaca_symbol, signal,
                                  direction, confidence_value, entry_order_id)
        fill_price, fill_confirmed, price_source = get_fill_price(
            client, str(order.id), float(price), alpaca_symbol
        )
        if fill_price is None:
            log_line(
                f"⚠️ ENTRY NOT CONFIRMED {alpaca_symbol}: "
                f"order {entry_order_id} ended without a usable fill price"
            )
            return
        if not fill_confirmed:
            log_line(
                f"⚠️ ENTRY PRICE ESTIMATED {alpaca_symbol}: using {price_source} "
                f"@ ${fill_price:.4f}"
            )
        log_trade_open(
            alpaca_symbol,
            direction,
            fill_price,
            signal,
            float(size_plan["final_notional"]),
            signal_price=float(price),
            entry_order_id=entry_order_id,
            fill_confirmed=fill_confirmed,
            price_source=price_source,
            signal_age_seconds=validation["signal_age_seconds"],
            entry_validation_price=validation["live_price"],
            signal_to_live_drift_bps=validation["absolute_move_bps"],
            execution_quality_flag=execution_quality_flag,
            execution_size_multiplier_used=execution_size_multiplier,
        )
    except Exception as e:
        log_line(f"❌ ORDER FAIL {alpaca_symbol}: {e}")


def trading_client_positions(client):
    """Wrapper so we can cache/mock easily later."""
    return client.get_all_positions()


# -------------------- MAIN LOOP --------------------
def run():
    # Single instance check
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE, "r") as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log_line(f"⛔ CRITICAL: Sniper already running (PID {pid}). Aborting.")
            sys.exit(0)
        except Exception:
            log_line("⚠️ Stale lockfile found. Taking over.")

    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))

    init_db()
    init_trade_log()
    load_daily_risk_state()
    t_client = get_client()

    last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    last_hb = 0

    log_line(f"🦅 {BOT_VERSION} ACTIVE")
    log_line(
        "loaded daily risk state: "
        f"trading_day={_daily_start_date}, "
        f"halt_mode={_halt_mode}, "
        f"start_balance=${float(_daily_start_balance or 0.0):.2f}"
    )
    log_line(
        f"💰 Trade size: ${TRADE_NOTIONAL_USD} | TP: {TAKE_PROFIT_PCT:.2%} | SL: {HARD_STOP_LOSS_PCT:.2%}"
    )
    log_line(f"🚨 Daily loss limit: ${DAILY_LOSS_LIMIT_USD} | Max positions: {MAX_OPEN_POSITIONS}")
    log_line(
        "🧱 Guardrails: "
        f"gross_cap=${MAX_GROSS_EXPOSURE_USD:.2f} "
        f"symbol_cap=${MAX_SINGLE_POSITION_GROSS_USD:.2f} "
        f"sector_cap=${MAX_SECTOR_GROSS_USD:.2f} "
        f"risk_cap={'n/a' if MAX_RISK_PER_TRADE_USD is None else '$' + format(MAX_RISK_PER_TRADE_USD, '.2f')}"
    )
    approved = get_approved_symbols()
    log_line(f"📈 LONG approved:  {', '.join(approved['buy'])}")
    log_line(f"📉 SHORT approved: {', '.join(approved['sell'])}")
    write_sniper_status(status="OK", heartbeat_seq=0, in_position=0, note="boot")
    post_discord(
        f"🦅 **SNIPER ONLINE** | {BOT_VERSION}"
        f" | TP {TAKE_PROFIT_PCT:.2%} SL {HARD_STOP_LOSS_PCT:.2%}"
        f" | max {MAX_OPEN_POSITIONS} pos | daily limit ${DAILY_LOSS_LIMIT_USD}"
    )

    try:
        while True:
            now = time.time()

            # Heartbeat every 60s
            if now - last_hb >= 60:
                market_context = get_market_context(REGIME_PATH, logger=log_line)
                log_market_regime_transition(market_context)
                log_line(
                    f"🫀 heartbeat | regime={market_context.get('final_regime', 'OPEN_NEUTRAL')} "
                    f"VIX={fmt_num(market_context.get('vix'), 1)} "
                    f"SPY={fmt_pct(market_context.get('spy_move_pct'))} "
                    f"QQQ={fmt_pct(market_context.get('qqq_move_pct'))} "
                    f"IWM={fmt_pct(market_context.get('iwm_move_pct'))} "
                    f"OIL={fmt_pct(market_context.get('oil_move_pct'))} "
                    f"TNX={fmt_num(market_context.get('tnx'), 3)}"
                )
                write_sniper_status(
                    status="OK", heartbeat_seq=0, in_position=0,
                    note=f"heartbeat|{market_context.get('final_regime', 'OPEN_NEUTRAL')}"
                )
                last_hb = now

            # Position management runs every cycle regardless of regime
            manage_positions(t_client)

            # Daily loss halt gate comes before signal evaluation.
            if not check_daily_loss_limit(t_client):
                last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
                time.sleep(POLL_SECONDS)
                continue

            # Regime gate — sell-only when VIX 25-30, fully blocked above 30
            market_context = get_market_context(REGIME_PATH, logger=log_line)
            regime_mode = str(market_context.get("mode", "OPEN")).upper()
            if regime_mode == "BLOCKED":
                last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
                time.sleep(POLL_SECONDS)
                continue

            # Check for new signals
            signals = get_new_signals(last_check)
            if signals:
                seen = set()
                for s in signals:
                    signal_rowid, signal_ts, sym, stype, price, confidence = s
                    direction = parse_signal_direction(stype)
                    if direction is None:
                        log_line(f"⛔ SKIP {sym}: Unknown signal direction for '{stype}'")
                        continue
                    signal_key = make_signal_key(s)
                    if is_signal_processed(signal_key):
                        if should_log_skip(f"idempotent:{sym}:{direction}", interval_s=300):
                            log_line(f"SKIP IDEMPOTENT {sym} {direction} ({signal_key})")
                        continue
                    dedup_key = (signal_ts[:13], sym, direction)  # one trade per hour per symbol per direction
                    if dedup_key in seen:
                        if should_log_skip(f"dupe:{sym}:{direction}", interval_s=300):
                            log_line(f"SKIP DUPE {sym} {direction} (already acted on this hour)")
                        continue
                    seen.add(dedup_key)
                    if regime_mode == "SELL_ONLY" and direction != "SHORT":
                        if should_log_skip(f"sell-only:{sym}:{direction}", interval_s=300):
                            log_line(f"📉 SELL-ONLY mode — skipping {direction} signal on {sym}")
                        continue
                    blocked, block_reason = should_block_direction(market_context, direction)
                    if blocked:
                        if should_log_skip(f"market-context:{sym}:{direction}:{block_reason}", interval_s=300):
                            log_line(
                                f"🌐 SKIP {sym} {direction}: {block_reason} "
                                f"(market={market_context.get('state', 'NEUTRAL')})"
                            )
                        continue
                    execute_entry(
                        t_client,
                        sym,
                        stype,
                        float(price),
                        confidence=confidence,
                        signal_key=signal_key,
                        signal_ts=signal_ts,
                        market_context=market_context,
                    )
                last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        log_line("🛑 Sniper stopped by user.")
    except Exception as e:
        log_line(f"💀 FATAL ERROR: {e}")
    finally:
        try:
            os.remove(LOCKFILE)
        except Exception:
            pass
        log_line("🔒 Lockfile removed. Sniper offline.")


if __name__ == "__main__":
    run()
