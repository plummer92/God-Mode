#!/usr/bin/env python3
"""
paper_sniper.py - Paper trading bot for wild signal experiments.
Uses Alpaca paper account only. Always force-closes at 3:45pm ET.
"""
import os
import sys
import sqlite3
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path

import pytz
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from app_paths import DATA_DIR, ENV_FILE

load_dotenv(ENV_FILE)

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
# Use dedicated wild-experiment account keys if configured, else fall back to standard paper keys
PAPER_KEY = os.getenv("APCA_WILD_PAPER_KEY_ID") or os.getenv("APCA_PAPER_KEY_ID")
PAPER_SECRET = os.getenv("APCA_WILD_PAPER_SECRET_KEY") or os.getenv("APCA_PAPER_SECRET_KEY")
PAPER_URL = os.getenv("APCA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

TRADE_NOTIONAL = 500.0
TAKE_PROFIT_PCT = 0.007
STOP_LOSS_PCT = 0.007
MAX_OPEN_POSITIONS = 20
MAX_SIGNAL_AGE_SECONDS = 1800
DEDUP_WINDOW_SECONDS = 300
DAILY_LOSS_LIMIT = 25.0

# Wild experiment: no approved_symbols.json filter — trade any godmode signal
# that passes RVOL threshold and is not a known bad actor.
WILD_RVOL_MIN = 2.0

# Hardcoded blacklist: penny stocks, halted/delisted frequent fliers, and
# symbols that routinely gap / have no Alpaca liquidity.
WILD_BLACKLIST = {
    "MULN", "FFIE", "MVIS", "NKLA", "GOEV", "RIDE", "WKHS", "CLOV", "SPCE",
    "BBBYQ", "APRN", "MLGO", "SOXS", "SOXL", "UVXY", "SVXY", "VIXY",
    "TQQQ", "SQQQ", "SPXS", "SPXU", "SPXL", "UPRO", "TNA", "TZA",
    "LABU", "LABD", "FNGU", "FNGD", "KOLD", "BOIL",
}
POLL_SECONDS = 10
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 45
MAX_GROSS_EXPOSURE_USD = float(os.getenv("PAPER_MAX_GROSS_EXPOSURE_USD", str(TRADE_NOTIONAL * MAX_OPEN_POSITIONS)))
MAX_DAILY_REALIZED_LOSS_USD = float(os.getenv("PAPER_MAX_DAILY_REALIZED_LOSS_USD", str(DAILY_LOSS_LIMIT)))
CLOSE_VERIFY_ATTEMPTS = int(os.getenv("PAPER_CLOSE_VERIFY_ATTEMPTS", "3"))
CLOSE_VERIFY_SLEEP_S = int(os.getenv("PAPER_CLOSE_VERIFY_SLEEP_SECONDS", "5"))
EOD_FINAL_VERIFY_ATTEMPTS = int(os.getenv("PAPER_EOD_FINAL_VERIFY_ATTEMPTS", "3"))
EOD_FINAL_VERIFY_SLEEP_S = int(os.getenv("PAPER_EOD_FINAL_VERIFY_SLEEP_SECONDS", "10"))

DB_PATH = str(DATA_DIR / "wolfe_signals.db")
STATE_DB_PATH = str(DATA_DIR / "paper_sniper_state.db")
LOG_FILE = str(DATA_DIR / "paper_sniper.log")
LOCKFILE = "/tmp/paper_sniper.lock"
LOG_TZ = pytz.timezone("America/Chicago")
ET_TZ = pytz.timezone("America/New_York")

_paper_daily_start_equity = 0.0
_paper_daily_start_date = None
_eod_close_done_date = ""
PDT_EQUITY_BLOCK_KEY = "pdt_equity_entry_block_date"
_close_hold_until = {}


def log(msg):
    ts = datetime.now(LOG_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        stdout_target = Path(os.readlink("/proc/self/fd/1")).resolve()
        log_target = Path(LOG_FILE).resolve()
        if stdout_target == log_target:
            return
    except Exception:
        pass
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def post_discord(msg: str):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=5)
    except Exception as e:
        log(f"Discord alert failed: {e}")


def get_client():
    if not PAPER_KEY or not PAPER_SECRET:
        raise RuntimeError("Missing APCA_PAPER_KEY_ID or APCA_PAPER_SECRET_KEY")
    return TradingClient(PAPER_KEY, PAPER_SECRET, paper=True, url_override=PAPER_URL)


def ensure_state_db():
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_signals (
                signal_key TEXT PRIMARY KEY,
                signal_ts TEXT,
                symbol TEXT,
                signal_type TEXT,
                direction TEXT,
                processed_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS exit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                symbol TEXT,
                exit_reason TEXT,
                intended_exit_time TEXT,
                actual_fill_price REAL,
                actual_fill_timestamp TEXT,
                broker_order_id TEXT,
                retry_used INTEGER,
                verification_result TEXT,
                pnl_usd REAL
            )
            """
        )
        try:
            cur.execute("ALTER TABLE exit_events ADD COLUMN pnl_usd REAL")
        except Exception:
            pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_flags (
                flag_key TEXT PRIMARY KEY,
                flag_value TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def utc_now():
    return datetime.now(pytz.UTC)


def utc_now_str():
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def order_filled_ts(order):
    filled_at = getattr(order, "filled_at", None)
    if isinstance(filled_at, datetime):
        return filled_at.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
    if not filled_at:
        return None
    try:
        parsed = datetime.fromisoformat(str(filled_at).replace("Z", "+00:00"))
        return parsed.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def log_exit_event(symbol: str, exit_reason: str, intended_exit_time: str,
                   actual_fill_price, actual_fill_timestamp, broker_order_id,
                   retry_used: bool, verification_result: str, pnl_usd=None):
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO exit_events (
                created_at, symbol, exit_reason, intended_exit_time,
                actual_fill_price, actual_fill_timestamp, broker_order_id,
                retry_used, verification_result, pnl_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_str(),
                str(symbol).upper(),
                exit_reason,
                intended_exit_time,
                actual_fill_price,
                actual_fill_timestamp,
                str(broker_order_id) if broker_order_id else None,
                1 if retry_used else 0,
                verification_result,
                pnl_usd,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_daily_realized_pnl():
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl_usd), 0)
            FROM exit_events
            WHERE verification_result LIKE 'closed:%'
              AND substr(created_at, 1, 10) = ?
            """,
            (datetime.now(LOG_TZ).strftime("%Y-%m-%d"),),
        ).fetchone()
        return 0.0 if row is None else float(row[0] or 0.0)
    except Exception:
        return 0.0
    finally:
        conn.close()


def get_position_for_symbol(client, symbol: str):
    try:
        for position in client.get_all_positions():
            if position.symbol == symbol:
                return position
    except Exception as e:
        log(f"Position lookup failed for {symbol}: {e}")
    return None


def _trading_day_str():
    return datetime.now(ET_TZ).strftime("%Y-%m-%d")


def get_runtime_flag(flag_key: str):
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        row = conn.execute(
            "SELECT flag_value FROM runtime_flags WHERE flag_key = ? LIMIT 1",
            (flag_key,),
        ).fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()


def set_runtime_flag(flag_key: str, flag_value: str):
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO runtime_flags (flag_key, flag_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(flag_key) DO UPDATE SET
                flag_value = excluded.flag_value,
                updated_at = excluded.updated_at
            """,
            (flag_key, flag_value, utc_now_str()),
        )
        conn.commit()
    finally:
        conn.close()


def is_equity_symbol(symbol: str):
    text = str(symbol or "").upper()
    return bool(text) and "-USD" not in text and "/" not in text and not text.startswith("^")


def is_pdt_block_error(error) -> bool:
    text = str(error or "").lower()
    return "pattern day trading" in text or "40310100" in text


def is_qty_held_for_orders_error(error) -> bool:
    text = str(error or "").lower()
    return (
        "40310000" in text
        or "insufficient qty available for order" in text
        or "held_for_orders" in text
    )


def pdt_equity_entry_block_active() -> bool:
    return get_runtime_flag(PDT_EQUITY_BLOCK_KEY) == _trading_day_str()


def activate_pdt_equity_entry_block(symbol: str, exit_reason: str, error):
    today_str = _trading_day_str()
    if pdt_equity_entry_block_active():
        return
    set_runtime_flag(PDT_EQUITY_BLOCK_KEY, today_str)
    msg = (
        f"BROKER EXIT BLOCKED {symbol}: {exit_reason} close denied by PDT protection. "
        f"Blocking new equity entries for {today_str} ET. Raw error: {error}"
    )
    log(msg)
    post_discord(f"🚨 PAPER PDT GUARD | {symbol} close denied | blocking new equity entries for rest of day")


def find_open_order_for_symbol(client, symbol: str):
    try:
        orders = client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.OPEN,
                symbols=[str(symbol).upper()],
                limit=50,
            )
        )
    except Exception:
        return None

    symbol_upper = str(symbol).upper()
    for order in orders or []:
        if str(getattr(order, "symbol", "")).upper() != symbol_upper:
            continue
        status = str(getattr(order, "status", "unknown")).lower()
        if status in {"new", "accepted", "pending_new", "partially_filled"}:
            return order
    return None


def set_close_hold(symbol: str, seconds: int | None = None):
    hold_seconds = int(seconds or max(CLOSE_VERIFY_SLEEP_S * CLOSE_VERIFY_ATTEMPTS, CLOSE_VERIFY_SLEEP_S))
    _close_hold_until[str(symbol).upper()] = time.time() + hold_seconds


def close_hold_active(symbol: str) -> bool:
    expiry = _close_hold_until.get(str(symbol).upper())
    return bool(expiry and expiry > time.time())


def clear_close_hold(symbol: str):
    _close_hold_until.pop(str(symbol).upper(), None)


def gross_exposure(positions) -> float:
    total = 0.0
    for position in positions or []:
        try:
            total += abs(float(getattr(position, "market_value", 0.0)))
        except Exception:
            try:
                total += abs(float(getattr(position, "qty", 0.0))) * abs(float(getattr(position, "current_price", 0.0)))
            except Exception:
                pass
    return total


def entry_kill_switch_reason(client, positions=None):
    open_positions = positions if positions is not None else client.get_all_positions()
    if len(open_positions) >= MAX_OPEN_POSITIONS:
        return f"max open positions reached ({len(open_positions)}/{MAX_OPEN_POSITIONS})"
    current_gross = gross_exposure(open_positions)
    if current_gross >= MAX_GROSS_EXPOSURE_USD:
        return f"gross exposure cap reached (${current_gross:.2f}/${MAX_GROSS_EXPOSURE_USD:.2f})"
    realized_loss = get_daily_realized_pnl()
    if realized_loss <= -MAX_DAILY_REALIZED_LOSS_USD:
        return f"daily realized loss limit hit (${abs(realized_loss):.2f}/${MAX_DAILY_REALIZED_LOSS_USD:.2f})"
    return None


def parse_signal_timestamp(signal_ts: str):
    if not signal_ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return pytz.UTC.localize(datetime.strptime(signal_ts, fmt))
        except ValueError:
            continue
    return None


def parse_signal_direction(signal_text: str, flow_m: float | None = None, change_pct: float | None = None):
    text = str(signal_text or "").upper()
    short_terms = (
        "ABSORPTION SELL",
        "STRONG SELL",
        "BULL TRAP",
        "BEAR",
        "SELL",
        "SHORT",
    )
    long_terms = (
        "ABSORPTION BUY",
        "STRONG BUY",
        "BEAR TRAP",
        "CLIMAX",
        "BULL",
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
    if "ABSORPTION WALL" in text and flow_m is not None:
        return "SHORT" if float(flow_m) > 0 else "LONG"
    if "FAKE-OUT" in text and change_pct is not None:
        if float(change_pct) > 0:
            return "SHORT"
        if float(change_pct) < 0:
            return "LONG"
    return None


def is_tradable_symbol(symbol: str):
    text = str(symbol or "").upper()
    if not text:
        return False
    if "=F" in text or "-USD" in text or text.startswith("^"):
        return False
    return True


def make_dedup_key(signal_ts: str, symbol: str, direction: str):
    parsed_ts = parse_signal_timestamp(signal_ts)
    if parsed_ts is None:
        return None
    bucket = int(parsed_ts.timestamp()) // DEDUP_WINDOW_SECONDS
    return f"{bucket}|{symbol.upper()}|{direction}"


def is_signal_processed(signal_key: str):
    if not signal_key:
        return False
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        row = conn.execute(
            "SELECT 1 FROM processed_signals WHERE signal_key = ? LIMIT 1",
            (signal_key,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def mark_signal_processed(signal_key: str, signal_ts: str, symbol: str, signal_type: str, direction: str):
    conn = sqlite3.connect(STATE_DB_PATH)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO processed_signals (
                signal_key, signal_ts, symbol, signal_type, direction, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                signal_key,
                signal_ts,
                symbol.upper(),
                signal_type,
                direction,
                utc_now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_new_signals(last_check):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Wild mode: fetch ALL signal types (no signal_type filter), include rvol
        cur.execute(
            """
            SELECT rowid, timestamp, symbol, signal_type, price, flow_m, change_pct, rvol
            FROM signals
            WHERE timestamp > ?
            ORDER BY timestamp ASC
            """,
            (last_check,),
        )
        return cur.fetchall()
    except Exception as e:
        log(f"Signal fetch error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def close_and_verify_position(client, symbol: str, exit_reason: str, intended_exit_time: str | None = None,
                              pnl_usd=None):
    intended_ts = intended_exit_time or utc_now_str()
    retry_used = False
    order_id = None
    exit_order = None
    verification_result = "still_open_after_verification"
    wait_for_existing_close = False

    for attempt in range(1, CLOSE_VERIFY_ATTEMPTS + 1):
        if attempt > 1:
            retry_used = True
            log(f"CLOSE VERIFY RETRY {symbol}: attempt={attempt}/{CLOSE_VERIFY_ATTEMPTS} reason=still_open_or_ambiguous")
        should_submit_close = not wait_for_existing_close and not close_hold_active(symbol)
        if not should_submit_close and close_hold_active(symbol):
            log(f"CLOSE VERIFY HOLD {symbol}: broker still reserving qty from an in-flight close; waiting before any new close submit")
        if order_id:
            try:
                existing_order = client.get_order_by_id(order_id)
                existing_status = str(getattr(existing_order, "status", "unknown")).lower()
                if existing_status in {"new", "accepted", "pending_new", "partially_filled"}:
                    should_submit_close = False
                    log(f"CLOSE VERIFY HOLD {symbol}: existing order_id={order_id} status={existing_status} still working")
            except Exception:
                pass

        if should_submit_close:
            try:
                exit_order = client.close_position(symbol)
                order_id = getattr(exit_order, "id", None)
            except Exception as e:
                log(f"CLOSE SUBMIT {symbol}: {e}")
                if is_equity_symbol(symbol) and is_pdt_block_error(e):
                    verification_result = "broker_blocked:pdt"
                    activate_pdt_equity_entry_block(symbol, exit_reason, e)
                    log_exit_event(
                        symbol,
                        exit_reason,
                        intended_ts,
                        None,
                        None,
                        order_id,
                        retry_used,
                        verification_result,
                        pnl_usd,
                    )
                    return False
                if is_qty_held_for_orders_error(e):
                    wait_for_existing_close = True
                    set_close_hold(symbol)
                    open_order = find_open_order_for_symbol(client, symbol)
                    if open_order is not None:
                        order_id = getattr(open_order, "id", None) or order_id
                        held_status = str(getattr(open_order, "status", "unknown")).lower()
                        log(
                            f"CLOSE VERIFY HOLD {symbol}: qty already reserved by broker "
                            f"order_id={order_id or 'n/a'} status={held_status}; waiting on broker fill"
                        )
                    else:
                        log(f"CLOSE VERIFY HOLD {symbol}: qty already reserved by broker; waiting before any new close submit")

        time.sleep(CLOSE_VERIFY_SLEEP_S)

        position = get_position_for_symbol(client, symbol)
        broker_fill_price = None
        broker_fill_ts = None
        status = "unknown"
        if order_id:
            try:
                exit_order = client.get_order_by_id(order_id)
                status = str(getattr(exit_order, "status", "unknown")).lower()
                fill_value = getattr(exit_order, "filled_avg_price", None)
                broker_fill_price = float(fill_value) if fill_value is not None else None
                broker_fill_ts = order_filled_ts(exit_order)
            except Exception as e:
                log(f"CLOSE VERIFY {symbol}: order lookup failed for {order_id}: {e}")

        if position is None:
            clear_close_hold(symbol)
            verification_result = f"closed:{status}"
            log(f"CLOSE VERIFIED {symbol}: reason={exit_reason} order_id={order_id or 'n/a'} result={verification_result}")
            log_exit_event(
                symbol,
                exit_reason,
                intended_ts,
                broker_fill_price,
                broker_fill_ts or utc_now_str(),
                order_id,
                retry_used,
                verification_result,
                pnl_usd,
            )
            return True

        log(f"CLOSE VERIFY WAIT {symbol}: attempt={attempt}/{CLOSE_VERIFY_ATTEMPTS} status={status} position_still_open")

    log(f"CLOSE VERIFICATION FAILED {symbol}: reason={exit_reason} result={verification_result}")
    log_exit_event(
        symbol,
        exit_reason,
        intended_ts,
        None,
        None,
        order_id,
        retry_used,
        verification_result,
        pnl_usd,
    )
    return False


def manage_positions(client):
    try:
        positions = client.get_all_positions()
        for p in positions:
            pnl_pct = float(p.unrealized_plpc)
            symbol = p.symbol
            side = "SHORT" if float(p.qty) < 0 else "LONG"
            if pnl_pct >= TAKE_PROFIT_PCT:
                log(f"TAKE PROFIT: {symbol} {side} +{pnl_pct:.2%} closing")
                try:
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    verified = close_and_verify_position(client, symbol, "take_profit", pnl_usd=pnl_usd)
                    if verified:
                        log(f"CLOSED {symbol} FOR PROFIT")
                        post_discord(f"📄 PAPER CLOSE | {symbol} TAKE PROFIT | +${pnl_usd:.2f} (+{pnl_pct:.2%})")
                except Exception as e:
                    log(f"CLOSE FAIL {symbol}: {e}")
            elif pnl_pct <= -STOP_LOSS_PCT:
                log(f"STOP LOSS: {symbol} {side} {pnl_pct:.2%} closing")
                try:
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    verified = close_and_verify_position(client, symbol, "stop_loss", pnl_usd=pnl_usd)
                    if verified:
                        log(f"STOP LOSS EXECUTED {symbol}")
                        post_discord(f"📄 PAPER CLOSE | {symbol} STOP LOSS | -${abs(pnl_usd):.2f} ({pnl_pct:.2%})")
                except Exception as e:
                    log(f"STOP LOSS FAIL {symbol}: {e}")
    except Exception as e:
        log(f"Position management error: {e}")


def check_daily_loss_limit(equity):
    global _paper_daily_start_equity, _paper_daily_start_date
    today = datetime.now(LOG_TZ).strftime("%Y-%m-%d")
    if _paper_daily_start_date != today:
        _paper_daily_start_equity = equity
        _paper_daily_start_date = today
        log(f"New trading day - paper starting equity: ${equity:.2f}")
    daily_pnl = equity - _paper_daily_start_equity
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        log(f"DAILY LOSS LIMIT HIT: down ${abs(daily_pnl):.2f} (limit ${DAILY_LOSS_LIMIT})")
        return False
    return True


def maybe_force_close_eod(client):
    global _eod_close_done_date
    now_et = datetime.now(ET_TZ)
    if now_et.weekday() >= 5:
        return
    today_str = now_et.strftime("%Y-%m-%d")
    close_time = now_et.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE, second=0, microsecond=0)
    if now_et < close_time or _eod_close_done_date == today_str:
        return
    try:
        positions = client.get_all_positions()
        if positions:
            msg = f"🔴 EOD FORCE CLOSE (3:45pm ET) - closing all {len(positions)} paper position(s). No overnight holds."
            log(msg)
            post_discord(msg)
            for p in positions:
                sym = p.symbol
                side = "SHORT" if float(p.qty) < 0 else "LONG"
                pnl_pct = float(p.unrealized_plpc)
                log(f"EOD FORCE CLOSE {sym} {side} {pnl_pct:.2%}")
                try:
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    close_and_verify_position(client, sym, "eod_force_close", pnl_usd=pnl_usd)
                except Exception as e:
                    log(f"EOD CLOSE FAIL {sym}: {e}")
            for attempt in range(1, EOD_FINAL_VERIFY_ATTEMPTS + 1):
                remaining = client.get_all_positions()
                if not remaining:
                    log(f"EOD FINAL VERIFY PASS: flat on attempt {attempt}/{EOD_FINAL_VERIFY_ATTEMPTS}")
                    break
                syms = ", ".join(p.symbol for p in remaining)
                log(f"EOD FINAL VERIFY RETRY: attempt {attempt}/{EOD_FINAL_VERIFY_ATTEMPTS} still open={syms}")
                for p in remaining:
                    pnl_pct = float(p.unrealized_plpc)
                    close_and_verify_position(
                        client,
                        p.symbol,
                        "eod_force_close",
                        pnl_usd=TRADE_NOTIONAL * pnl_pct,
                    )
                if attempt < EOD_FINAL_VERIFY_ATTEMPTS:
                    time.sleep(EOD_FINAL_VERIFY_SLEEP_S)
            final_remaining = client.get_all_positions()
            if final_remaining:
                syms = ", ".join(p.symbol for p in final_remaining)
                log(f"EOD FINAL VERIFY FAILED: still open={syms}")
                post_discord(f"🚨 PAPER EOD CLOSE FAILED | still open: {syms}")
        else:
            log("EOD FORCE CLOSE (3:45pm ET): already flat")
    except Exception as e:
        log(f"EOD force-close error: {e}")
    _eod_close_done_date = today_str


def can_open_new_positions_now():
    now_et = datetime.now(ET_TZ)
    if now_et.weekday() >= 5:
        return False
    close_time = now_et.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE, second=0, microsecond=0)
    return now_et < close_time


def execute_signal(client, symbol: str, price: float, signal: str, direction: str):
    try:
        if not is_tradable_symbol(symbol):
            log(f"SKIP {symbol}: not tradable via Alpaca paper")
            return False
        if price <= 0:
            log(f"SKIP {symbol}: invalid signal price {price}")
            return False
        positions = client.get_all_positions()
        kill_switch_reason = entry_kill_switch_reason(client, positions=positions)
        if kill_switch_reason:
            log(f"ENTRY BLOCKED {symbol}: {kill_switch_reason}")
            return False
        if is_equity_symbol(symbol) and pdt_equity_entry_block_active():
            log(f"ENTRY BLOCKED {symbol}: equity entries paused for today after broker denied an earlier close (PDT protection)")
            return False
        held_symbols = {p.symbol for p in positions}
        if symbol in held_symbols:
            log(f"SKIP {symbol}: already have position")
            return False
        if len(positions) >= MAX_OPEN_POSITIONS:
            log(f"SKIP {symbol}: max positions reached ({MAX_OPEN_POSITIONS})")
            return False

        qty = max(1, int(TRADE_NOTIONAL / price))
        side = OrderSide.SELL if direction == "SHORT" else OrderSide.BUY
        client.submit_order(
            MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.DAY,
            )
        )
        log(f"{direction} ENTERED: {symbol} ${TRADE_NOTIONAL:.2f} | {signal}")
        post_discord(f"📄 PAPER {direction} ENTERED | {symbol} @ ~${price:.2f} | {signal} | ${TRADE_NOTIONAL:.0f} notional")
        return True
    except Exception as e:
        log(f"ENTRY FAIL {symbol} {direction}: {e}")
        return False


def run():
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log(f"Already running (PID {pid}). Exiting.")
            sys.exit(0)
        except (ProcessLookupError, ValueError, OSError):
            log("Stale lockfile found. Taking over.")
    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))

    try:
        _run()
    finally:
        try:
            os.remove(LOCKFILE)
        except Exception:
            pass


def _run():
    ensure_state_db()
    log("PAPER SNIPER starting - wild experiment mode, no approved-symbols filter")
    log(
        f"Trade size: ${TRADE_NOTIONAL:.2f} | TP: {TAKE_PROFIT_PCT:.2%} | "
        f"SL: {STOP_LOSS_PCT:.2%} | Max positions: {MAX_OPEN_POSITIONS}"
    )
    log(
        f"Signal max age: {MAX_SIGNAL_AGE_SECONDS}s | Dedup window: {DEDUP_WINDOW_SECONDS}s | "
        f"EOD force close: 3:45pm ET | RVOL min: {WILD_RVOL_MIN} | Blacklist: {len(WILD_BLACKLIST)} symbols"
    )
    if pdt_equity_entry_block_active():
        log(f"PDT EQUITY ENTRY GUARD ACTIVE for {_trading_day_str()} ET - new equity entries paused after earlier broker exit denial")
    else:
        log(f"PDT EQUITY ENTRY GUARD INACTIVE for {_trading_day_str()} ET")
    post_discord(
        f"📄 PAPER SNIPER ONLINE | wild mode (no roster filter) | ${TRADE_NOTIONAL:.0f}/trade | "
        f"TP {TAKE_PROFIT_PCT:.2%} SL {STOP_LOSS_PCT:.2%} | RVOL≥{WILD_RVOL_MIN} | 3:45pm ET flat"
    )

    client = get_client()
    last_check = (datetime.utcnow() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")

    acct = client.get_account()
    log(f"Paper account equity: ${float(acct.equity):.2f}")

    while True:
        try:
            maybe_force_close_eod(client)

            clock = client.get_clock()
            if not clock.is_open:
                time.sleep(30)
                continue

            manage_positions(client)

            acct = client.get_account()
            equity = float(acct.equity or 0.0)
            if not check_daily_loss_limit(equity):
                time.sleep(POLL_SECONDS)
                continue

            signals = get_new_signals(last_check)
            if signals and can_open_new_positions_now():
                for rowid, signal_ts, symbol, signal_type, price, flow_m, change_pct, rvol in signals:
                    sym_upper = str(symbol).upper()

                    # Wild experiment filters (approved_symbols.json intentionally bypassed)
                    rvol_val = float(rvol) if rvol is not None else 0.0
                    if rvol_val < WILD_RVOL_MIN:
                        log(f"SKIP {sym_upper}: rvol={rvol_val:.2f} < {WILD_RVOL_MIN}")
                        continue
                    if sym_upper in WILD_BLACKLIST:
                        log(f"SKIP {sym_upper}: blacklisted")
                        continue

                    direction = parse_signal_direction(signal_type, flow_m=flow_m, change_pct=change_pct)
                    if direction is None:
                        log(f"SKIP {sym_upper}: unknown direction for signal '{signal_type}'")
                        continue

                    parsed_ts = parse_signal_timestamp(signal_ts)
                    if parsed_ts is None:
                        log(f"SKIP {sym_upper}: invalid timestamp '{signal_ts}'")
                        continue

                    signal_age_seconds = max(0.0, (utc_now() - parsed_ts).total_seconds())
                    if signal_age_seconds > MAX_SIGNAL_AGE_SECONDS:
                        log(
                            f"SKIP {sym_upper} {direction}: stale signal "
                            f"age={signal_age_seconds:.1f}s signal=${float(price):.4f}"
                        )
                        continue

                    signal_key = make_dedup_key(signal_ts, sym_upper, direction)
                    if is_signal_processed(signal_key):
                        log(f"SKIP DUPE {sym_upper} {direction}: already acted on this 5-minute window")
                        continue

                    if execute_signal(client, sym_upper, float(price), signal_type, direction):
                        mark_signal_processed(signal_key, signal_ts, sym_upper, signal_type, direction)

            last_check = (datetime.utcnow() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
            positions = client.get_all_positions()
            log(f"heartbeat | paper equity=${equity:.2f} | positions={len(positions)}")
            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            log("Paper sniper stopped")
            break
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    run()
