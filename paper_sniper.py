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

import pytz
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

load_dotenv("/home/theplummer92/.env")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
PAPER_KEY = os.getenv("APCA_PAPER_KEY_ID")
PAPER_SECRET = os.getenv("APCA_PAPER_SECRET_KEY")
PAPER_URL = os.getenv("APCA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

TRADE_NOTIONAL = 500.0
TAKE_PROFIT_PCT = 0.007
STOP_LOSS_PCT = 0.007
MAX_OPEN_POSITIONS = 20
MAX_SIGNAL_AGE_SECONDS = 1800
DEDUP_WINDOW_SECONDS = 300
DAILY_LOSS_LIMIT = 25.0
POLL_SECONDS = 10
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 45

DB_PATH = "/home/theplummer92/wolfe_signals.db"
STATE_DB_PATH = "/home/theplummer92/paper_sniper_state.db"
LOG_FILE = "/home/theplummer92/paper_sniper.log"
LOCKFILE = "/tmp/paper_sniper.lock"
LOG_TZ = pytz.timezone("America/Chicago")
ET_TZ = pytz.timezone("America/New_York")

_paper_daily_start_equity = 0.0
_paper_daily_start_date = None
_eod_close_done_date = ""


def log(msg):
    ts = datetime.now(LOG_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
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
        conn.commit()
    finally:
        conn.close()


def utc_now():
    return datetime.now(pytz.UTC)


def parse_signal_timestamp(signal_ts: str):
    if not signal_ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return pytz.UTC.localize(datetime.strptime(signal_ts, fmt))
        except ValueError:
            continue
    return None


def parse_signal_direction(signal_text: str):
    text = str(signal_text or "").upper()
    short_terms = (
        "ABSORPTION SELL",
        "STRONG SELL",
        "BEAR",
        "SELL",
        "SHORT",
    )
    long_terms = (
        "ABSORPTION BUY",
        "STRONG BUY",
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
        cur.execute(
            """
            SELECT rowid, timestamp, symbol, signal_type, price
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
                    client.close_position(symbol)
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    log(f"CLOSED {symbol} FOR PROFIT")
                    post_discord(f"📄 PAPER CLOSE | {symbol} TAKE PROFIT | +${pnl_usd:.2f} (+{pnl_pct:.2%})")
                except Exception as e:
                    log(f"CLOSE FAIL {symbol}: {e}")
            elif pnl_pct <= -STOP_LOSS_PCT:
                log(f"STOP LOSS: {symbol} {side} {pnl_pct:.2%} closing")
                try:
                    client.close_position(symbol)
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
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
                    client.close_position(sym)
                except Exception as e:
                    log(f"EOD CLOSE FAIL {sym}: {e}")
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
    log("PAPER SNIPER starting - wild experiment mode, all signal types")
    log(
        f"Trade size: ${TRADE_NOTIONAL:.2f} | TP: {TAKE_PROFIT_PCT:.2%} | "
        f"SL: {STOP_LOSS_PCT:.2%} | Max positions: {MAX_OPEN_POSITIONS}"
    )
    log(
        f"Signal max age: {MAX_SIGNAL_AGE_SECONDS}s | Dedup window: {DEDUP_WINDOW_SECONDS}s | "
        f"EOD force close: 3:45pm ET"
    )
    post_discord(
        f"📄 PAPER SNIPER ONLINE | wild mode | ${TRADE_NOTIONAL:.0f}/trade | "
        f"TP {TAKE_PROFIT_PCT:.2%} SL {STOP_LOSS_PCT:.2%} | 3:45pm ET flat"
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
                for rowid, signal_ts, symbol, signal_type, price in signals:
                    direction = parse_signal_direction(signal_type)
                    if direction is None:
                        log(f"SKIP {symbol}: unknown direction for signal '{signal_type}'")
                        continue

                    parsed_ts = parse_signal_timestamp(signal_ts)
                    if parsed_ts is None:
                        log(f"SKIP {symbol}: invalid timestamp '{signal_ts}'")
                        continue

                    signal_age_seconds = max(0.0, (utc_now() - parsed_ts).total_seconds())
                    if signal_age_seconds > MAX_SIGNAL_AGE_SECONDS:
                        log(
                            f"SKIP {symbol} {direction}: stale signal "
                            f"age={signal_age_seconds:.1f}s signal=${float(price):.4f}"
                        )
                        continue

                    signal_key = make_dedup_key(signal_ts, symbol, direction)
                    if is_signal_processed(signal_key):
                        log(f"SKIP DUPE {symbol} {direction}: already acted on this 5-minute window")
                        continue

                    if execute_signal(client, str(symbol).upper(), float(price), signal_type, direction):
                        mark_signal_processed(signal_key, signal_ts, symbol, signal_type, direction)

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
