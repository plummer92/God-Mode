#!/usr/bin/env python3
"""
paper_sniper.py - Paper trading bot for SHORT signals only
Uses Alpaca paper account to test short strategies before sizing up on live account.
"""
import os, sys, sqlite3, time, json, requests
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv("/home/theplummer92/.env")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def post_discord(msg: str):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=5)
    except Exception as e:
        log(f"Discord alert failed: {e}")

# Paper account config
PAPER_KEY    = os.getenv("APCA_PAPER_KEY_ID")
PAPER_SECRET = os.getenv("APCA_PAPER_SECRET_KEY")
PAPER_URL    = os.getenv("APCA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

# Strategy config - based on strategy lab results
TRADE_NOTIONAL   = 50.0   # $50 per trade for paper short testing
TAKE_PROFIT_PCT  = 0.04
STOP_LOSS_PCT    = 0.02
MAX_POSITIONS    = 5
DAILY_LOSS_LIMIT = 25.0
POLL_SECONDS     = 10

APPROVED_PATH = "/home/theplummer92/approved_symbols.json"

def load_short_approved():
    try:
        with open(APPROVED_PATH) as f:
            data = json.load(f)
        return [s.upper() for s in data.get("sell", data.get("short", []))]
    except Exception:
        return ["IWM","SPY","QQQ","NVDA","TSLA","META","AMZN","GME","AMD","COIN"]

DB_PATH  = "/home/theplummer92/wolfe_signals.db"
LOG_FILE = "/home/theplummer92/paper_sniper.log"
LOCKFILE = "/tmp/paper_sniper.lock"
CST      = pytz.timezone("America/Chicago")

_paper_daily_start_equity = 0.0
_paper_daily_start_date   = None

def log(msg):
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def get_client():
    return TradingClient(PAPER_KEY, PAPER_SECRET, paper=True, url_override=PAPER_URL)

def get_new_signals(last_check):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("""
            SELECT timestamp, symbol, signal_type, price FROM signals
            WHERE timestamp > ?
            AND (signal_type LIKE '%STRONG SELL%' OR signal_type LIKE '%ABSORPTION SELL%' OR (signal_type LIKE '%CLIMAX%' AND rvol >= 5.0))
            ORDER BY timestamp ASC
        """, (last_check,))
        return cur.fetchall()
    except:
        return []
    finally:
        if conn:
            conn.close()

def get_regime():
    try:
        with open("/home/theplummer92/regime_snapshot.json") as f:
            return json.load(f)
    except:
        return {"vix": 20, "regime": "NEUTRAL"}

def manage_positions(client):
    try:
        positions = client.get_all_positions()
        for p in positions:
            pnl_pct = float(p.unrealized_plpc)
            symbol  = p.symbol
            side    = "SHORT" if float(p.qty) < 0 else "LONG"
            if pnl_pct >= TAKE_PROFIT_PCT:
                log(f"TAKE PROFIT: {symbol} {side} +{pnl_pct:.2%} closing")
                try:
                    client.close_position(symbol)
                    log(f"CLOSED {symbol} FOR PROFIT")
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    post_discord(f"📄 **PAPER CLOSE** | {symbol} TAKE PROFIT | +${pnl_usd:.2f} (+{pnl_pct:.2%})")
                except Exception as e:
                    log(f"CLOSE FAIL {symbol}: {e}")
            elif pnl_pct <= -STOP_LOSS_PCT:
                log(f"STOP LOSS: {symbol} {side} {pnl_pct:.2%} closing")
                try:
                    client.close_position(symbol)
                    log(f"STOP LOSS EXECUTED {symbol}")
                    pnl_usd = TRADE_NOTIONAL * pnl_pct
                    post_discord(f"📄 **PAPER CLOSE** | {symbol} STOP LOSS | -${abs(pnl_usd):.2f} ({pnl_pct:.2%})")
                except Exception as e:
                    log(f"STOP LOSS FAIL {symbol}: {e}")
            elif pnl_pct < -0.01:
                log(f"MONITOR: {symbol} {side} {pnl_pct:.2%}")
    except Exception as e:
        log(f"Position management error: {e}")

def execute_short(client, symbol, price, signal):
    try:
        positions = client.get_all_positions()
        syms = [p.symbol for p in positions]
        if symbol in syms:
            log(f"SKIP {symbol}: already have position")
            return
        if len(positions) >= MAX_POSITIONS:
            log(f"SKIP {symbol}: max positions reached")
            return
        if symbol not in load_short_approved():
            log(f"SKIP {symbol}: not in short approved list")
            return

        qty = max(1, int(TRADE_NOTIONAL / price))
        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        ))
        log(f"SHORT ENTERED: {symbol} ${TRADE_NOTIONAL} | {signal}")
        post_discord(f"📄 **PAPER SHORT ENTERED** | {symbol} @ ~${price:.2f} | {signal} | ${TRADE_NOTIONAL} notional")
    except Exception as e:
        log(f"SHORT FAIL {symbol}: {e}")

def check_daily_loss_limit(equity):
    global _paper_daily_start_equity, _paper_daily_start_date
    today = datetime.now(CST).strftime("%Y-%m-%d")
    if _paper_daily_start_date != today:
        _paper_daily_start_equity = equity
        _paper_daily_start_date   = today
        log(f"New trading day — paper starting equity: ${equity:.2f}")
    daily_pnl = equity - _paper_daily_start_equity
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        log(f"DAILY LOSS LIMIT HIT: down ${abs(daily_pnl):.2f} (limit ${DAILY_LOSS_LIMIT}) — standing down")
        return False
    return True


def run():
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log(f"⛔ Already running (PID {pid}). Exiting.")
            sys.exit(0)
        except (ProcessLookupError, ValueError, OSError):
            log("⚠️ Stale lockfile found. Taking over.")
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
    log("PAPER SNIPER starting - SHORT only, paper account")
    log(f"Trade size: ${TRADE_NOTIONAL} | TP: {TAKE_PROFIT_PCT:.0%} | SL: {STOP_LOSS_PCT:.0%}")
    log(f"Short approved: {', '.join(load_short_approved())}")
    post_discord(f"📄 **PAPER SNIPER ONLINE** | TP {TAKE_PROFIT_PCT:.0%} SL {STOP_LOSS_PCT:.0%} | ${TRADE_NOTIONAL}/trade")

    client     = get_client()
    last_check = (datetime.utcnow() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")

    # Log starting equity
    acct = client.get_account()
    log(f"Paper account equity: ${float(acct.equity):.2f}")

    while True:
        try:
            # Check market hours
            clock = client.get_clock()
            if not clock.is_open:
                time.sleep(60)
                continue

            # Check regime
            regime = get_regime()
            vix = float(regime.get("vix") or 20)

            # Manage existing positions
            manage_positions(client)

            # Check daily loss limit
            acct   = client.get_account()
            equity = float(acct.equity or 0)
            if not check_daily_loss_limit(equity):
                time.sleep(60)
                continue

            # Get new SELL signals
            signals = get_new_signals(last_check)
            if signals:
                # Deduplicate - one signal per hour per symbol
                seen = set()
                for sig in signals:
                    ts_str, symbol, sig_type, price = sig
                    key = (ts_str[:13], symbol)
                    if key in seen:
                        continue
                    seen.add(key)
                    execute_short(client, symbol, float(price), sig_type)
                last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

            # Heartbeat every minute
            log(f"heartbeat | paper equity=${equity:.2f} | VIX={vix:.1f} | positions={len(client.get_all_positions())}")
            time.sleep(60)

        except KeyboardInterrupt:
            log("Paper sniper stopped")
            break
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run()
