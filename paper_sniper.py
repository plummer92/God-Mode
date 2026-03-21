#!/usr/bin/env python3
"""
paper_sniper.py - Paper trading bot for SHORT signals
Uses Alpaca paper account to simulate IBKR short trades
Tracks performance so we have real data when we switch to live IBKR
"""
import os, sqlite3, time, json
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv("/home/theplummer92/.env")

# Paper account config
PAPER_KEY    = os.getenv("APCA_PAPER_KEY_ID")
PAPER_SECRET = os.getenv("APCA_PAPER_SECRET_KEY")
PAPER_URL    = os.getenv("APCA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")

# Strategy config - based on strategy lab results
TRADE_NOTIONAL   = 50.0   # $50 per trade (simulating $500 IBKR account)
TAKE_PROFIT_PCT  = 0.04
STOP_LOSS_PCT    = 0.02
MAX_POSITIONS    = 5
DAILY_LOSS_LIMIT = 25.0
POLL_SECONDS     = 10

# Best short symbols from strategy lab
SHORT_APPROVED = ["IWM","SPY","QQQ","NVDA","TSLA","META","AMZN","GME","AMD","COIN"]

DB_PATH  = "/home/theplummer92/wolfe_signals.db"
LOG_FILE = "/home/theplummer92/paper_sniper.log"
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
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("""
            SELECT timestamp, symbol, signal_type, price FROM signals
            WHERE timestamp > ?
            AND (signal_type LIKE '%STRONG SELL%' OR signal_type LIKE '%ABSORPTION SELL%')
            ORDER BY timestamp ASC
        """, (last_check,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except:
        return []

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
                except Exception as e:
                    log(f"CLOSE FAIL {symbol}: {e}")
            elif pnl_pct <= -STOP_LOSS_PCT:
                log(f"STOP LOSS: {symbol} {side} {pnl_pct:.2%} closing")
                try:
                    client.close_position(symbol)
                    log(f"STOP LOSS EXECUTED {symbol}")
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
        if symbol not in SHORT_APPROVED:
            log(f"SKIP {symbol}: not in short approved list")
            return

        client.submit_order(MarketOrderRequest(
            symbol=symbol,
            notional=TRADE_NOTIONAL,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        ))
        log(f"SHORT ENTERED: {symbol} ${TRADE_NOTIONAL} | {signal}")
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
    log("PAPER SNIPER starting - SHORT only, paper account")
    log(f"Trade size: ${TRADE_NOTIONAL} | TP: {TAKE_PROFIT_PCT:.0%} | SL: {STOP_LOSS_PCT:.0%}")
    log(f"Short approved: {', '.join(SHORT_APPROVED)}")

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
