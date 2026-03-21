import os
import sys
import json
import time
import sqlite3
from datetime import datetime, timezone, date, timedelta
import pytz
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# -------------------- CONFIG --------------------
TRADE_NOTIONAL_USD = 10       # Trade size per signal
HARD_STOP_LOSS_PCT = 0.02     # -2.0% hard stop loss
TAKE_PROFIT_PCT = 0.04        # +4.0% take profit
DAILY_LOSS_LIMIT_USD = 5.00   # Stop trading if down $5 in one day
MAX_OPEN_POSITIONS = 5        # Never hold more than 5 positions at once

# Paths
DB_PATH = "/home/theplummer92/wolfe_signals.db"
SNIPER_LOG = "/home/theplummer92/sniper.log"
REGIME_PATH = "/home/theplummer92/regime_snapshot.json"
LOCKFILE = "/tmp/sniper_bot.lock"
POLL_SECONDS = 10

BOT_VERSION = "SNIPER V8.0 (SHORTING ENABLED)"
STATUS_DB = DB_PATH

# -------------------- SETUP --------------------
cst_tz = pytz.timezone("America/Chicago")
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")


# -------------------- LOGGING --------------------
_daily_start_balance = 0.0
_daily_start_date = None

def log_line(msg: str):
    ts = datetime.now(cst_tz).strftime("%Y-%m-%d %H:%M:%S")
    full = f"[{ts}] {msg}"
    print(full, flush=True)
    with open(SNIPER_LOG, "a", encoding="utf-8") as f:
        f.write(full + "\n")


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

TRADE_LOG_DB = "/home/theplummer92/trade_log.db"

def init_trade_log():
    conn = sqlite3.connect(TRADE_LOG_DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, side TEXT, direction TEXT,
        entry_price REAL, exit_price REAL,
        entry_time TEXT, exit_time TEXT,
        pnl_pct REAL, pnl_usd REAL,
        signal_type TEXT, notional REAL,
        outcome TEXT, vix REAL, regime TEXT
    )""")
    conn.commit(); conn.close()

def log_trade_open(symbol, direction, entry_price, signal_type):
    try:
        regime_data = get_regime()
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""INSERT INTO trades
            (symbol, side, direction, entry_price, entry_time, signal_type, notional, outcome, vix, regime)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol, "buy" if direction=="LONG" else "sell", direction,
             entry_price, utc_now_str(), signal_type, TRADE_NOTIONAL_USD,
             "open", regime_data.get("vix",0), regime_data.get("regime","UNKNOWN")))
        conn.commit(); conn.close()
    except Exception as e:
        log_line(f"⚠️ Trade log open error: {e}")

def log_trade_close(symbol, exit_price, pnl_pct, outcome):
    try:
        pnl_usd = TRADE_NOTIONAL_USD * pnl_pct
        conn = sqlite3.connect(TRADE_LOG_DB)
        cur = conn.cursor()
        cur.execute("""UPDATE trades SET
            exit_price=?, exit_time=?, pnl_pct=?, pnl_usd=?, outcome=?
            WHERE symbol=? AND outcome='open'
            ORDER BY id DESC LIMIT 1""",
            (exit_price, utc_now_str(), pnl_pct, pnl_usd, outcome, symbol))
        conn.commit(); conn.close()
    except Exception as e:
        log_line(f"⚠️ Trade log close error: {e}")


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
    Returns trading mode based on regime and VIX:
    - OPEN      : all signals allowed (VIX < 25)
    - SELL_ONLY : only SELL/SHORT signals allowed (VIX 25-30, RISK_OFF)
    - BLOCKED   : no new entries at all (VIX >= 30)
    """
    r = get_regime()
    regime = r["regime"]
    vix = r["vix"]
    if vix >= 30:
        log_line(f"\U0001f321\ufe0f REGIME BLOCK: VIX={vix:.1f} >= 30 — too dangerous, standing down")
        return "BLOCKED"
    if vix >= 25:
        log_line(f"\U0001f4c9 REGIME SELL-ONLY: VIX={vix:.1f} >= 25 — shorts only, longs paused")
        return "SELL_ONLY"
    return "OPEN"

def is_regime_safe() -> bool:
    return get_regime_mode() != "BLOCKED"

def check_daily_loss_limit(trading_client) -> bool:
    """
    Returns True if safe to trade (haven't hit daily loss limit).
    Returns False if daily loss >= DAILY_LOSS_LIMIT_USD.
    """
    global _daily_start_balance, _daily_start_date

    today = str(date.today())

    try:
        acct = trading_client.get_account()
        equity = float(acct.equity)

        # Reset baseline at start of each new day
        if _daily_start_date != today:
            _daily_start_balance = equity
            _daily_start_date = today
            log_line(f"📅 New trading day — starting equity: ${equity:.2f}")

        daily_pnl = equity - _daily_start_balance
        if daily_pnl <= -DAILY_LOSS_LIMIT_USD:
            log_line(
                f"🚨 DAILY LOSS LIMIT HIT: down ${abs(daily_pnl):.2f} today "
                f"(limit: ${DAILY_LOSS_LIMIT_USD}) — no more trades today"
            )
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
        con.close()
    except Exception as e:
        log_line(f"❌ STATUS WRITE FAIL: {e}")


# -------------------- CLIENT --------------------
def get_client():
    return TradingClient(API_KEY, SECRET_KEY, paper=False)


# -------------------- DB INIT --------------------
def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                timestamp TEXT, symbol TEXT, signal_type TEXT, price REAL,
                rvol REAL, flow_m REAL, confidence INTEGER, sector TEXT, change_pct REAL
            )
        ''')
        conn.commit()
        conn.close()
    except Exception:
        pass


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
    try:
        approved_path = "/home/theplummer92/approved_symbols.json"
        with open(approved_path, "r") as f:
            data = json.load(f)
        # If JSON has buy/sell split, use it. Otherwise apply backtest-based defaults.
        if "buy" in data and "sell" in data:
            return {
                "buy":  [s.upper() for s in data["buy"]],
                "sell": [s.upper() for s in data["sell"]],
            }
    except Exception:
        pass

    # Backtest-optimised defaults
    return {
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


# -------------------- POSITION MANAGEMENT --------------------
def manage_positions(trading_client):
    """
    Check all open positions every cycle.
    Handles both LONG and SHORT positions correctly.
    - Long:  profit when price goes UP   → close when pnl >= +4% or <= -2%
    - Short: profit when price goes DOWN → Alpaca's unrealized_plpc handles
              the sign correctly for shorts automatically
    """
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            symbol = p.symbol
            entry  = float(p.avg_entry_price)
            if entry == 0:
                continue

            # unrealized_plpc is already signed correctly for both long and short
            pnl_pct = float(p.unrealized_plpc)
            side    = "SHORT" if float(p.qty) < 0 else "LONG"

            if pnl_pct >= TAKE_PROFIT_PCT:
                log_line(f"💰 TAKE PROFIT: {symbol} {side} +{pnl_pct:.2%} → closing")
                try:
                    trading_client.close_position(symbol)
                    log_line(f"✅ CLOSED {symbol} {side} FOR PROFIT")
                    log_trade_close(symbol, float(p.unrealized_plpc) * float(p.avg_entry_price) + float(p.avg_entry_price), pnl_pct, "take_profit")
                except Exception as e:
                    log_line(f"❌ CLOSE FAIL {symbol}: {e}")

            elif pnl_pct <= -HARD_STOP_LOSS_PCT:
                log_line(f"🛑 HARD STOP: {symbol} {side} {pnl_pct:.2%} → closing")
                try:
                    trading_client.close_position(symbol)
                    log_line(f"✅ STOP LOSS EXECUTED {symbol} {side}")
                    log_trade_close(symbol, float(p.unrealized_plpc) * float(p.avg_entry_price) + float(p.avg_entry_price), pnl_pct, "stop_loss")
                except Exception as e:
                    log_line(f"❌ STOP LOSS FAIL {symbol}: {e}")

            elif pnl_pct < -0.01:
                log_line(f"📉 MONITOR: {symbol} {side} {pnl_pct:.2%}")

    except Exception as e:
        log_line(f"⚠️ Position management error: {e}")


# -------------------- SIGNALS --------------------
def get_new_signals(last_check_ts: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        query = """
            SELECT timestamp, symbol, signal_type, price
            FROM signals
            WHERE timestamp > ?
            AND (signal_type LIKE '%STRONG%' OR signal_type LIKE '%ABSORPTION%')
            ORDER BY timestamp ASC
        """
        cursor.execute(query, (last_check_ts,))
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception:
        return []


# -------------------- TRADE EXECUTION --------------------
def execute_entry(client, symbol: str, signal: str, price: float):
    """
    Execute a trade with full shorting support.

    BUY signals  → go LONG  (buy approved symbols)
    SELL signals → go SHORT (short sell approved symbols, margin account)

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

    # Determine signal direction
    is_buy_signal  = "BUY"  in signal.upper()
    is_sell_signal = "SELL" in signal.upper()
    if not is_buy_signal and not is_sell_signal:
        return

    # 3. Check approved lists
    approved = get_approved_symbols()
    if is_buy_signal and alpaca_symbol not in approved["buy"]:
        log_line(f"⛔ SKIP LONG  {alpaca_symbol}: Not in buy-approved list")
        return
    if is_sell_signal and alpaca_symbol not in approved["sell"]:
        log_line(f"⛔ SKIP SHORT {alpaca_symbol}: Not in sell-approved list")
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
        log_line(f"🛡️ SKIP {alpaca_symbol}: Already have an open position")
        return

    # 5. Max positions check
    if len(positions) >= MAX_OPEN_POSITIONS:
        log_line(f"🚦 SKIP {alpaca_symbol}: Max {MAX_OPEN_POSITIONS} positions reached")
        return

    # 6. Market hours check (stocks only — crypto trades 24/7)
    if not is_crypto:
        try:
            if not client.get_clock().is_open:
                log_line(f"💤 SKIP {alpaca_symbol}: Market closed")
                return
        except Exception:
            pass

    # 7. Execute
    side      = OrderSide.BUY  if is_buy_signal  else OrderSide.SELL
    direction = "LONG"         if is_buy_signal  else "SHORT"
    tif       = TimeInForce.DAY

    try:
        log_line(f"🚀 SNIPING {alpaca_symbol} {direction} @ ~${float(price):.2f} | {signal}")
        client.submit_order(MarketOrderRequest(
            symbol=alpaca_symbol,
            notional=TRADE_NOTIONAL_USD,
            side=side,
            time_in_force=tif,
        ))
        log_line(f"✅ ORDER SENT: {alpaca_symbol} ${TRADE_NOTIONAL_USD} {direction}")
        log_trade_open(alpaca_symbol, direction, price, signal)
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
    t_client = get_client()

    last_check = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    last_hb = 0

    log_line(f"🦅 {BOT_VERSION} ACTIVE")
    log_line(f"💰 Trade size: ${TRADE_NOTIONAL_USD} | Stop: {HARD_STOP_LOSS_PCT*100:.0f}% | TP: {TAKE_PROFIT_PCT*100:.0f}%")
    log_line(f"🚨 Daily loss limit: ${DAILY_LOSS_LIMIT_USD} | Max positions: {MAX_OPEN_POSITIONS}")
    approved = get_approved_symbols()
    log_line(f"📈 LONG approved:  {', '.join(approved['buy'])}")
    log_line(f"📉 SHORT approved: {', '.join(approved['sell'])}")
    write_sniper_status(status="OK", heartbeat_seq=0, in_position=0, note="boot")

    try:
        while True:
            now = time.time()

            # Heartbeat every 60s
            if now - last_hb >= 60:
                regime_info = get_regime()
                log_line(
                    f"🫀 heartbeat | regime={regime_info['regime']} "
                    f"VIX={regime_info['vix']:.1f}"
                )
                write_sniper_status(
                    status="OK", heartbeat_seq=0, in_position=0,
                    note=f"heartbeat|{regime_info['regime']}"
                )
                last_hb = now

            # Position management runs every cycle regardless of regime
            manage_positions(t_client)

            # Regime gate — sell-only when VIX 25-30, fully blocked above 30
            regime_mode = get_regime_mode()
            if regime_mode == "BLOCKED":
                time.sleep(POLL_SECONDS)
                continue
            # Daily loss limit gate
            if not check_daily_loss_limit(t_client):
                time.sleep(POLL_SECONDS)
                continue
            # Check for new signals
            signals = get_new_signals(last_check)
            if signals:
                seen = set()
                for s in signals:
                    sym, stype, price = s[1], s[2], s[3]
                    side = "SELL" if any(x in stype.upper() for x in ("SELL","SHORT","ABSORPTION SELL")) else "BUY"
                    dedup_key = (s[0][:13], sym, side)  # one trade per hour per symbol per direction
                    if dedup_key in seen:
                        log_line(f"SKIP DUPE {sym} {side} (already acted on this hour)")
                        continue
                    seen.add(dedup_key)
                    if regime_mode == "SELL_ONLY" and side.upper() not in ("SELL", "SHORT"):
                        log_line(f"📉 SELL-ONLY mode — skipping {side} signal on {sym}")
                        continue
                    execute_entry(t_client, sym, stype, float(price))
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

