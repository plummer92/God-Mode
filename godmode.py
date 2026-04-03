#!/usr/bin/env python3
"""
WOLFE GODMODE (V7.0)
Purpose:
- Scan a watchlist every N seconds using yfinance (5m bars)
- Compute RVOL + simple "money flow" proxy
- Emit signals to:
    1) market_log.csv (all rows)
    2) wolfe_signals.db (non-neutral signals)
- Track ABSORPTION -> RESOLUTION outcomes to create labeled patterns for ML
- Write:
    - absorption_watchlist.csv (for dashboard)
    - absorption_resolutions.csv (for ML labels)
- NEW:
    - regime_snapshot.json (macro regime snapshot)
    - macro_features table (timestamped macro features for ML)

Notes:
- This is a data/labeling scanner. It is NOT the trader.
- Futures tickers like ZW=F may be "unknown" to Alpaca (sniper),
  but yfinance can still fetch them here for analysis.
"""

import os
import sys
import time
import json
import sqlite3
import csv
import signal
from datetime import datetime
from typing import Dict, List, Optional

import yfinance as yf
from dotenv import load_dotenv
from colorama import Fore, Back, Style, init

# ---------------- INIT ----------------
init(autoreset=True)
load_dotenv()

BASE_DIR = "/home/theplummer92"

# ---------------- PATHS / OUTPUTS ----------------
CSV_FILENAME = os.getenv("MARKET_LOG_PATH", f"{BASE_DIR}/market_log.csv")
REGIME_SNAPSHOT_PATH = os.getenv("REGIME_SNAPSHOT_PATH", f"{BASE_DIR}/regime_snapshot.json")
DB_PATH = os.getenv("DB_PATH", f"{BASE_DIR}/wolfe_signals.db")

ABS_WATCHLIST_PATH = os.getenv("ABS_WATCHLIST_PATH", f"{BASE_DIR}/absorption_watchlist.csv")
ABS_RESOLUTIONS_PATH = os.getenv("ABS_RESOLUTIONS_PATH", f"{BASE_DIR}/absorption_resolutions.csv")

# ---------------- LOOP SETTINGS ----------------
SLEEP_SECONDS = int(os.getenv("GODMODE_SLEEP_SECONDS", "300"))   # 5 minutes
MIN_BARS = int(os.getenv("MIN_BARS", "50"))                     # RVOL baseline window from 5m bars

# ---------------- ALERTS ----------------
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()

# ---------------- ABSORPTION -> RESOLUTION PARAMS ----------------
# "absorption" = high RVOL + tiny move + big $flow
ABS_RVOL_MIN = float(os.getenv("ABS_RVOL_MIN", "4.0"))
ABS_MOVE_MAX = float(os.getenv("ABS_MOVE_MAX", "0.001"))          # <= 0.10%
ABS_FLOW_MIN_M = float(os.getenv("ABS_FLOW_MIN_M", "50.0"))       # absolute $M required to track

# resolution criteria
RESOLVE_UP_PCT = float(os.getenv("RESOLVE_UP_PCT", "0.002"))      # +0.20%
RESOLVE_DN_PCT = float(os.getenv("RESOLVE_DN_PCT", "0.002"))      # -0.20%
RESOLVE_MIN_RVOL = float(os.getenv("RESOLVE_MIN_RVOL", "1.5"))
RESOLVE_WINDOW_SCANS = int(os.getenv("RESOLVE_WINDOW_SCANS", "18"))  # 18 scans * 5m = 90m

# ---------------- ASSET LIST ----------------
# ---------------- ASSET LIST ----------------
ASSETS: Dict[str, List[str]] = {
    "CRYPTO": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"],
    "TECH": ["NVDA", "TSLA", "AAPL", "MSFT", "AMD", "AMZN", "META", "COIN", "NFLX", "GME"],
    "INDICES": ["SPY", "QQQ", "IWM"],
    "COMMODITIES": ["CL=F", "NG=F", "GC=F", "SI=F", "HG=F", "ZC=F", "ZW=F"],
    "MACRO": ["^TNX", "DX-Y.NYB", "^VIX"],  # used both as "assets" and for regime features
}
HUNTER_SYMBOLS_PATH = os.path.join(BASE_DIR, "symbol_hunt_top20.json")
_last_hunter_watch_symbols: List[str] = []
_last_hunter_watch_mtime: float | None = None
# ---------------- CSV HEADERS ----------------
MARKET_HEADER = ["Timestamp", "Sector", "Ticker", "Price", "Change_Pct", "RVOL", "Money_Flow_M", "Signal"]

WATCH_HEADER = [
    "Start_Timestamp", "Ticker", "Sector", "Level_Price",
    "Start_Flow_M", "Start_RVOL", "Bias", "Scans_Since"
]

RES_HEADER = [
    "Start_Timestamp", "Resolve_Timestamp", "Ticker", "Sector",
    "Level_Price", "Resolve_Price", "Outcome",
    "Start_Flow_M", "Resolve_Flow_M", "Start_RVOL", "Resolve_RVOL",
    "Resolve_Change_Pct", "Scans_To_Resolve"
]

# ---------------- LOGGING ----------------
def log(msg: str) -> None:
    # Ensure journalctl sees output promptly
    print(msg, flush=True)

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---------------- UTIL: CSV HELPERS ----------------
def _ensure_csv(path: str, header: List[str]) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)

def _append_row(path: str, header: List[str], row: Dict) -> None:
    _ensure_csv(path, header)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([row.get(h, "") for h in header])

def _write_rows(path: str, header: List[str], rows: List[Dict]) -> None:
    _ensure_csv(path, header)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(h, "") for h in header])

def ensure_market_log_header() -> None:
    _ensure_csv(CSV_FILENAME, MARKET_HEADER)


def _load_dynamic_assets() -> Dict[str, List[str]]:
    global _last_hunter_watch_symbols, _last_hunter_watch_mtime

    assets = {sector: list(symbols) for sector, symbols in ASSETS.items() if sector != "HUNTER"}
    hunter_symbols: List[str] = []
    current_mtime: float | None = None

    try:
        current_mtime = os.path.getmtime(HUNTER_SYMBOLS_PATH)
        with open(HUNTER_SYMBOLS_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        top_sell = payload.get("top_sell", []) if isinstance(payload, dict) else []
        existing = {ticker for group in assets.values() for ticker in group}
        hunter_symbols = [
            str(symbol).strip().upper()
            for symbol in top_sell
            if str(symbol).strip() and str(symbol).strip().upper() not in existing
        ]
    except FileNotFoundError:
        hunter_symbols = []
    except Exception as e:
        log(f"{Fore.YELLOW}⚠️ Hunter symbol refresh failed: {e}")
        hunter_symbols = list(_last_hunter_watch_symbols)

    if hunter_symbols:
        assets["HUNTER"] = hunter_symbols

    if hunter_symbols != _last_hunter_watch_symbols:
        added = sorted(set(hunter_symbols) - set(_last_hunter_watch_symbols))
        removed = sorted(set(_last_hunter_watch_symbols) - set(hunter_symbols))
        log(
            f"{Fore.CYAN}🔄 Hunter watchlist refresh: "
            f"count {len(_last_hunter_watch_symbols)} -> {len(hunter_symbols)}"
            + (f" | added={','.join(added)}" if added else "")
            + (f" | removed={','.join(removed)}" if removed else "")
        )
        _last_hunter_watch_symbols = list(hunter_symbols)
    _last_hunter_watch_mtime = current_mtime
    return assets

# ---------------- DATABASE ----------------
def init_db() -> None:
    """
    Creates/opens SQLite DB and ensures required tables exist:
      - signals
      - macro_features
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        timestamp TEXT,
        symbol TEXT,
        signal_type TEXT,
        price REAL,
        rvol REAL,
        flow_m REAL,
        confidence INTEGER,
        sector TEXT,
        change_pct REAL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS macro_features (
        timestamp TEXT,
        vix REAL,
        tnx REAL,
        dxy REAL,
        risk_regime TEXT
    )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp ON signals(symbol, timestamp)")

    conn.commit()
    conn.close()
    log(f"{Fore.GREEN}✅ DB ready: {DB_PATH}")

def save_signal_to_db(symbol: str, sector: str, signal_type: str,
                      price: float, change_pct: float, rvol: float, flow_m: float) -> None:
    confidence = 50
    if "ABSORPTION" in signal_type:
        confidence += 30
    if "CLIMAX" in signal_type:
        confidence += 40
    if rvol > 10:
        confidence += 10
    confidence = min(confidence, 100)

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO signals (
                timestamp, symbol, signal_type, price, rvol, flow_m,
                confidence, sector, change_pct
            ) VALUES (
                datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, (symbol, signal_type, price, rvol, flow_m, confidence, sector, float(change_pct)))
        conn.commit()
    except Exception as e:
        log(f"{Fore.RED}DB Write Error (signals): {e}")
    finally:
        if conn:
            conn.close()

def save_macro_features(vix: Optional[float], tnx: Optional[float], dxy: Optional[float], regime: str) -> None:
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO macro_features (timestamp, vix, tnx, dxy, risk_regime)
            VALUES (datetime('now'), ?, ?, ?, ?)
        """, (vix, tnx, dxy, regime))
        conn.commit()
    except Exception as e:
        log(f"{Fore.RED}DB Write Error (macro_features): {e}")
    finally:
        if conn:
            conn.close()
# ---------------- SIGNAL LOGIC ----------------
def analyze_signal(rvol: float, change_pct: float, flow_m: float) -> str:
    """
    Returns signal label. Flow and price direction must agree for a valid signal.
    If flow is positive but price is falling, buyers are getting run over (BULL TRAP).
    If flow is negative but price is rising, sellers are getting squeezed (BEAR TRAP).
    """
    if rvol > 4.0 and abs(change_pct) < 0.001:
        return "🛡️ ABSORPTION SELL" if flow_m > 0 else "🛡️ ABSORPTION BUY"
    if rvol < 1.0 and abs(change_pct) > 0.005:
        return "⚠️ FAKE-OUT (Low Vol)"
    if rvol > 8.0:
        return "🔥 CLIMAX"
    if rvol > 2.5:
        if abs(flow_m) < 5.0:
            return "Neutral"
        if flow_m > 0:
            # Buy flow: only valid if price is actually going up
            return "⭐⭐⭐ STRONG BUY FLOW" if change_pct >= 0 else "⚠️ BULL TRAP"
        else:
            # Sell flow: only valid if price is actually going down
            return "⭐⭐⭐ STRONG SELL FLOW" if change_pct <= 0 else "⚠️ BEAR TRAP"
    return "Neutral"
def get_flow_math(symbol: str, price: float, vol: float, open_p: float, close_p: float) -> float:
    """
    Not true orderflow — it's a proxy.
    For equities/futures: (price * volume) signed by candle direction.
    For crypto: yfinance volume is usually already in coin units, so we keep it as-is
    (still signed) and scale later to "M".
    """
    direction = 1.0 if close_p >= open_p else -1.0
    if "-USD" in symbol:
        return vol * direction
    return (price * vol) * direction

def is_absorption_candidate(rvol: float, change_pct: float, flow_m: float) -> bool:
    return (rvol >= ABS_RVOL_MIN) and (abs(change_pct) <= ABS_MOVE_MAX) and (abs(flow_m) >= ABS_FLOW_MIN_M)

# ---------------- DISCORD (OPTIONAL) ----------------
def post_discord(symbol: str, signal_lbl: str, rvol: float, flow_m: float) -> None:
    if not DISCORD_WEBHOOK:
        return
    if "Neutral" in signal_lbl:
        return

    emoji = "🟢" if flow_m > 0 else "🔴"
    msg = f"🚨 **WHALE ALERT** `{symbol}`\n**{signal_lbl}**\nRVOL: `{rvol:.2f}x` | Flow: {emoji} `{flow_m:+.1f}M`"
    try:
        import requests
        requests.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=6)
    except Exception:
        pass

# ---------------- DATA FETCHING ----------------
def _download_batch(tickers: List[str]):
    return yf.download(
        tickers,
        period="5d",
        interval="5m",
        progress=False,
        group_by="column",
        threads=True,
    )

def _download_one(symbol: str):
    return yf.download(
        symbol,
        period="5d",
        interval="5m",
        progress=False,
        group_by="column",
        threads=False,
    )

def get_symbol_frame(data, symbol: str):
    """
    Handles yfinance returning either:
      - MultiIndex columns for batches: data["Close"][symbol]
      - Single symbol frame: data["Close"]
    """
    try:
        opens = data["Open"][symbol].dropna()
        closes = data["Close"][symbol].dropna()
        vols = data["Volume"][symbol].dropna()
        return opens, closes, vols
    except Exception:
        try:
            opens = data["Open"].dropna()
            closes = data["Close"].dropna()
            vols = data["Volume"].dropna()
            return opens, closes, vols
        except Exception:
            return None, None, None

# ---------------- REGIME SNAPSHOT ----------------
def derive_regime(vix: Optional[float], tnx: Optional[float], dxy: Optional[float]) -> str:
    """
    Mirrors sniper_bot's VIX-based regime logic exactly.
    OPEN / SELL_ONLY / BLOCKED — no other states.
    """
    v = vix if vix is not None else 0.0
    if v >= 30:
        return "BLOCKED"
    if v >= 25:
        return "SELL_ONLY"
    return "OPEN"

def write_regime_snapshot(vix: Optional[float], tnx: Optional[float], dxy: Optional[float], regime: str) -> None:
    payload = {
        "timestamp": now_str(),
        "vix": vix,
        "tnx": tnx,
        "dxy": dxy,
        "regime": regime,
    }
    try:
        with open(REGIME_SNAPSHOT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        log(f"{Fore.RED}Regime snapshot write error: {e}")
# ---------------- GRACEFUL SHUTDOWN ----------------
RUNNING  = True
LOCKFILE = "/tmp/godmode.lock"

def _handle_signal(sig, frame) -> None:
    global RUNNING
    RUNNING = False
    log(f"\n{Fore.YELLOW}🛑 Received signal {sig}. Shutting down cleanly...")
    try:
        os.remove(LOCKFILE)
    except Exception:
        pass

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ---------------- MAIN LOOP ----------------
def run_god_mode_pro() -> None:
    log(f"{Back.WHITE}{Fore.BLACK} 📱 GODMODE V7.0: Scanner starting... {Style.RESET_ALL}")
    init_db()
    ensure_market_log_header()
    _ensure_csv(ABS_WATCHLIST_PATH, WATCH_HEADER)
    _ensure_csv(ABS_RESOLUTIONS_PATH, RES_HEADER)

    # Prevent spamming alerts
    last_alert_ts: Dict[str, float] = {}

    # absorption state: symbol -> state dict
    absorption: Dict[str, Dict] = {}
    _last_prune_date = ""

    while RUNNING:
        try:
            ts = now_str()
            assets = _load_dynamic_assets()
            all_tickers = [ticker for group in assets.values() for ticker in group]

            # Daily prune: delete signals older than 30 days (runs once per calendar day)
            today_date = datetime.now().strftime("%Y-%m-%d")
            if today_date != _last_prune_date:
                try:
                    conn = sqlite3.connect(DB_PATH)
                    deleted = conn.execute(
                        "DELETE FROM signals WHERE timestamp < datetime('now', '-30 days')"
                    ).rowcount
                    conn.commit()
                    conn.close()
                    if deleted:
                        log(f"{Fore.CYAN}🗑️  Pruned {deleted} signals older than 30 days")
                except Exception as pe:
                    log(f"{Fore.RED}Prune error: {pe}")
                _last_prune_date = today_date

            # download batch w/ small retry
            batch = None
            for attempt in range(2):
                try:
                    batch = _download_batch(all_tickers)
                    break
                except Exception:
                    batch = None
                    time.sleep(1.5)

            # ----- Macro snapshot (VIX/TNX/DXY) -----
            vix_val = None
            tnx_val = None
            dxy_val = None
            try:
                # pull from batch if available
                if batch is not None:
                    for sym in ["^VIX", "^TNX", "DX-Y.NYB"]:
                        o, c, v = get_symbol_frame(batch, sym)
                        if c is not None and len(c) > 0:
                            val = float(c.iloc[-1])
                            if sym == "^VIX":
                                vix_val = val
                            elif sym == "^TNX":
                                tnx_val = val
                            elif sym == "DX-Y.NYB":
                                dxy_val = val
            except Exception:
                pass

            regime = derive_regime(vix_val, tnx_val, dxy_val)
            write_regime_snapshot(vix_val, tnx_val, dxy_val, regime)
            save_macro_features(vix_val, tnx_val, dxy_val, regime)

            log(f"[{ts}] Scan | regime={regime} | VIX={vix_val} TNX={tnx_val} DXY={dxy_val}")

            # SPY 20-bar MA trend filter — suppresses long signals in downtrends
            spy_trend_bullish: Optional[bool] = None
            try:
                _, spy_closes, _ = get_symbol_frame(batch, "SPY") if batch is not None else (None, None, None)
                if spy_closes is not None and len(spy_closes) >= 20:
                    spy_ma20 = float(spy_closes.iloc[-20:].mean())
                    spy_price = float(spy_closes.iloc[-1])
                    spy_trend_bullish = spy_price > spy_ma20
                    trend_str = "BULL" if spy_trend_bullish else "BEAR"
                    log(f"SPY trend: {trend_str} (${spy_price:.2f} vs MA20 ${spy_ma20:.2f})")
            except Exception:
                spy_trend_bullish = None

            watch_rows: List[Dict] = []

            # ----- per symbol scanning -----
            for sector, tickers in assets.items():
                for symbol in tickers:
                    try:
                        data = batch
                        opens, closes, volumes = get_symbol_frame(data, symbol) if data is not None else (None, None, None)

                        # fallback
                        if opens is None or closes is None or volumes is None or len(closes) < MIN_BARS:
                            one = _download_one(symbol)
                            opens, closes, volumes = get_symbol_frame(one, symbol)

                        if opens is None or closes is None or volumes is None or len(closes) < MIN_BARS:
                            continue

                        price = float(closes.iloc[-1])
                        open_p = float(opens.iloc[-1])
                        vol = float(volumes.iloc[-1])

                        avg_vol = float(volumes.iloc[-MIN_BARS:-1].mean())
                        rvol = (vol / avg_vol) if avg_vol > 0 else 1.0

                        change_pct = (price - open_p) / open_p if open_p else 0.0
                        flow_m = float(get_flow_math(symbol, price, vol, open_p, price) / 1_000_000)

                        signal_lbl = analyze_signal(rvol, change_pct, flow_m)

                        # market log (everything)
                        _append_row(CSV_FILENAME, MARKET_HEADER, {
                            "Timestamp": ts,
                            "Sector": sector,
                            "Ticker": symbol,
                            "Price": round(price, 6),
                            "Change_Pct": round(change_pct, 6),
                            "RVOL": round(rvol, 2),
                            "Money_Flow_M": round(flow_m, 2),
                            "Signal": signal_lbl
                        })

                        # DB + alert only non-neutral
                        if "Neutral" not in signal_lbl:
                            is_buy_signal = "BUY" in signal_lbl and "SELL" not in signal_lbl
                            trend_blocked = is_buy_signal and spy_trend_bullish is False

                            if trend_blocked:
                                log(f"TREND GATE: {signal_lbl} on {symbol} suppressed — SPY below MA20")
                            else:
                                save_signal_to_db(
                                    symbol=symbol,
                                    sector=sector,
                                    signal_type=signal_lbl,
                                    price=price,
                                    change_pct=change_pct,
                                    rvol=rvol,
                                    flow_m=flow_m
                                )

                                now_epoch = time.time()
                                if (now_epoch - last_alert_ts.get(symbol, 0) > 1200) or ("CLIMAX" in signal_lbl):
                                    log(f"🚀 {signal_lbl}: {symbol} | Flow: ${flow_m:+.1f}M | RVOL={rvol:.2f} | Δ={change_pct*100:.2f}%")
                                    post_discord(symbol, signal_lbl, rvol, flow_m)
                                    last_alert_ts[symbol] = now_epoch

                        # ----- Absorption set -----
                        if is_absorption_candidate(rvol, change_pct, flow_m):
                            if symbol not in absorption:
                                bias = "DEFEND_BID" if flow_m < 0 else "DEFEND_OFFER"
                                absorption[symbol] = {
                                    "level_price": price,
                                    "start_ts": ts,
                                    "sector": sector,
                                    "start_flow_m": flow_m,
                                    "start_rvol": rvol,
                                    "bias": bias,
                                    "scans_seen": 0
                                }

                        # ----- Absorption resolve -----
                        if symbol in absorption:
                            st = absorption[symbol]
                            st["scans_seen"] += 1

                            level = float(st["level_price"])
                            up = (price - level) / level if level else 0.0
                            dn = (level - price) / level if level else 0.0

                            outcome = None

                            # Breakout: up move + rvol ok + flow improved toward neutral/positive
                            if (up >= RESOLVE_UP_PCT) and (rvol >= RESOLVE_MIN_RVOL) and (flow_m > (st["start_flow_m"] * 0.25)):
                                outcome = "BREAKOUT"

                            # Breakdown: down move + rvol ok + flow negative
                            if (dn >= RESOLVE_DN_PCT) and (rvol >= RESOLVE_MIN_RVOL) and (flow_m < 0):
                                outcome = "BREAKDOWN"

                            if outcome:
                                _append_row(ABS_RESOLUTIONS_PATH, RES_HEADER, {
                                    "Start_Timestamp": st["start_ts"],
                                    "Resolve_Timestamp": ts,
                                    "Ticker": symbol,
                                    "Sector": st["sector"],
                                    "Level_Price": round(level, 6),
                                    "Resolve_Price": round(price, 6),
                                    "Outcome": outcome,
                                    "Start_Flow_M": round(st["start_flow_m"], 2),
                                    "Resolve_Flow_M": round(flow_m, 2),
                                    "Start_RVOL": round(st["start_rvol"], 2),
                                    "Resolve_RVOL": round(rvol, 2),
                                    "Resolve_Change_Pct": round(change_pct, 6),
                                    "Scans_To_Resolve": st["scans_seen"],
                                })
                                absorption.pop(symbol, None)
                            elif st["scans_seen"] >= RESOLVE_WINDOW_SCANS:
                                absorption.pop(symbol, None)

                    except Exception:
                        # keep scanner alive
                        continue

            # Write watchlist artifact for dashboard
            for sym, st in absorption.items():
                watch_rows.append({
                    "Start_Timestamp": st["start_ts"],
                    "Ticker": sym,
                    "Sector": st["sector"],
                    "Level_Price": round(float(st["level_price"]), 6),
                    "Start_Flow_M": round(float(st["start_flow_m"]), 2),
                    "Start_RVOL": round(float(st["start_rvol"]), 2),
                    "Bias": st["bias"],
                    "Scans_Since": st["scans_seen"],
                })
            _write_rows(ABS_WATCHLIST_PATH, WATCH_HEADER, watch_rows)

            # sleep
            for _ in range(int(SLEEP_SECONDS)):
                if not RUNNING:
                    break
                time.sleep(1)

        except Exception as e:
            log(f"{Fore.RED}Loop Error: {e}")
            time.sleep(10)

    log(f"{Fore.YELLOW}✅ GODMODE stopped.")

if __name__ == "__main__":
    if os.path.exists(LOCKFILE):
        try:
            with open(LOCKFILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            log(f"⛔ GODMODE already running (PID {pid}). Exiting.")
            sys.exit(0)
        except (ProcessLookupError, ValueError, OSError):
            log("⚠️ Stale lockfile found. Taking over.")
    with open(LOCKFILE, "w") as f:
        f.write(str(os.getpid()))
    try:
        run_god_mode_pro()
    finally:
        try:
            os.remove(LOCKFILE)
        except Exception:
            pass
