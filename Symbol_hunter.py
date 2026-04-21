#!/usr/bin/env python3
"""symbol_hunter.py v5 — broader discovery, full scans, and market snapshots"""
import argparse
import importlib.util
import os
import sys
from datetime import datetime, timedelta

import json
import numpy as np
import pandas as pd
import yfinance as yf

TRADING_DEV_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "trading-dev",
)
BOOTSTRAP_PATH = os.path.join(TRADING_DEV_DIR, "bootstrap_path.py")
_bootstrap_spec = importlib.util.spec_from_file_location("bootstrap_path", BOOTSTRAP_PATH)
_bootstrap_module = importlib.util.module_from_spec(_bootstrap_spec)
assert _bootstrap_spec is not None and _bootstrap_spec.loader is not None
_bootstrap_spec.loader.exec_module(_bootstrap_module)
_bootstrap_module.ensure_trading_dev_first(TRADING_DEV_DIR)

LOOKBACK_DAYS  = 60
MIN_TRADES     = 3
MIN_WIN_RATE   = 0.60
MIN_AVG_RETURN = 0.003
OUTPUT_CSV     = os.path.expanduser("~/symbol_hunt_results.csv")
OUTPUT_JSON    = os.path.expanduser("~/symbol_hunt_top20.json")
SNAPSHOT_CSV   = "/home/theplummer92/market_snapshot.csv"
SNAPSHOT_JSON  = "/home/theplummer92/market_snapshot.json"
APPROVED_PATH  = os.path.expanduser("~/approved_symbols.json")
REGIME_PATH    = os.path.expanduser("~/regime_snapshot.json")
BORROWABLE_SYMBOLS_PATH = os.path.expanduser(
    os.getenv("BORROWABLE_SYMBOLS_PATH", "~/borrowable_symbols.json")
)

COOLING_RESCAN_DAYS  = 7    # re-scan cooling_off symbols not seen in this many days
VIX_CHANGE_THRESHOLD = 5.0  # VIX move of this magnitude = market condition change
SECTOR_ROTATION_PCT  = 0.05 # 5% spread between best/worst sector ETF 5d return

RVOL_THRESHOLD      = 1.5   # standard hunt threshold
RVOL_FULL_SCAN      = 3.0   # higher bar for full-universe scans
FLOW_THRESHOLD      = 0.00003

SNAPSHOT_MIN_PRICE = float(os.getenv("SNAPSHOT_MIN_PRICE", "5.0"))
SNAPSHOT_MIN_AVG_VOLUME = float(os.getenv("SNAPSHOT_MIN_AVG_VOLUME", "500000"))
SNAPSHOT_MIN_AVG_DOLLAR_VOLUME = float(os.getenv("SNAPSHOT_MIN_AVG_DOLLAR_VOLUME", "10000000"))
SNAPSHOT_TOP_N = int(os.getenv("SNAPSHOT_TOP_N", "25"))

# Fallback static universe (used if Wikipedia fetch fails)
_STATIC_UNIVERSE = [
    "AAPL","MSFT","NVDA","AMD","INTC","QCOM","AVGO","TXN","MU","AMAT",
    "LRCX","KLAC","MRVL","SNPS","CDNS","FTNT","PANW","CRWD","ZS","OKTA",
    "NET","DDOG","SNOW","MDB","TEAM","HUBS","PAYC","NOW","CRM",
    "ADBE","ORCL","IBM","HPQ","DELL","CSCO","ANET","NTAP",
    "JPM","BAC","GS","MS","WFC","C","USB","PNC","TFC","COF",
    "AXP","V","MA","PYPL","COIN","HOOD","SCHW","IBKR","ICE",
    "AMZN","TSLA","NKE","SBUX","MCD","YUM","CMG","DPZ","DKNG","PENN",
    "MGM","LVS","WYNN","RCL","CCL","NCLH","UAL","DAL","AAL","LUV",
    "UNH","JNJ","PFE","MRK","ABBV","BMY","GILD","AMGN","BIIB","REGN",
    "MRNA","VRTX","ILMN","IQV","CRL","TMO",
    "WMT","TGT","COST","HD","LOW","BBY","GME","AMC",
    "XOM","CVX","COP","EOG","DVN","MPC","VLO","PSX","OXY",
    "NFLX","META","GOOGL","SNAP","PINS","SPOT","WBD","DIS",
    "SPY","QQQ","IWM","XLF","XLK","XLE","XLV","XLI","ARKK","SOXS",
    "MSTR","RIOT","MARA","HUT","CLSK","WULF","IREN",
]

SECTOR_MAP = {
    # Technology / software / semis
    "AAPL": "TECH", "MSFT": "TECH", "NVDA": "TECH", "AMD": "TECH", "INTC": "TECH",
    "QCOM": "TECH", "AVGO": "TECH", "TXN": "TECH", "MU": "TECH", "AMAT": "TECH",
    "LRCX": "TECH", "KLAC": "TECH", "MRVL": "TECH", "SNPS": "TECH", "CDNS": "TECH",
    "FTNT": "TECH", "PANW": "TECH", "CRWD": "TECH", "ZS": "TECH", "OKTA": "TECH",
    "NET": "TECH", "DDOG": "TECH", "SNOW": "TECH", "MDB": "TECH", "TEAM": "TECH",
    "HUBS": "TECH", "PAYC": "TECH", "NOW": "TECH", "CRM": "TECH", "ADBE": "TECH",
    "ORCL": "TECH", "IBM": "TECH", "HPQ": "TECH", "DELL": "TECH", "CSCO": "TECH",
    "ANET": "TECH", "NTAP": "TECH", "ASML": "TECH", "TSM": "TECH", "ARM": "TECH",
    "SMCI": "TECH", "PLTR": "TECH", "SHOP": "TECH", "UBER": "TECH", "ABNB": "TECH",
    "INTU": "TECH", "RBLX": "TECH", "U": "TECH", "LMND": "TECH", "OPEN": "TECH",
    "UPST": "TECH", "SOFI": "TECH",

    # Financials
    "JPM": "FINANCIALS", "BAC": "FINANCIALS", "GS": "FINANCIALS", "MS": "FINANCIALS",
    "WFC": "FINANCIALS", "C": "FINANCIALS", "USB": "FINANCIALS", "PNC": "FINANCIALS",
    "TFC": "FINANCIALS", "COF": "FINANCIALS", "AXP": "FINANCIALS", "V": "FINANCIALS",
    "MA": "FINANCIALS", "PYPL": "FINANCIALS", "SCHW": "FINANCIALS", "IBKR": "FINANCIALS",
    "ICE": "FINANCIALS", "HOOD": "FINANCIALS", "SSNC": "FINANCIALS", "FNF": "FINANCIALS",
    "FAF": "FINANCIALS", "VCTR": "FINANCIALS", "WTFC": "FINANCIALS", "CATY": "FINANCIALS",
    "PACW": "FINANCIALS", "WAL": "FINANCIALS",

    # Consumer / retail / travel / media
    "AMZN": "CONSUMER", "TSLA": "CONSUMER", "NKE": "CONSUMER", "SBUX": "CONSUMER",
    "MCD": "CONSUMER", "YUM": "CONSUMER", "CMG": "CONSUMER", "DPZ": "CONSUMER",
    "DKNG": "CONSUMER", "PENN": "CONSUMER", "MGM": "CONSUMER", "LVS": "CONSUMER",
    "WYNN": "CONSUMER", "RCL": "CONSUMER", "CCL": "CONSUMER", "NCLH": "CONSUMER",
    "UAL": "CONSUMER", "DAL": "CONSUMER", "AAL": "CONSUMER", "LUV": "CONSUMER",
    "WMT": "CONSUMER", "TGT": "CONSUMER", "COST": "CONSUMER", "HD": "CONSUMER",
    "LOW": "CONSUMER", "BBY": "CONSUMER", "NFLX": "CONSUMER", "META": "COMMUNICATION",
    "GOOGL": "COMMUNICATION", "SNAP": "COMMUNICATION", "PINS": "COMMUNICATION",
    "SPOT": "COMMUNICATION", "WBD": "COMMUNICATION", "DIS": "COMMUNICATION",
    "CMCSA": "COMMUNICATION", "TMUS": "COMMUNICATION", "VZ": "COMMUNICATION",
    "T": "COMMUNICATION", "XPEV": "CONSUMER", "NIO": "CONSUMER", "LI": "CONSUMER",
    "RIVN": "CONSUMER", "LCID": "CONSUMER", "WKHS": "CONSUMER", "RIDE": "CONSUMER",
    "GOEV": "CONSUMER", "FSR": "CONSUMER", "NKLA": "CONSUMER", "EXPR": "CONSUMER",
    "WISH": "CONSUMER", "SKLZ": "CONSUMER", "BARK": "CONSUMER",

    # Healthcare / biotech
    "UNH": "HEALTHCARE", "JNJ": "HEALTHCARE", "PFE": "HEALTHCARE", "MRK": "HEALTHCARE",
    "ABBV": "HEALTHCARE", "BMY": "HEALTHCARE", "GILD": "HEALTHCARE", "AMGN": "HEALTHCARE",
    "BIIB": "HEALTHCARE", "REGN": "HEALTHCARE", "MRNA": "HEALTHCARE", "VRTX": "HEALTHCARE",
    "ILMN": "HEALTHCARE", "IQV": "HEALTHCARE", "CRL": "HEALTHCARE", "TMO": "HEALTHCARE",
    "INSP": "HEALTHCARE", "TMDX": "HEALTHCARE", "NVCR": "HEALTHCARE", "BEAM": "HEALTHCARE",
    "EDIT": "HEALTHCARE", "NTLA": "HEALTHCARE", "CRSP": "HEALTHCARE", "FATE": "HEALTHCARE",
    "BLUE": "HEALTHCARE", "SAGE": "HEALTHCARE", "SAVA": "HEALTHCARE", "ACAD": "HEALTHCARE",
    "EXEL": "HEALTHCARE", "FOLD": "HEALTHCARE", "MRTX": "HEALTHCARE", "KRTX": "HEALTHCARE",
    "ALNY": "HEALTHCARE", "PTGX": "HEALTHCARE", "ARWR": "HEALTHCARE", "MDGL": "HEALTHCARE",

    # Energy / materials / industrials
    "XOM": "ENERGY", "CVX": "ENERGY", "COP": "ENERGY", "EOG": "ENERGY", "DVN": "ENERGY",
    "MPC": "ENERGY", "VLO": "ENERGY", "PSX": "ENERGY", "OXY": "ENERGY", "SLB": "ENERGY",
    "SM": "ENERGY", "PDCE": "ENERGY", "CTRA": "ENERGY", "MTDR": "ENERGY", "VTLE": "ENERGY",
    "PR": "ENERGY", "ESTE": "ENERGY", "REX": "ENERGY", "CPE": "ENERGY", "SBOW": "ENERGY",
    "BA": "INDUSTRIALS", "CAT": "INDUSTRIALS", "DE": "INDUSTRIALS", "GE": "INDUSTRIALS",
    "MMM": "INDUSTRIALS", "HON": "INDUSTRIALS", "SPCE": "INDUSTRIALS", "RKT": "INDUSTRIALS",
    "JOBY": "INDUSTRIALS", "ACHR": "INDUSTRIALS", "LILM": "INDUSTRIALS", "EVTL": "INDUSTRIALS",
    "BLADE": "INDUSTRIALS", "SURF": "INDUSTRIALS", "SKYH": "INDUSTRIALS", "EDBL": "INDUSTRIALS",

    # Speculative / crypto proxies
    "GME": "SPECULATIVE", "AMC": "SPECULATIVE", "ARKK": "SPECULATIVE", "SOXS": "SPECULATIVE",
    "MSTR": "CRYPTO_PROXY", "RIOT": "CRYPTO_PROXY", "MARA": "CRYPTO_PROXY", "HUT": "CRYPTO_PROXY",
    "CLSK": "CRYPTO_PROXY", "WULF": "CRYPTO_PROXY", "IREN": "CRYPTO_PROXY", "BTBT": "CRYPTO_PROXY",
    "CIFR": "CRYPTO_PROXY", "BBBY": "SPECULATIVE", "CLOV": "SPECULATIVE", "DATS": "SPECULATIVE",
    "STPK": "SPECULATIVE", "AJAX": "SPECULATIVE", "BB": "SPECULATIVE", "KOSS": "SPECULATIVE",
    "NAKD": "SPECULATIVE", "SNDL": "SPECULATIVE", "TLRY": "SPECULATIVE", "ACB": "SPECULATIVE",
    "CGC": "SPECULATIVE", "CRON": "SPECULATIVE", "APHA": "SPECULATIVE",

    # ETFs / macro
    "SPY": "ETF", "QQQ": "ETF", "IWM": "ETF", "XLF": "ETF", "XLK": "ETF", "XLE": "ETF",
    "XLV": "ETF", "XLI": "ETF", "DIA": "ETF", "SMH": "ETF", "XLY": "ETF", "XLP": "ETF",
    "XLB": "ETF", "XLU": "ETF", "XLRE": "ETF",
}


def fetch_full_universe():
    """
    Fetches S&P 500 tickers from Wikipedia and supplements with a Russell 1000
    extended list. Falls back to the static universe if the fetch fails.
    Returns a deduplicated list of ticker strings.
    """
    tickers = set()

    # S&P 500 via Wikipedia
    try:
        sp500_df = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            attrs={"id": "constituents"}
        )[0]
        sp500 = sp500_df["Symbol"].str.replace(".", "-", regex=False).tolist()
        tickers.update(sp500)
        print(f"  Wikipedia S&P 500: {len(sp500)} tickers fetched")
    except Exception as e:
        print(f"  Warning: S&P 500 Wikipedia fetch failed ({e}), using static list")
        tickers.update(_STATIC_UNIVERSE)

    # Russell 1000 extension (symbols beyond typical S&P 500 coverage)
    russell_extra = [
        # Mid-cap tech
        "RBLX","U","DKNG","HOOD","SOFI","AFRM","UPST","LMND","ROOT","OPEN",
        "COIN","MSTR","RIOT","MARA","CLSK","WULF","IREN","HUT","BTBT","CIFR",
        # Mid-cap finance
        "IBKR","SCHW","SSNC","FNF","FAF","VCTR","WTFC","CATY","PACW","WAL",
        # Mid-cap healthcare
        "INSP","TMDX","NVCR","BEAM","EDIT","NTLA","CRSP","FATE","BLUE","SAGE",
        # Mid-cap consumer/retail
        "XPEV","NIO","LI","RIVN","LCID","WKHS","RIDE","GOEV","FSR","NKLA",
        # Mid-cap energy
        "SM","PDCE","CTRA","MTDR","VTLE","PR","ESTE","REX","CPE","SBOW",
        # Mid-cap industrials
        "SPCE","RKT","JOBY","ACHR","LILM","EVTL","BLADE","SURF","SKYH","EDBL",
        # Biotech
        "SAVA","ACAD","EXEL","FOLD","MRTX","KRTX","ALNY","PTGX","ARWR","MDGL",
        # Other high-RVOL names
        "AMC","BBBY","EXPR","CLOV","WISH","SKLZ","BARK","DATS","STPK","AJAX",
        "GME","BB","KOSS","NAKD","SNDL","TLRY","ACB","CGC","CRON","APHA",
    ]
    tickers.update(russell_extra)

    # Clean up bad ticker formats
    clean = sorted(t.strip().upper() for t in tickers if t and "." not in t)
    return clean


SP500_UNIVERSE = list(dict.fromkeys(_STATIC_UNIVERSE))
print(
    f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Symbol Hunter v5 — "
    f"{len(SP500_UNIVERSE)} symbols (static), full universe on demand"
)


def analyze_snapshot_signal(rvol, change_pct, flow_m):
    """Discovery-side signal classifier aligned with godmode labels."""
    if rvol > 4.0 and abs(change_pct) < 0.001:
        return "ABSORPTION_SELL" if flow_m > 0 else "ABSORPTION_BUY"
    if rvol < 1.0 and abs(change_pct) > 0.005:
        return "FAKE_OUT"
    if rvol > 8.0:
        return "CLIMAX"
    if rvol > 2.5:
        if abs(flow_m) < 5.0:
            return "NEUTRAL"
        if flow_m > 0:
            return "STRONG_BUY" if change_pct >= 0 else "BULL_TRAP"
        return "STRONG_SELL" if change_pct <= 0 else "BEAR_TRAP"
    return "NEUTRAL"


def signal_bias(signal_type):
    if signal_type in {"STRONG_BUY", "ABSORPTION_BUY"}:
        return "long"
    if signal_type in {"STRONG_SELL", "ABSORPTION_SELL"}:
        return "short"
    if signal_type == "CLIMAX":
        return "event"
    if "TRAP" in signal_type or signal_type == "FAKE_OUT":
        return "caution"
    return "neutral"


def signal_rank(signal_type):
    ordering = {
        "STRONG_SELL": 1,
        "STRONG_BUY": 2,
        "ABSORPTION_SELL": 3,
        "ABSORPTION_BUY": 4,
        "CLIMAX": 5,
        "BEAR_TRAP": 6,
        "BULL_TRAP": 7,
        "FAKE_OUT": 8,
        "NEUTRAL": 9,
    }
    return ordering.get(signal_type, 99)


def classify_sector(symbol):
    return SECTOR_MAP.get(symbol.upper(), "UNKNOWN")


def load_borrowable_symbols():
    """Optional local borrowability allowlist for short-side discovery filtering."""
    if not os.path.exists(BORROWABLE_SYMBOLS_PATH):
        return None

    try:
        with open(BORROWABLE_SYMBOLS_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"  Warning: could not parse borrowable symbols file: {e}")
        return None

    if isinstance(payload, dict):
        for key in ("symbols", "borrowable", "shortable", "easy_to_borrow"):
            values = payload.get(key)
            if isinstance(values, list):
                return {str(v).strip().upper() for v in values if str(v).strip()}
    if isinstance(payload, list):
        return {str(v).strip().upper() for v in payload if str(v).strip()}

    print("  Warning: borrowable symbols file format not understood; ignoring")
    return None


def iter_snapshot_universe(full_scan):
    return fetch_full_universe() if full_scan else list(SP500_UNIVERSE)


def fetch_snapshot_row(ticker):
    """Return a current-market snapshot row for one symbol, or None if unusable."""
    df = yf.download(
        ticker,
        period="10d",
        interval="1h",
        progress=False,
        auto_adjust=False,
    )
    if df is None or len(df) < 60:
        return None

    df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close", "Volume", "Open"])
    if len(df) < 60:
        return None

    df = df.copy()
    df["rvol"] = df["Volume"] / df["Volume"].rolling(20).mean()
    price_change = df["Close"].pct_change()
    flow_raw = price_change * df["Volume"]
    flow_ma = flow_raw.rolling(5).mean()
    df["flow_m"] = flow_ma / (df["Volume"].rolling(5).mean() * df["Close"] + 1e-9)
    df["change_pct"] = (df["Close"] - df["Open"]) / df["Open"]
    df["avg_volume_20"] = df["Volume"].rolling(20).mean()
    df["avg_dollar_volume_20"] = (df["Close"] * df["Volume"]).rolling(20).mean()
    df = df.dropna(
        subset=["rvol", "flow_m", "change_pct", "avg_volume_20", "avg_dollar_volume_20"]
    )
    if df.empty:
        return None

    row = df.iloc[-1]
    price = float(row["Close"])
    avg_volume = float(row["avg_volume_20"])
    avg_dollar_volume = float(row["avg_dollar_volume_20"])
    if price < SNAPSHOT_MIN_PRICE:
        return None
    if avg_volume < SNAPSHOT_MIN_AVG_VOLUME:
        return None
    if avg_dollar_volume < SNAPSHOT_MIN_AVG_DOLLAR_VOLUME:
        return None

    rvol = float(row["rvol"])
    flow_m = float(row["flow_m"])
    change_pct = float(row["change_pct"])
    signal_type = analyze_snapshot_signal(rvol, change_pct, flow_m)
    magnitude = (abs(change_pct) * 1000.0) + (rvol * 5.0) + (abs(flow_m) * 10.0)

    return {
        "symbol": ticker,
        "as_of": pd.Timestamp(row.name).isoformat(),
        "sector": classify_sector(ticker),
        "price": round(price, 4),
        "avg_volume_20": round(avg_volume, 0),
        "avg_dollar_volume_20": round(avg_dollar_volume, 2),
        "rvol": round(rvol, 4),
        "flow_m": round(flow_m, 6),
        "change_pct": round(change_pct, 6),
        "signal_type": signal_type,
        "bias": signal_bias(signal_type),
        "score": round(magnitude, 4),
    }


def summarize_snapshot(rows):
    signal_counts = {}
    sector_stats = {}
    bias_counts = {}
    for row in rows:
        signal_counts[row["signal_type"]] = signal_counts.get(row["signal_type"], 0) + 1
        bias_counts[row["bias"]] = bias_counts.get(row["bias"], 0) + 1
        stats = sector_stats.setdefault(
            row["sector"],
            {"count": 0, "score_sum": 0.0, "leaders": []},
        )
        stats["count"] += 1
        stats["score_sum"] += row["score"]
        stats["leaders"].append((row["score"], row["symbol"], row["signal_type"]))

    top_signals = sorted(signal_counts.items(), key=lambda x: (-x[1], x[0]))
    top_sectors = []
    for sector, stats in sector_stats.items():
        leaders = sorted(stats["leaders"], key=lambda x: (-x[0], x[1]))[:3]
        top_sectors.append(
            {
                "sector": sector,
                "count": stats["count"],
                "avg_score": round(stats["score_sum"] / stats["count"], 2),
                "leaders": [symbol for _, symbol, _ in leaders],
            }
        )
    top_sectors.sort(key=lambda x: (-x["avg_score"], -x["count"], x["sector"]))
    market_tone = "mixed"
    longish = bias_counts.get("long", 0)
    shortish = bias_counts.get("short", 0)
    if shortish > longish * 1.2:
        market_tone = "short-leaning"
    elif longish > shortish * 1.2:
        market_tone = "long-leaning"

    return {
        "market_tone": market_tone,
        "bias_counts": bias_counts,
        "signal_counts": top_signals,
        "sector_summary": top_sectors,
    }


def write_market_snapshot_outputs(rows, full_scan, universe_count, borrowable):
    ranked_rows = sorted(
        rows,
        key=lambda x: (
            signal_rank(x["signal_type"]),
            -x["score"],
            -abs(x["change_pct"]),
            x["symbol"],
        )
    )
    pd.DataFrame(ranked_rows).to_csv(SNAPSHOT_CSV, index=False)

    top_rows = ranked_rows[:SNAPSHOT_TOP_N]
    grouped = {}
    for row in top_rows:
        grouped.setdefault(row["signal_type"], []).append(row["symbol"])

    summary = {
        "generated": datetime.now().isoformat(),
        "universe_mode": "full" if full_scan else "static",
        "universe_count": universe_count,
        "passed_filters": len(ranked_rows),
        "filters": {
            "min_price": SNAPSHOT_MIN_PRICE,
            "min_avg_volume_20": SNAPSHOT_MIN_AVG_VOLUME,
            "min_avg_dollar_volume_20": SNAPSHOT_MIN_AVG_DOLLAR_VOLUME,
            "borrowability_file": BORROWABLE_SYMBOLS_PATH if borrowable is not None else None,
        },
        "top_symbols": [row["symbol"] for row in top_rows],
        "by_signal_type": grouped,
        "summary": summarize_snapshot(ranked_rows),
        "rows": top_rows,
    }
    with open(SNAPSHOT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return ranked_rows, top_rows, summary


def run_market_snapshot(full_scan=False):
    """
    Build a broader discovery-side market snapshot from a tradable equity universe.
    Filters out illiquid/low-price names, optionally applies a borrowability allowlist
    to short-side ideas, and writes ranked CSV/JSON outputs.
    """
    universe = iter_snapshot_universe(full_scan)
    borrowable = load_borrowable_symbols()

    print(
        f"\n--- Market Snapshot ({len(universe)} symbols"
        f", min_price=${SNAPSHOT_MIN_PRICE:.2f}"
        f", min_adv=${SNAPSHOT_MIN_AVG_DOLLAR_VOLUME:,.0f}) ---"
    )
    rows = []
    total = len(universe)
    try:
        for i, ticker in enumerate(universe, 1):
            print(f"  [{i:4d}/{total}] {ticker:<8}", end="", flush=True)
            try:
                row = fetch_snapshot_row(ticker)
                if row is None:
                    print("  — filtered")
                    continue

                borrowable_state = "unknown"
                if row["bias"] == "short" and borrowable is not None:
                    row["borrowable"] = ticker in borrowable
                    borrowable_state = "yes" if row["borrowable"] else "no"
                    if not row["borrowable"]:
                        print("  — no borrow")
                        continue
                else:
                    row["borrowable"] = None

                print(
                    f"  {row['signal_type']:<16} RVOL={row['rvol']:.2f}"
                    f" Δ={row['change_pct']*100:+.2f}%"
                    f" score={row['score']:.1f}"
                    + (f" borrow={borrowable_state}" if row["bias"] == "short" else "")
                )
                rows.append(row)
                if len(rows) % 20 == 0:
                    write_market_snapshot_outputs(rows, full_scan, len(universe), borrowable)
                    print(
                        f"    checkpoint saved ({len(rows)} rows) -> "
                        f"{SNAPSHOT_CSV}, {SNAPSHOT_JSON}"
                    )
            except Exception as e:
                print(f"  — error: {ticker} ({type(e).__name__}: {e})")
                continue
    except KeyboardInterrupt:
        write_market_snapshot_outputs(rows, full_scan, len(universe), borrowable)
        print(
            f"\nSnapshot interrupted. Partial outputs written to "
            f"{SNAPSHOT_CSV} and {SNAPSHOT_JSON}."
        )
        raise

    rows, top_rows, summary = write_market_snapshot_outputs(
        rows, full_scan, len(universe), borrowable
    )

    if not rows:
        print("No market snapshot candidates passed filters.")
        print(f"Partial/final snapshot CSV: {SNAPSHOT_CSV}")
        print(f"Partial/final snapshot JSON: {SNAPSHOT_JSON}")
        return []

    print(f"\n✅ Snapshot CSV: {SNAPSHOT_CSV}")
    print(f"✅ Snapshot JSON: {SNAPSHOT_JSON}")
    print(f"\n🏆 Top Snapshot Names ({min(SNAPSHOT_TOP_N, len(top_rows))}):")
    for row in top_rows:
        print(
            f"   {row['symbol']:<8} {row['sector']:<13} {row['signal_type']:<16}"
            f" RVOL={row['rvol']:.2f} Δ={row['change_pct']*100:+.2f}%"
            f" Score={row['score']:.1f}"
        )
    snapshot_summary = summary["summary"]
    print("\n📌 Market Snapshot Summary:")
    print(
        f"   Tone: {snapshot_summary['market_tone']}"
        f" | long={snapshot_summary['bias_counts'].get('long', 0)}"
        f" short={snapshot_summary['bias_counts'].get('short', 0)}"
        f" event={snapshot_summary['bias_counts'].get('event', 0)}"
    )
    print("   Signal mix:")
    for signal_type, count in snapshot_summary["signal_counts"][:5]:
        print(f"     {signal_type:<16} {count}")
    print("   Strongest sectors:")
    for sector in snapshot_summary["sector_summary"][:5]:
        leaders = ", ".join(sector["leaders"])
        print(
            f"     {sector['sector']:<13} avg_score={sector['avg_score']:.2f}"
            f" count={sector['count']} leaders={leaders}"
        )
    return rows

def hunt_symbol(ticker):
    try:
        end   = datetime.today()
        start = end - timedelta(days=LOOKBACK_DAYS)
        df = yf.download(ticker, start=start, end=end,
                         interval="1h", progress=False, auto_adjust=False)
        if df is None or len(df) < 30:
            return None

        # CRITICAL: flatten multi-level columns
        df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=["Close","Volume"])
        if len(df) < 30:
            return None

        df = df.copy()
        df["rvol"]   = df["Volume"] / df["Volume"].rolling(20).mean()
        pc           = df["Close"].pct_change()
        flow         = pc * df["Volume"]
        flow_ma      = flow.rolling(5).mean()
        df["flow_m"] = flow_ma / (df["Volume"].rolling(5).mean() * df["Close"] + 1e-9)
        df["change"] = pc
        df = df.dropna(subset=["rvol","flow_m","change"])

        results = []
        for i in range(20, len(df)-1):
            rvol   = float(df["rvol"].iloc[i])
            flow_m = float(df["flow_m"].iloc[i])
            change = float(df["change"].iloc[i])
            close  = float(df["Close"].iloc[i])

            if not (rvol >= RVOL_THRESHOLD and
                    flow_m <= -FLOW_THRESHOLD and
                    change <= -0.003):
                continue

            entry = close
            exit_ = float(df["Close"].iloc[i+1])
            ret   = (entry - exit_) / entry

            hi = float(df["High"].iloc[i+1])
            lo = float(df["Low"].iloc[i+1])
            if (hi - entry) / entry >= 0.02:
                ret = -0.02
            elif (entry - lo) / entry >= 0.04:
                ret = 0.04

            results.append({"ret": ret, "win": ret > 0})

        if len(results) < MIN_TRADES:
            return None

        win_rate = sum(1 for r in results if r["win"]) / len(results)
        avg_ret  = np.mean([r["ret"] for r in results])

        return {
            "symbol":    ticker,
            "n_signals": len(results),
            "win_rate":  round(win_rate, 4),
            "avg_return":round(avg_ret, 4),
        }
    except Exception:
        return None

def market_conditions_changed():
    """
    Returns True if market conditions have shifted enough to warrant
    re-evaluating cooling_off symbols:
      - VIX has moved 5+ points from the last regime snapshot, OR
      - Sector ETFs show 5%+ spread in 5-day returns (rotation signal)
    """
    # VIX delta vs stored snapshot
    try:
        with open(REGIME_PATH) as f:
            snap = json.load(f)
        snap_vix = float(snap.get("vix", 0))
        live = yf.download("^VIX", period="2d", interval="1d", progress=False, auto_adjust=False)
        live.columns = live.columns.get_level_values(0)
        current_vix = float(live["Close"].dropna().iloc[-1])
        if abs(current_vix - snap_vix) >= VIX_CHANGE_THRESHOLD:
            print(f"  Market shift: VIX {snap_vix:.1f} → {current_vix:.1f} "
                  f"(Δ {current_vix - snap_vix:+.1f})")
            return True
    except Exception as e:
        print(f"  Warning: VIX check failed: {e}")

    # Sector rotation: compare 5-day returns across major sector ETFs
    try:
        sectors = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE"]
        data = yf.download(sectors, period="7d", interval="1d", progress=False, auto_adjust=False)
        closes = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data[["Close"]]
        ret5d = (closes.iloc[-1] - closes.iloc[0]) / closes.iloc[0]
        spread = float(ret5d.max() - ret5d.min())
        if spread >= SECTOR_ROTATION_PCT:
            best  = ret5d.idxmax()
            worst = ret5d.idxmin()
            print(f"  Market shift: sector rotation {spread:.1%} spread "
                  f"(best={best} {ret5d[best]:+.1%}, worst={worst} {ret5d[worst]:+.1%})")
            return True
    except Exception as e:
        print(f"  Warning: sector rotation check failed: {e}")

    return False


def evaluate_cooling_off():
    """
    Re-scans cooling_off symbols that haven't been evaluated in 7+ days.
    If market conditions have changed, queues them into pending_retest.
    Updates approved_symbols.json with results.
    """
    try:
        with open(APPROVED_PATH) as f:
            approved = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("  No approved_symbols.json found; skipping cooling_off re-evaluation.")
        return

    cooling_off = approved.get("cooling_off", [])
    if not cooling_off:
        print("  No symbols in cooling_off.")
        return

    history     = approved.get("cooling_off_history", {})
    now         = datetime.utcnow()
    cutoff      = (now - timedelta(days=COOLING_RESCAN_DAYS)).isoformat()
    due         = [s for s in cooling_off
                   if history.get(s, "1970-01-01") < cutoff]

    print(f"\n--- Cooling-off re-evaluation ---")
    print(f"  cooling_off: {cooling_off}")
    print(f"  Due for rescan ({COOLING_RESCAN_DAYS}d+): {due}")

    if not due:
        print("  All cooling_off symbols were evaluated recently.")
        return

    conditions_changed = market_conditions_changed()
    pending_retest = set(approved.get("pending_retest", []))
    requeued = []

    for sym in due:
        print(f"  Rescanning {sym}...", end=" ", flush=True)
        r = hunt_symbol(sym)
        history[sym] = now.isoformat()

        if r is None:
            print("no data")
            continue

        qualifies = r["win_rate"] >= MIN_WIN_RATE and r["avg_return"] >= MIN_AVG_RETURN
        print(f"WR={r['win_rate']:.0%} AvgRet={r['avg_return']:.2%} N={r['n_signals']}", end="")

        if qualifies and conditions_changed:
            pending_retest.add(sym)
            requeued.append(sym)
            print(" → requeued for strategy_lab")
        elif qualifies:
            print(" ✅ (conditions unchanged — stays cooling_off)")
        else:
            print(" ❌ still weak")

    approved["cooling_off_history"] = history
    approved["pending_retest"]      = sorted(pending_retest)
    approved["updated"]             = now.isoformat()

    with open(APPROVED_PATH, "w") as f:
        json.dump(approved, f, indent=2)

    if requeued:
        print(f"\n  Queued for strategy_lab retest: {requeued}")


def hunt_symbol_full(ticker):
    """Like hunt_symbol but uses RVOL_FULL_SCAN (3x) threshold — for broad universe scans."""
    try:
        end   = datetime.today()
        start = end - timedelta(days=LOOKBACK_DAYS)
        df = yf.download(ticker, start=start, end=end,
                         interval="1h", progress=False, auto_adjust=False)
        if df is None or len(df) < 30:
            return None
        df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=["Close", "Volume"])
        if len(df) < 30:
            return None
        df = df.copy()
        df["rvol"]   = df["Volume"] / df["Volume"].rolling(20).mean()
        pc           = df["Close"].pct_change()
        flow         = pc * df["Volume"]
        flow_ma      = flow.rolling(5).mean()
        df["flow_m"] = flow_ma / (df["Volume"].rolling(5).mean() * df["Close"] + 1e-9)
        df["change"] = pc
        df = df.dropna(subset=["rvol", "flow_m", "change"])

        results = []
        for i in range(20, len(df) - 1):
            rvol   = float(df["rvol"].iloc[i])
            flow_m = float(df["flow_m"].iloc[i])
            change = float(df["change"].iloc[i])
            close  = float(df["Close"].iloc[i])

            # Stricter RVOL bar for full-universe scan
            if not (rvol >= RVOL_FULL_SCAN and
                    flow_m <= -FLOW_THRESHOLD and
                    change <= -0.003):
                continue

            entry = close
            exit_ = float(df["Close"].iloc[i + 1])
            ret   = (entry - exit_) / entry

            hi = float(df["High"].iloc[i + 1])
            lo = float(df["Low"].iloc[i + 1])
            if (hi - entry) / entry >= 0.02:
                ret = -0.02
            elif (entry - lo) / entry >= 0.04:
                ret = 0.04

            results.append({"ret": ret, "win": ret > 0})

        if len(results) < MIN_TRADES:
            return None

        win_rate = sum(1 for r in results if r["win"]) / len(results)
        avg_ret  = np.mean([r["ret"] for r in results])

        return {
            "symbol":     ticker,
            "n_signals":  len(results),
            "win_rate":   round(win_rate, 4),
            "avg_return": round(avg_ret, 4),
            "rvol_bar":   RVOL_FULL_SCAN,
        }
    except Exception:
        return None


def run_full_universe_scan():
    """
    Scans the full S&P 500 + Russell 1000 universe (fetched fresh from Wikipedia)
    using the 3x RVOL bar. Prints results and returns a list of qualifying symbols.
    """
    universe = fetch_full_universe()
    print(f"\n--- Full Universe Scan ({len(universe)} symbols, RVOL >= {RVOL_FULL_SCAN}x) ---")
    results = []
    total   = len(universe)
    for i, ticker in enumerate(universe, 1):
        print(f"  [{i:4d}/{total}] {ticker:<8}", end="", flush=True)
        r = hunt_symbol_full(ticker)
        if r:
            q = r["win_rate"] >= MIN_WIN_RATE and r["avg_return"] >= MIN_AVG_RETURN
            print(f"  WR={r['win_rate']:.0%}  AvgRet={r['avg_return']:.2%}  N={r['n_signals']}"
                  + (" ✅" if q else ""))
            results.append(r)
        else:
            print("  —")

    qualifiers = [r for r in results
                  if r["win_rate"] >= MIN_WIN_RATE and r["avg_return"] >= MIN_AVG_RETURN]
    qualifiers.sort(key=lambda x: (x["win_rate"], x["avg_return"]), reverse=True)
    print(f"\n  Full-scan qualifiers: {[r['symbol'] for r in qualifiers[:20]]}")
    return qualifiers


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Discovery engine for symbol hunts, broader scans, and market snapshots."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use the broader full-universe fetch instead of the static discovery list.",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Write a ranked market snapshot instead of the historical qualifier hunt.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    full_scan = args.full

    # Re-evaluate cooling_off symbols before running the main hunt
    evaluate_cooling_off()

    if args.snapshot:
        run_market_snapshot(full_scan=full_scan)
        return

    if full_scan:
        run_full_universe_scan()
        return

    results = []
    total   = len(SP500_UNIVERSE)
    for i, ticker in enumerate(SP500_UNIVERSE, 1):
        print(f"  [{i:3d}/{total}] {ticker:<8}", end="", flush=True)
        r = hunt_symbol(ticker)
        if r:
            q = r["win_rate"] >= MIN_WIN_RATE and r["avg_return"] >= MIN_AVG_RETURN
            print(f"  WR={r['win_rate']:.0%}  AvgRet={r['avg_return']:.2%}  N={r['n_signals']}"
                  + (" ✅" if q else ""))
            results.append(r)
        else:
            print("  — skip")

    if not results:
        print("No results.")
        return

    results.sort(key=lambda x: (x["win_rate"], x["avg_return"]), reverse=True)
    qualifiers = [r for r in results
                  if r["win_rate"] >= MIN_WIN_RATE and r["avg_return"] >= MIN_AVG_RETURN]
    top20 = [r["symbol"] for r in qualifiers[:20]]

    pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Results: {OUTPUT_CSV}")
    print(f"\n🏆 Qualifiers ({len(qualifiers)}):")
    for r in qualifiers[:20]:
        print(f"   {r['symbol']:<8} WR={r['win_rate']:.0%}  AvgRet={r['avg_return']:.2%}  N={r['n_signals']}")

    with open(OUTPUT_JSON, "w") as f:
        json.dump({"generated": datetime.now().isoformat(), "top_sell": top20}, f, indent=2)
    print(f"✅ Top 20: {OUTPUT_JSON}")

    try:
        approved = json.load(open(APPROVED_PATH))
        new = set(top20) - set(approved.get("sell", []))
        if new:
            print(f"\n🆕 New candidates: {', '.join(sorted(new))}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
