#!/usr/bin/env python3
"""symbol_hunter.py v4 — cooling_off re-evaluation + full universe scan"""
import yfinance as yf
import pandas as pd
import numpy as np
import json, os
from datetime import datetime, timedelta

LOOKBACK_DAYS  = 60
MIN_TRADES     = 3
MIN_WIN_RATE   = 0.60
MIN_AVG_RETURN = 0.003
OUTPUT_CSV     = os.path.expanduser("~/symbol_hunt_results.csv")
OUTPUT_JSON    = os.path.expanduser("~/symbol_hunt_top20.json")
APPROVED_PATH  = os.path.expanduser("~/approved_symbols.json")
REGIME_PATH    = os.path.expanduser("~/regime_snapshot.json")

COOLING_RESCAN_DAYS  = 7    # re-scan cooling_off symbols not seen in this many days
VIX_CHANGE_THRESHOLD = 5.0  # VIX move of this magnitude = market condition change
SECTOR_ROTATION_PCT  = 0.05 # 5% spread between best/worst sector ETF 5d return

RVOL_THRESHOLD = 1.5
FLOW_THRESHOLD = 0.00003

SP500_UNIVERSE = [
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
SP500_UNIVERSE = list(dict.fromkeys(SP500_UNIVERSE))
print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Symbol Hunter v3 — {len(SP500_UNIVERSE)} symbols")

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


def main():
    # Re-evaluate cooling_off symbols before running the main hunt
    evaluate_cooling_off()

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
