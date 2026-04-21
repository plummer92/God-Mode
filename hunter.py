import json
import sqlite3
import os
import time
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from hunter_config import (
    HUNT_CRYPTO,
    MIN_STRONG_SIGNALS, ROLLING_WINDOW_SIGNALS,
    MIN_WIN_RATE, MIN_PROFIT_FACTOR,
    APPROVED_TARGET_COUNT, OUTPUT_APPROVED_JSON,
    SCORES_DB_PATH, SIGNALS_DB_PATH
)
from app_paths import DATA_DIR

# Load Auth
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
crypto_client = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)


def resolve_output_path() -> str:
    configured = str(OUTPUT_APPROVED_JSON)
    if os.getenv("HUNTER_ALLOW_LIVE_APPROVED_OVERRIDE", "").strip().lower() in {"1", "true", "yes"}:
        return configured
    if os.path.abspath(configured) == str(DATA_DIR / "approved_symbols.json"):
        safe_path = str(DATA_DIR / "hunter_approved_symbols.json")
        print(
            "⚠️ HUNTER SAFE OUTPUT: redirecting approved list write "
            f"from {configured} to {safe_path}"
        )
        return safe_path
    return configured

def ensure_tables(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS signal_outcomes (
        event_hash TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        ts_utc TEXT NOT NULL,
        price_at_signal REAL NOT NULL,
        price_1h REAL,
        ret_1h REAL,
        labeled_at_utc TEXT
    );
    CREATE TABLE IF NOT EXISTS symbol_scores (
        symbol TEXT PRIMARY KEY,
        n INTEGER NOT NULL,
        win_rate REAL NOT NULL,
        avg_ret REAL NOT NULL,
        profit_factor REAL NOT NULL,
        score REAL NOT NULL,
        updated_at_utc TEXT NOT NULL
    );
    """)

def get_historical_price(symbol, target_time):
    """Fetch price closest to target_time from Alpaca."""
    try:
        start = target_time - timedelta(minutes=5)
        end = target_time + timedelta(minutes=5)
        
        if "/" in symbol: # Crypto
            req = CryptoBarsRequest(symbol_or_symbols=[symbol], timeframe=TimeFrame.Minute, start=start, end=end)
            bars = crypto_client.get_crypto_bars(req).df
        else: # Stock
            req = StockBarsRequest(symbol_or_symbols=[symbol], timeframe=TimeFrame.Minute, start=start, end=end)
            bars = stock_client.get_stock_bars(req).df
            
        if bars.empty: return None
        
        # Reset index to find closest time
        if hasattr(bars.index, "names") and "timestamp" in bars.index.names:
            bars = bars.reset_index().set_index("timestamp")
        
        # Get bar closest to target_time
        idx = bars.index.get_indexer([target_time], method='nearest')[0]
        return float(bars.iloc[idx]["close"])
    except Exception:
        # Silent fail to keep logs clean
        return None

def main():
    print("🦅 WOLFE HUNTER: Starting Audit...")
    output_path = resolve_output_path()
    scores_conn = sqlite3.connect(SCORES_DB_PATH)
    ensure_tables(scores_conn)
    
    # 1. GET PROCESSED IDs (From Score DB)
    try:
        existing = pd.read_sql_query("SELECT event_hash FROM signal_outcomes", scores_conn)
        existing_ids = set(existing["event_hash"].astype(str))
    except Exception:
        existing_ids = set()

    # 2. READ NEW SIGNALS (From Signal DB)
    sig_conn = sqlite3.connect(SIGNALS_DB_PATH)
    
    # Grab ALL strong signals first
    query = """
        SELECT rowid, symbol, signal_type, timestamp, price 
        FROM signals 
        WHERE (signal_type LIKE '%STRONG BUY%' OR signal_type LIKE '%STRONG SELL%')
    """
    all_signals = pd.read_sql_query(query, sig_conn)
    sig_conn.close()
    
    # Filter in Python (Cross-DB Logic)
    if not all_signals.empty:
        all_signals["rowid"] = all_signals["rowid"].astype(str)
        new_signals = all_signals[~all_signals["rowid"].isin(existing_ids)]
    else:
        new_signals = pd.DataFrame()
    
    # 3. INSERT UNLABELED
    if not new_signals.empty:
        print(f"📥 Importing {len(new_signals)} new signals...")
        rows = []
        for _, r in new_signals.iterrows():
            side = "BUY" if "BUY" in r["signal_type"] else "SELL"
            try:
                # Ensure UTC
                ts = pd.to_datetime(r["timestamp"])
                if ts.tzinfo is None:
                    # Assume stored as UTC string, or localize if needed
                    pass 
            except:
                continue
                
            rows.append((
                str(r["rowid"]), r["symbol"], side, ts.isoformat(), r["price"], None, None, None
            ))
        
        scores_conn.executemany("INSERT OR IGNORE INTO signal_outcomes VALUES (?,?,?,?,?,?,?,?)", rows)
        scores_conn.commit()

    # 4. LABEL OUTCOMES (+1 Hour)
    # Find signals that are >1 hour old but have no result
    unlabeled = pd.read_sql_query("""
        SELECT event_hash, symbol, side, ts_utc, price_at_signal 
        FROM signal_outcomes 
        WHERE price_1h IS NULL
    """, scores_conn)
    
    if not unlabeled.empty:
        print(f"⏳ Processing {len(unlabeled)} outcomes (checking Alpaca)...")
        
        count = 0
        updates = []
        
        for _, r in unlabeled.iterrows():
            # Handle timestamp parsing carefully
            try:
                signal_time = pd.to_datetime(r["ts_utc"]).replace(tzinfo=None) # naive UTC
            except:
                continue

            check_time = signal_time + timedelta(hours=1)
            
            # Only check if enough time passed
            if datetime.utcnow() < check_time:
                continue
                
            # Fetch Result
            # Convert check_time to aware UTC for Alpaca API
            target_aware = check_time.replace(tzinfo=datetime.utcnow().astimezone().tzinfo)
            exit_price = get_historical_price(r["symbol"], target_aware)
            
            if exit_price:
                ret = (exit_price - r["price_at_signal"]) / r["price_at_signal"]
                if r["side"] == "SELL": ret = -ret
                
                updates.append((exit_price, ret, datetime.utcnow().isoformat(), r["event_hash"]))
                count += 1
                
                if count % 10 == 0:
                    print(f"   ... verified {count} trades")
                
                time.sleep(0.05) # Rate limit protection

        if updates:
            scores_conn.executemany("""
                UPDATE signal_outcomes SET price_1h=?, ret_1h=?, labeled_at_utc=? WHERE event_hash=?
            """, updates)
            scores_conn.commit()
            print(f"✅ Labeled {len(updates)} completed signals.")

    # 5. COMPUTE SCORES
    print("📊 Updating Scores...")
    df = pd.read_sql_query("SELECT symbol, ret_1h FROM signal_outcomes WHERE ret_1h IS NOT NULL", scores_conn)
    
    if not df.empty:
        score_rows = []
        for sym, g in df.groupby("symbol"):
            g = g.tail(ROLLING_WINDOW_SIGNALS)
            if len(g) < MIN_STRONG_SIGNALS: continue
            
            win_rate = (g["ret_1h"] > 0).mean()
            avg_ret = g["ret_1h"].mean()
            
            gross_win = g[g["ret_1h"] > 0]["ret_1h"].sum()
            gross_loss = abs(g[g["ret_1h"] < 0]["ret_1h"].sum())
            pf = (gross_win / gross_loss) if gross_loss > 0 else 10.0
            
            # Formula: WinRate + ProfitFactor + Consistency
            score = (win_rate * 50) + (pf * 20) + (avg_ret * 1000)
            
            score_rows.append((sym, len(g), win_rate, avg_ret, pf, score, datetime.utcnow().isoformat()))
            
        scores_conn.executemany("""
            INSERT OR REPLACE INTO symbol_scores VALUES (?,?,?,?,?,?,?)
        """, score_rows)
        scores_conn.commit()

    # 6. APPROVE LIST
    final_scores = pd.read_sql_query(f"SELECT * FROM symbol_scores WHERE win_rate >= {MIN_WIN_RATE} AND profit_factor >= {MIN_PROFIT_FACTOR} ORDER BY score DESC LIMIT {APPROVED_TARGET_COUNT}", scores_conn)
    approved = final_scores["symbol"].tolist()
    
    # Always ensure Crypto is watching if not in list
    for c in HUNT_CRYPTO:
        if c not in approved: approved.append(c)

    # Save to JSON
    with open(output_path, "w") as f:
        json.dump({"approved": approved, "updated": datetime.utcnow().isoformat()}, f)
    
    print(f"🏆 APPROVED LIST ({len(approved)}): {approved}")
    print(f"📄 Hunter output written to: {output_path}")
    scores_conn.close()

if __name__ == "__main__":
    main()
