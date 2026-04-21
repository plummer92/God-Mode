import sqlite3
import pandas as pd
from app_paths import DATA_DIR
DB_PATH = str(DATA_DIR / "wolfe_signals.db")

def run_backtest():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Fetch all historical signals
    print("⏳ Loading signal history...")
    query = """
        SELECT timestamp, symbol, price, flow_m, rvol, signal_type, confidence
        FROM signals
        ORDER BY timestamp ASC
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("❌ No signals found to backtest.")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 2. SIMULATION LOGIC
    # We approximate the trade exit by using the *next* signal's price for that symbol.
    
    results = []
    
    print(f"🔄 Simulating trades on {len(df)} signals...")
    
    # Group by symbol to track price changes per ticker
    grouped = df.groupby('symbol')
    
    for symbol, group in grouped:
        group = group.sort_values('timestamp')
        
        # Shift price to get the "Next Price" (approximate exit)
        group['exit_price'] = group['price'].shift(-1)
        group['time_held'] = group['timestamp'].shift(-1) - group['timestamp']
        
        # Filter for valid trades (where we have a next data point to exit at)
        trades = group.dropna(subset=['exit_price'])
        
        for index, row in trades.iterrows():
            trade_type = None
            
            # --- STRATEGY 1: WHALE FADE (Absorption) ---
            # Logic: High RVOL + Low Price Move + Big Flow -> Reversal
            
            # Buy Absorption (Selling Flow but Price Held) -> LONG
            if "ABSORPTION" in row['signal_type'] and row['flow_m'] < -10 and row['rvol'] > 2.0:
                trade_type = "LONG"
            
            # Sell Absorption (Buying Flow but Price Capped) -> SHORT
            elif "ABSORPTION" in row['signal_type'] and row['flow_m'] > 10 and row['rvol'] > 2.0:
                trade_type = "SHORT"
            
            # --- STRATEGY 2: MOMENTUM (Trend Surfing) ---
            # Logic: Strong Flow -> Follow it
            elif "STRONG BUY" in row['signal_type']:
                trade_type = "LONG"
            elif "STRONG SELL" in row['signal_type']:
                trade_type = "SHORT"
            
            if trade_type:
                # Calculate Profit
                if trade_type == "LONG":
                    pnl_pct = (row['exit_price'] - row['price']) / row['price']
                else:
                    pnl_pct = (row['price'] - row['exit_price']) / row['price']
                
                # --- NEW FILTER: Ignore Noise ---
                # If PnL is practically zero (duplicate signal), ignore it.
                if abs(pnl_pct) < 0.0001: 
                    continue
                # --------------------------------
                
                results.append({
                    'Symbol': symbol,
                    'Strategy': "Absorption" if "ABSORPTION" in row['signal_type'] else "Momentum",
                    'Type': trade_type,
                    'Entry_Time': row['timestamp'],
                    'Entry': row['price'],
                    'Exit': row['exit_price'],
                    'PnL': pnl_pct * 100  # in %
                })
    
    conn.close()
    
    # 3. REPORTING
    if not results:
        print("⚠️ No valid trade setups found (after filtering noise).")
        return

    res_df = pd.DataFrame(results)
    
    print("\n" + "="*40)
    print("🧪 BACKTEST RESULTS (Hypothetical)")
    print("="*40)
    print(f"Total Trades: {len(res_df)}")
    print(f"Win Rate:     {len(res_df[res_df['PnL'] > 0]) / len(res_df) * 100:.2f}%")
    print(f"Avg PnL:      {res_df['PnL'].mean():.2f}% per trade")
    print(f"Total Return: {res_df['PnL'].sum():.2f}% (uncompounded)")
    
    print("\n📊 By Strategy:")
    print(res_df.groupby('Strategy')['PnL'].describe())
    
    print("\n🏆 Top 3 Winners:")
    print(res_df.sort_values("PnL", ascending=False).head(3)[['Symbol', 'Type', 'PnL', 'Entry_Time']].to_string(index=False))

    print("\n💀 Top 3 Losers:")
    print(res_df.sort_values("PnL", ascending=True).head(3)[['Symbol', 'Type', 'PnL', 'Entry_Time']].to_string(index=False))

if __name__ == "__main__":
    run_backtest()
