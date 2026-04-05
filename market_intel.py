import sqlite3
import pandas as pd

DB_PATH = "/home/theplummer92/wolfe_signals.db"

def get_intel():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. TOTAL FLOW BY TICKER (Where is the money going?)
    print("\n💰 --- TOP 5 TICKERS BY TOTAL FLOW (Millions) ---")
    query_flow = """
        SELECT symbol, 
               COUNT(*) as signal_count,
               SUM(flow_m) as total_flow_m, 
               AVG(confidence) as avg_conf
        FROM signals 
        GROUP BY symbol 
        ORDER BY total_flow_m DESC 
        LIMIT 5
    """
    df_flow = pd.read_sql_query(query_flow, conn)
    print(df_flow.to_string(index=False))

    # 2. SECTOR MOMENTUM (Where is the rotation?)
    print("\n🏗️  --- SECTOR FLOW BREAKDOWN ---")
    query_sector = """
        SELECT sector, 
               COUNT(*) as signals, 
               SUM(flow_m) as net_flow_m 
        FROM signals 
        GROUP BY sector 
        ORDER BY net_flow_m DESC
    """
    df_sector = pd.read_sql_query(query_sector, conn)
    print(df_sector.to_string(index=False))

    # 3. TOP 'CLIMAX' EVENTS (The biggest spikes)
    print("\n🔥 --- TOP 3 CLIMAX EVENTS (Panic/FOMO) ---")
    query_climax = """
        SELECT timestamp, symbol, price, rvol, flow_m 
        FROM signals 
        WHERE signal_type LIKE '%CLIMAX%' 
        ORDER BY flow_m DESC 
        LIMIT 3
    """
    df_climax = pd.read_sql_query(query_climax, conn)
    if not df_climax.empty:
        print(df_climax.to_string(index=False))
    else:
        print("No CLIMAX events found yet.")

    conn.close()

if __name__ == "__main__":
    get_intel()
