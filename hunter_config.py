# hunter_config.py

# --- THE 100-SYMBOL UNIVERSE ---
HUNT_SYMBOLS = [
    # --- ETFs (The Tide) ---
    "SPY","QQQ","IWM","DIA","SMH","XLK","XLF","XLE",
    # --- Mag 7 / Mega Cap ---
    "AAPL","MSFT","AMZN","NVDA","META","GOOGL","TSLA",
    # --- Semis / AI ---
    "AMD","AVGO","ASML","TSM","MU","INTC","ARM","SMCI",
    # --- High Liquid Momentum ---
    "COIN","PLTR","NET","CRM","ORCL","SNOW","SHOP","UBER","ABNB","NFLX",
    "PANW","CRWD","DDOG","ZS","NOW","ADBE","INTU","QCOM","TXN","AMAT","LRCX","KLAC",
    # --- Finance ---
    "JPM","BAC","WFC","GS","MS","C","SCHW",
    # --- Energy ---
    "XOM","CVX","SLB","OXY",
    # --- Industrial ---
    "BA","CAT","DE","GE","MMM","HON",
    # --- Healthcare ---
    "UNH","JNJ","PFE","LLY","MRK","ABBV",
    # --- Retail ---
    "COST","WMT","TGT","HD","LOW","NKE","MCD","SBUX",
    # --- Comms ---
    "DIS","CMCSA","TMUS","VZ","T",
    # --- Crypto Proxies ---
    "MSTR", "MARA", "RIOT", "CLSK"
]

# --- CRYPTO (Alpaca Format) ---
HUNT_CRYPTO = ["BTC/USD", "ETH/USD", "SOL/USD"]

# --- SCORING RULES ---
MIN_STRONG_SIGNALS = 5           # Need at least 5 signals to get a score
ROLLING_WINDOW_SIGNALS = 50      # Look at last 50 signals
MIN_WIN_RATE = 0.51              # Must win > 51% of time
MIN_PROFIT_FACTOR = 1.10         # Must make $1.10 for every $1.00 lost

APPROVED_TARGET_COUNT = 30       # Top 30 tickers get approved
OUTPUT_APPROVED_JSON = "/home/theplummer92/approved_symbols.json"

# --- PATHS ---
SCORES_DB_PATH = "/home/theplummer92/wolfe_scores.db"
SIGNALS_DB_PATH = "/home/theplummer92/wolfe_signals.db"

