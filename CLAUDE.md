# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This System Is

**Wolfe God-Mode** — an automated stock/crypto trading system running on a GCP server. It scans markets for signals, executes trades via Alpaca or IBKR, and monitors everything via Discord and a Streamlit dashboard.

## Running the Bots

All production bots run as systemd services. To restart after code changes:

```bash
sudo systemctl restart sniper.service
sudo systemctl restart paper-sniper.service
sudo systemctl restart strategy-lab.service
sudo systemctl restart dashboard.service
```

Unit files live in `/etc/systemd/system/`. After creating or editing a unit file, run `sudo systemctl daemon-reload` before starting/enabling.

Check logs:
```bash
journalctl -u sniper.service -n 50 --no-pager
journalctl -u sniper.service -f
tail -f /home/theplummer92/sniper.log
tail -f /home/theplummer92/crypto_raider.log
```

## Running Scripts Manually

Always activate the venv first:
```bash
source ~/venv/bin/activate
```

Key one-off scripts:
```bash
python3 godmode.py              # Signal scanner (runs continuously)
python3 sniper_bot.py           # Execution bot (runs continuously)
python3 crypto_raider.py        # Crypto bot (runs continuously)
streamlit run dashboard_gui.py --server.port 8501   # Dashboard
python3 label_events.py --db ~/market_intel.db --since-days 3   # Label signal outcomes
python3 hunter.py               # Score symbols and update approved list
python3 backtest.py             # Deep backtest against yfinance
python3 db_inspector.py         # Read-only DB diagnostics
python3 diagnostic.py           # Check live positions vs stop-loss logic
```

## Deployment

Push to `main` branch triggers GitHub Actions (`.github/workflows/deploy.yml`), which SSHs to the server, runs `git pull`, and restarts all four systemd services.

## Architecture

### Data Flow

```
godmode.py  (scanner, every 5min via yfinance)
    -> wolfe_signals.db  (signals table)
    -> market_log.csv
    -> regime_snapshot.json  (VIX/DXY/TNX macro regime)
    -> market_intel.db  (events table, for label_events.py)

sniper_bot.py  (reads wolfe_signals.db, places trades)
    -> trade_log.db
    -> wolfe_signals.db  (sniper_status table)

crypto_raider.py  (standalone crypto bot, Alpaca-only)
    -> crypto_raider.log

label_events.py  (fetches outcome prices via Alpaca, writes labels)
    -> market_intel.db  (labels table, keyed by event_hash)

hunter.py  (scores symbols, updates approved lineup)
    -> wolfe_scores.db
    -> approved_symbols.json

roster_manager_(1).py  (reads strategy_lab.db, updates approved_symbols.json)
    -> approved_symbols.json

watchtower.py  (tails log files, sends Discord alerts)
dashboard_gui.py  (Streamlit, reads wolfe_signals.db + regime_snapshot.json)
```

### Key Databases

| File | Purpose |
|------|---------|
| `wolfe_signals.db` | Core: signals, sniper_status, macro_features tables |
| `market_intel.db` | Events + labels (used by label_events.py) |
| `trade_log.db` | Trade history with PnL, regime, outcomes |
| `strategy_lab.db` | Strategy lab backtest results |
| `wolfe_scores.db` | Per-symbol scoring from hunter.py |

### Broker Abstraction

`sniper_bot.py` supports two brokers via the `BROKER` env var:
- `BROKER=alpaca` (default) — uses Alpaca SDK directly
- `BROKER=ibkr` — uses `ibkr_client.py`, a drop-in wrapper around IBKR Client Portal REST API (runs locally at `https://localhost:5000`)

`ibkr_client.py` mirrors Alpaca's `TradingClient` interface (`get_all_positions`, `get_account`, `get_clock`, `submit_order`, `close_position`).

### Symbol Format Conventions

- **Alpaca crypto**: `BTC/USD`
- **Yahoo Finance crypto**: `BTC-USD`
- Use `symbols.py` (`to_yahoo_symbol` / `to_alpaca_symbol`) for conversion
- Futures (`CL=F`, `GC=F`) and indices (`^VIX`, `^TNX`) are yahoo-only; Alpaca/IBKR will reject them

### Signal Types

Signals in `wolfe_signals.db` follow these key types:
- `STRONG BUY FLOW` / `STRONG SELL FLOW` — momentum signals
- `ABSORPTION WALL` — high RVOL + tiny price move + big flow (reversal setup)
- `CLIMAX` — volume spike exhaustion

### Regime Filter (`regime_snapshot.json`)

Written by `godmode.py` every scan. `sniper_bot.py` reads it before any trade:
- `OPEN` — all trades allowed
- `SELL_ONLY` — no new longs
- `BLOCKED` — no new trades

### Approved Symbols

`approved_symbols.json` is the live trading whitelist. Updated by:
- `hunter.py` — scores symbols from `wolfe_signals.db` using win rate + profit factor
- `roster_manager_(1).py` — pulls top performers from `strategy_lab.db` (min 70% win rate, min score 100)

`ALWAYS_ELIGIBLE` in roster_manager: `SPY`, `IWM`, `QQQ`.

## Environment Variables (`.env`)

```
APCA_API_KEY_ID=
APCA_API_SECRET_KEY=
DISCORD_WEBHOOK_URL=
BROKER=alpaca          # or ibkr
IBKR_ACCOUNT_ID=       # if BROKER=ibkr
IBKR_GATEWAY_URL=https://localhost:5000/v1/api
```

## Sniper Bot Risk Parameters

Defined at the top of `sniper_bot.py`:
- `TRADE_NOTIONAL_USD = 10` — per-trade size
- `HARD_STOP_LOSS_PCT = 0.02` — 2% stop loss
- `TAKE_PROFIT_PCT = 0.04` — 4% take profit
- `DAILY_LOSS_LIMIT_USD = 5.00` — kills trading for the day
- `MAX_OPEN_POSITIONS = 5`
