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
tail -f /home/theplummer92/paper_sniper.log
```

## Running Scripts Manually

Always activate the venv first:
```bash
source ~/venv/bin/activate
```

Key one-off scripts:
```bash
python3 godmode.py              # Signal scanner (runs continuously, 5-min loop)
python3 sniper_bot.py           # Live execution bot (runs continuously, 10s poll)
python3 paper_sniper.py         # Paper account SELL-only bot (runs continuously)
python3 strategy_lab.py         # Backtests strategy combos every 6 hours
python3 roster_manager.py       # Updates approved_symbols.json from strategy_lab.db
python3 Symbol_hunter.py        # Scans SP500 universe for best signals, outputs approved list
python3 daily_report.py         # Post 4pm daily P&L summary to Discord
python3 morning_brief.py        # Post 9:30am briefing to Discord
python3 summary.py              # Print all-time performance summary to stdout
streamlit run dashboard_db.py --server.port 8501   # Dashboard
```

## Deployment

Push to `main` branch triggers GitHub Actions (`.github/workflows/deploy.yml`), which SSHs to the server, runs `git pull`, and restarts all four systemd services.

## Architecture

### Data Flow

```
godmode.py  (scanner, every 5min via yfinance 5m bars)
    -> wolfe_signals.db  (signals + macro_features tables)
    -> market_log.csv    (every ticker, every scan)
    -> regime_snapshot.json  (VIX/TNX/DXY + derived regime)
    -> absorption_watchlist.csv / absorption_resolutions.csv

sniper_bot.py  (polls wolfe_signals.db every 10s, places live trades)
    -> trade_log.db      (trades table: entry, exit, PnL, regime)
    -> wolfe_signals.db  (sniper_status table, heartbeat)

paper_sniper.py  (polls wolfe_signals.db, SHORT-only, paper account)
    -> paper_sniper.log

strategy_lab.py  (runs every 6h, tests RVOL/TP/SL/hold combos on 30d history)
    -> strategy_lab.db   (results + leaderboard tables)

roster_manager.py  (reads strategy_lab.db, updates approved whitelist)
    -> approved_symbols.json

Symbol_hunter.py  (scans SP500 universe, writes top performers)
    -> ~/symbol_hunt_results.csv
    -> ~/symbol_hunt_top20.json
    -> approved_symbols.json

daily_report.py / morning_brief.py  (read trade_log.db, post to Discord)
```

### Key Databases

| File | Purpose |
|------|---------|
| `wolfe_signals.db` | Core: `signals`, `sniper_status`, `macro_features` tables |
| `trade_log.db` | Trade history with PnL, regime, outcomes |
| `strategy_lab.db` | Strategy lab backtest results + leaderboard |

All database and log paths are hardcoded to `/home/theplummer92/` in most files. `godmode.py` allows overriding via env vars (e.g. `DB_PATH`, `REGIME_SNAPSHOT_PATH`); the other scripts do not.

### Broker Abstraction

`sniper_bot.py` supports two brokers via the `BROKER` env var:
- `BROKER=alpaca` (default) — uses Alpaca SDK directly
- `BROKER=ibkr` — uses `ibkr_client.py`, a drop-in wrapper around IBKR Client Portal REST API (runs locally at `https://localhost:5000`)

`ibkr_client.py` mirrors Alpaca's `TradingClient` interface (`get_all_positions`, `get_account`, `get_clock`, `submit_order`, `close_position`).

`paper_sniper.py` always uses Alpaca paper account regardless of `BROKER`.

### Symbol Format Conventions

- **Alpaca crypto**: `BTC/USD`
- **Yahoo Finance crypto**: `BTC-USD`
- Sniper converts Yahoo → Alpaca format by replacing `-` with `/`
- Futures (`CL=F`, `GC=F`) and indices (`^VIX`, `^TNX`) are yahoo-only; sniper skips any symbol containing `=`

### Signal Types

Signals in `wolfe_signals.db` follow these key types:
- `STRONG BUY FLOW` / `STRONG SELL FLOW` — momentum signals (rvol > 2.5, flow > $5M, price direction agrees)
- `ABSORPTION WALL` / `ABSORPTION BUY` / `ABSORPTION SELL` — high RVOL + tiny price move + big flow (reversal setup)
- `CLIMAX` — volume spike exhaustion (rvol > 8.0)
- `BULL TRAP` / `BEAR TRAP` — flow and price direction disagree
- `FAKE-OUT (Low Vol)` — big price move on low volume

**Sniper only acts on** `STRONG%` and `ABSORPTION%` signals (see `get_new_signals` query). CLIMAX, traps, and fake-outs are logged but not traded.

### Regime Filter (`regime_snapshot.json`)

Written by `godmode.py` every scan. Contains: `vix`, `tnx`, `dxy`, `regime` (string), `timestamp`.

`sniper_bot.py` derives trading mode from VIX directly:
- VIX < 25 → `OPEN` (all trades allowed)
- VIX 25–29 → `SELL_ONLY` (no new longs)
- VIX >= 30 → `BLOCKED` (no new trades)

`godmode.py` also applies a SPY 20-bar MA trend filter: BUY signals are suppressed when SPY price is below its 20-bar MA.

### Approved Symbols (`approved_symbols.json`)

Live trading whitelist with separate buy/sell lists:
```json
{ "buy": [...], "sell": [...] }
```
Sniper reads both lists independently — buy-approved for longs, sell-approved for shorts. The file also contains legacy `approved` and `short` keys for backward compatibility.

Updated by:
- `roster_manager.py` — pulls top performers from `strategy_lab.db` (min 70% win rate, min 20 trades, min score 100); `SPY`, `IWM`, `QQQ` are always included
- `Symbol_hunter.py` — scans SP500 universe using 60-day history, min 60% win rate

### Sniper Bot Risk Parameters

Defined at the top of `sniper_bot.py`:
- `TRADE_NOTIONAL_USD = 10` — per-trade size (live); `$50` in paper_sniper.py
- `HARD_STOP_LOSS_PCT = 0.02` — 2% stop loss
- `TAKE_PROFIT_PCT = 0.04` — 4% take profit
- `DAILY_LOSS_LIMIT_USD = 5.00` — kills trading for the day
- `MAX_OPEN_POSITIONS = 5`

Sniper uses a lockfile at `/tmp/sniper_bot.lock` to prevent duplicate instances. If the file exists but the PID is dead, it takes over automatically.

`unrealized_plpc` from Alpaca is already sign-correct for both LONG and SHORT positions — no manual inversion needed.

## Environment Variables (`.env`)

```
APCA_API_KEY_ID=          # Live account
APCA_API_SECRET_KEY=
APCA_PAPER_KEY_ID=        # Paper account (paper_sniper.py)
APCA_PAPER_SECRET_KEY=
APCA_PAPER_BASE_URL=https://paper-api.alpaca.markets
DISCORD_WEBHOOK_URL=
BROKER=alpaca             # or ibkr
IBKR_ACCOUNT_ID=          # if BROKER=ibkr
IBKR_GATEWAY_URL=https://localhost:5000/v1/api
```

`godmode.py` also reads: `GODMODE_SLEEP_SECONDS` (default 300), `MIN_BARS` (default 50), `ABS_RVOL_MIN`, `ABS_MOVE_MAX`, `ABS_FLOW_MIN_M`, `RESOLVE_UP_PCT`, `RESOLVE_DN_PCT`, `RESOLVE_WINDOW_SCANS`.
