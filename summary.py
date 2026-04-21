#!/usr/bin/env python3
"""summary.py — Print all-time trading performance summary."""
import sqlite3
from datetime import datetime
from app_paths import DATA_DIR

DB_PATH = str(DATA_DIR / "trade_log.db")


def fetch_all_closed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, direction, entry_price, exit_price,
               pnl_usd, pnl_pct, outcome, signal_type, entry_time, exit_time
        FROM trades
        WHERE outcome != 'open'
        ORDER BY exit_time ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def fetch_open():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, direction, entry_price, entry_time, signal_type
        FROM trades
        WHERE outcome = 'open'
        ORDER BY entry_time ASC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def hr(char="-", width=60):
    print(char * width)


def main():
    closed   = fetch_all_closed()
    open_pos = fetch_open()

    hr("=")
    print("  WOLFE GOD-MODE — ALL-TIME PERFORMANCE SUMMARY")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    hr("=")

    if not closed:
        print("\n  No closed trades on record.\n")
    else:
        pnls     = [r[5] for r in closed if r[5] is not None]
        total    = sum(pnls)
        wins     = [p for p in pnls if p > 0]
        losses   = [p for p in pnls if p <= 0]
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win  = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        best     = max(closed, key=lambda r: r[5] if r[5] else -9999)
        worst    = min(closed, key=lambda r: r[5] if r[5] else 9999)

        print(f"\n  Closed Trades :  {len(closed)}")
        print(f"  Total P&L     :  ${total:+.2f}")
        print(f"  Win Rate      :  {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
        print(f"  Avg Win       :  ${avg_win:+.2f}")
        print(f"  Avg Loss      :  ${avg_loss:+.2f}")
        if wins and losses:
            rr = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            print(f"  Reward/Risk   :  {rr:.2f}x")

        print(f"\n  Best Trade    :  {best[1]} {best[2]}  ${best[5]:+.2f}  ({best[9][:10]})")
        print(f"  Worst Trade   :  {worst[1]} {worst[2]}  ${worst[5]:+.2f}  ({worst[9][:10]})")

        # Outcome breakdown
        outcomes = {}
        for r in closed:
            outcomes[r[7]] = outcomes.get(r[7], 0) + 1
        print(f"\n  Outcomes:")
        for k, v in sorted(outcomes.items(), key=lambda x: -x[1]):
            print(f"    {k:<20} {v}")

        # All closed trades table
        hr()
        print(f"  {'#':<4} {'Symbol':<6} {'Dir':<6} {'Entry':>8} {'Exit':>8} {'P&L':>8} {'%':>7}  {'Outcome':<14} {'Date'}")
        hr()
        for r in closed:
            id_, sym, direction, entry, exit_p, pnl_usd, pnl_pct, outcome, sig, entry_time, exit_time = r
            exit_str = f"${exit_p:.2f}" if exit_p else "  N/A"
            pnl_str  = f"${pnl_usd:+.2f}" if pnl_usd is not None else "   N/A"
            pct_str  = f"{pnl_pct*100:+.1f}%" if pnl_pct is not None else "    N/A"
            date_str = (exit_time or entry_time or "")[:10]
            print(f"  {id_:<4} {sym:<6} {direction:<6} ${entry:>7.2f} {exit_str:>8} {pnl_str:>8} {pct_str:>7}  {outcome:<14} {date_str}")

    # Open positions
    hr()
    print(f"\n  Open Positions: {len(open_pos)}")
    if open_pos:
        hr("-", 60)
        for sym, direction, entry, entry_time, sig in open_pos:
            print(f"  {sym:<6} {direction:<6} @ ${entry:.2f}  since {entry_time[:16]}  |  {sig}")
    else:
        print("  None")

    hr("=")


if __name__ == "__main__":
    main()
