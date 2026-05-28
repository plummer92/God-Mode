#!/usr/bin/env python3
"""Research backtest over labeled God Mode signal outcomes."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from app_paths import DATA_DIR
from audit_report import clean_label


DB_PATH = DATA_DIR / "wolfe_signals.db"
DEFAULT_SINCE = "2026-05-24 14:52:00"
DEFAULT_MIN_SAMPLE = 10
DEFAULT_SLIPPAGE_BPS = 2.0
DEFAULT_SPREAD_BPS = 3.0


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def cost_pct(slippage_bps: float, spread_bps: float) -> float:
    # slippage_bps is per side. return_pct is already in percentage points.
    return ((2.0 * slippage_bps) + spread_bps) / 100.0


def net_expr(cost: float) -> str:
    return f"(o.return_pct - {cost:.8f})"


def fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, params))


def render_rows(rows: list[sqlite3.Row], columns: list[tuple[str, str]], limit: int | None = None) -> list[str]:
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        return ["  None yet"]
    lines = []
    for row in rows:
        parts = []
        for key, label in columns:
            value = row[key]
            if key == "signal_type":
                value = clean_label(value)
            parts.append(f"{label}={value}")
        lines.append("  " + " | ".join(parts))
    return lines


def build_report(
    *,
    since: str,
    min_sample: int,
    slippage_bps: float,
    spread_bps: float,
) -> str:
    if not Path(DB_PATH).exists():
        return f"Signal DB not found: {DB_PATH}"

    cost = cost_pct(slippage_bps, spread_bps)
    net = net_expr(cost)
    with connect() as conn:
        signal_count = conn.execute(
            "SELECT COUNT(1) FROM signals WHERE timestamp >= ?",
            (since,),
        ).fetchone()[0]
        outcome_count = conn.execute(
            """
            SELECT COUNT(1)
            FROM signal_outcomes
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            """,
            (since,),
        ).fetchone()[0]
        signal_range = conn.execute(
            """
            SELECT MIN(signal_ts), MAX(signal_ts)
            FROM signal_outcomes
            WHERE signal_ts >= ?
            """,
            (since,),
        ).fetchone()
        split_row = conn.execute(
            """
            SELECT signal_ts
            FROM signal_outcomes
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            ORDER BY signal_ts
            LIMIT 1 OFFSET CAST(? * 0.7 AS INT)
            """,
            (since, outcome_count),
        ).fetchone()
        split_ts = split_row[0] if split_row else None

        horizon_rows = fetch_rows(
            conn,
            f"""
            SELECT horizon,
                   COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN o.return_pct > 0 THEN 1.0 ELSE 0.0 END), 1) gross_win_pct,
                   ROUND(AVG(o.return_pct), 4) gross_avg_pct,
                   ROUND(100.0 * AVG(CASE WHEN {net} > 0 THEN 1.0 ELSE 0.0 END), 1) net_win_pct,
                   ROUND(AVG({net}), 4) net_avg_pct
            FROM signal_outcomes o
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            GROUP BY horizon
            ORDER BY CASE horizon
                WHEN '5m' THEN 1 WHEN '15m' THEN 2 WHEN '30m' THEN 3
                WHEN '1h' THEN 4 WHEN '1d' THEN 5 ELSE 9 END
            """,
            (since,),
        )

        best_rows = fetch_rows(
            conn,
            f"""
            SELECT horizon, signal_type, direction,
                   COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN {net} > 0 THEN 1.0 ELSE 0.0 END), 1) net_win_pct,
                   ROUND(AVG({net}), 4) net_avg_pct,
                   ROUND(AVG(o.return_pct), 4) gross_avg_pct
            FROM signal_outcomes o
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY net_avg_pct DESC
            LIMIT 12
            """,
            (since, min_sample),
        )

        worst_rows = fetch_rows(
            conn,
            f"""
            SELECT horizon, signal_type, direction,
                   COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN {net} > 0 THEN 1.0 ELSE 0.0 END), 1) net_win_pct,
                   ROUND(AVG({net}), 4) net_avg_pct,
                   ROUND(AVG(o.return_pct), 4) gross_avg_pct
            FROM signal_outcomes o
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY net_avg_pct ASC
            LIMIT 12
            """,
            (since, min_sample),
        )

        symbol_rows = fetch_rows(
            conn,
            f"""
            SELECT horizon, symbol, direction,
                   COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN {net} > 0 THEN 1.0 ELSE 0.0 END), 1) net_win_pct,
                   ROUND(AVG({net}), 4) net_avg_pct
            FROM signal_outcomes o
            WHERE signal_ts >= ?
              AND outcome IN ('WIN', 'LOSS')
              AND return_pct IS NOT NULL
            GROUP BY horizon, symbol, direction
            HAVING n >= ?
            ORDER BY net_avg_pct DESC
            LIMIT 12
            """,
            (since, min_sample),
        )

        split_rows: list[sqlite3.Row] = []
        if split_ts:
            split_rows = fetch_rows(
                conn,
                f"""
                SELECT CASE WHEN signal_ts < ? THEN 'train' ELSE 'test' END split,
                       horizon,
                       COUNT(1) n,
                       ROUND(100.0 * AVG(CASE WHEN {net} > 0 THEN 1.0 ELSE 0.0 END), 1) net_win_pct,
                       ROUND(AVG({net}), 4) net_avg_pct
                FROM signal_outcomes o
                WHERE signal_ts >= ?
                  AND outcome IN ('WIN', 'LOSS')
                  AND return_pct IS NOT NULL
                GROUP BY split, horizon
                ORDER BY split, CASE horizon
                    WHEN '5m' THEN 1 WHEN '15m' THEN 2 WHEN '30m' THEN 3
                    WHEN '1h' THEN 4 WHEN '1d' THEN 5 ELSE 9 END
                """,
                (split_ts, since),
            )

    lines = [
        "WOLFE SIGNAL RESEARCH BACKTEST",
        f"Since: {since}",
        f"Signal window: {signal_range[0]} -> {signal_range[1]}",
        f"Signals: {signal_count}",
        f"Tradable labeled outcomes: {outcome_count}",
        f"Costs: slippage={slippage_bps:g} bps/side, spread={spread_bps:g} bps, total={cost:.4f}%",
        "",
        "Net Results By Horizon",
        *render_rows(
            horizon_rows,
            [
                ("horizon", "h"),
                ("n", "n"),
                ("gross_win_pct", "gross_win%"),
                ("gross_avg_pct", "gross_avg%"),
                ("net_win_pct", "net_win%"),
                ("net_avg_pct", "net_avg%"),
            ],
        ),
        "",
        "Best Net Signal Buckets",
        *render_rows(
            best_rows,
            [
                ("horizon", "h"),
                ("signal_type", "signal"),
                ("direction", "dir"),
                ("n", "n"),
                ("net_win_pct", "net_win%"),
                ("net_avg_pct", "net_avg%"),
                ("gross_avg_pct", "gross_avg%"),
            ],
        ),
        "",
        "Worst Net Signal Buckets",
        *render_rows(
            worst_rows,
            [
                ("horizon", "h"),
                ("signal_type", "signal"),
                ("direction", "dir"),
                ("n", "n"),
                ("net_win_pct", "net_win%"),
                ("net_avg_pct", "net_avg%"),
                ("gross_avg_pct", "gross_avg%"),
            ],
        ),
        "",
        "Best Net Symbol Buckets",
        *render_rows(
            symbol_rows,
            [
                ("horizon", "h"),
                ("symbol", "sym"),
                ("direction", "dir"),
                ("n", "n"),
                ("net_win_pct", "net_win%"),
                ("net_avg_pct", "net_avg%"),
            ],
        ),
        "",
        "Walk-Forward Sanity Split",
        f"  split_ts={split_ts or 'not enough data'}",
        *render_rows(
            split_rows,
            [
                ("split", "split"),
                ("horizon", "h"),
                ("n", "n"),
                ("net_win_pct", "net_win%"),
                ("net_avg_pct", "net_avg%"),
            ],
        ),
        "",
        "Read This As",
        "  Research only. Small samples can flip quickly.",
        "  A bucket needs stable net edge across the test split before it deserves automation.",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--min-sample", type=int, default=DEFAULT_MIN_SAMPLE)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--spread-bps", type=float, default=DEFAULT_SPREAD_BPS)
    args = parser.parse_args()
    print(
        build_report(
            since=args.since,
            min_sample=max(1, args.min_sample),
            slippage_bps=max(0.0, args.slippage_bps),
            spread_bps=max(0.0, args.spread_bps),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
