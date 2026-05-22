#!/usr/bin/env python3
"""Summarize what the market audit database has found so far."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from app_paths import DATA_DIR, ENV_FILE

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


SIGNALS_DB = DATA_DIR / "wolfe_signals.db"
MIN_SAMPLE = 50


def load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(ENV_FILE)


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SIGNALS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def clean_label(value: str | None) -> str:
    if not value:
        return "UNKNOWN"
    text = str(value)
    replacements = {
        "\U0001f6e1\ufe0f ": "",
        "\U0001f525 ": "",
        "\u2b50\u2b50\u2b50 ": "",
        "\u26a0\ufe0f ": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def table(rows: list[sqlite3.Row], columns: list[tuple[str, str]], limit: int | None = None) -> list[str]:
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        return ["  None yet"]
    rendered = []
    for row in rows:
        parts = []
        for key, label in columns:
            value = row[key]
            if key == "signal_type":
                value = clean_label(value)
            parts.append(f"{label}={value}")
        rendered.append("  " + " | ".join(parts))
    return rendered


def scalar(cur: sqlite3.Cursor, sql: str, default=None):
    row = cur.execute(sql).fetchone()
    if row is None:
        return default
    return row[0]


def fetch_rows(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return list(cur.execute(sql, params))


def table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def build_report(min_sample: int = MIN_SAMPLE) -> str:
    if not Path(SIGNALS_DB).exists():
        return f"Audit DB not found: {SIGNALS_DB}"

    with connect() as conn:
        cur = conn.cursor()
        if not table_exists(cur, "signals") or not table_exists(cur, "signal_outcomes"):
            return f"Audit DB exists but is missing required tables: {SIGNALS_DB}"
        signal_count = scalar(cur, "SELECT COUNT(1) FROM signals", 0)
        outcome_count = scalar(cur, "SELECT COUNT(1) FROM signal_outcomes", 0)
        signal_range = cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM signals").fetchone()
        recent_24h = scalar(cur, "SELECT COUNT(1) FROM signals WHERE timestamp >= datetime('now','-1 day')", 0)

        horizon_rows = fetch_rows(
            cur,
            """
            SELECT horizon, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
            GROUP BY horizon
            ORDER BY CASE horizon
                WHEN '5m' THEN 1 WHEN '15m' THEN 2 WHEN '30m' THEN 3
                WHEN '1h' THEN 4 WHEN '1d' THEN 5 ELSE 9 END
            """,
        )

        signal_combo_rows = fetch_rows(
            cur,
            """
            SELECT horizon, signal_type, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct DESC
            """,
            (min_sample,),
        )

        worst_combo_rows = fetch_rows(
            cur,
            """
            SELECT horizon, signal_type, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct ASC
            """,
            (min_sample,),
        )

        symbol_best_rows = fetch_rows(
            cur,
            """
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
            GROUP BY symbol, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct DESC
            LIMIT 10
            """,
            (min_sample,),
        )

        symbol_worst_rows = fetch_rows(
            cur,
            """
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
            GROUP BY symbol, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct ASC
            LIMIT 10
            """,
            (min_sample,),
        )

        no_data_rows = fetch_rows(
            cur,
            """
            SELECT horizon,
                   COUNT(1) total,
                   SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) no_data,
                   ROUND(100.0 * SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) / COUNT(1), 1) no_data_pct
            FROM signal_outcomes
            GROUP BY horizon
            ORDER BY horizon
            """,
        )

    lines = [
        "WOLFE MARKET AUDIT REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Dataset",
        f"  Signals retained: {signal_count:,}",
        f"  Labeled outcomes: {outcome_count:,}",
        f"  Signal window: {signal_range[0] if signal_range else 'N/A'} -> {signal_range[1] if signal_range else 'N/A'}",
        f"  Signals last 24h: {recent_24h:,}",
        "",
        "Overall Edge By Horizon",
    ]
    lines.extend(table(horizon_rows, [("horizon", "horizon"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Best Signal + Horizon Combos"])
    lines.extend(
        table(
            signal_combo_rows,
            [("horizon", "h"), ("signal_type", "signal"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")],
            limit=10,
        )
    )

    lines.extend(["", "Worst Signal + Horizon Combos"])
    lines.extend(
        table(
            worst_combo_rows,
            [("horizon", "h"), ("signal_type", "signal"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")],
            limit=10,
        )
    )

    lines.extend(["", "Best 1-Day Symbol Tendencies"])
    lines.extend(table(symbol_best_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Worst 1-Day Symbol Tendencies"])
    lines.extend(table(symbol_worst_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Data Quality"])
    lines.extend(table(no_data_rows, [("horizon", "h"), ("total", "total"), ("no_data", "no_data"), ("no_data_pct", "no_data%")]))

    lines.extend(
        [
            "",
            "Read This As",
            "  This is research, not a trade signal.",
            "  Short-term edges under ~0.05% are probably not tradable after spread/slippage.",
            "  The useful thing is the ranking: which patterns deserve deeper study and which should be ignored.",
        ]
    )
    return "\n".join(lines)


def post_to_discord(message: str) -> None:
    import requests

    webhook = os.getenv("DISCORD_WEBHOOK", "").strip() or os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("DISCORD_WEBHOOK is not set")
    response = requests.post(webhook, json={"content": f"```text\n{message[:1800]}\n```"}, timeout=10)
    response.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-sample", type=int, default=MIN_SAMPLE)
    parser.add_argument("--discord", action="store_true", help="Post report to Discord")
    args = parser.parse_args()

    load_env()
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    report = build_report(max(1, args.min_sample))
    print(report)
    if args.discord:
        post_to_discord(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
