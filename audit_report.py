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
DEFAULT_SINCE = "2026-05-24 14:52:00"
MIN_SAMPLE = 50
TRUST_MIN_WIN_RATE = 55.0
TRUST_MIN_AVG_EDGE = 0.05
AVOID_MAX_AVG_EDGE = -0.05


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


def data_quality_grade(no_data_rows: list[sqlite3.Row]) -> tuple[str, str]:
    if not no_data_rows:
        return "UNKNOWN", "no outcome quality rows yet"
    no_data_pcts = [float(row["no_data_pct"] or 0.0) for row in no_data_rows]
    avg_no_data = sum(no_data_pcts) / len(no_data_pcts)
    intraday_no_data = [
        float(row["no_data_pct"] or 0.0)
        for row in no_data_rows
        if row["horizon"] in {"5m", "15m", "30m", "1h"}
    ]
    max_intraday = max(intraday_no_data) if intraday_no_data else avg_no_data
    if avg_no_data <= 20 and max_intraday <= 30:
        return "A", f"avg no-data {avg_no_data:.1f}%"
    if avg_no_data <= 35 and max_intraday <= 45:
        return "B", f"avg no-data {avg_no_data:.1f}%"
    if avg_no_data <= 50 and max_intraday <= 65:
        return "C", f"avg no-data {avg_no_data:.1f}%; short horizons are noisy"
    return "D", f"avg no-data {avg_no_data:.1f}%; treat rankings as directional, not final"


def scalar(cur: sqlite3.Cursor, sql: str, default=None, params: tuple = ()):
    row = cur.execute(sql, params).fetchone()
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


def build_report(min_sample: int = MIN_SAMPLE, since: str | None = None) -> str:
    if not Path(SIGNALS_DB).exists():
        return f"Audit DB not found: {SIGNALS_DB}"

    signal_filter = "WHERE timestamp >= ?" if since else ""
    outcome_since = "AND signal_ts >= ?" if since else ""
    outcome_where_since = "WHERE signal_ts >= ?" if since else ""
    signal_params = (since,) if since else ()
    outcome_params = (since,) if since else ()

    with connect() as conn:
        cur = conn.cursor()
        if not table_exists(cur, "signals") or not table_exists(cur, "signal_outcomes"):
            return f"Audit DB exists but is missing required tables: {SIGNALS_DB}"
        signal_count = scalar(cur, f"SELECT COUNT(1) FROM signals {signal_filter}", 0, signal_params)
        outcome_count = scalar(cur, f"SELECT COUNT(1) FROM signal_outcomes {outcome_where_since}", 0, outcome_params)
        signal_range = cur.execute(
            f"SELECT MIN(timestamp), MAX(timestamp) FROM signals {signal_filter}",
            signal_params,
        ).fetchone()
        recent_24h = scalar(cur, "SELECT COUNT(1) FROM signals WHERE timestamp >= datetime('now','-1 day')", 0)

        horizon_rows = fetch_rows(
            cur,
            f"""
            SELECT horizon, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY horizon
            ORDER BY CASE horizon
                WHEN '5m' THEN 1 WHEN '15m' THEN 2 WHEN '30m' THEN 3
                WHEN '1h' THEN 4 WHEN '1d' THEN 5 ELSE 9 END
            """,
            outcome_params,
        )

        signal_combo_rows = fetch_rows(
            cur,
            f"""
            SELECT horizon, signal_type, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct DESC
            """,
            (*outcome_params, min_sample),
        )

        worst_combo_rows = fetch_rows(
            cur,
            f"""
            SELECT horizon, signal_type, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY horizon, signal_type, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct ASC
            """,
            (*outcome_params, min_sample),
        )

        symbol_best_rows = fetch_rows(
            cur,
            f"""
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY symbol, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct DESC
            LIMIT 10
            """,
            (*outcome_params, min_sample),
        )

        symbol_worst_rows = fetch_rows(
            cur,
            f"""
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY symbol, direction
            HAVING n >= ?
            ORDER BY avg_edge_pct ASC
            LIMIT 10
            """,
            (*outcome_params, min_sample),
        )

        no_data_rows = fetch_rows(
            cur,
            f"""
            SELECT horizon,
                   COUNT(1) total,
                   SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) no_data,
                   ROUND(100.0 * SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) / COUNT(1), 1) no_data_pct
            FROM signal_outcomes
            {outcome_where_since}
            GROUP BY horizon
            ORDER BY horizon
            """,
            outcome_params,
        )

        source_quality_rows = fetch_rows(
            cur,
            f"""
            SELECT COALESCE(source, 'UNKNOWN') source,
                   COUNT(1) total,
                   SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) no_data,
                   ROUND(100.0 * SUM(CASE WHEN outcome='NO_DATA' THEN 1 ELSE 0 END) / COUNT(1), 1) no_data_pct,
                   SUM(CASE WHEN outcome IN ('WIN','LOSS','FLAT') THEN 1 ELSE 0 END) labeled
            FROM signal_outcomes
            {outcome_where_since}
            GROUP BY COALESCE(source, 'UNKNOWN')
            ORDER BY total DESC
            LIMIT 10
            """,
            outcome_params,
        )

        session_rows = fetch_rows(
            cur,
            f"""
            SELECT o.horizon, COALESCE(s.time_session, 'UNKNOWN') time_session,
                   COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN o.outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(o.return_pct), 4) avg_edge_pct
            FROM signal_outcomes o
            JOIN signals s ON s.rowid = o.signal_rowid
            WHERE o.outcome IN ('WIN','LOSS')
              AND o.return_pct IS NOT NULL
              AND o.horizon IN ('15m','1h','1d')
              {"AND o.signal_ts >= ?" if since else ""}
            GROUP BY o.horizon, COALESCE(s.time_session, 'UNKNOWN')
            HAVING n >= ?
            ORDER BY o.horizon, avg_edge_pct DESC
            """,
            (*outcome_params, min_sample),
        )

        recent_combo_rows = fetch_rows(
            cur,
            f"""
            SELECT o.horizon, o.signal_type, o.direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN o.outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(o.return_pct), 4) avg_edge_pct
            FROM signal_outcomes o
            WHERE o.outcome IN ('WIN','LOSS')
              AND o.return_pct IS NOT NULL
              AND o.signal_ts >= datetime('now', '-7 days')
              {"AND o.signal_ts >= ?" if since else ""}
            GROUP BY o.horizon, o.signal_type, o.direction
            HAVING n >= ?
            ORDER BY avg_edge_pct DESC
            """,
            (*outcome_params, max(10, min_sample // 2)),
        )

        trusted_symbol_rows = fetch_rows(
            cur,
            f"""
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY symbol, direction
            HAVING n >= ? AND win_rate >= ? AND avg_edge_pct >= ?
            ORDER BY avg_edge_pct DESC
            LIMIT 12
            """,
            (*outcome_params, min_sample, TRUST_MIN_WIN_RATE, TRUST_MIN_AVG_EDGE),
        )

        avoid_symbol_rows = fetch_rows(
            cur,
            f"""
            SELECT symbol, direction, COUNT(1) n,
                   ROUND(100.0 * AVG(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), 1) win_rate,
                   ROUND(AVG(return_pct), 4) avg_edge_pct
            FROM signal_outcomes
            WHERE horizon='1d' AND outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
              {outcome_since}
            GROUP BY symbol, direction
            HAVING n >= ? AND avg_edge_pct <= ?
            ORDER BY avg_edge_pct ASC
            LIMIT 12
            """,
            (*outcome_params, min_sample, AVOID_MAX_AVG_EDGE),
        )

    grade, grade_reason = data_quality_grade(no_data_rows)
    lines = [
        "WOLFE MARKET AUDIT REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Dataset",
        f"  Scope: {'post-fix signals since ' + since if since else 'full retained database'}",
        f"  Signals retained: {signal_count:,}",
        f"  Labeled outcomes: {outcome_count:,}",
        f"  Signal window: {signal_range[0] if signal_range else 'N/A'} -> {signal_range[1] if signal_range else 'N/A'}",
        f"  Signals last 24h: {recent_24h:,}",
        f"  Data quality grade: {grade} ({grade_reason})",
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

    lines.extend(["", "Recent 7-Day Leaders"])
    lines.extend(
        table(
            recent_combo_rows,
            [("horizon", "h"), ("signal_type", "signal"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")],
            limit=8,
        )
    )

    lines.extend(["", "Edge By Time Session"])
    lines.extend(
        table(
            session_rows,
            [("horizon", "h"), ("time_session", "session"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")],
            limit=12,
        )
    )

    lines.extend(["", "Trusted 1-Day Symbol Buckets"])
    lines.extend(table(trusted_symbol_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Avoid 1-Day Symbol Buckets"])
    lines.extend(table(avoid_symbol_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Best 1-Day Symbol Tendencies"])
    lines.extend(table(symbol_best_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Worst 1-Day Symbol Tendencies"])
    lines.extend(table(symbol_worst_rows, [("symbol", "sym"), ("direction", "dir"), ("n", "n"), ("win_rate", "win%"), ("avg_edge_pct", "avg%")]))

    lines.extend(["", "Data Quality"])
    lines.extend(table(no_data_rows, [("horizon", "h"), ("total", "total"), ("no_data", "no_data"), ("no_data_pct", "no_data%")]))

    lines.extend(["", "Label Source Quality"])
    lines.extend(
        table(
            source_quality_rows,
            [("source", "source"), ("total", "total"), ("labeled", "labeled"), ("no_data", "no_data"), ("no_data_pct", "no_data%")],
        )
    )

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
    parser.add_argument("--since", default=None, help="Only include signals/outcomes on or after this UTC timestamp")
    parser.add_argument("--discord", action="store_true", help="Post report to Discord")
    args = parser.parse_args()

    load_env()
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    report = build_report(max(1, args.min_sample), since=args.since)
    print(report)
    if args.discord:
        post_to_discord(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
