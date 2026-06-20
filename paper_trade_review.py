#!/usr/bin/env python3
"""Build a weekly paper-trade review from paper_sniper_state.db."""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from reporting import PAPER_STATE_DB_PATH, post_to_discord


ET = ZoneInfo("America/New_York")
DEFAULT_DAYS = 7
DISCORD_CHUNK_SIZE = 1800


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(PAPER_STATE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_money(value) -> str:
    return f"${float(value or 0):+.2f}"


def _fmt_num(value, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}"


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _since_from_days(days: int) -> str:
    since = datetime.now(ET) - timedelta(days=max(1, days))
    return since.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")


def _entry_for_exit(conn: sqlite3.Connection, symbol: str, exit_at: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT created_at, signal_ts, signal_type, direction, earnings_window,
               time_session, price, rvol
        FROM paper_signal_events
        WHERE action = 'ENTERED'
          AND symbol = ?
          AND created_at <= ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (symbol, exit_at),
    ).fetchone()


def _add_group(groups: dict[tuple, list], key: tuple, pnl: float) -> None:
    row = groups.setdefault(key, [0, 0.0, 0, 0])
    row[0] += 1
    row[1] += pnl
    if pnl > 0:
        row[2] += 1
    elif pnl < 0:
        row[3] += 1


def _group_lines(title: str, groups: dict[tuple, list], limit: int = 10) -> list[str]:
    lines = ["", title]
    if not groups:
        lines.append("  None")
        return lines
    sorted_items = sorted(groups.items(), key=lambda item: item[1][1])
    for key, (count, pnl, wins, losses) in sorted_items[:limit]:
        label = " | ".join(str(part) for part in key)
        lines.append(f"  {label} | n={count} | P&L={_fmt_money(pnl)} | {wins}W/{losses}L")
    if len(sorted_items) > limit:
        lines.append("  ...")
    return lines


def _message_chunks(message: str, size: int = DISCORD_CHUNK_SIZE) -> list[str]:
    if len(message) <= size:
        return [message]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in message.splitlines():
        next_len = len(line) + 1
        if current and current_len + next_len > size:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        if next_len > size:
            chunks.append(line[:size])
            continue
        current.append(line)
        current_len += next_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def post_report_to_discord(report: str) -> None:
    chunks = _message_chunks(report)
    if len(chunks) == 1:
        post_to_discord(f"```text\n{chunks[0]}\n```")
        return
    for idx, chunk in enumerate(chunks, start=1):
        post_to_discord(f"```text\n{chunk}\n\npart {idx}/{len(chunks)}\n```")


def build_report(days: int = DEFAULT_DAYS, since: str | None = None, trade_limit: int = 30) -> str:
    since = since or _since_from_days(days)
    if not PAPER_STATE_DB_PATH.exists():
        return f"Paper Trade Review\nNo paper state DB found at {PAPER_STATE_DB_PATH}"

    conn = _connect()
    try:
        if not _table_exists(conn, "exit_events") or not _table_exists(conn, "paper_signal_events"):
            return "Paper Trade Review\nPaper event tables are not initialized yet."

        exits = conn.execute(
            """
            SELECT id, created_at, symbol, exit_reason, actual_fill_price, pnl_usd
            FROM exit_events
            WHERE created_at >= ?
              AND pnl_usd IS NOT NULL
              AND verification_result LIKE 'closed:%'
            ORDER BY created_at ASC, id ASC
            """,
            (since,),
        ).fetchall()

        entered = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM paper_signal_events
            WHERE created_at >= ? AND action = 'ENTERED'
            """,
            (since,),
        ).fetchone()["n"]
        blocked = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM paper_signal_events
            WHERE created_at >= ? AND action = 'BLOCKED'
            """,
            (since,),
        ).fetchone()["n"]

        total_pnl = sum(float(row["pnl_usd"] or 0) for row in exits)
        wins = sum(1 for row in exits if float(row["pnl_usd"] or 0) > 0)
        losses = sum(1 for row in exits if float(row["pnl_usd"] or 0) < 0)
        win_rate = (wins / len(exits) * 100.0) if exits else 0.0

        by_bucket: dict[tuple, list] = {}
        by_context: dict[tuple, list] = {}
        by_symbol: dict[tuple, list] = {}
        trade_lines: list[str] = []

        for row in exits:
            pnl = float(row["pnl_usd"] or 0)
            entry = _entry_for_exit(conn, row["symbol"], row["created_at"])
            if entry is None:
                bucket = ("NO_ENTRY", "")
                context = ("NO_ENTRY", "", "", "")
                signal_type = "NO_ENTRY"
                direction = ""
                earnings = "UNKNOWN"
                session = "UNKNOWN"
                entry_price = None
                rvol = None
            else:
                signal_type = str(entry["signal_type"] or "UNKNOWN")
                direction = str(entry["direction"] or "UNKNOWN")
                earnings = str(entry["earnings_window"] or "UNKNOWN")
                session = str(entry["time_session"] or "UNKNOWN")
                entry_price = entry["price"]
                rvol = entry["rvol"]
                bucket = (signal_type, direction)
                context = (signal_type, direction, earnings, session)

            _add_group(by_bucket, bucket, pnl)
            _add_group(by_context, context, pnl)
            _add_group(by_symbol, (str(row["symbol"]),), pnl)

            trade_lines.append(
                "  "
                f"{row['created_at'][:16]} | {row['symbol']} {direction} | "
                f"{signal_type} | {earnings}/{session} | "
                f"entry={_fmt_num(entry_price)} rvol={_fmt_num(rvol)} | "
                f"{row['exit_reason']} | {_fmt_money(pnl)}"
            )

        lines = [
            "PAPER TRADE REVIEW",
            f"Window since: {since} UTC",
            f"Signals entered: {int(entered)} | blocked: {int(blocked)}",
            (
                f"Closed trades: {len(exits)} | P&L: {_fmt_money(total_pnl)} | "
                f"Win rate: {win_rate:.1f}% ({wins}W/{losses}L)"
            ),
        ]

        lines.extend(_group_lines("Worst Context Buckets", by_context, limit=8))
        lines.extend(_group_lines("Worst Symbols", by_symbol, limit=8))
        lines.extend(_group_lines("Signal Buckets", by_bucket, limit=8))

        lines.append("")
        lines.append(f"Trades ({min(len(trade_lines), trade_limit)}/{len(trade_lines)})")
        lines.extend(trade_lines[:trade_limit] or ["  None"])
        if len(trade_lines) > trade_limit:
            lines.append("  ...")

        lines.append("")
        lines.append("Read This As")
        if total_pnl < 0:
            lines.append("  Negative P&L means the paper execution rules need tightening, not that every signal is useless.")
        else:
            lines.append("  Positive P&L is encouraging, but this is still a small paper sample.")
        lines.append("  Buckets with repeated losses should be blocked, cooled down, or tested with a different holding period.")
        lines.append("  Buckets with audit edge but paper losses may need different TP/SL or longer holds.")
        return "\n".join(lines)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a weekly paper-trade review")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--since", default=None, help="UTC timestamp, e.g. 2026-06-13 00:00:00")
    parser.add_argument("--trade-limit", type=int, default=30)
    parser.add_argument("--discord", action="store_true")
    args = parser.parse_args()

    report = build_report(days=args.days, since=args.since, trade_limit=max(1, args.trade_limit))
    print(report)
    if args.discord:
        post_report_to_discord(report)


if __name__ == "__main__":
    main()
