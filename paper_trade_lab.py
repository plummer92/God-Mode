#!/usr/bin/env python3
"""Read-only paper trade and guardrail lab.

This script inspects paper_sniper_state.db and wolfe_signals.db without writing
to either database. It is meant for research, not trade execution.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from app_paths import DATA_DIR
from reporting import PAPER_STATE_DB_PATH


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SIGNALS_DB_PATH = DATA_DIR / "wolfe_signals.db"
DEFAULT_DAYS = 7
DEFAULT_HORIZON = "1h"
DEFAULT_NOTIONAL = 500.0
FLAT_BAND_PCT = 0.05
HORIZONS = ("5m", "15m", "30m", "1h", "1d")
EVENT_HORIZONS = HORIZONS


def _sqlite_ro_uri(path: Path) -> str:
    resolved = path.expanduser().resolve()
    text = resolved.as_posix()
    safe = ":/" if ":" in text[:4] else "/"
    return f"file:{quote(text, safe=safe)}?mode=ro"


def _connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_ro_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _fmt_money(value: Any) -> str:
    return f"${float(value or 0):+.2f}"


def _clean_label(value: Any) -> str:
    text = " ".join(str(value or "UNKNOWN").split())
    text = text.encode("ascii", "ignore").decode("ascii").strip()
    return text or "UNKNOWN"


def _normalize_key(value: Any, default: str = "UNKNOWN") -> str:
    text = _clean_label(value)
    return text if text != "UNKNOWN" else default


def _parse_since(value: str | None, days: int) -> str:
    if not value:
        since = datetime.now(ET) - timedelta(days=max(1, days))
        return since.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")

    text = value.strip().replace("T", " ")
    if len(text) == 10:
        text = f"{text} 00:00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _new_stats() -> dict[str, float]:
    return {"n": 0.0, "pnl": 0.0, "wins": 0.0, "losses": 0.0, "flats": 0.0}


def _add_stat(stats: dict[str, float], pnl: float) -> None:
    stats["n"] += 1
    stats["pnl"] += pnl
    if pnl > 0:
        stats["wins"] += 1
    elif pnl < 0:
        stats["losses"] += 1
    else:
        stats["flats"] += 1


def _add_pnl(groups: dict[tuple[str, ...], dict[str, float]], key: tuple[str, ...], pnl: float) -> None:
    stats = groups.setdefault(key, _new_stats())
    _add_stat(stats, pnl)


def _stats_line(label: str, stats: dict[str, float], extra: str = "") -> str:
    n = int(stats["n"])
    wr = (stats["wins"] / n * 100.0) if n else 0.0
    avg = (stats["pnl"] / n) if n else 0.0
    suffix = f" | {extra}" if extra else ""
    return (
        f"  {label} | n={n} | P&L={_fmt_money(stats['pnl'])} | "
        f"avg={_fmt_money(avg)} | WR={wr:.1f}% "
        f"({int(stats['wins'])}W/{int(stats['losses'])}L/{int(stats['flats'])}F){suffix}"
    )


def _ranked_group_lines(
    title: str,
    groups: dict[tuple[str, ...], dict[str, float]],
    *,
    limit: int,
    best: bool,
) -> list[str]:
    lines = ["", title]
    if not groups:
        lines.append("  None")
        return lines

    ordered = sorted(groups.items(), key=lambda item: item[1]["pnl"], reverse=best)
    for key, stats in ordered[:limit]:
        lines.append(_stats_line(" | ".join(key), stats))
    if len(ordered) > limit:
        lines.append("  ...")
    return lines


def _reason_category(reason: Any) -> str:
    text = " ".join(str(reason or "unknown").strip().split())
    lower = text.lower()
    if "all long paper entries disabled" in lower:
        return "all long entries disabled"
    if "fake-out short blocked" in lower:
        session = text.split("session=", 1)[-1].strip().upper() if "session=" in text else "UNKNOWN"
        return f"fake-out short blocked {session}"
    if "earnings_window=" in lower:
        window = text.split("earnings_window=", 1)[-1].strip().upper()
        return f"earnings window {window}"
    if "weak long bucket" in lower:
        return "weak long bucket"
    if "daily paper entry cap reached" in lower:
        return "daily entry cap"
    if "recent paper loser cooldown" in lower:
        return "recent loser cooldown"
    if "rvol" in lower:
        return "rvol/liquidity filter"
    return _clean_label(text)[:80]


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


def _fetch_closed_exits(conn: sqlite3.Connection, since: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, created_at, symbol, exit_reason, actual_fill_price,
                   verification_result, pnl_usd
            FROM exit_events
            WHERE created_at >= ?
              AND pnl_usd IS NOT NULL
              AND verification_result LIKE 'closed:%'
            ORDER BY created_at ASC, id ASC
            """,
            (since,),
        )
    )


def _fetch_signal_events(conn: sqlite3.Connection, since: str, action: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, created_at, signal_ts, symbol, signal_type, direction,
                   action, reason, earnings_window, time_session, price, rvol
            FROM paper_signal_events
            WHERE created_at >= ?
              AND action = ?
            ORDER BY created_at ASC, id ASC
            """,
            (since, action),
        )
    )


def _fetch_all_signal_events(conn: sqlite3.Connection, since: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, created_at, signal_ts, symbol, signal_type, direction,
                   action, reason, earnings_window, time_session, price, rvol
            FROM paper_signal_events
            WHERE created_at >= ?
              AND action IN ('ENTERED', 'BLOCKED')
            ORDER BY created_at ASC, id ASC
            """,
            (since,),
        )
    )


def _lookup_outcome(
    conn: sqlite3.Connection,
    event: sqlite3.Row,
    horizon: str,
) -> sqlite3.Row | None:
    symbol = str(event["symbol"] or "").upper()
    direction = str(event["direction"] or "").upper()
    signal_ts = str(event["signal_ts"] or "").replace("T", " ")[:19]
    if not symbol or not signal_ts:
        return None

    exact = conn.execute(
        """
        SELECT horizon, signal_ts, target_ts, symbol, direction,
               signal_price, target_price, return_pct, outcome, source
        FROM signal_outcomes
        WHERE horizon = ?
          AND symbol = ?
          AND signal_ts = ?
          AND UPPER(COALESCE(direction, '')) = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (horizon, symbol, signal_ts, direction),
    ).fetchone()
    if exact is not None:
        return exact

    return conn.execute(
        """
        SELECT horizon, signal_ts, target_ts, symbol, direction,
               signal_price, target_price, return_pct, outcome, source
        FROM signal_outcomes
        WHERE horizon = ?
          AND symbol = ?
          AND UPPER(COALESCE(direction, '')) = ?
          AND ABS(strftime('%s', signal_ts) - strftime('%s', ?)) <= 60
        ORDER BY ABS(strftime('%s', signal_ts) - strftime('%s', ?)) ASC, id DESC
        LIMIT 1
        """,
        (horizon, symbol, direction, signal_ts, signal_ts),
    ).fetchone()


def _lookup_any_direction_outcome(
    conn: sqlite3.Connection,
    event: sqlite3.Row,
    horizon: str,
) -> sqlite3.Row | None:
    symbol = str(event["symbol"] or "").upper()
    signal_ts = str(event["signal_ts"] or "").replace("T", " ")[:19]
    if not symbol or not signal_ts:
        return None

    exact = conn.execute(
        """
        SELECT horizon, signal_ts, target_ts, symbol, direction,
               signal_price, target_price, return_pct, outcome, source
        FROM signal_outcomes
        WHERE horizon = ?
          AND symbol = ?
          AND signal_ts = ?
        ORDER BY CASE WHEN outcome = 'NO_DATA' THEN 1 ELSE 0 END, id DESC
        LIMIT 1
        """,
        (horizon, symbol, signal_ts),
    ).fetchone()
    if exact is not None:
        return exact

    return conn.execute(
        """
        SELECT horizon, signal_ts, target_ts, symbol, direction,
               signal_price, target_price, return_pct, outcome, source
        FROM signal_outcomes
        WHERE horizon = ?
          AND symbol = ?
          AND ABS(strftime('%s', signal_ts) - strftime('%s', ?)) <= 60
        ORDER BY CASE WHEN outcome = 'NO_DATA' THEN 1 ELSE 0 END,
                 ABS(strftime('%s', signal_ts) - strftime('%s', ?)) ASC,
                 id DESC
        LIMIT 1
        """,
        (horizon, symbol, signal_ts, signal_ts),
    ).fetchone()


def _event_return_pct(event: sqlite3.Row, outcome: sqlite3.Row) -> float | None:
    if outcome["return_pct"] is None and (
        outcome["signal_price"] is None or outcome["target_price"] is None
    ):
        return None

    event_direction = str(event["direction"] or "").upper()
    outcome_direction = str(outcome["direction"] or "").upper()
    if event_direction == outcome_direction and outcome["return_pct"] is not None:
        return float(outcome["return_pct"])

    try:
        signal_price = float(outcome["signal_price"])
        target_price = float(outcome["target_price"])
    except (TypeError, ValueError):
        return None
    if signal_price <= 0:
        return None
    if event_direction == "SHORT":
        return ((signal_price - target_price) / signal_price) * 100.0
    return ((target_price - signal_price) / signal_price) * 100.0


def _classify_return(return_pct: float | None) -> str:
    if return_pct is None:
        return ""
    if abs(return_pct) < FLAT_BAND_PCT:
        return "FLAT"
    return "WIN" if return_pct > 0 else "LOSS"


def _event_horizon_outcome(
    signals_conn: sqlite3.Connection | None,
    event: sqlite3.Row,
    horizon: str,
) -> tuple[sqlite3.Row | None, float | None]:
    if signals_conn is None:
        return None, None
    outcome = _lookup_outcome(signals_conn, event, horizon)
    if outcome is None:
        outcome = _lookup_any_direction_outcome(signals_conn, event, horizon)
    if outcome is None:
        return None, None
    return outcome, _event_return_pct(event, outcome)


def _format_outcome_cell(outcome: sqlite3.Row | None, return_pct: float | None) -> str:
    if outcome is None:
        return "MISSING"
    raw_outcome = str(outcome["outcome"] or "UNKNOWN").upper()
    if raw_outcome == "NO_DATA":
        return raw_outcome
    adjusted_outcome = _classify_return(return_pct)
    if return_pct is None or not adjusted_outcome:
        return raw_outcome
    return f"{return_pct:+.2f}%/{adjusted_outcome}"


def _build_event_rows(
    signals_conn: sqlite3.Connection | None,
    events: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        row: dict[str, Any] = {
            "id": event["id"],
            "created_at": event["created_at"],
            "signal_ts": event["signal_ts"],
            "symbol": _normalize_key(event["symbol"]),
            "signal_type": _normalize_key(event["signal_type"]),
            "direction": _normalize_key(event["direction"]),
            "action": _normalize_key(event["action"]),
            "reason": _clean_label(event["reason"]),
            "reason_category": _reason_category(event["reason"]) if event["action"] == "BLOCKED" else "",
            "earnings_window": _normalize_key(event["earnings_window"]),
            "time_session": _normalize_key(event["time_session"]),
            "price": event["price"],
            "rvol": event["rvol"],
        }
        for horizon in EVENT_HORIZONS:
            outcome, return_pct = _event_horizon_outcome(signals_conn, event, horizon)
            prefix = f"{horizon}_"
            raw_outcome = "" if outcome is None else str(outcome["outcome"] or "")
            adjusted_outcome = "" if str(raw_outcome).upper() == "NO_DATA" else _classify_return(return_pct)
            row[f"{prefix}cell"] = _format_outcome_cell(outcome, return_pct)
            row[f"{prefix}return_pct"] = "" if return_pct is None else round(return_pct, 6)
            row[f"{prefix}outcome"] = adjusted_outcome
            row[f"{prefix}raw_outcome"] = raw_outcome
            row[f"{prefix}matched_direction"] = "" if outcome is None else str(outcome["direction"] or "")
            row[f"{prefix}source"] = "" if outcome is None else str(outcome["source"] or "")
            row[f"{prefix}target_ts"] = "" if outcome is None else str(outcome["target_ts"] or "")
        rows.append(row)
    return rows


def _event_table_lines(event_rows: list[dict[str, Any]], limit: int) -> list[str]:
    lines = ["", f"Multi-Horizon Event Table ({min(len(event_rows), limit)}/{len(event_rows)})"]
    if not event_rows:
        lines.append("  None")
        return lines
    for row in event_rows[-limit:]:
        reason = row["reason_category"] or row["action"]
        lines.append(
            "  "
            f"{str(row['created_at'])[:16]} | {row['action']:<7} | "
            f"{row['symbol']} {row['direction']} | {row['signal_type']} | "
            f"{row['earnings_window']}/{row['time_session']} | {reason}"
        )
        lines.append(
            "    "
            + " | ".join(f"{h}={row[f'{h}_cell']}" for h in EVENT_HORIZONS)
        )
    return lines


def _write_event_csv(path: Path, event_rows: list[dict[str, Any]]) -> int:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    base_fields = [
        "id",
        "created_at",
        "signal_ts",
        "symbol",
        "signal_type",
        "direction",
        "action",
        "reason_category",
        "reason",
        "earnings_window",
        "time_session",
        "price",
        "rvol",
    ]
    horizon_fields: list[str] = []
    for horizon in EVENT_HORIZONS:
        horizon_fields.extend(
            [
                f"{horizon}_cell",
                f"{horizon}_return_pct",
                f"{horizon}_outcome",
                f"{horizon}_raw_outcome",
                f"{horizon}_matched_direction",
                f"{horizon}_source",
                f"{horizon}_target_ts",
            ]
        )
    fieldnames = base_fields + horizon_fields
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(event_rows)
    return len(event_rows)


def _summarize_actual_trades(
    conn: sqlite3.Connection,
    exits: list[sqlite3.Row],
) -> tuple[dict[str, Any], list[str]]:
    by_symbol: dict[tuple[str, ...], dict[str, float]] = {}
    by_bucket: dict[tuple[str, ...], dict[str, float]] = {}
    by_context: dict[tuple[str, ...], dict[str, float]] = {}
    trade_lines: list[str] = []
    total = _new_stats()

    for row in exits:
        pnl = float(row["pnl_usd"] or 0.0)
        _add_stat(total, pnl)
        entry = _entry_for_exit(conn, str(row["symbol"]), str(row["created_at"]))
        signal_type = _normalize_key(entry["signal_type"] if entry else "NO_ENTRY")
        direction = _normalize_key(entry["direction"] if entry else "UNKNOWN")
        earnings = _normalize_key(entry["earnings_window"] if entry else "UNKNOWN")
        session = _normalize_key(entry["time_session"] if entry else "UNKNOWN")
        symbol = _normalize_key(row["symbol"])

        _add_pnl(by_symbol, (symbol,), pnl)
        _add_pnl(by_bucket, (signal_type, direction), pnl)
        _add_pnl(by_context, (signal_type, direction, earnings, session), pnl)
        trade_lines.append(
            "  "
            f"{str(row['created_at'])[:16]} | {symbol} {direction} | "
            f"{signal_type} | {earnings}/{session} | {row['exit_reason']} | {_fmt_money(pnl)}"
        )

    return (
        {
            "total": total,
            "by_symbol": by_symbol,
            "by_bucket": by_bucket,
            "by_context": by_context,
        },
        trade_lines,
    )


def _summarize_blocked(
    signals_conn: sqlite3.Connection | None,
    blocked: list[sqlite3.Row],
    *,
    horizon: str,
    notional: float,
) -> dict[str, Any]:
    category_counts: Counter[str] = Counter()
    by_category: dict[tuple[str, ...], dict[str, float]] = {}
    by_bucket: dict[tuple[str, ...], dict[str, float]] = {}
    by_symbol: dict[tuple[str, ...], dict[str, float]] = {}
    missing = 0
    no_data = 0
    matched = 0

    for row in blocked:
        category = _reason_category(row["reason"])
        symbol = _normalize_key(row["symbol"])
        signal_type = _normalize_key(row["signal_type"])
        direction = _normalize_key(row["direction"])
        category_counts[category] += 1

        outcome = _lookup_outcome(signals_conn, row, horizon) if signals_conn is not None else None
        if outcome is None and signals_conn is not None:
            outcome = _lookup_any_direction_outcome(signals_conn, row, horizon)
        if outcome is None:
            missing += 1
            continue
        return_pct = _event_return_pct(row, outcome)
        if return_pct is None or str(outcome["outcome"] or "").upper() == "NO_DATA":
            no_data += 1
            continue

        matched += 1
        pnl = notional * return_pct / 100.0
        _add_pnl(by_category, (category,), pnl)
        _add_pnl(by_bucket, (signal_type, direction), pnl)
        _add_pnl(by_symbol, (symbol,), pnl)

    return {
        "category_counts": category_counts,
        "by_category": by_category,
        "by_bucket": by_bucket,
        "by_symbol": by_symbol,
        "missing": missing,
        "no_data": no_data,
        "matched": matched,
    }


def _blocked_category_lines(
    counts: Counter[str],
    what_if: dict[tuple[str, ...], dict[str, float]],
    *,
    horizon: str,
    limit: int,
) -> list[str]:
    lines = ["", "Blocked Categories"]
    if not counts:
        lines.append("  None")
        return lines

    for category, count in counts.most_common(limit):
        stats = what_if.get((category,))
        if stats:
            block_value = -stats["pnl"]
            lines.append(
                _stats_line(
                    category,
                    stats,
                    extra=f"blocked={count} | block_value={_fmt_money(block_value)}",
                )
            )
        else:
            lines.append(f"  {category} | blocked={count} | no matched {_clean_label(horizon)} outcome")
    if len(counts) > limit:
        lines.append("  ...")
    return lines


def build_report(
    *,
    since: str | None = None,
    days: int = DEFAULT_DAYS,
    horizon: str = DEFAULT_HORIZON,
    notional: float = DEFAULT_NOTIONAL,
    state_db: Path = PAPER_STATE_DB_PATH,
    signals_db: Path = SIGNALS_DB_PATH,
    csv_path: Path | None = None,
    limit: int = 8,
    trade_limit: int = 30,
    event_limit: int = 24,
) -> str:
    since_utc = _parse_since(since, days)
    state_db = Path(state_db).expanduser()
    signals_db = Path(signals_db).expanduser()
    if horizon not in HORIZONS:
        raise ValueError(f"Unsupported horizon {horizon!r}; choose one of {', '.join(HORIZONS)}")
    if not state_db.exists():
        return f"Paper Trade Lab\nNo paper state DB found at {state_db}"

    with _connect_ro(state_db) as state_conn:
        required = ("exit_events", "paper_signal_events")
        missing_tables = [name for name in required if not _table_exists(state_conn, name)]
        if missing_tables:
            return f"Paper Trade Lab\nMissing paper state table(s): {', '.join(missing_tables)}"

        exits = _fetch_closed_exits(state_conn, since_utc)
        entered = _fetch_signal_events(state_conn, since_utc, "ENTERED")
        blocked = _fetch_signal_events(state_conn, since_utc, "BLOCKED")
        events = _fetch_all_signal_events(state_conn, since_utc)
        actual, trade_lines = _summarize_actual_trades(state_conn, exits)

        signals_conn = None
        signals_status = f"missing at {signals_db}"
        event_rows: list[dict[str, Any]] = []
        try:
            if signals_db.exists():
                signals_conn = _connect_ro(signals_db)
                signals_status = "available"
                if not _table_exists(signals_conn, "signal_outcomes"):
                    signals_status = "missing signal_outcomes table"
                    signals_conn.close()
                    signals_conn = None
            blocked_summary = _summarize_blocked(
                signals_conn,
                blocked,
                horizon=horizon,
                notional=notional,
            )
            event_rows = _build_event_rows(signals_conn, events)
        finally:
            if signals_conn is not None:
                signals_conn.close()

    csv_status = ""
    if csv_path is not None:
        written = _write_event_csv(Path(csv_path), event_rows)
        csv_status = f"CSV exported: {Path(csv_path).expanduser()} ({written} rows)"

    actual_total = actual["total"]
    blocked_proxy_pnl = sum(stats["pnl"] for stats in blocked_summary["by_category"].values())
    block_value = -blocked_proxy_pnl
    matched = int(blocked_summary["matched"])
    no_data = int(blocked_summary["no_data"])
    missing = int(blocked_summary["missing"])

    lines = [
        "PAPER TRADE LAB",
        f"Window since: {since_utc} UTC",
        f"State DB: {state_db}",
        f"Signal outcomes DB: {signals_status}",
        *([csv_status] if csv_status else []),
        "",
        "Actual Entered Trades",
        f"  entered={len(entered)} | closed={len(exits)} | blocked={len(blocked)}",
        _stats_line("closed P&L", actual_total),
        "",
        f"Blocked What-If ({horizon} horizon-close proxy, ${notional:.0f} notional)",
        (
            f"  matched={matched} | no_data={no_data} | missing_outcome={missing} | "
            f"if_entered={_fmt_money(blocked_proxy_pnl)} | block_value={_fmt_money(block_value)}"
        ),
        "  Positive block_value means blocking helped on this proxy; negative means possible opportunity cost.",
    ]

    lines.extend(_ranked_group_lines("Best Symbols", actual["by_symbol"], limit=limit, best=True))
    lines.extend(_ranked_group_lines("Worst Symbols", actual["by_symbol"], limit=limit, best=False))
    lines.extend(_ranked_group_lines("Best Signal Buckets", actual["by_bucket"], limit=limit, best=True))
    lines.extend(_ranked_group_lines("Worst Signal Buckets", actual["by_bucket"], limit=limit, best=False))
    lines.extend(_ranked_group_lines("Worst Context Buckets", actual["by_context"], limit=limit, best=False))
    lines.extend(
        _blocked_category_lines(
            blocked_summary["category_counts"],
            blocked_summary["by_category"],
            horizon=horizon,
            limit=limit,
        )
    )
    lines.extend(
        _ranked_group_lines(
            f"Most Helpful Blocks By Bucket ({horizon} proxy)",
            blocked_summary["by_bucket"],
            limit=limit,
            best=False,
        )
    )
    lines.extend(
        _ranked_group_lines(
            f"Most Costly Blocks By Bucket ({horizon} proxy)",
            blocked_summary["by_bucket"],
            limit=limit,
            best=True,
        )
    )
    lines.extend(
        _ranked_group_lines(
            f"Blocked Symbol What-If ({horizon} proxy)",
            blocked_summary["by_symbol"],
            limit=limit,
            best=True,
        )
    )

    lines.extend(_event_table_lines(event_rows, limit=event_limit))

    lines.append("")
    lines.append(f"Closed Trade Detail ({min(len(trade_lines), trade_limit)}/{len(trade_lines)})")
    lines.extend(trade_lines[:trade_limit] or ["  None"])
    if len(trade_lines) > trade_limit:
        lines.append("  ...")

    lines.append("")
    lines.append("Read This As")
    lines.append("  This is read-only research; it does not place orders or modify either SQLite DB.")
    lines.append("  Blocked what-if uses signal_outcomes forward returns, not broker TP/SL fill simulation.")
    lines.append("  Multi-horizon rows show direction-adjusted returns for the paper event direction.")
    lines.append("  Use it to decide which guardrails deserve more data, tighter blocking, or a paper-only A/B test.")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only paper trade and guardrail lab")
    parser.add_argument("--since", default=None, help="UTC timestamp or date, e.g. 2026-06-20 00:00:00")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--horizon", choices=HORIZONS, default=DEFAULT_HORIZON)
    parser.add_argument("--notional", type=float, default=DEFAULT_NOTIONAL)
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--trade-limit", type=int, default=30)
    parser.add_argument("--event-limit", type=int, default=24)
    parser.add_argument("--csv", type=Path, default=None, help="Optional path for full event-level CSV export")
    parser.add_argument("--state-db", type=Path, default=PAPER_STATE_DB_PATH)
    parser.add_argument("--signals-db", type=Path, default=SIGNALS_DB_PATH)
    args = parser.parse_args()

    print(
        build_report(
            since=args.since,
            days=max(1, args.days),
            horizon=args.horizon,
            notional=max(1.0, args.notional),
            state_db=args.state_db,
            signals_db=args.signals_db,
            csv_path=args.csv,
            limit=max(1, args.limit),
            trade_limit=max(1, args.trade_limit),
            event_limit=max(1, args.event_limit),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
