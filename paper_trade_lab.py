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
from typing import Any, Callable
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
DEFAULT_COST_BPS = 7.0


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


def _is_action(row: dict[str, Any], action: str) -> bool:
    return str(row.get("action") or "").upper() == action.upper()


def _is_short(row: dict[str, Any]) -> bool:
    return str(row.get("direction") or "").upper() == "SHORT"


def _is_long(row: dict[str, Any]) -> bool:
    return str(row.get("direction") or "").upper() == "LONG"


def _signal_has(row: dict[str, Any], term: str) -> bool:
    return term.upper() in str(row.get("signal_type") or "").upper()


def _session_is(row: dict[str, Any], session: str) -> bool:
    return str(row.get("time_session") or "").upper() == session.upper()


def _is_earnings_blocked(row: dict[str, Any]) -> bool:
    earnings = str(row.get("earnings_window") or "").upper()
    reason = str(row.get("reason_category") or "").lower()
    return earnings in {"EARNINGS_TODAY", "PRE_EARNINGS"} or reason.startswith("earnings window")


def _is_fakeout_short_prime(row: dict[str, Any]) -> bool:
    return _is_short(row) and _signal_has(row, "FAKE-OUT") and _session_is(row, "PRIME")


def _is_short_daily_cap(row: dict[str, Any]) -> bool:
    return _is_short(row) and str(row.get("reason_category") or "") == "daily entry cap"


def _is_entered_short(row: dict[str, Any]) -> bool:
    return _is_action(row, "ENTERED") and _is_short(row)


def _is_any_short_candidate(row: dict[str, Any]) -> bool:
    return _is_short(row) and not _is_earnings_blocked(row)


def _is_fakeout_short(row: dict[str, Any]) -> bool:
    return _is_short(row) and _signal_has(row, "FAKE-OUT")


def _is_fakeout_short_session(row: dict[str, Any], sessions: set[str]) -> bool:
    return _is_fakeout_short(row) and str(row.get("time_session") or "").upper() in sessions


def _is_blocked_long(row: dict[str, Any]) -> bool:
    return _is_action(row, "BLOCKED") and _is_long(row)


def _scenario_specs():
    return [
        (
            "Current guardrails (longs blocked)",
            "actual ENTERED short events only",
            lambda row: _is_action(row, "ENTERED") and _is_short(row),
        ),
        (
            "Actual recorded entries",
            "actual ENTERED events, including historical longs",
            lambda row: _is_action(row, "ENTERED"),
        ),
        (
            "Allow FAKE-OUT SHORT PRIME",
            "entered shorts plus blocked FAKE-OUT SHORT PRIME; all longs blocked",
            lambda row: (
                (_is_action(row, "ENTERED") and _is_short(row))
                or (_is_action(row, "BLOCKED") and _is_fakeout_short_prime(row))
            ),
        ),
        (
            "Allow short daily-cap overflow",
            "entered shorts plus short events blocked only by daily cap",
            lambda row: (
                (_is_action(row, "ENTERED") and _is_short(row))
                or (_is_action(row, "BLOCKED") and _is_short_daily_cap(row))
            ),
        ),
        (
            "Allow PRIME fake-out + daily-cap shorts",
            "entered shorts plus FAKE-OUT SHORT PRIME and short daily-cap blocks",
            lambda row: (
                (_is_action(row, "ENTERED") and _is_short(row))
                or (
                    _is_action(row, "BLOCKED")
                    and _is_short(row)
                    and (_is_fakeout_short_prime(row) or _is_short_daily_cap(row))
                )
            ),
        ),
        (
            "STRONG SELL FLOW shorts only",
            "all matching short events except earnings-window blocks",
            lambda row: (
                _is_short(row)
                and _signal_has(row, "STRONG SELL FLOW")
                and not _is_earnings_blocked(row)
            ),
        ),
        (
            "STRONG/ABSORPTION SELL shorts",
            "all matching short events except earnings-window blocks",
            lambda row: (
                _is_short(row)
                and (_signal_has(row, "STRONG SELL FLOW") or _signal_has(row, "ABSORPTION SELL"))
                and not _is_earnings_blocked(row)
            ),
        ),
        (
            "All shorts except earnings windows",
            "all entered/blocked short events except EARNINGS_TODAY and PRE_EARNINGS",
            lambda row: _is_short(row) and not _is_earnings_blocked(row),
        ),
    ]


def _shadow_policy_specs() -> list[tuple[str, str, Callable[[dict[str, Any]], bool]]]:
    return [
        (
            "Current live paper shorts",
            "actual entered short events under the deployed guardrails",
            lambda row: _is_entered_short(row),
        ),
        (
            "All entered trades",
            "actual entered events, including any historical longs",
            lambda row: _is_action(row, "ENTERED"),
        ),
        (
            "No FAKE-OUT shorts",
            "short candidates except fake-out and earnings-window blocks",
            lambda row: _is_any_short_candidate(row) and not _is_fakeout_short(row),
        ),
        (
            "FAKE-OUT SHORT PRIME only",
            "fake-out shorts in PRIME, including blocked shadow events",
            lambda row: _is_fakeout_short_session(row, {"PRIME"}) and not _is_earnings_blocked(row),
        ),
        (
            "FAKE-OUT SHORT PRE only",
            "fake-out shorts in PRE, including blocked shadow events",
            lambda row: _is_fakeout_short_session(row, {"PRE"}) and not _is_earnings_blocked(row),
        ),
        (
            "FAKE-OUT SHORT NORMAL only",
            "fake-out shorts in NORMAL, including blocked shadow events",
            lambda row: _is_fakeout_short_session(row, {"NORMAL"}) and not _is_earnings_blocked(row),
        ),
        (
            "STRONG SELL FLOW only",
            "strong sell-flow shorts except earnings-window blocks",
            lambda row: _is_any_short_candidate(row) and _signal_has(row, "STRONG SELL FLOW"),
        ),
        (
            "ABSORPTION SELL only",
            "absorption-sell shorts except earnings-window blocks",
            lambda row: _is_any_short_candidate(row) and _signal_has(row, "ABSORPTION SELL"),
        ),
        (
            "CLIMAX shorts only",
            "climax shorts except earnings-window blocks",
            lambda row: _is_any_short_candidate(row) and _signal_has(row, "CLIMAX"),
        ),
        (
            "No EOD short entries",
            "short candidates except EOD and earnings-window blocks",
            lambda row: _is_any_short_candidate(row) and not _session_is(row, "EOD"),
        ),
        (
            "PRIME/NORMAL shorts",
            "short candidates in PRIME or NORMAL except earnings-window blocks",
            lambda row: _is_any_short_candidate(row)
            and str(row.get("time_session") or "").upper() in {"PRIME", "NORMAL"},
        ),
        (
            "All shorts except earnings",
            "all entered/blocked short events except EARNINGS_TODAY and PRE_EARNINGS",
            lambda row: _is_any_short_candidate(row),
        ),
        (
            "Blocked longs shadow",
            "long events blocked by current guardrails, for opportunity-cost monitoring only",
            lambda row: _is_blocked_long(row),
        ),
    ]


def _new_ab_stats() -> dict[str, float]:
    stats = _new_stats()
    stats.update({"selected": 0.0, "missing": 0.0, "no_data": 0.0})
    return stats


def _score_event_rows(event_rows: list[dict[str, Any]], notional: float) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for name, description, predicate in _scenario_specs():
        horizon_stats = {horizon: _new_ab_stats() for horizon in EVENT_HORIZONS}
        selected_rows = [row for row in event_rows if predicate(row)]
        for row in selected_rows:
            for horizon in EVENT_HORIZONS:
                stats = horizon_stats[horizon]
                stats["selected"] += 1
                cell = str(row.get(f"{horizon}_cell") or "")
                raw_return = row.get(f"{horizon}_return_pct")
                if raw_return == "" or raw_return is None:
                    if cell == "NO_DATA":
                        stats["no_data"] += 1
                    else:
                        stats["missing"] += 1
                    continue
                pnl = notional * float(raw_return) / 100.0
                _add_stat(stats, pnl)
        results[name] = {
            "description": description,
            "selected": len(selected_rows),
            "horizons": horizon_stats,
        }
    return results


def _score_shadow_policy_rows(
    event_rows: list[dict[str, Any]],
    *,
    notional: float,
    cost_bps: float,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    cost_pct = max(0.0, cost_bps) / 100.0
    for name, description, predicate in _shadow_policy_specs():
        horizon_stats = {horizon: _new_ab_stats() for horizon in EVENT_HORIZONS}
        selected_rows = [row for row in event_rows if predicate(row)]
        for row in selected_rows:
            for horizon in EVENT_HORIZONS:
                stats = horizon_stats[horizon]
                stats["selected"] += 1
                cell = str(row.get(f"{horizon}_cell") or "")
                raw_return = row.get(f"{horizon}_return_pct")
                if raw_return == "" or raw_return is None:
                    if cell == "NO_DATA":
                        stats["no_data"] += 1
                    else:
                        stats["missing"] += 1
                    continue
                net_return = float(raw_return) - cost_pct
                pnl = notional * net_return / 100.0
                _add_stat(stats, pnl)
        results[name] = {
            "description": description,
            "selected": len(selected_rows),
            "horizons": horizon_stats,
        }
    return results


def _row_net_pnl(
    row: dict[str, Any],
    *,
    horizon: str,
    notional: float,
    cost_bps: float,
) -> float | None:
    raw_return = row.get(f"{horizon}_return_pct")
    if raw_return == "" or raw_return is None:
        return None
    net_return = float(raw_return) - (max(0.0, cost_bps) / 100.0)
    return notional * net_return / 100.0


def _tournament_policy_specs() -> list[tuple[str, str, Callable[[dict[str, Any]], bool]]]:
    signal_filters: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("ALL_SHORTS", lambda row: True),
        ("FAKE-OUT", lambda row: _signal_has(row, "FAKE-OUT")),
        ("STRONG_SELL_FLOW", lambda row: _signal_has(row, "STRONG SELL FLOW")),
        ("ABSORPTION_SELL", lambda row: _signal_has(row, "ABSORPTION SELL")),
        ("CLIMAX", lambda row: _signal_has(row, "CLIMAX")),
    ]
    session_filters: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("ANY_SESSION", lambda row: True),
        ("PRIME", lambda row: _session_is(row, "PRIME")),
        ("NORMAL", lambda row: _session_is(row, "NORMAL")),
        ("PRE", lambda row: _session_is(row, "PRE")),
        ("EOD", lambda row: _session_is(row, "EOD")),
        ("NO_EOD", lambda row: not _session_is(row, "EOD")),
        ("PRIME_OR_NORMAL", lambda row: str(row.get("time_session") or "").upper() in {"PRIME", "NORMAL"}),
    ]

    specs: list[tuple[str, str, Callable[[dict[str, Any]], bool]]] = [
        (
            "LIVE_CURRENT",
            "actual entered shorts under deployed guardrails",
            lambda row: _is_entered_short(row),
        )
    ]
    for signal_name, signal_predicate in signal_filters:
        for session_name, session_predicate in session_filters:
            name = f"{signal_name} | {session_name}"
            description = "auto tournament short policy, earnings blocks excluded"
            specs.append(
                (
                    name,
                    description,
                    lambda row, sp=signal_predicate, tp=session_predicate: (
                        _is_any_short_candidate(row) and sp(row) and tp(row)
                    ),
                )
            )
    specs.append(
        (
            "BLOCKED_LONGS_SHADOW",
            "long events blocked by current guardrails, opportunity-cost monitor",
            lambda row: _is_blocked_long(row),
        )
    )
    return specs


def _score_tournament_policy(
    selected_rows: list[dict[str, Any]],
    *,
    notional: float,
    cost_bps: float,
) -> dict[str, dict[str, float]]:
    horizon_stats = {horizon: _new_ab_stats() for horizon in EVENT_HORIZONS}
    for row in selected_rows:
        for horizon in EVENT_HORIZONS:
            stats = horizon_stats[horizon]
            stats["selected"] += 1
            pnl = _row_net_pnl(row, horizon=horizon, notional=notional, cost_bps=cost_bps)
            if pnl is None:
                cell = str(row.get(f"{horizon}_cell") or "")
                if cell == "NO_DATA":
                    stats["no_data"] += 1
                else:
                    stats["missing"] += 1
                continue
            _add_stat(stats, pnl)
    return horizon_stats


def _concentration_warnings(
    selected_rows: list[dict[str, Any]],
    *,
    horizon: str,
    notional: float,
    cost_bps: float,
) -> tuple[list[str], dict[str, Any]]:
    by_symbol: dict[str, float] = {}
    by_day: dict[str, float] = {}
    scored = 0
    total_pnl = 0.0
    positive_pnl = 0.0
    for row in selected_rows:
        pnl = _row_net_pnl(row, horizon=horizon, notional=notional, cost_bps=cost_bps)
        if pnl is None:
            continue
        scored += 1
        total_pnl += pnl
        positive_pnl += max(0.0, pnl)
        symbol = _normalize_key(row.get("symbol"))
        day = str(row.get("created_at") or "")[:10] or "UNKNOWN"
        by_symbol[symbol] = by_symbol.get(symbol, 0.0) + pnl
        by_day[day] = by_day.get(day, 0.0) + pnl

    warnings: list[str] = []
    top_symbol = max(by_symbol.items(), key=lambda item: item[1], default=("NONE", 0.0))
    top_day = max(by_day.items(), key=lambda item: item[1], default=("NONE", 0.0))
    denominator = positive_pnl if positive_pnl > 0.01 else 1.0
    top_symbol_share = max(0.0, top_symbol[1]) / denominator
    top_day_share = max(0.0, top_day[1]) / denominator
    if scored and len(by_symbol) < 3:
        warnings.append(f"symbol breadth low ({len(by_symbol)})")
    if scored and len(by_day) < 3:
        warnings.append(f"day breadth low ({len(by_day)})")
    if total_pnl > 0 and top_symbol_share > 0.50:
        warnings.append(f"symbol concentration {top_symbol[0]} {top_symbol_share * 100:.0f}%")
    if total_pnl > 0 and top_day_share > 0.50:
        warnings.append(f"day concentration {top_day[0]} {top_day_share * 100:.0f}%")
    meta = {
        "scored": scored,
        "symbols": len(by_symbol),
        "days": len(by_day),
        "top_symbol": top_symbol[0],
        "top_symbol_pnl": top_symbol[1],
        "top_day": top_day[0],
        "top_day_pnl": top_day[1],
        "total_pnl": total_pnl,
    }
    return warnings, meta


def _policy_status(
    *,
    best_stats: dict[str, float],
    one_day_stats: dict[str, float],
    min_sample: int,
    warnings: list[str],
    horizon: str,
) -> str:
    if int(best_stats["n"]) < min_sample:
        return "WATCH_EARLY"
    if best_stats["pnl"] <= 0:
        return "REJECT"
    if horizon != "1d" and int(one_day_stats["n"]) >= min_sample and one_day_stats["pnl"] < 0:
        return "DANGER_BAD_1D_TAIL"
    if warnings:
        return "WATCH_OVERFIT"
    return "PROMOTE_CANDIDATE"


def _build_policy_tournament(
    event_rows: list[dict[str, Any]],
    *,
    notional: float,
    cost_bps: float,
    min_sample: int,
) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    for name, description, predicate in _tournament_policy_specs():
        selected_rows = [row for row in event_rows if predicate(row)]
        horizons = _score_tournament_policy(selected_rows, notional=notional, cost_bps=cost_bps)
        best_horizon = max(EVENT_HORIZONS, key=lambda horizon: horizons[horizon]["pnl"])
        best_stats = horizons[best_horizon]
        warnings, concentration = _concentration_warnings(
            selected_rows,
            horizon=best_horizon,
            notional=notional,
            cost_bps=cost_bps,
        )
        status = _policy_status(
            best_stats=best_stats,
            one_day_stats=horizons["1d"],
            min_sample=min_sample,
            warnings=warnings,
            horizon=best_horizon,
        )
        policies.append(
            {
                "name": name,
                "description": description,
                "selected": len(selected_rows),
                "horizons": horizons,
                "best_horizon": best_horizon,
                "best_stats": best_stats,
                "status": status,
                "warnings": warnings,
                "concentration": concentration,
            }
        )
    return policies


def _tournament_policy_line(
    policy: dict[str, Any],
    baseline_horizons: dict[str, dict[str, float]] | None = None,
) -> str:
    stats = policy["best_stats"]
    scored = int(stats["n"])
    selected = int(stats["selected"])
    wr = (stats["wins"] / scored * 100.0) if scored else 0.0
    avg = stats["pnl"] / scored if scored else 0.0
    delta = ""
    if baseline_horizons is not None:
        baseline = baseline_horizons.get(policy["best_horizon"])
        if baseline is not None:
            delta = f" | delta={_fmt_money(stats['pnl'] - baseline['pnl'])}"
    warnings = "; ".join(policy["warnings"][:2]) if policy["warnings"] else "clean"
    return (
        f"  {policy['name']} [{policy['status']}] best={policy['best_horizon']} "
        f"selected={selected} scored={scored} P&L={_fmt_money(stats['pnl'])}{delta} "
        f"avg={_fmt_money(avg)} WR={wr:.1f}% | {warnings}"
    )


def _tournament_section_lines(
    title: str,
    policies: list[dict[str, Any]],
    *,
    statuses: set[str],
    limit: int,
    baseline_horizons: dict[str, dict[str, float]] | None,
) -> list[str]:
    lines = ["", title]
    selected = [policy for policy in policies if policy["status"] in statuses]
    selected.sort(
        key=lambda policy: (
            policy["best_stats"]["pnl"],
            policy["best_stats"]["n"],
        ),
        reverse=True,
    )
    if not selected:
        lines.append("  None")
        return lines
    for policy in selected[:limit]:
        lines.append(_tournament_policy_line(policy, baseline_horizons=baseline_horizons))
    if len(selected) > limit:
        lines.append("  ...")
    return lines


def _ab_line(horizon: str, stats: dict[str, float], baseline: dict[str, float] | None) -> str:
    n = int(stats["n"])
    selected = int(stats["selected"])
    wr = (stats["wins"] / n * 100.0) if n else 0.0
    avg = (stats["pnl"] / n) if n else 0.0
    if baseline is None:
        delta_text = "delta=n/a"
        verdict = "baseline"
    else:
        delta = stats["pnl"] - baseline["pnl"]
        delta_text = f"delta={_fmt_money(delta)}"
        verdict = "improved" if delta > 0.01 else "hurt" if delta < -0.01 else "flat"
    return (
        f"    {horizon:<3} | selected={selected:<3} scored={n:<3} "
        f"P&L={_fmt_money(stats['pnl'])} {delta_text} {verdict} | "
        f"avg={_fmt_money(avg)} WR={wr:.1f}% | "
        f"missing={int(stats['missing'])} no_data={int(stats['no_data'])}"
    )


def _scenario_lines(
    results: dict[str, dict[str, Any]],
    *,
    primary_horizon: str,
    notional: float,
    scenario_limit: int,
) -> list[str]:
    lines = ["", f"Guardrail A/B Simulator (${notional:.0f}/event horizon-close proxy)"]
    if not results:
        lines.append("  None")
        return lines

    baseline_name = "Current guardrails (longs blocked)"
    baseline = results.get(baseline_name, {}).get("horizons", {})
    primary_baseline = baseline.get(primary_horizon)
    ranking = sorted(
        results.items(),
        key=lambda item: item[1]["horizons"][primary_horizon]["pnl"],
        reverse=True,
    )

    lines.append(f"Ranking by {primary_horizon} P&L vs {baseline_name}:")
    for name, result in ranking[:scenario_limit]:
        stats = result["horizons"][primary_horizon]
        baseline_stats = None if name == baseline_name else primary_baseline
        lines.append("  " + name)
        lines.append(_ab_line(primary_horizon, stats, baseline_stats))
    if len(ranking) > scenario_limit:
        lines.append("  ...")

    lines.append("")
    lines.append("Scenario Details")
    for name, result in results.items():
        lines.append(f"  {name} - {result['description']}")
        for horizon in EVENT_HORIZONS:
            baseline_stats = None if name == baseline_name else baseline.get(horizon)
            lines.append(_ab_line(horizon, result["horizons"][horizon], baseline_stats))
    return lines


def _load_event_rows(
    *,
    since_utc: str,
    state_db: Path,
    signals_db: Path,
) -> tuple[list[dict[str, Any]], str]:
    state_db = Path(state_db).expanduser()
    signals_db = Path(signals_db).expanduser()
    if not state_db.exists():
        return [], f"missing paper state DB at {state_db}"

    with _connect_ro(state_db) as state_conn:
        if not _table_exists(state_conn, "paper_signal_events"):
            return [], "missing paper_signal_events table"
        events = _fetch_all_signal_events(state_conn, since_utc)

    signals_conn = None
    signals_status = f"missing at {signals_db}"
    try:
        if signals_db.exists():
            signals_conn = _connect_ro(signals_db)
            signals_status = "available"
            if not _table_exists(signals_conn, "signal_outcomes"):
                signals_status = "missing signal_outcomes table"
                signals_conn.close()
                signals_conn = None
        return _build_event_rows(signals_conn, events), signals_status
    finally:
        if signals_conn is not None:
            signals_conn.close()


def _scenario_stats(
    results: dict[str, dict[str, Any]],
    scenario: str,
    horizon: str,
) -> dict[str, float]:
    return results[scenario]["horizons"][horizon]


def _scenario_delta(
    results: dict[str, dict[str, Any]],
    scenario: str,
    horizon: str,
    baseline: str = "Current guardrails (longs blocked)",
) -> float:
    return _scenario_stats(results, scenario, horizon)["pnl"] - _scenario_stats(results, baseline, horizon)["pnl"]


def _delta_summary(
    results: dict[str, dict[str, Any]],
    scenario: str,
    *,
    baseline: str = "Current guardrails (longs blocked)",
) -> str:
    helped: list[str] = []
    hurt: list[str] = []
    flat: list[str] = []
    for horizon in EVENT_HORIZONS:
        delta = _scenario_delta(results, scenario, horizon, baseline)
        item = f"{horizon} ({_fmt_money(delta)})"
        if delta > 0.01:
            helped.append(item)
        elif delta < -0.01:
            hurt.append(item)
        else:
            flat.append(item)
    parts: list[str] = []
    if helped:
        parts.append("helped " + ", ".join(helped))
    if hurt:
        parts.append("hurt " + ", ".join(hurt))
    if flat:
        parts.append("flat " + ", ".join(flat))
    return "; ".join(parts) if parts else "no scored comparison"


def _primary_verdict(delta: float) -> str:
    if delta > 25:
        return "consider paper-only test"
    if delta > 0:
        return "watch"
    if delta < 0:
        return "keep blocked"
    return "keep"


def _closed_entry_stats(
    *,
    since_utc: str,
    state_db: Path,
    signal_term: str,
    direction: str,
    sessions: set[str],
) -> dict[str, float]:
    stats = _new_stats()
    state_db = Path(state_db).expanduser()
    if not state_db.exists():
        return stats

    with _connect_ro(state_db) as conn:
        if not _table_exists(conn, "exit_events") or not _table_exists(conn, "paper_signal_events"):
            return stats
        for exit_row in _fetch_closed_exits(conn, since_utc):
            entry = _entry_for_exit(conn, str(exit_row["symbol"]), str(exit_row["created_at"]))
            if entry is None:
                continue
            signal_text = str(entry["signal_type"] or "").upper()
            entry_direction = str(entry["direction"] or "").upper()
            entry_session = str(entry["time_session"] or "UNKNOWN").upper()
            if signal_term.upper() not in signal_text:
                continue
            if entry_direction != direction.upper():
                continue
            if entry_session not in sessions:
                continue
            _add_stat(stats, float(exit_row["pnl_usd"] or 0.0))
    return stats


def _actual_sample_line(label: str, stats: dict[str, float], min_sample: int) -> str:
    n = int(stats["n"])
    wr = (stats["wins"] / n * 100.0) if n else 0.0
    if n < min_sample:
        verdict = "too early, keep collecting"
    elif stats["pnl"] > 0:
        verdict = "sample ready and green"
    elif stats["pnl"] < 0:
        verdict = "sample ready but negative"
    else:
        verdict = "sample ready but flat"
    return (
        f"{label}: {_fmt_money(stats['pnl'])} actual, n={n}, "
        f"WR={wr:.1f}% - {verdict}"
    )


def _filter_event_rows_since(event_rows: list[dict[str, Any]], since_utc: str) -> list[dict[str, Any]]:
    threshold = since_utc.replace("T", " ")[:19]
    return [row for row in event_rows if str(row.get("created_at") or "")[:19] >= threshold]


def _shadow_policy_line(
    name: str,
    stats: dict[str, float],
    *,
    baseline: dict[str, float] | None,
    min_sample: int,
) -> str:
    selected = int(stats["selected"])
    scored = int(stats["n"])
    wr = (stats["wins"] / scored * 100.0) if scored else 0.0
    avg = (stats["pnl"] / scored) if scored else 0.0
    if baseline is None:
        delta_text = "baseline"
    else:
        delta_text = f"delta={_fmt_money(stats['pnl'] - baseline['pnl'])}"
    if scored < min_sample:
        sample_text = "too early"
    elif stats["pnl"] > 0:
        sample_text = "green"
    elif stats["pnl"] < 0:
        sample_text = "red"
    else:
        sample_text = "flat"
    return (
        f"  {name}: selected={selected} scored={scored} "
        f"P&L={_fmt_money(stats['pnl'])} {delta_text} | "
        f"avg={_fmt_money(avg)} WR={wr:.1f}% | {sample_text}"
    )


def _shadow_leaderboard_lines(
    title: str,
    results: dict[str, dict[str, Any]],
    *,
    horizon: str,
    limit: int,
    min_sample: int,
) -> list[str]:
    lines = ["", title]
    if not results:
        lines.append("  None")
        return lines
    baseline_name = "Current live paper shorts"
    baseline_stats = results.get(baseline_name, {}).get("horizons", {}).get(horizon)
    ranked = sorted(
        results.items(),
        key=lambda item: (
            item[1]["horizons"][horizon]["pnl"],
            item[1]["horizons"][horizon]["n"],
        ),
        reverse=True,
    )
    lines.append(f"  Ranked by {horizon} net proxy P&L after costs")
    for name, result in ranked[:limit]:
        stats = result["horizons"][horizon]
        baseline = None if name == baseline_name else baseline_stats
        lines.append(_shadow_policy_line(name, stats, baseline=baseline, min_sample=min_sample))
    if len(ranked) > limit:
        lines.append("  ...")
    return lines


def _shadow_multi_horizon_lines(
    title: str,
    results: dict[str, dict[str, Any]],
    *,
    names: list[str],
    min_sample: int,
) -> list[str]:
    lines = ["", title]
    for name in names:
        result = results.get(name)
        if not result:
            continue
        pieces: list[str] = []
        for horizon in EVENT_HORIZONS:
            stats = result["horizons"][horizon]
            scored = int(stats["n"])
            tag = "early" if scored < min_sample else "ok"
            pieces.append(f"{horizon}:{_fmt_money(stats['pnl'])}/n={scored}/{tag}")
        lines.append(f"  {name}: " + " | ".join(pieces))
    if len(lines) == 2:
        lines.append("  None")
    return lines


def _best_shadow_candidate(
    results: dict[str, dict[str, Any]],
    *,
    horizon: str,
    min_sample: int,
) -> tuple[str, dict[str, float]] | None:
    candidates: list[tuple[str, dict[str, float]]] = []
    for name, result in results.items():
        if name == "Current live paper shorts":
            continue
        stats = result["horizons"][horizon]
        if stats["n"] >= min_sample:
            candidates.append((name, stats))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1]["pnl"])


def build_shadow_policy_report(
    *,
    since: str | None = None,
    days: int = 30,
    recent_days: int = 7,
    horizon: str = DEFAULT_HORIZON,
    notional: float = DEFAULT_NOTIONAL,
    min_sample: int = 25,
    limit: int = 8,
    cost_bps: float = DEFAULT_COST_BPS,
    state_db: Path = PAPER_STATE_DB_PATH,
    signals_db: Path = SIGNALS_DB_PATH,
) -> str:
    if horizon not in HORIZONS:
        raise ValueError(f"Unsupported horizon {horizon!r}; choose one of {', '.join(HORIZONS)}")

    since_utc = _parse_since(since, days)
    recent_since_utc = _parse_since(None, max(1, recent_days))
    event_rows, signals_status = _load_event_rows(
        since_utc=since_utc,
        state_db=Path(state_db),
        signals_db=Path(signals_db),
    )
    if not event_rows:
        return (
            "Paper Shadow Policy Lab\n"
            f"Since: {since_utc} UTC\n"
            f"No paper signal events found ({signals_status})."
        )

    recent_rows = _filter_event_rows_since(event_rows, recent_since_utc)
    full_results = _score_shadow_policy_rows(event_rows, notional=notional, cost_bps=cost_bps)
    recent_results = _score_shadow_policy_rows(recent_rows, notional=notional, cost_bps=cost_bps)
    tournament = _build_policy_tournament(
        event_rows,
        notional=notional,
        cost_bps=cost_bps,
        min_sample=min_sample,
    )
    recent_tournament = _build_policy_tournament(
        recent_rows,
        notional=notional,
        cost_bps=cost_bps,
        min_sample=min_sample,
    )

    baseline = full_results["Current live paper shorts"]["horizons"][horizon]
    live_policy = next((policy for policy in tournament if policy["name"] == "LIVE_CURRENT"), None)
    live_horizons = None if live_policy is None else live_policy["horizons"]
    best_full = _best_shadow_candidate(full_results, horizon=horizon, min_sample=min_sample)
    if best_full is None:
        best_line = "No challenger has enough scored samples yet."
    else:
        best_name, best_stats = best_full
        best_line = (
            f"Best sampled challenger: {best_name} "
            f"{_fmt_money(best_stats['pnl'])} vs current {_fmt_money(baseline['pnl'])} "
            f"on {horizon} net proxy."
        )

    watch_names = [
        "Current live paper shorts",
        "No FAKE-OUT shorts",
        "FAKE-OUT SHORT PRIME only",
        "FAKE-OUT SHORT NORMAL only",
        "STRONG SELL FLOW only",
        "ABSORPTION SELL only",
        "No EOD short entries",
        "All shorts except earnings",
        "Blocked longs shadow",
    ]

    lines = [
        "Paper Shadow Policy Lab",
        f"Full window since: {since_utc} UTC | recent window: last {max(1, recent_days)}d",
        f"Primary horizon: {horizon} | notional=${notional:.0f}/event | cost={cost_bps:.1f} bps/event",
        f"Signal outcomes DB: {signals_status}",
        f"Events: full={len(event_rows)} | recent={len(recent_rows)}",
        "",
        "Decision Frame",
        f"  {best_line}",
        f"  Promote only after n>={min_sample}, green net P&L, and no ugly 1d tail.",
        "  Keep live execution conservative while shadow policies collect more reps.",
    ]
    lines.extend(
        _shadow_leaderboard_lines(
            f"Full Window Leaderboard ({horizon})",
            full_results,
            horizon=horizon,
            limit=limit,
            min_sample=min_sample,
        )
    )
    lines.extend(
        _shadow_leaderboard_lines(
            f"Recent Leaderboard ({horizon})",
            recent_results,
            horizon=horizon,
            limit=limit,
            min_sample=min_sample,
        )
    )
    lines.extend(
        _tournament_section_lines(
            "Policy Tournament - Promote Candidates",
            tournament,
            statuses={"PROMOTE_CANDIDATE"},
            limit=limit,
            baseline_horizons=live_horizons,
        )
    )
    lines.extend(
        _tournament_section_lines(
            "Policy Tournament - Watch / Overfit",
            tournament,
            statuses={"WATCH_OVERFIT", "WATCH_EARLY", "DANGER_BAD_1D_TAIL"},
            limit=limit,
            baseline_horizons=live_horizons,
        )
    )
    lines.extend(
        _tournament_section_lines(
            "Policy Tournament - Reject",
            tournament,
            statuses={"REJECT"},
            limit=limit,
            baseline_horizons=live_horizons,
        )
    )
    lines.extend(
        _tournament_section_lines(
            "Recent Tournament Watchlist",
            recent_tournament,
            statuses={"PROMOTE_CANDIDATE", "WATCH_OVERFIT", "DANGER_BAD_1D_TAIL"},
            limit=limit,
            baseline_horizons=None,
        )
    )
    lines.extend(
        _shadow_multi_horizon_lines(
            "Key Policies Across Horizons",
            full_results,
            names=watch_names,
            min_sample=min_sample,
        )
    )
    lines.append("")
    lines.append("Read This As")
    lines.append("  This is read-only shadow research from entered + blocked events; it does not place orders.")
    lines.append("  P&L is horizon-close proxy after estimated costs, not broker TP/SL fill simulation.")
    lines.append("  The point is faster learning: many policies are scored while only one conservative policy trades.")
    return "\n".join(lines)


def build_guardrail_ab_summary(
    *,
    since: str | None = None,
    days: int = DEFAULT_DAYS,
    horizon: str = DEFAULT_HORIZON,
    notional: float = DEFAULT_NOTIONAL,
    min_sample: int = 10,
    state_db: Path = PAPER_STATE_DB_PATH,
    signals_db: Path = SIGNALS_DB_PATH,
) -> str:
    if horizon not in HORIZONS:
        raise ValueError(f"Unsupported horizon {horizon!r}; choose one of {', '.join(HORIZONS)}")

    since_utc = _parse_since(since, days)
    event_rows, signals_status = _load_event_rows(
        since_utc=since_utc,
        state_db=Path(state_db),
        signals_db=Path(signals_db),
    )
    if not event_rows:
        return (
            "Paper Guardrail A/B Summary\n"
            f"Since: {since_utc} UTC\n"
            f"No paper signal events found ({signals_status})."
        )

    results = _score_event_rows(event_rows, notional)
    baseline = "Current guardrails (longs blocked)"
    fakeout = "Allow FAKE-OUT SHORT PRIME"
    daily_cap = "Allow short daily-cap overflow"
    actual = "Actual recorded entries"
    combo = "Allow PRIME fake-out + daily-cap shorts"

    baseline_stats = _scenario_stats(results, baseline, horizon)
    fakeout_delta = _scenario_delta(results, fakeout, horizon)
    daily_cap_delta = _scenario_delta(results, daily_cap, horizon)
    long_block_delta = baseline_stats["pnl"] - _scenario_stats(results, actual, horizon)["pnl"]
    combo_delta = _scenario_delta(results, combo, horizon)
    fakeout_prime_actual = _closed_entry_stats(
        since_utc=since_utc,
        state_db=Path(state_db),
        signal_term="FAKE-OUT",
        direction="SHORT",
        sessions={"PRIME"},
    )
    fakeout_pre_actual = _closed_entry_stats(
        since_utc=since_utc,
        state_db=Path(state_db),
        signal_term="FAKE-OUT",
        direction="SHORT",
        sessions={"PRE"},
    )

    if fakeout_delta > 25 and _scenario_delta(results, fakeout, "5m") < 0:
        fakeout_action = "consider paper-only test for 1h+ holds, not 5m scalps"
    else:
        fakeout_action = _primary_verdict(fakeout_delta)

    daily_cap_action = _primary_verdict(daily_cap_delta)
    long_action = "keep long block" if long_block_delta >= 0 else "review long block"
    if combo_delta > max(fakeout_delta, daily_cap_delta, 0):
        suggested = (
            f"{long_action}; watch fake-out PRIME plus daily-cap shorts as the best {horizon} variant."
        )
    elif fakeout_delta > 0:
        suggested = f"{long_action}; {fakeout_action}."
    elif daily_cap_delta > 0:
        suggested = f"{long_action}; watch daily-cap overflow."
    else:
        suggested = f"{long_action}; keep current short guardrails."

    selected = int(baseline_stats["selected"])
    scored = int(baseline_stats["n"])
    lines = [
        "Paper Guardrail A/B Summary",
        f"Since: {since_utc} UTC | primary={horizon} | notional=${notional:.0f}",
        f"Signal outcomes DB: {signals_status}",
        "",
        "Current guardrails vs alternates",
        (
            f"Current guardrails: selected={selected} scored={scored} "
            f"{horizon} P&L={_fmt_money(baseline_stats['pnl'])}"
        ),
        f"FAKE-OUT SHORT PRIME: {_delta_summary(results, fakeout)} | {fakeout_action}",
        _actual_sample_line("FAKE-OUT SHORT PRIME actual", fakeout_prime_actual, min_sample),
        _actual_sample_line("FAKE-OUT SHORT PRE actual", fakeout_pre_actual, min_sample),
        f"Daily cap overflow: {_delta_summary(results, daily_cap)} | {daily_cap_action}",
        f"Long block: {_delta_summary(results, baseline, baseline=actual)} | {long_action}",
        f"PRIME fake-out + daily-cap shorts: {_delta_summary(results, combo)} | {_primary_verdict(combo_delta)}",
        f"Suggested action: {suggested}",
        "",
        "Read This As",
        "This is read-only research using horizon-close proxy P&L, not broker TP/SL fills.",
    ]
    return "\n".join(lines)


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
    scenario_limit: int = 8,
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
    scenario_results = _score_event_rows(event_rows, notional)

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

    lines.extend(
        _scenario_lines(
            scenario_results,
            primary_horizon=horizon,
            notional=notional,
            scenario_limit=scenario_limit,
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
    lines.append("  A/B scenarios score hypothetical allow/block rules from recorded entered and blocked events.")
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
    parser.add_argument("--scenario-limit", type=int, default=8)
    parser.add_argument("--min-sample", type=int, default=10)
    parser.add_argument("--recent-days", type=int, default=7)
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    parser.add_argument("--csv", type=Path, default=None, help="Optional path for full event-level CSV export")
    parser.add_argument("--state-db", type=Path, default=PAPER_STATE_DB_PATH)
    parser.add_argument("--signals-db", type=Path, default=SIGNALS_DB_PATH)
    parser.add_argument("--ab-summary", action="store_true", help="Print the compact Discord guardrail A/B summary")
    parser.add_argument("--shadow-summary", action="store_true", help="Print the compact shadow policy leaderboard")
    args = parser.parse_args()

    if args.shadow_summary:
        print(
            build_shadow_policy_report(
                since=args.since,
                days=max(1, args.days),
                recent_days=max(1, args.recent_days),
                horizon=args.horizon,
                notional=max(1.0, args.notional),
                min_sample=max(1, args.min_sample),
                limit=max(1, args.limit),
                cost_bps=max(0.0, args.cost_bps),
                state_db=args.state_db,
                signals_db=args.signals_db,
            )
        )
        return 0

    if args.ab_summary:
        print(
            build_guardrail_ab_summary(
                since=args.since,
                days=max(1, args.days),
                horizon=args.horizon,
                notional=max(1.0, args.notional),
                min_sample=max(1, args.min_sample),
                state_db=args.state_db,
                signals_db=args.signals_db,
            )
        )
        return 0

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
            scenario_limit=max(1, args.scenario_limit),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
