#!/usr/bin/env python3
"""Read-only signal scoring lab for God Mode research.

This is intentionally not an execution module. It learns simple, explainable
bucket expectancy from labeled signal outcomes, validates it on a chronological
holdout split, compares it with the current paper guardrails, and scores recent
signals as ALLOW_RESEARCH / BLOCK_RESEARCH / WATCH / NOT_ENOUGH_DATA.
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from app_paths import DATA_DIR
from audit_report import clean_label


SIGNALS_DB = DATA_DIR / "wolfe_signals.db"
DEFAULT_SINCE = "2026-05-24 14:52:00"
DEFAULT_HORIZON = "1h"
HORIZONS = ("5m", "15m", "30m", "1h", "1d")
EARNINGS_BLOCKS = {"EARNINGS_TODAY", "PRE_EARNINGS"}


@dataclass
class Stats:
    n: int = 0
    net_sum: float = 0.0
    gross_sum: float = 0.0
    wins: int = 0
    losses: int = 0
    flats: int = 0

    def add(self, gross_pct: float, net_pct: float) -> None:
        self.n += 1
        self.gross_sum += gross_pct
        self.net_sum += net_pct
        if net_pct > 0:
            self.wins += 1
        elif net_pct < 0:
            self.losses += 1
        else:
            self.flats += 1

    @property
    def net_avg(self) -> float:
        return self.net_sum / self.n if self.n else 0.0

    @property
    def gross_avg(self) -> float:
        return self.gross_sum / self.n if self.n else 0.0

    @property
    def win_pct(self) -> float:
        return 100.0 * self.wins / self.n if self.n else 0.0


@dataclass
class Score:
    predicted_net_pct: float
    predicted_win_pct: float
    sample_n: int
    reason: str
    decision: str


def _sqlite_ro_uri(path: Path) -> str:
    resolved = path.expanduser().resolve()
    text = resolved.as_posix()
    safe = ":/" if ":" in text[:4] else "/"
    return f"file:{quote(text, safe=safe)}?mode=ro"


def connect_ro(path: Path = SIGNALS_DB) -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_ro_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        is not None
    )


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _sql_col(cols: set[str], name: str, default: str = "NULL") -> str:
    return f"s.{name}" if name in cols else f"{default} AS {name}"


def _norm(value: Any, default: str = "UNKNOWN") -> str:
    text = clean_label(str(value or default)).strip().upper()
    return text or default


def _horizon(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in HORIZONS else DEFAULT_HORIZON


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return default
        return value
    except Exception:
        return default


def cost_pct(slippage_bps: float, spread_bps: float) -> float:
    # return_pct is in percentage points; bps / 100 converts to pct.
    return ((2.0 * slippage_bps) + spread_bps) / 100.0


def current_guardrail(row: sqlite3.Row | dict[str, Any]) -> str:
    signal = _norm(row["signal_type"])
    direction = _norm(row["direction"])
    earnings = _norm(row["earnings_window"])
    if earnings in EARNINGS_BLOCKS:
        return "BLOCK_EARNINGS"
    if direction == "LONG":
        return "BLOCK_LONG"
    if "FAKE-OUT" in signal and direction == "SHORT":
        return "BLOCK_FAKEOUT_SHORT"
    return "ALLOW"


def load_labeled_rows(conn: sqlite3.Connection, since: str, cost: float) -> list[dict[str, Any]]:
    sig_cols = columns(conn, "signals")
    select_parts = [
        "o.signal_rowid",
        "o.horizon",
        "o.symbol",
        "o.signal_ts",
        "o.signal_type",
        "o.direction",
        "o.return_pct",
        "o.outcome",
        "COALESCE(o.source, 'UNKNOWN') AS source",
        _sql_col(sig_cols, "time_session", "'UNKNOWN'"),
        _sql_col(sig_cols, "earnings_window", "'UNKNOWN'"),
        _sql_col(sig_cols, "news_flag", "'UNKNOWN'"),
        _sql_col(sig_cols, "catalyst_type", "'UNKNOWN'"),
        _sql_col(sig_cols, "sector", "'UNKNOWN'"),
        _sql_col(sig_cols, "rvol", "NULL"),
        _sql_col(sig_cols, "flow_m", "NULL"),
        _sql_col(sig_cols, "change_pct", "NULL"),
        _sql_col(sig_cols, "confidence", "NULL"),
    ]
    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM signal_outcomes o
        LEFT JOIN signals s ON s.rowid = o.signal_rowid
        WHERE o.signal_ts >= ?
          AND o.horizon IN ({",".join("?" for _ in HORIZONS)})
          AND o.outcome IN ('WIN', 'LOSS', 'FLAT')
          AND o.return_pct IS NOT NULL
        ORDER BY o.signal_ts ASC, o.signal_rowid ASC, o.horizon ASC
        """,
        (since, *HORIZONS),
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        gross = _num(row["return_pct"])
        out.append(
            {
                "signal_rowid": row["signal_rowid"],
                "horizon": _horizon(row["horizon"]),
                "symbol": _norm(row["symbol"]),
                "signal_ts": row["signal_ts"],
                "signal_type": _norm(row["signal_type"]),
                "direction": _norm(row["direction"]),
                "return_pct": gross,
                "net_pct": gross - cost,
                "outcome": _norm(row["outcome"]),
                "source": _norm(row["source"]),
                "time_session": _norm(row["time_session"]),
                "earnings_window": _norm(row["earnings_window"]),
                "news_flag": _norm(row["news_flag"]),
                "catalyst_type": _norm(row["catalyst_type"]),
                "sector": _norm(row["sector"]),
                "rvol": _num(row["rvol"]),
                "flow_m": _num(row["flow_m"]),
                "change_pct": _num(row["change_pct"]),
                "confidence": _num(row["confidence"]),
            }
        )
    return out


def load_recent_signals(conn: sqlite3.Connection, limit: int, horizon: str) -> list[dict[str, Any]]:
    sig_cols = columns(conn, "signals")
    select_parts = [
        "s.rowid AS signal_rowid",
        "s.timestamp AS signal_ts",
        "s.symbol",
        "s.signal_type",
        "CASE WHEN UPPER(COALESCE(s.signal_type, '')) LIKE '%SELL%' THEN 'SHORT' "
        "WHEN UPPER(COALESCE(s.signal_type, '')) LIKE '%BUY%' THEN 'LONG' "
        "WHEN COALESCE(s.flow_m, 0) < 0 THEN 'SHORT' ELSE 'LONG' END AS direction",
        _sql_col(sig_cols, "time_session", "'UNKNOWN'"),
        _sql_col(sig_cols, "earnings_window", "'UNKNOWN'"),
        _sql_col(sig_cols, "news_flag", "'UNKNOWN'"),
        _sql_col(sig_cols, "catalyst_type", "'UNKNOWN'"),
        _sql_col(sig_cols, "sector", "'UNKNOWN'"),
        _sql_col(sig_cols, "rvol", "NULL"),
        _sql_col(sig_cols, "flow_m", "NULL"),
        _sql_col(sig_cols, "change_pct", "NULL"),
        _sql_col(sig_cols, "confidence", "NULL"),
    ]
    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM signals s
        ORDER BY s.timestamp DESC, s.rowid DESC
        LIMIT ?
        """,
        (limit,),
    )
    recent: list[dict[str, Any]] = []
    for row in rows:
        recent.append(
            {
                "signal_rowid": row["signal_rowid"],
                "horizon": _horizon(horizon),
                "symbol": _norm(row["symbol"]),
                "signal_ts": row["signal_ts"],
                "signal_type": _norm(row["signal_type"]),
                "direction": _norm(row["direction"]),
                "time_session": _norm(row["time_session"]),
                "earnings_window": _norm(row["earnings_window"]),
                "news_flag": _norm(row["news_flag"]),
                "catalyst_type": _norm(row["catalyst_type"]),
                "sector": _norm(row["sector"]),
                "rvol": _num(row["rvol"]),
                "flow_m": _num(row["flow_m"]),
                "change_pct": _num(row["change_pct"]),
                "confidence": _num(row["confidence"]),
            }
        )
    return recent


def split_rows(rows: list[dict[str, Any]], train_frac: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    if not rows:
        return [], [], None
    idx = max(1, min(len(rows) - 1, int(len(rows) * train_frac)))
    return rows[:idx], rows[idx:], rows[idx]["signal_ts"]


def add_stats(groups: dict[tuple[Any, ...], Stats], key: tuple[Any, ...], row: dict[str, Any]) -> None:
    groups.setdefault(key, Stats()).add(row["return_pct"], row["net_pct"])


def build_groups(rows: list[dict[str, Any]]) -> dict[str, dict[tuple[Any, ...], Stats]]:
    groups: dict[str, dict[tuple[Any, ...], Stats]] = {
        "horizon": {},
        "bucket": {},
        "context": {},
        "symbol": {},
        "symbol_context": {},
        "session": {},
        "earnings": {},
    }
    for row in rows:
        add_stats(groups["horizon"], (row["horizon"],), row)
        add_stats(groups["bucket"], (row["horizon"], row["signal_type"], row["direction"]), row)
        add_stats(
            groups["context"],
            (
                row["horizon"],
                row["signal_type"],
                row["direction"],
                row["time_session"],
                row["earnings_window"],
            ),
            row,
        )
        add_stats(groups["symbol"], (row["horizon"], row["symbol"], row["direction"]), row)
        add_stats(
            groups["symbol_context"],
            (row["horizon"], row["symbol"], row["signal_type"], row["direction"]),
            row,
        )
        add_stats(groups["session"], (row["horizon"], row["direction"], row["time_session"]), row)
        add_stats(groups["earnings"], (row["horizon"], row["direction"], row["earnings_window"]), row)
    return groups


def _weighted(parts: list[tuple[Stats | None, float]]) -> tuple[float, float, int, str]:
    net_num = 0.0
    win_num = 0.0
    denom = 0.0
    max_n = 0
    used: list[str] = []
    for stats, cap in parts:
        if not stats or not stats.n:
            continue
        weight = min(float(stats.n), cap)
        net_num += stats.net_avg * weight
        win_num += stats.win_pct * weight
        denom += weight
        max_n = max(max_n, stats.n)
        used.append(str(stats.n))
    if not denom:
        return 0.0, 0.0, 0, "no history"
    return net_num / denom, win_num / denom, max_n, "n=" + "/".join(used)


def score_row(
    groups: dict[str, dict[tuple[Any, ...], Stats]],
    row: dict[str, Any],
    min_sample: int,
    allow_threshold_pct: float,
    block_threshold_pct: float,
    min_win_pct: float,
) -> Score:
    horizon = row["horizon"]
    parts = [
        (groups["horizon"].get((horizon,)), 25.0),
        (groups["bucket"].get((horizon, row["signal_type"], row["direction"])), 90.0),
        (
            groups["context"].get(
                (
                    horizon,
                    row["signal_type"],
                    row["direction"],
                    row["time_session"],
                    row["earnings_window"],
                )
            ),
            75.0,
        ),
        (groups["symbol"].get((horizon, row["symbol"], row["direction"])), 35.0),
        (
            groups["symbol_context"].get(
                (horizon, row["symbol"], row["signal_type"], row["direction"])
            ),
            25.0,
        ),
        (groups["session"].get((horizon, row["direction"], row["time_session"])), 20.0),
        (groups["earnings"].get((horizon, row["direction"], row["earnings_window"])), 15.0),
    ]
    net_pct, win_pct, _, reason = _weighted(parts)
    specific_sample_n = max((stats.n for stats, _cap in parts[1:] if stats), default=0)
    if specific_sample_n < min_sample:
        decision = "NOT_ENOUGH_DATA"
    elif net_pct >= allow_threshold_pct and win_pct >= min_win_pct:
        decision = "ALLOW_RESEARCH"
    elif net_pct <= block_threshold_pct or win_pct < 45.0:
        decision = "BLOCK_RESEARCH"
    else:
        decision = "WATCH"
    return Score(net_pct, win_pct, specific_sample_n, reason, decision)


def _fmt_pct(value: float) -> str:
    return f"{value:+.4f}%"


def _stats_summary(stats: Stats) -> str:
    return (
        f"n={stats.n} net_avg={_fmt_pct(stats.net_avg)} "
        f"gross_avg={_fmt_pct(stats.gross_avg)} win%={stats.win_pct:.1f}"
    )


def _rank_group_lines(
    title: str,
    groups: dict[tuple[Any, ...], Stats],
    *,
    limit: int,
    min_sample: int,
    reverse: bool,
) -> list[str]:
    lines = ["", title]
    rows = [(key, stats) for key, stats in groups.items() if stats.n >= min_sample]
    rows.sort(key=lambda item: item[1].net_avg, reverse=reverse)
    if not rows:
        lines.append("  None yet")
        return lines
    for key, stats in rows[:limit]:
        lines.append(f"  {' | '.join(map(str, key))} | {_stats_summary(stats)}")
    return lines


def evaluate(
    groups: dict[str, dict[tuple[Any, ...], Stats]],
    rows: list[dict[str, Any]],
    min_sample: int,
    allow_threshold_pct: float,
    block_threshold_pct: float,
    min_win_pct: float,
) -> dict[str, dict[str, Stats]]:
    out: dict[str, dict[str, Stats]] = {}
    for row in rows:
        horizon = row["horizon"]
        score = score_row(
            groups,
            row,
            min_sample,
            allow_threshold_pct,
            block_threshold_pct,
            min_win_pct,
        )
        current_key = "current_allow" if current_guardrail(row) == "ALLOW" else "current_block"
        model_key = "model_allow" if score.decision == "ALLOW_RESEARCH" else "model_block"
        by_h = out.setdefault(
            horizon,
            {
                "all": Stats(),
                "current_allow": Stats(),
                "current_block": Stats(),
                "model_allow": Stats(),
                "model_block": Stats(),
                "model_not_enough": Stats(),
            },
        )
        for key in ("all", current_key, model_key):
            if key in by_h:
                by_h[key].add(row["return_pct"], row["net_pct"])
        if score.decision == "NOT_ENOUGH_DATA":
            by_h["model_not_enough"].add(row["return_pct"], row["net_pct"])
    return out


def build_report(
    *,
    db_path: Path = SIGNALS_DB,
    since: str = DEFAULT_SINCE,
    horizon: str = DEFAULT_HORIZON,
    min_sample: int = 25,
    recent_limit: int = 20,
    train_frac: float = 0.70,
    slippage_bps: float = 2.0,
    spread_bps: float = 3.0,
    allow_threshold_pct: float = 0.05,
    block_threshold_pct: float = -0.03,
    min_win_pct: float = 52.0,
) -> str:
    db_path = Path(db_path).expanduser()
    if not db_path.exists():
        return f"Signal DB not found: {db_path}"
    horizon = horizon if horizon in HORIZONS else DEFAULT_HORIZON
    cost = cost_pct(slippage_bps, spread_bps)

    with connect_ro(db_path) as conn:
        if not table_exists(conn, "signals") or not table_exists(conn, "signal_outcomes"):
            return f"Signal score DB is missing required tables: {db_path}"

        rows = load_labeled_rows(conn, since, cost)
        train_rows, test_rows, split_ts = split_rows(rows, train_frac)
        train_groups = build_groups(train_rows)
        full_groups = build_groups(rows)
        eval_rows = evaluate(
            train_groups,
            test_rows,
            min_sample,
            allow_threshold_pct,
            block_threshold_pct,
            min_win_pct,
        )
        recent_rows = load_recent_signals(conn, recent_limit, horizon)

    signal_ids = {row["signal_rowid"] for row in rows}
    usable_by_horizon = {}
    for row in rows:
        stats = usable_by_horizon.setdefault(row["horizon"], Stats())
        stats.add(row["return_pct"], row["net_pct"])

    lines = [
        "WOLFE SIGNAL SCORE LAB",
        f"Since: {since}",
        f"Rows: labeled_outcomes={len(rows)} unique_signals={len(signal_ids)} train={len(train_rows)} test={len(test_rows)}",
        f"Split timestamp: {split_ts or 'not enough data'}",
        f"Primary horizon for recent scoring: {horizon}",
        f"Costs: slippage={slippage_bps:g} bps/side, spread={spread_bps:g} bps, total={cost:.4f}%",
        f"Decision thresholds: allow net>={allow_threshold_pct:.3f}% and win%>={min_win_pct:.1f}; block net<={block_threshold_pct:.3f}% or win%<45",
        "",
        "Overall Net Data By Horizon",
    ]
    for h in HORIZONS:
        stats = usable_by_horizon.get(h)
        if stats:
            lines.append(f"  {h} | {_stats_summary(stats)}")

    lines.extend(
        _rank_group_lines(
            "Best Learned Context Buckets",
            full_groups["context"],
            limit=12,
            min_sample=min_sample,
            reverse=True,
        )
    )
    lines.extend(
        _rank_group_lines(
            "Worst Learned Context Buckets",
            full_groups["context"],
            limit=12,
            min_sample=min_sample,
            reverse=False,
        )
    )

    lines.append("")
    lines.append("Walk-Forward Guardrail Comparison")
    if not eval_rows:
        lines.append("  None yet")
    for h in HORIZONS:
        row = eval_rows.get(h)
        if not row:
            continue
        current = row["current_allow"]
        model = row["model_allow"]
        all_stats = row["all"]
        delta = model.net_sum - current.net_sum
        lines.append(
            f"  {h} | test_n={all_stats.n} | current_allow={current.n} "
            f"net_sum={_fmt_pct(current.net_sum)} avg={_fmt_pct(current.net_avg)} | "
            f"model_allow={model.n} net_sum={_fmt_pct(model.net_sum)} "
            f"avg={_fmt_pct(model.net_avg)} | delta={_fmt_pct(delta)} | "
            f"not_enough={row['model_not_enough'].n}"
        )

    lines.append("")
    lines.append("Recent Signal Scores")
    if not recent_rows:
        lines.append("  None")
    for row in recent_rows:
        score = score_row(
            full_groups,
            row,
            min_sample,
            allow_threshold_pct,
            block_threshold_pct,
            min_win_pct,
        )
        guardrail = current_guardrail(row)
        lines.append(
            f"  {row['signal_ts']} | {row['symbol']} | {row['direction']} | "
            f"{row['signal_type']} | {row['time_session']}/{row['earnings_window']} | "
            f"score={_fmt_pct(score.predicted_net_pct)} win%={score.predicted_win_pct:.1f} "
            f"sample={score.sample_n} | model={score.decision} | guardrail={guardrail}"
        )

    lines.extend(
        [
            "",
            "Read This As",
            "  This is read-only research. It does not place orders or modify the DB.",
            "  The model is an explainable smoothed-bucket scorer, not a black-box predictor.",
            "  Treat ALLOW_RESEARCH as a candidate for review/Discord reporting, not permission to trade.",
        ]
    )
    return "\n".join(lines)


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    groups: dict[str, dict[tuple[Any, ...], Stats]],
    min_sample: int,
    allow_threshold_pct: float,
    block_threshold_pct: float,
    min_win_pct: float,
) -> None:
    fields = [
        "signal_ts",
        "horizon",
        "symbol",
        "direction",
        "signal_type",
        "time_session",
        "earnings_window",
        "return_pct",
        "net_pct",
        "predicted_net_pct",
        "predicted_win_pct",
        "sample_n",
        "model_decision",
        "current_guardrail",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            score = score_row(
                groups,
                row,
                min_sample,
                allow_threshold_pct,
                block_threshold_pct,
                min_win_pct,
            )
            writer.writerow(
                {
                    "signal_ts": row["signal_ts"],
                    "horizon": row["horizon"],
                    "symbol": row["symbol"],
                    "direction": row["direction"],
                    "signal_type": row["signal_type"],
                    "time_session": row["time_session"],
                    "earnings_window": row["earnings_window"],
                    "return_pct": row["return_pct"],
                    "net_pct": row["net_pct"],
                    "predicted_net_pct": score.predicted_net_pct,
                    "predicted_win_pct": score.predicted_win_pct,
                    "sample_n": score.sample_n,
                    "model_decision": score.decision,
                    "current_guardrail": current_guardrail(row),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=SIGNALS_DB)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--horizon", choices=HORIZONS, default=DEFAULT_HORIZON)
    parser.add_argument("--min-sample", type=int, default=25)
    parser.add_argument("--recent-limit", type=int, default=20)
    parser.add_argument("--train-frac", type=float, default=0.70)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--spread-bps", type=float, default=3.0)
    parser.add_argument("--allow-threshold-pct", type=float, default=0.05)
    parser.add_argument("--block-threshold-pct", type=float, default=-0.03)
    parser.add_argument("--min-win-pct", type=float, default=52.0)
    parser.add_argument("--csv", type=Path, default=None, help="Optional scored holdout CSV export")
    args = parser.parse_args()

    report = build_report(
        db_path=args.db,
        since=args.since,
        horizon=args.horizon,
        min_sample=max(1, args.min_sample),
        recent_limit=max(1, args.recent_limit),
        train_frac=min(0.95, max(0.10, args.train_frac)),
        slippage_bps=max(0.0, args.slippage_bps),
        spread_bps=max(0.0, args.spread_bps),
        allow_threshold_pct=args.allow_threshold_pct,
        block_threshold_pct=args.block_threshold_pct,
        min_win_pct=args.min_win_pct,
    )
    print(report)

    if args.csv:
        with connect_ro(args.db) as conn:
            cost = cost_pct(max(0.0, args.slippage_bps), max(0.0, args.spread_bps))
            rows = load_labeled_rows(conn, args.since, cost)
        train_rows, test_rows, _ = split_rows(rows, min(0.95, max(0.10, args.train_frac)))
        groups = build_groups(train_rows)
        write_csv(
            args.csv,
            test_rows,
            groups,
            max(1, args.min_sample),
            args.allow_threshold_pct,
            args.block_threshold_pct,
            args.min_win_pct,
        )
        print(f"\nCSV written: {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
