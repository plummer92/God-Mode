import json
import time
from typing import Callable

import yfinance as yf


MARKET_CONTEXT_CACHE_SECONDS = 300
MARKET_CONTEXT_TICKERS = {
    "SPY": "spy_move_pct",
    "QQQ": "qqq_move_pct",
    "IWM": "iwm_move_pct",
    "^VIX": "vix",
    "USO": "oil_move_pct",
    "^TNX": "tnx",
}

PANIC_VIX_LEVEL = 30.0
RISK_OFF_VIX_ENTRY = 25.5   # OPEN → SELL_ONLY threshold (hysteresis high)
RISK_OFF_VIX_EXIT = 24.5    # SELL_ONLY → OPEN threshold (hysteresis low)
RISK_OFF_VIX_LEVEL = RISK_OFF_VIX_ENTRY  # legacy alias used by classify_market_state
RISK_ON_VIX_LEVEL = 20.0

PANIC_INDEX_DROP_PCT = -2.0
RISK_OFF_SPY_DROP_PCT = -0.75
RISK_OFF_QQQ_DROP_PCT = -1.00
RISK_OFF_IWM_DROP_PCT = -1.20

RISK_ON_SPY_RISE_PCT = 0.60
RISK_ON_QQQ_RISE_PCT = 0.80
RELIEF_RALLY_SPY_RISE_PCT = 1.00
RELIEF_RALLY_QQQ_RISE_PCT = 1.20
RELIEF_RALLY_IWM_RISE_PCT = 0.75
RELIEF_RALLY_VIX_DROP_PCT = -5.0

LONG_MULTIPLIERS = {
    "PANIC": 1.0,
    "RISK_OFF": 0.90,
    "NEUTRAL": 1.0,
    "RISK_ON": 1.05,
    "RELIEF_RALLY": 1.0,
}

SHORT_MULTIPLIERS = {
    "PANIC": 0.90,
    "RISK_OFF": 1.0,
    "NEUTRAL": 1.0,
    "RISK_ON": 0.90,
    "RELIEF_RALLY": 1.0,
}


_CACHE: dict | None = None
_CACHE_TS = 0.0
_LAST_FAILURE_LOG_TS = 0.0


def _safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _patch_snapshot(snapshot_path: str, updates: dict) -> None:
    """Merge `updates` into the existing snapshot JSON without overwriting other fields."""
    try:
        try:
            with open(snapshot_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        data.update(updates)
        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def _load_snapshot(snapshot_path: str) -> dict:
    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _snapshot_mode(snapshot: dict) -> str:
    for key in ("mode", "regime"):
        value = str(snapshot.get(key) or "").upper()
        if value in {"OPEN", "SELL_ONLY", "BLOCKED"}:
            return value
    return "OPEN"


def _base_context(snapshot: dict, prev_mode: str | None = None) -> dict:
    mode = classify_regime_mode(_safe_float(snapshot.get("vix")), snapshot_mode=_snapshot_mode(snapshot), prev_mode=prev_mode)
    return {
        "state": "NEUTRAL",
        "mode": mode,
        "final_regime": build_final_regime_label(mode, "NEUTRAL"),
        "spy_move_pct": None,
        "qqq_move_pct": None,
        "iwm_move_pct": None,
        "vix": _safe_float(snapshot.get("vix")),
        "vix_change_pct": None,
        "oil_symbol": "USO",
        "oil_move_pct": None,
        "tnx": _safe_float(snapshot.get("tnx")),
        "tnx_change_pct": None,
        "hyg_lqd_ratio": _safe_float(snapshot.get("hyg_lqd_ratio")),
        "put_call_ratio": _safe_float(snapshot.get("put_call_ratio")),
        "asof": snapshot.get("timestamp", ""),
        "source": "snapshot_fallback",
        "fetch_ok": False,
    }


def _extract_series_value(frame, symbol: str, field: str):
    if frame is None or getattr(frame, "empty", True):
        return None

    if getattr(frame.columns, "nlevels", 1) > 1:
        candidates = [(field, symbol), (symbol, field)]
        for column in candidates:
            if column in frame.columns:
                series = frame[column].dropna()
                if not series.empty:
                    return series
        return None

    if field in frame.columns:
        series = frame[field].dropna()
        if not series.empty:
            return series
    return None


def _compute_move_pct(series):
    if series is None or len(series) < 2:
        return None
    prev_close = _safe_float(series.iloc[-2])
    last_close = _safe_float(series.iloc[-1])
    if prev_close in (None, 0.0) or last_close is None:
        return None
    return ((last_close / prev_close) - 1.0) * 100.0


def _fetch_context(snapshot_path: str, prev_mode: str | None = None) -> dict:
    snapshot = _load_snapshot(snapshot_path)
    result = _base_context(snapshot, prev_mode=prev_mode)

    _CREDIT_TICKERS = ["HYG", "LQD", "^PCALL"]
    data = yf.download(
        list(MARKET_CONTEXT_TICKERS.keys()) + _CREDIT_TICKERS,
        period="5d",
        interval="1d",
        progress=False,
        auto_adjust=False,
        threads=False,
    )

    spy_close = _extract_series_value(data, "SPY", "Close")
    qqq_close = _extract_series_value(data, "QQQ", "Close")
    iwm_close = _extract_series_value(data, "IWM", "Close")
    vix_close = _extract_series_value(data, "^VIX", "Close")
    oil_close = _extract_series_value(data, "USO", "Close")
    tnx_close = _extract_series_value(data, "^TNX", "Close")

    result["spy_move_pct"] = _compute_move_pct(spy_close)
    result["qqq_move_pct"] = _compute_move_pct(qqq_close)
    result["iwm_move_pct"] = _compute_move_pct(iwm_close)
    result["oil_move_pct"] = _compute_move_pct(oil_close)
    result["tnx_change_pct"] = _compute_move_pct(tnx_close)

    if vix_close is not None and len(vix_close) >= 1:
        result["vix"] = _safe_float(vix_close.iloc[-1])
        result["vix_change_pct"] = _compute_move_pct(vix_close)
    if tnx_close is not None and len(tnx_close) >= 1:
        result["tnx"] = _safe_float(tnx_close.iloc[-1])

    # --- Credit stress & sentiment indicators (observation only, no trading logic) ---
    hyg_close = _extract_series_value(data, "HYG", "Close")
    lqd_close = _extract_series_value(data, "LQD", "Close")
    pcall_close = _extract_series_value(data, "^PCALL", "Close")

    hyg_lqd_ratio = None
    if hyg_close is not None and len(hyg_close) >= 1 and lqd_close is not None and len(lqd_close) >= 1:
        hyg = _safe_float(hyg_close.iloc[-1])
        lqd = _safe_float(lqd_close.iloc[-1])
        if hyg is not None and lqd is not None and lqd != 0.0:
            hyg_lqd_ratio = round(hyg / lqd, 4)

    put_call_ratio = None
    if pcall_close is not None and len(pcall_close) >= 1:
        put_call_ratio = _safe_float(pcall_close.iloc[-1])

    result["hyg_lqd_ratio"] = hyg_lqd_ratio
    result["put_call_ratio"] = put_call_ratio

    _patch_snapshot(snapshot_path, {
        "hyg_lqd_ratio": hyg_lqd_ratio,
        "put_call_ratio": put_call_ratio,
    })

    result["fetch_ok"] = any(
        result[key] is not None
        for key in ("spy_move_pct", "qqq_move_pct", "iwm_move_pct", "vix", "oil_move_pct", "tnx")
    )
    result["source"] = "yfinance" if result["fetch_ok"] else "snapshot_fallback"
    result["state"] = classify_market_state(result)
    result["mode"] = classify_regime_mode(result.get("vix"), snapshot_mode=_snapshot_mode(snapshot), prev_mode=prev_mode)
    result["final_regime"] = build_final_regime_label(result["mode"], result["state"])
    return result


def classify_market_state(context: dict) -> str:
    spy = _safe_float(context.get("spy_move_pct"))
    qqq = _safe_float(context.get("qqq_move_pct"))
    iwm = _safe_float(context.get("iwm_move_pct"))
    vix = _safe_float(context.get("vix"))
    vix_change = _safe_float(context.get("vix_change_pct"))
    mode = str(context.get("mode", "OPEN")).upper()

    if mode == "BLOCKED" or (vix is not None and vix >= PANIC_VIX_LEVEL):
        return "PANIC"

    if (
        spy is not None and qqq is not None and iwm is not None
        and spy <= PANIC_INDEX_DROP_PCT
        and qqq <= PANIC_INDEX_DROP_PCT
        and iwm <= PANIC_INDEX_DROP_PCT
    ):
        return "PANIC"

    if (
        spy is not None and qqq is not None and iwm is not None and vix_change is not None
        and spy >= RELIEF_RALLY_SPY_RISE_PCT
        and qqq >= RELIEF_RALLY_QQQ_RISE_PCT
        and iwm >= RELIEF_RALLY_IWM_RISE_PCT
        and vix_change <= RELIEF_RALLY_VIX_DROP_PCT
    ):
        return "RELIEF_RALLY"

    if mode == "SELL_ONLY" or (vix is not None and vix >= RISK_OFF_VIX_LEVEL):
        return "RISK_OFF"

    if (
        spy is not None and qqq is not None
        and spy <= RISK_OFF_SPY_DROP_PCT
        and qqq <= RISK_OFF_QQQ_DROP_PCT
    ):
        return "RISK_OFF"

    if spy is not None and iwm is not None and spy <= RISK_OFF_SPY_DROP_PCT and iwm <= RISK_OFF_IWM_DROP_PCT:
        return "RISK_OFF"

    if (
        spy is not None and qqq is not None and vix is not None
        and spy >= RISK_ON_SPY_RISE_PCT
        and qqq >= RISK_ON_QQQ_RISE_PCT
        and vix < RISK_ON_VIX_LEVEL
    ):
        return "RISK_ON"

    return "NEUTRAL"


def classify_regime_mode(vix, snapshot_mode: str | None = None, prev_mode: str | None = None) -> str:
    severity = {"OPEN": 0, "SELL_ONLY": 1, "BLOCKED": 2}
    normalized_snapshot_mode = str(snapshot_mode or "").upper()
    snapshot_level = severity.get(normalized_snapshot_mode, -1)
    level = _safe_float(vix)

    # Hysteresis: transitions use asymmetric thresholds to prevent flip-flops.
    prev = str(prev_mode or "OPEN").upper()
    if prev not in severity:
        prev = "OPEN"

    if level is None:
        vix_mode = "OPEN"
    elif level >= PANIC_VIX_LEVEL:
        vix_mode = "BLOCKED"
    elif prev == "BLOCKED":
        # Step down from BLOCKED once VIX < 30
        vix_mode = "SELL_ONLY"
    elif prev == "OPEN" and level >= RISK_OFF_VIX_ENTRY:
        vix_mode = "SELL_ONLY"
    elif prev == "SELL_ONLY" and level <= RISK_OFF_VIX_EXIT:
        vix_mode = "OPEN"
    else:
        vix_mode = prev  # hold current mode (hysteresis band)

    if snapshot_level > severity[vix_mode]:
        return normalized_snapshot_mode
    return vix_mode


def build_final_regime_label(mode: str, state: str) -> str:
    normalized_mode = str(mode or "OPEN").upper()
    normalized_state = str(state or "NEUTRAL").upper()
    return f"{normalized_mode}_{normalized_state}"


def get_market_context(snapshot_path: str, logger: Callable[[str], None] | None = None) -> dict:
    global _CACHE, _CACHE_TS, _LAST_FAILURE_LOG_TS

    now = time.time()
    if _CACHE is not None and (now - _CACHE_TS) < MARKET_CONTEXT_CACHE_SECONDS:
        return _CACHE

    prev_mode = _CACHE.get("mode") if _CACHE is not None else None
    try:
        context = _fetch_context(snapshot_path, prev_mode=prev_mode)
        _CACHE = context
        _CACHE_TS = now
        return context
    except Exception as exc:
        if logger and (now - _LAST_FAILURE_LOG_TS) >= MARKET_CONTEXT_CACHE_SECONDS:
            logger(f"⚠️ Market context unavailable: {exc} — defaulting to snapshot regime")
            _LAST_FAILURE_LOG_TS = now
        snapshot = _load_snapshot(snapshot_path)
        fallback = _base_context(snapshot, prev_mode=prev_mode)
        fallback["source"] = "neutral_fallback"
        _CACHE = fallback
        _CACHE_TS = now
        return fallback


def should_block_direction(context: dict, direction: str) -> tuple[bool, str]:
    state = str(context.get("state", "NEUTRAL")).upper()
    side = str(direction or "").upper()

    if state == "PANIC" and side == "LONG":
        return True, "panic tape blocks fresh longs"
    if state == "RISK_OFF" and side == "LONG":
        return True, "risk-off tape blocks fresh longs"
    if state == "RELIEF_RALLY" and side == "SHORT":
        return True, "relief rally blocks fresh shorts"
    return False, ""


def market_multiplier_for_direction(context: dict, direction: str) -> float:
    state = str(context.get("state", "NEUTRAL")).upper()
    side = str(direction or "").upper()
    if side == "LONG":
        return float(LONG_MULTIPLIERS.get(state, 1.0))
    return float(SHORT_MULTIPLIERS.get(state, 1.0))
