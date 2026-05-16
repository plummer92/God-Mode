#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${GOD_MODE_DATA_DIR:-$HOME}"
EXPORT_DIR="${1:-$DATA_DIR/audit_exports}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$EXPORT_DIR/god-mode-audit-$STAMP.tgz"

mkdir -p "$EXPORT_DIR"

FILES=(
  "wolfe_signals.db"
  "strategy_lab.db"
  "market_intel.db"
  "approved_symbols.json"
  "candidate_symbols.json"
  "regime_snapshot.json"
  "signal_weights.json"
  "market_log.csv"
  "absorption_watchlist.csv"
  "absorption_resolutions.csv"
  "symbol_hunt_results.csv"
  "symbol_hunt_top20.json"
  "signal_outcomes.log"
  "strategy_lab.log"
  "market_observer.log"
)

existing=()
for file in "${FILES[@]}"; do
  if [[ -e "$DATA_DIR/$file" ]]; then
    existing+=("$file")
  fi
done

if [[ "${#existing[@]}" -eq 0 ]]; then
  echo "No audit artifacts found in $DATA_DIR" >&2
  exit 1
fi

tar -C "$DATA_DIR" -czf "$OUT" "${existing[@]}"
echo "$OUT"
