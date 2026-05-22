#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

section() {
  printf '\n===== %s =====\n' "$1"
}

run() {
  local title="$1"
  shift
  section "$title"
  "$@" 2>&1 || true
}

run "git" git --no-pager log --oneline -3
run "repo status" git status --short --branch
run "god mode status" python3 status.py

section "unit files"
for unit in godmode.service scheduled-reports.service market-observer.timer signal-outcomes.timer strategy-lab-run.timer; do
  systemctl cat "$unit" >/dev/null 2>&1 && echo "$unit: installed" || echo "$unit: missing"
done

section "timers"
systemctl list-timers --no-pager 2>&1 | grep -E 'market-observer|signal-outcomes|strategy-lab' || true

section "godmode processes"
pgrep -af 'godmode.py' || true

run "failed units" systemctl --failed --no-pager
run "memory" free -h
run "disk" df -h / /home

section "processes"
ps -eo pid,comm,%mem,%cpu,rss --sort=-rss | head -20 || true

section "sqlite counts"
python3 - <<'PY' 2>&1 || true
import sqlite3
from app_paths import DATA_DIR

db = DATA_DIR / "wolfe_signals.db"
print(f"db={db}")
if not db.exists():
    print("wolfe_signals.db not found")
    raise SystemExit

con = sqlite3.connect(db)
cur = con.cursor()
for label, sql in [
    ("signals", "SELECT COUNT(*) FROM signals"),
    ("signals_24h", "SELECT COUNT(*) FROM signals WHERE timestamp >= datetime('now', '-1 day')"),
    ("signal_outcomes", "SELECT COUNT(*) FROM signal_outcomes"),
    ("latest_signal", "SELECT MAX(timestamp) FROM signals"),
]:
    try:
        print(f"{label}: {cur.execute(sql).fetchone()[0]}")
    except Exception as exc:
        print(f"{label}: error: {exc}")
con.close()
PY

run "godmode logs" journalctl -u godmode.service -n 40 --no-pager
run "signal outcomes logs" journalctl -u signal-outcomes.service -n 40 --no-pager
run "strategy lab logs" journalctl -u strategy-lab-run.service -n 40 --no-pager
run "scheduled reports logs" journalctl -u scheduled-reports.service -n 30 --no-pager
