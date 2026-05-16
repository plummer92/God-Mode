#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

export GOD_MODE_DATA_DIR="${GOD_MODE_DATA_DIR:-$REPO_DIR}"
python3 -m streamlit run "$REPO_DIR/dashboard_db.py" --server.port 8501
