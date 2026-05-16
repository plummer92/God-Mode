$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repo

if (-not $env:GOD_MODE_DATA_DIR -or $env:GOD_MODE_DATA_DIR.Trim() -eq "") {
    $env:GOD_MODE_DATA_DIR = $repo
}

python -m streamlit run "$repo\dashboard_db.py" --server.port 8501
