#!/usr/bin/env python3
"""Shared path helpers for running God-Mode from a single repo directory."""

from __future__ import annotations

import os
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent
HOME_DIR = Path.home()
DATA_DIR = Path(os.getenv("GOD_MODE_DATA_DIR", str(HOME_DIR))).expanduser()
ENV_FILE = Path(os.getenv("GOD_MODE_ENV_FILE", str(DATA_DIR / ".env"))).expanduser()
VENV_PYTHON = Path(
    os.getenv("GOD_MODE_VENV_PYTHON", str(HOME_DIR / "venv" / "bin" / "python3"))
).expanduser()


def data_path(*parts: str) -> str:
    return str(DATA_DIR.joinpath(*parts))


def repo_path(*parts: str) -> str:
    return str(REPO_DIR.joinpath(*parts))
