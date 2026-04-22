#!/usr/bin/env python3
"""Project import bootstrap utilities."""

from __future__ import annotations

import sys


def ensure_trading_dev_first(trading_dev_dir: str) -> str:
    normalized = str(trading_dev_dir)
    sys.path[:] = [path for path in sys.path if path != normalized]
    sys.path.insert(0, normalized)
    return normalized
