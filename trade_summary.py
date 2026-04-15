#!/usr/bin/env python3
"""Print or post an on-demand summary across all closed trades."""

from __future__ import annotations

import argparse

from reporting import build_trade_summary, post_to_discord


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--post-discord",
        action="store_true",
        help="Also post the summary to Discord.",
    )
    args = parser.parse_args()

    message = build_trade_summary()
    print(message)
    if args.post_discord:
        post_to_discord(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
