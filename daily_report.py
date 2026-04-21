#!/usr/bin/env python3
"""Post the 4:00pm ET daily performance report to Discord or stdout."""

from __future__ import annotations

import argparse

from reporting import build_daily_report, post_to_discord


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        help="Trade date to report in YYYY-MM-DD format. Defaults to today in ET.",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Print the report without posting to Discord.",
    )
    args = parser.parse_args()

    message = build_daily_report(args.date)
    print(message)
    if not args.stdout_only:
        post_to_discord(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
