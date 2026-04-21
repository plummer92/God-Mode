#!/usr/bin/env python3
"""Post the 9:30am ET morning briefing to Discord or stdout."""

from __future__ import annotations

import argparse

from reporting import build_morning_brief, post_to_discord


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Print the briefing without posting to Discord.",
    )
    args = parser.parse_args()

    message = build_morning_brief()
    print(message)
    if not args.stdout_only:
        post_to_discord(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
