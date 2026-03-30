#!/usr/bin/env python3
"""
Manual tweet poster — takes tweet text as a CLI argument.

Usage:
    python scripts/jobs/post_tweet.py "tweet text here"
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_daily import post_tweet  # noqa: E402

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/jobs/post_tweet.py \"tweet text\"", file=sys.stderr)
        sys.exit(1)

    text = sys.argv[1]
    tweet_id = post_tweet(text)
    print(tweet_id)
