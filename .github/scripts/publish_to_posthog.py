#!/usr/bin/env python3
"""Read a metrics JSON file and publish events to PostHog."""

import argparse
import json
import os
import sys
from pathlib import Path

from utils.posthog.ci_reporter import PostHogCIReporter


def main() -> int:
    parser = argparse.ArgumentParser(description="Send workflow metrics to PostHog")
    parser.add_argument("--input", required=True, help="Path to metrics JSON file")
    args = parser.parse_args()

    api_key = os.environ.get("POSTHOG_API_KEY")
    if not api_key:
        print("Error: POSTHOG_API_KEY environment variable is not set", file=sys.stderr)
        return 1

    data = json.loads(Path(args.input).read_text())
    PostHogCIReporter(api_key).send(data)
    print(f"Metrics from {args.input} sent to PostHog")
    return 0


if __name__ == "__main__":
    sys.exit(main())
