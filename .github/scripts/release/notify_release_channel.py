#!/usr/bin/env python3
"""Post a message to a release line's Slack channel.

The channel is derived from the release version so every patch/RC of a line
posts to the same channel. Used by release automation to announce branch cuts
and Docker build starts.

Exits 0 on success, 2 on a usage error (missing token / unparseable version),
and 1 if the Slack API call fails.
"""

import argparse
import os
import sys

import version_util
from utils.slack_notifier import blockkit, slack_client


def main() -> int:
    args = _parse_args()

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("ERROR: SLACK_BOT_TOKEN environment variable is required.", file=sys.stderr)
        return 2

    channel = version_util.release_channel(args.version)
    if channel is None:
        print(f"ERROR: could not derive a release channel from version {args.version!r}.", file=sys.stderr)
        return 2

    text = f"{args.emoji} {args.message}" if args.emoji else args.message
    blocks = [blockkit.section(text)]

    try:
        slack_client.post_message(
            channel=channel,
            blocks=blocks,
            token=token,
            text=args.message,
        )
    except slack_client.SlackApiError as exc:
        print(f"ERROR: Slack notification to #{channel} failed: {exc}", file=sys.stderr)
        return 1

    print(f"Posted release notification to #{channel}.")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version",
        required=True,
        help="Release version or tag",
    )
    parser.add_argument(
        "--message",
        required=True,
        help="Message body (Slack mrkdwn supported, e.g. '<url|text>')",
    )
    parser.add_argument(
        "--emoji",
        default="",
        help="Optional leading emoji, e.g. ':rocket:'",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
