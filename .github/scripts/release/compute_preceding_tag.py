#!/usr/bin/env python3
"""
Compute the preceding tag to use as the base for GitHub release notes.

For `--notes-from-tag` in `gh release create`, we need the tag that immediately
precedes the one being cut so that "What's changed" covers only the new delta:

  - RC release: last RC for the same version; if this is rc1 (no prior RC),
    fall back to the last official release tag
  - Final release: last official release tag before this version

Tag parsing/classification lives in ``version_util``. Custom or
extra-component tags match neither pattern and are ignored when selecting
a predecessor.

If no preceding tag is found (e.g. very first release), an empty string is
written so the caller can skip passing --notes-from-tag entirely.

Usage:
  python3 compute_preceding_tag.py --tag <tag> --output-file /tmp/preceding_tag.txt

Environment variables:
  GH_TOKEN             GitHub token (read-only)
  GITHUB_REPOSITORY    Set automatically by GitHub Actions (e.g. org/repo)
"""

import argparse
import os
import subprocess
import sys
from typing import Optional

import version_util
from version_util import fetch_release_tags


def compute_preceding_tag(tag: str, all_tags: list[str]) -> Optional[str]:
    """
    Given the tag being cut, return the tag that should be used as
    ``--notes-from-tag``.  Returns None when no suitable predecessor exists.
    """
    rc = version_util.parse_rc(tag)
    if rc is not None:
        version, rc_num = rc

        if rc_num > 1:
            # Highest existing RC below rc_num; scanning tolerates deleted RCs.
            prior_rcs = [
                (parsed.rc, t)
                for t in all_tags
                if (parsed := version_util.parse_rc(t)) is not None
                and parsed.version == version
                and parsed.rc < rc_num
            ]
            if prior_rcs:
                return max(prior_rcs)[1]

        # rc1 (or no prior RC found): fall back to the last official release.
        return version_util.last_official_before(all_tags, version)

    official = version_util.parse_official(tag)
    if official is not None:
        return version_util.last_official_before(all_tags, official)

    print(
        f"Error: tag '{tag}' does not match any known pattern (RC or official).",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute the preceding tag for GitHub release notes."
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="The tag being cut.",
    )
    parser.add_argument(
        "--output-file",
        required=True,
        help="File to write the preceding tag to (empty string if none found).",
    )
    args = parser.parse_args()

    repo = os.environ["GITHUB_REPOSITORY"]

    try:
        all_tags = fetch_release_tags(repo)
    except subprocess.CalledProcessError as exc:
        print(f"Error: failed to fetch release tags: {exc}", file=sys.stderr)
        sys.exit(1)

    preceding = compute_preceding_tag(args.tag, all_tags)

    if preceding:
        print(f"Preceding tag for release notes: {preceding}")
    else:
        print(f"No preceding tag found for {args.tag}; --notes-from-tag will be omitted.")
        preceding = ""

    with open(args.output_file, "w") as f:
        f.write(preceding)


if __name__ == "__main__":
    main()
