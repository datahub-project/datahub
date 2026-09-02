#!/usr/bin/env python3
"""
Compute the next RC tag for a given release version.

Queries all GitHub releases for the repository and finds the highest existing
RC number for the given version, then outputs the next one.

Scoped to a specific version so parallel release cycles are tracked
independently.

Usage:
  python3 compute_next_rc.py --version 1.0.0 --output-file /tmp/next_rc_tag.txt

Environment variables:
  GH_TOKEN             GitHub token (read-only)
  GITHUB_REPOSITORY    Set automatically by GitHub Actions (e.g. org/repo)

Output:
  Writes the next RC tag to the file specified by --output-file.
"""

import argparse
import os
import subprocess
import sys

from release_variables import VERSION_SUFFIX
from utils import version_util
from utils.version_util import fetch_release_tags, git_tag_exists


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute the next RC tag for a release version.")
    parser.add_argument("--version", required=True, help="Release version without 'v' prefix (e.g. 1.0.0)")
    parser.add_argument("--output-file", required=True, help="File to write the computed tag to")
    args = parser.parse_args()

    repo = os.environ["GITHUB_REPOSITORY"]

    try:
        tags = fetch_release_tags(repo)
    except subprocess.CalledProcessError as exc:
        print(f"Error: failed to fetch release tags: {exc}", file=sys.stderr)
        sys.exit(1)

    target = version_util.parse_version_str(args.version)
    if target is None:
        print(
            f"Error: --version '{args.version}' must be in MAJOR.MINOR.HOTFIX form (e.g. 1.0.0).",
            file=sys.stderr,
        )
        sys.exit(1)

    rc_numbers = [
        parsed.rc
        for tag in tags
        if (parsed := version_util.parse_rc(tag)) is not None
        and parsed.version == target
    ]

    next_rc = max(rc_numbers) + 1 if rc_numbers else 1
    tag = f"v{args.version}rc{next_rc}{VERSION_SUFFIX}"

    if git_tag_exists(repo, tag):
        print(
            f"Error: git tag '{tag}' already exists without a corresponding release.\n"
            f"This is likely from a previous failed run. Delete the orphaned tag manually:\n"
            f"  gh api --method DELETE repos/{repo}/git/refs/tags/{tag}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Next RC tag: {tag}")

    with open(args.output_file, "w") as f:
        f.write(tag)


if __name__ == "__main__":
    main()
