#!/usr/bin/env python3
"""
Verify that a hotfix branch contains all commits from its corresponding release branch.

Calls the GitHub compare API server-side — no deep clone required.

Usage:
  python3 check_hotfix_sync.py --version 1.0.0 --repo org/repo [--summary-file /path/to/summary]

Environment variables:
  GH_TOKEN  GitHub token (read-only)
"""

import argparse
import json
import os
import subprocess
import sys

_BORDER = "━" * 60


def compare_branches(repo: str, base: str, head: str) -> dict:
    """Return the GitHub compare payload for base...head.

    Branch names containing '/' are percent-encoded so the URL path is unambiguous.
    """
    encoded_base = base.replace("/", "%2F")
    encoded_head = head.replace("/", "%2F")
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/compare/{encoded_base}...{encoded_head}"],
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"GitHub API returned unexpected response (not JSON): {result.stdout[:200]!r}"
        ) from exc


def _format_commit_line(commit: dict) -> str:
    sha = commit["sha"][:7]
    message = commit["commit"]["message"].splitlines()[0]
    date = commit["commit"]["committer"]["date"][:10]
    return f"  • {sha}  {message} ({date})"


def _write_summary(path: str, content: str) -> None:
    if path:
        with open(path, "a") as f:
            f.write(content + "\n")


def check_sync(version: str, repo: str, summary_file: str = "") -> int:
    """Return 0 if hotfix branch contains all release branch commits, 1 otherwise."""
    release_branch = f"releases/v{version}"
    hotfix_branch = f"hotfixes/v{version}"

    try:
        data = compare_branches(repo, hotfix_branch, release_branch)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or ""
        if "404" in stderr or "Not Found" in stderr:
            print(
                f"Error: branch '{release_branch}' or '{hotfix_branch}' does not exist. "
                "Run the Cut Branch workflow first."
            )
        else:
            print(f"Error calling GitHub API: {stderr or exc}")
        return 1

    ahead_by: int = data.get("ahead_by", 0)

    if ahead_by == 0:
        print(
            f"\n{_BORDER}\n"
            f"  ✓ Hotfix sync check passed\n"
            f"  {release_branch} has no commits missing from {hotfix_branch}\n"
            f"{_BORDER}\n"
        )
        return 0

    commits: list = data.get("commits", [])
    hidden = ahead_by - len(commits)

    commit_lines = "\n".join(_format_commit_line(c) for c in commits)
    hidden_note = (
        f"\n  ...and {hidden} more (API truncated list — verify, then re-run Cut Tag "
        f"with skip_hotfix_sync_check if this was a squash merge)"
        if hidden > 0
        else ""
    )

    body = (
        f"\n{_BORDER}\n"
        f"  HOTFIX SYNC CHECK FAILED\n"
        f"  {release_branch} has {ahead_by} commit(s) not yet in {hotfix_branch}\n"
        f"\n"
        f"  Missing commits:\n"
        f"{commit_lines}{hidden_note}\n"
        f"\n"
        f"  Action required:\n"
        f"    Ask engineering to merge {release_branch} into {hotfix_branch}\n"
        f"    so those commits are ancestors, then re-run Cut Tag.\n"
        f"    For a verified squash merge, re-run with skip_hotfix_sync_check.\n"
        f"{_BORDER}\n"
    )
    print(body)

    md_lines = [
        f"- `{c['sha'][:7]}` {c['commit']['message'].splitlines()[0]} "
        f"({c['commit']['committer']['date'][:10]})"
        for c in commits
    ]
    if hidden > 0:
        md_lines.append(f"\n...and {hidden} more (API truncated list)")

    md = (
        f"## ❌ Hotfix Sync Check Failed\n\n"
        f"`{release_branch}` has **{ahead_by}** commit(s) not yet in `{hotfix_branch}`\n\n"
        f"### Missing commits\n\n"
        + "\n".join(md_lines)
        + f"\n\n### Action required\n\n"
        f"Ask engineering to merge `{release_branch}` into `{hotfix_branch}` "
        f"so those commits are ancestors, then re-run Cut Tag. "
        f"For a verified squash merge, re-run with `skip_hotfix_sync_check`.\n"
    )
    _write_summary(summary_file, md)

    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify that a hotfix branch contains all commits from its release branch."
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Release version without 'v' prefix (e.g. 1.0.0)",
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="GitHub repository in org/repo format (e.g. acryldata/datahub-fork)",
    )
    parser.add_argument(
        "--summary-file",
        default="",
        help="Path to append Markdown summary to (e.g. $GITHUB_STEP_SUMMARY)",
    )
    args = parser.parse_args()

    if "GH_TOKEN" not in os.environ:
        print("Error: GH_TOKEN environment variable is not set.")
        sys.exit(1)

    sys.exit(check_sync(args.version, args.repo, args.summary_file))


if __name__ == "__main__":
    main()
