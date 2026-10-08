"""Command-line entry point."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional

import report_aspect_changes as rac

from rollback import pipeline, repo, report


_REF_VERSION_RE = re.compile(r"^v?(\d+(?:\.\d+){2,3})")


def ref_version(ref: str) -> Optional[tuple[int, ...]]:
    """Version from a release tag or branch name (e.g. v1.7.0.1, releases/v1.8.0)."""
    name = ref.removeprefix("refs/tags/").removeprefix("origin/")
    name = name.removeprefix("releases/").removeprefix("hotfixes/")
    m = _REF_VERSION_RE.match(name)
    return tuple(int(p) for p in m.group(1).split(".")) if m else None


def order_warning(
    current: str, target: str, current_sha: str, target_sha: str
) -> Optional[str]:
    """Warn when N looks older than N-1, i.e. the arguments are probably swapped.

    Compares release versions when both refs carry one. Otherwise falls back to
    commit dates, since release tags live on parallel branches and ancestry
    can't order them.
    """
    if current_sha == target_sha:
        return None
    cur_v, tgt_v = ref_version(current), ref_version(target)
    if cur_v is not None and tgt_v is not None:
        reversed_order = cur_v < tgt_v
    else:
        try:
            cur_t = int(repo.git("log", "-1", "--format=%ct", current_sha).strip())
            tgt_t = int(repo.git("log", "-1", "--format=%ct", target_sha).strip())
        except (subprocess.CalledProcessError, ValueError):
            return None
        reversed_order = cur_t < tgt_t
    if not reversed_order:
        return None
    return (
        f"Current (N) `{current}` is older than target (N-1) `{target}`. "
        f"--current and --target may be swapped; findings describe the "
        f"wrong direction."
    )


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=("Rollback compatibility report between two DataHub releases."),
    )
    parser.add_argument(
        "--current",
        required=True,
        help="Current build (N) — tag, branch, or SHA",
    )
    parser.add_argument(
        "--target",
        default=None,
        help=(
            "Target build (N-1) — tag, branch, or SHA (default: latest stable "
            "release tag — v*-cloud in acryl-fork repos, v* in OSS DataHub)"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write markdown report to this file (default: stdout)",
    )
    parser.add_argument(
        "--json",
        dest="emit_json",
        action="store_true",
        help="Also emit a JSON report alongside markdown",
    )
    parser.add_argument(
        "--repo-url",
        default=None,
        help=(
            "Web URL of the repository, for commit and PR links (default: the "
            "GitHub Actions repository, else the origin remote)"
        ),
    )
    args = parser.parse_args(argv)
    if args.emit_json and args.output and Path(args.output).suffix == ".json":
        # The JSON report goes next to the markdown with a .json suffix.
        parser.error(
            "with --json, --output is the markdown path; don't end it in .json"
        )

    # Same baseline rule as the PDL change report, so both tools agree on N-1.
    if args.target is None:
        args.target = rac.resolve_base()
        print(f"Resolved target (N-1): {args.target}", file=sys.stderr)
    args.current = repo.resolve_ref_name(args.current)
    args.target = repo.resolve_ref_name(args.target)

    findings, current_sha, target_sha = pipeline.run(
        args.current, args.target, args.repo_url or repo.repo_url()
    )
    warning = order_warning(args.current, args.target, current_sha, target_sha)
    if warning:
        print(f"Warning: {warning}", file=sys.stderr)

    md = report.render_rollback_report(
        findings, args.current, args.target, current_sha, target_sha, warning
    )

    if args.output:
        Path(args.output).write_text(md, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(md)

    if args.emit_json:
        json_path = str(Path(args.output or "rollback-report.md").with_suffix(".json"))
        json_out = report.render_json_report(
            findings,
            args.current,
            args.target,
            current_sha,
            target_sha,
            warning,
        )
        Path(json_path).write_text(json_out, encoding="utf-8")
        print(f"JSON report written to {json_path}", file=sys.stderr)
