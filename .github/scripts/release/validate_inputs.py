#!/usr/bin/env python3
"""
Validate workflow_dispatch inputs for the Cut Branch and Cut Tag workflows.

Usage:
  python3 validate_inputs.py cut-branch --version 1.0.0 --branch-type release
  python3 validate_inputs.py cut-tag --version 1.0.0 --branch-type release --release-type rc
  python3 validate_inputs.py cut-tag --version 1.0.0 --branch-type hotfix --release-type final --hotfix-version 1.0.1
  python3 validate_inputs.py parse-ref --ref releases/v1.0.0

Environment variables (cut-branch / cut-tag):
  GH_TOKEN             GitHub token for duplicate release checks (read-only)
  GITHUB_REPOSITORY    Set automatically by GitHub Actions (e.g. org/repo)

Environment variables (parse-ref):
  GITHUB_OUTPUT        Set automatically by GitHub Actions; parsed values are
                       appended here when present.
"""

import argparse
import os
import subprocess
import sys

from release_variables import VERSION_SUFFIX
import version_util

# Cut Tag runs on the release branch, so branch_type/version come from its name.
# The version grammar itself is validated by version_util.
BRANCH_PREFIXES = {
    "releases/v": "release",
    "hotfixes/v": "hotfix",
}


def validate_version_format(version: str, label: str) -> None:
    if version_util.parse_version_str(version) is None:
        print(
            f"Error: {label} '{version}' is not valid. "
            "Expected format: X.Y.Z (e.g. 1.0.0, no 'v' prefix)"
        )
        sys.exit(1)


def release_exists(tag: str, repo: str) -> bool:
    result = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo],
        capture_output=True,
    )
    return result.returncode == 0


def validate_cut_branch(version: str, branch_type: str) -> None:
    validate_version_format(version, "version")

    print(f"Inputs valid: branch_type={branch_type}, version={version}")


def validate_cut_tag(
    version: str,
    branch_type: str,
    release_type: str,
    hotfix_version: str,
) -> None:
    repo = os.environ["GITHUB_REPOSITORY"]

    # ── version format ────────────────────────────────────────────────
    validate_version_format(version, "version")

    # ── hotfix-specific checks ────────────────────────────────────────
    if branch_type == "hotfix":
        if not hotfix_version:
            print("Error: 'hotfix_version' is required when branch_type is 'hotfix'.")
            sys.exit(1)

        validate_version_format(hotfix_version, "hotfix_version")

        v_major, v_minor, v_patch = version.split(".")
        h_major, h_minor, h_patch = hotfix_version.split(".")

        # major.minor must match — hotfixes only increment the patch
        if (v_major, v_minor) != (h_major, h_minor):
            version_prefix = f"{v_major}.{v_minor}"
            print(
                f"Error: hotfix_version '{hotfix_version}' must share the same major.minor "
                f"as version '{version}'.\n"
                f"Expected: {version_prefix}.x (e.g. {version_prefix}.1)"
            )
            sys.exit(1)

        # patch must be strictly greater
        if int(h_patch) <= int(v_patch):
            print(
                f"Error: hotfix_version patch ({h_patch}) must be greater than "
                f"version patch ({v_patch})."
            )
            sys.exit(1)

        # for final releases, tag must not already exist
        # (RC duplicates are handled by compute_next_rc.py)
        if release_type == "final":
            tag = f"v{hotfix_version}{VERSION_SUFFIX}"
            if release_exists(tag, repo):
                print(f"Error: release '{tag}' already exists. Cannot create a duplicate.")
                sys.exit(1)

    # ── final release duplicate check ─────────────────────────────────
    if branch_type == "release" and release_type == "final":
        tag = f"v{version}{VERSION_SUFFIX}"
        if release_exists(tag, repo):
            print(f"Error: release '{tag}' already exists. Cannot create a duplicate.")
            sys.exit(1)

    print(
        f"Inputs valid: branch_type={branch_type}, version={version}, "
        f"release_type={release_type}"
    )


def parse_release_ref(ref: str) -> tuple[str, str]:
    """Map the dispatch branch to ``(branch_type, version)``.

    ``releases/v{X.Y.Z}`` -> ``("release", "X.Y.Z")``,
    ``hotfixes/v{X.Y.Z}`` -> ``("hotfix", "X.Y.Z")``; any other ref exits
    non-zero, guarding that Cut Tag only runs on a release branch.
    """
    for prefix, branch_type in BRANCH_PREFIXES.items():
        if ref.startswith(prefix):
            version = ref[len(prefix) :]
            # Use the pattern (not parse_version_str) to check the shape without
            # emitting parse_version_str's warning on the invalid-ref path.
            if version_util.VERSION_STR_PATTERN.fullmatch(version):
                return branch_type, version

    print(
        f"Error: branch '{ref}' is not a release branch. The Cut Tag workflow "
        "must be dispatched from a 'releases/v{version}' or 'hotfixes/v{version}' "
        "branch (e.g. releases/v1.0.0, hotfixes/v1.0.1)."
    )
    sys.exit(1)


def emit_parsed_ref(ref: str) -> None:
    branch_type, version = parse_release_ref(ref)
    lines = [f"branch_type={branch_type}", f"version={version}"]

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as fh:
            fh.write("\n".join(lines) + "\n")

    # Always echo for the workflow log (and local runs without GITHUB_OUTPUT).
    print(f"Resolved branch_type={branch_type}, version={version} from ref '{ref}'")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate workflow_dispatch inputs for release workflows."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    branch = sub.add_parser("cut-branch", help="Validate Cut Branch inputs")
    branch.add_argument("--version", required=True, help="e.g. 1.0.0")
    branch.add_argument(
        "--branch-type", required=True, choices=["release", "hotfix"]
    )

    tag = sub.add_parser("cut-tag", help="Validate Cut Tag inputs")
    tag.add_argument("--version", required=True, help="e.g. 1.0.0")
    tag.add_argument("--branch-type", required=True, choices=["release", "hotfix"])
    tag.add_argument("--release-type", required=True, choices=["rc", "final"])
    tag.add_argument("--hotfix-version", default="", help="e.g. 1.0.1")

    ref = sub.add_parser(
        "parse-ref",
        help="Infer branch_type and version from the dispatch branch name",
    )
    ref.add_argument("--ref", required=True, help="e.g. releases/v1.0.0")

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "cut-branch":
        validate_cut_branch(args.version, args.branch_type)
    elif args.command == "parse-ref":
        emit_parsed_ref(args.ref)
    else:
        validate_cut_tag(args.version, args.branch_type, args.release_type, args.hotfix_version)


if __name__ == "__main__":
    main()
