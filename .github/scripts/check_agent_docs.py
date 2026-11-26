#!/usr/bin/env python3
"""Lint agent-instruction files: AGENTS.md, CLAUDE.md, BUGBOT.md, Cursor rules, .claude docs.

Formatters already cover these files. This checks what they cannot:

- a per-file line budget, so instruction files stay small enough to be followed;
- that every repo path written in backticks or as a relative Markdown link exists.

Findings recorded in the baseline file are tolerated so existing files can be
cleaned up incrementally. The baseline only shrinks: an entry that no longer
triggers is itself an error until removed with ``--update-baseline``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

AGENT_FILE_NAMES = {"AGENTS.md", "CLAUDE.md", "BUGBOT.md", "SKILL.md"}
# Auto-loaded instruction files. Skill reference material is read on demand and
# is not budgeted.
AGENT_PATH_GLOBS = (
    ".claude/agents/*.md",
    ".claude/commands/*.md",
    ".claude/rules/**/*.md",
    ".cursor/rules/*.mdc",
    ".github/copilot-instructions.md",
)
SKIPPED_PARTS = {"node_modules", "venv", ".venv", "build", "dist"}

LINE_BUDGETS = {"BUGBOT.md": 120}
DEFAULT_LINE_BUDGET = 200

BASELINE_FILE = Path(".github/scripts/check_agent_docs_baseline.txt")

FENCE_RE = re.compile(r"^[ \t]*(`{3,}|~{3,})")
BACKTICK_RE = re.compile(r"`([^`\n]+)`")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
PATH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
PATH_FORBIDDEN_CHARS = set("*<>{}$()=|?\"' ")
LINE_SUFFIX_RE = re.compile(r":\d+(-\d+)?$")


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    kind: str  # "budget" or "ref"
    key: str  # line count for budget, the dead token for ref
    message: str

    def __str__(self) -> str:
        return f"{self.path.as_posix()}:{self.line}: {self.message}"

    def baseline_entry(self) -> str:
        return f"{self.path.as_posix()}\t{self.kind}\t{self.key}"


def tracked_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def tracked_agent_files(root: Path, tracked: list[str]) -> list[Path]:
    files: list[Path] = []
    for posix in tracked:
        rel = Path(posix)
        if SKIPPED_PARTS.intersection(rel.parts):
            continue
        if rel.name in AGENT_FILE_NAMES or any(
            rel.match(glob) for glob in AGENT_PATH_GLOBS
        ):
            files.append(root / rel)
    return files


def _is_gitignored(path: str, root: Path) -> bool:
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--no-index", path],
        cwd=root,
        capture_output=True,
    )
    return result.returncode == 0


def line_budget_for(path: Path) -> int:
    return LINE_BUDGETS.get(path.name, DEFAULT_LINE_BUDGET)


def _is_path_candidate(token: str) -> bool:
    """A backticked token is checked only when it looks like a concrete repo path.

    Skipped: tokens without a slash, URLs, absolute/flag/import-like tokens,
    ellipses, single-segment directory names (`tests/`), and two-word phrases
    with no file extension (`try/finally`, `origin/master`).
    """
    if "/" not in token or "://" in token or "..." in token:
        return False
    if token.startswith(("/", "-", "@", "~")):
        return False
    if PATH_FORBIDDEN_CHARS.intersection(token):
        return False
    if not PATH_TOKEN_RE.match(token):
        return False
    segments = [s for s in token.split("/") if s]
    if len(segments) < 2:
        return False
    return "." in segments[-1] or token.endswith("/") or len(segments) >= 3


def _normalize_token(token: str) -> str:
    token = token.split("#", 1)[0]
    token = LINE_SUFFIX_RE.sub("", token)
    return token.rstrip("/")


def _resolves(token: str, file_path: Path, root: Path, tracked: list[str]) -> bool:
    """True when the token names something real: a file or directory relative to
    the instruction file or any of its parents, a gitignored (generated) path, or
    a path suffix that some tracked file contains (docs often name files by their
    tail, e.g. `source/sql/sql_config.py`)."""
    if not token:
        return True
    directory = file_path.parent
    while True:
        candidate = directory / token
        # Directory ignore patterns (`foo/`) only match a path with a trailing slash.
        if (
            candidate.exists()
            or _is_gitignored(str(candidate), root)
            or _is_gitignored(str(candidate) + "/", root)
        ):
            return True
        if directory == root or root not in directory.parents:
            break
        directory = directory.parent
    needle = "/" + token
    return any(("/" + path).find(needle) >= 0 for path in tracked)


def _referenced_tokens(lines: list[str]) -> Iterable[tuple[int, str]]:
    in_fence = False
    fence_marker = ""
    for index, line in enumerate(lines, start=1):
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)
            if not in_fence:
                in_fence, fence_marker = True, marker
            elif marker[0] == fence_marker[0] and len(marker) >= len(fence_marker):
                in_fence, fence_marker = False, ""
            continue
        if in_fence:
            continue
        for match in BACKTICK_RE.finditer(line):
            token = match.group(1).strip()
            if _is_path_candidate(token):
                yield index, token
        for match in MARKDOWN_LINK_RE.finditer(line):
            target = match.group(1)
            if target.startswith(("#", "/", "mailto:")) or "://" in target:
                continue
            yield index, target


def lint_file(
    path: Path, root: Path, tracked: list[str] | None = None
) -> list[Finding]:
    rel = path.relative_to(root)
    tracked = tracked if tracked is not None else []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return [Finding(rel, 1, "ref", "", "Unable to decode file as UTF-8")]

    findings: list[Finding] = []
    budget = line_budget_for(path)
    if len(lines) > budget:
        findings.append(
            Finding(
                rel,
                len(lines),
                "budget",
                str(len(lines)),
                f"{len(lines)} lines exceeds the {budget}-line budget for {path.name}; "
                "cut or signpost before adding",
            )
        )

    seen: set[str] = set()
    for line_no, token in _referenced_tokens(lines):
        normalized = _normalize_token(token)
        if normalized in seen or _resolves(normalized, path, root, tracked):
            continue
        seen.add(normalized)
        findings.append(
            Finding(
                rel,
                line_no,
                "ref",
                normalized,
                f"`{token}` does not exist relative to this file, its parents, or the repo root",
            )
        )
    return findings


def load_baseline(baseline_path: Path) -> set[str]:
    if not baseline_path.exists():
        return set()
    entries: set[str] = set()
    for raw in baseline_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            entries.add(line)
    return entries


def apply_baseline(
    findings: list[Finding], baseline: set[str]
) -> tuple[list[Finding], list[str]]:
    """Split findings into those not covered by the baseline, plus stale baseline entries.

    A budget entry records a ceiling: the file may shrink below it but not grow.
    """
    budget_ceilings: dict[str, int] = {}
    ref_entries: set[str] = set()
    for entry in baseline:
        parts = entry.split("\t")
        if len(parts) != 3:
            continue
        path, kind, key = parts
        if kind == "budget" and key.isdigit():
            budget_ceilings[path] = int(key)
        elif kind == "ref":
            ref_entries.add(entry)

    unexpected: list[Finding] = []
    used: set[str] = set()
    for finding in findings:
        posix = finding.path.as_posix()
        if finding.kind == "budget" and posix in budget_ceilings:
            ceiling = budget_ceilings[posix]
            used.add(f"{posix}\tbudget\t{ceiling}")
            if int(finding.key) > ceiling:
                unexpected.append(finding)
            continue
        entry = finding.baseline_entry()
        if entry in ref_entries:
            used.add(entry)
            continue
        unexpected.append(finding)

    stale = sorted(entry for entry in baseline if entry not in used)
    return unexpected, stale


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files to lint. Defaults to every tracked agent-instruction file.",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Rewrite the baseline file with the current findings and exit 0.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    baseline_path = root / BASELINE_FILE
    tracked = tracked_files(root)
    paths = (
        [Path(p) if Path(p).is_absolute() else root / p for p in args.paths]
        if args.paths
        else tracked_agent_files(root, tracked)
    )

    findings: list[Finding] = []
    for path in sorted(set(paths)):
        if path.exists():
            findings.extend(lint_file(path, root, tracked))

    if args.update_baseline:
        header = [
            "# Known findings tolerated by .github/scripts/check_agent_docs.py.",
            "# Format: <path>\\t<budget|ref>\\t<line count ceiling | dead token>.",
            "# Fix the finding and remove its line; never add new entries by hand.",
        ]
        body = sorted({f.baseline_entry() for f in findings})
        baseline_path.write_text("\n".join(header + body) + "\n", encoding="utf-8")
        print(f"Wrote {len(body)} baseline entries to {BASELINE_FILE.as_posix()}")
        return 0

    unexpected, stale = apply_baseline(findings, load_baseline(baseline_path))
    if unexpected or stale:
        print("Agent-instruction docs lint failed:", file=sys.stderr)
        for finding in unexpected:
            print(finding, file=sys.stderr)
        for entry in stale:
            print(
                f"{BASELINE_FILE.as_posix()}: stale entry no longer needed or too loose: "
                f"{entry.replace(chr(9), ' ')} (run with --update-baseline)",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
