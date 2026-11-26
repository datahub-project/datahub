#!/usr/bin/env python3
"""Lint agent-instruction files: AGENTS.md, CLAUDE.md, BUGBOT.md, Cursor rules, .claude docs.

Formatters already cover these files. This checks what they cannot:

- the size of the guide chain a coding agent loads for a directory: root
  ``AGENTS.md`` plus every nested ``AGENTS.md`` above it, capped in characters
  because that is what the agent pays, not file count;
- a per-file line budget for ``BUGBOT.md``, which a review bot loads separately;
- that every ``AGENTS.md`` has a sibling ``CLAUDE.md`` that is exactly ``@AGENTS.md``,
  and no ``CLAUDE.md`` carries content of its own;
- that every repo path written in backticks or as a relative Markdown link exists.

Findings recorded in the baseline file are tolerated so existing files can be
cleaned up incrementally. The baseline only shrinks: ``--update-baseline`` drops
entries that no longer trigger and lowers ceilings, but refuses to add entries
or raise a ceiling unless ``--allow-growth`` is given explicitly.
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote

GUIDE_FILE_NAMES = {"AGENTS.md", "CLAUDE.md"}
AGENT_FILE_NAMES = GUIDE_FILE_NAMES | {"BUGBOT.md", "SKILL.md"}
# Auto-loaded instruction files. Skill reference material is read on demand and
# is not checked.
AGENT_PATH_GLOBS = (
    ".claude/agents/*.md",
    ".claude/commands/*.md",
    ".claude/rules/*.md",
    ".claude/rules/*/*.md",
    ".cursor/rules/*.mdc",
    ".github/copilot-instructions.md",
)
SKIPPED_PARTS = {"node_modules", "venv", ".venv", "build", "dist"}

# What a coding agent loads for one directory: the guide chain from the repo
# root down to that directory. ~1,200 lines at this corpus's density.
STACK_CHAR_BUDGET = 48_000
# A review bot loads BUGBOT.md on its own, next to the diff.
LINE_BUDGETS = {"BUGBOT.md": 120}
SHIM_CONTENT = "@AGENTS.md"

BASELINE_FILE = Path(".github/scripts/check_agent_docs_baseline.txt")

FENCE_RE = re.compile(r"^[ \t]*(`{3,}|~{3,})")
BACKTICK_RE = re.compile(r"`([^`\n]+)`")
# [text](target), [text](target "title"), and reference definitions [label]: target
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REFERENCE_DEF_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+\"[^\"]*\")?\s*$")
PATH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
LINE_SUFFIX_RE = re.compile(r":\d+(-\d+)?$")
GLOB_CHARS = set("*?[")

CEILING_KINDS = {"stack", "budget"}
EXACT_KINDS = {"ref", "shim"}


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    kind: str  # "stack", "budget", "shim" or "ref"
    key: str  # chars or lines for the size kinds, the dead token for ref, "" for shim
    message: str

    def __str__(self) -> str:
        return f"{self.path.as_posix()}:{self.line}: {self.message}"

    def baseline_entry(self) -> str:
        return f"{self.path.as_posix()}\t{self.kind}\t{self.key}"


# --------------------------------------------------------------------------- git


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
            fnmatch.fnmatchcase(posix, glob) for glob in AGENT_PATH_GLOBS
        ):
            files.append(root / rel)
    return files


def gitignored(paths: list[str], root: Path) -> dict[str, tuple[str, str]]:
    """Map each ignored path in ``paths`` to ``(gitignore source, pattern)``.

    One subprocess call. A directory pattern such as ``foo/`` only matches a path
    with a trailing slash, so callers pass both forms when they mean a directory.
    """
    if not paths:
        return {}
    result = subprocess.run(
        ["git", "check-ignore", "-v", "--no-index", "--stdin"],
        cwd=root,
        input="\n".join(paths) + "\n",
        capture_output=True,
        text=True,
    )
    # Exit code 1 means "nothing ignored"; 128 means "not a git repo".
    matches: dict[str, tuple[str, str]] = {}
    for line in result.stdout.splitlines():
        head, _, path = line.rpartition("\t")
        source_and_line, _, pattern = head.rpartition(":")
        source, _, _ = source_and_line.rpartition(":")
        if path:
            matches[path] = (source, pattern)
    return matches


def pattern_covers_file(file_rel: Path, source: str, pattern: str) -> bool:
    """Whether an ignore pattern is a blanket rule over the linted file's own
    directory (``.claude/*`` for a file under ``.claude/``). The file is tracked
    only thanks to a negation, so such a match says nothing about the probe."""
    base = Path(source).parent
    stem = pattern.lstrip("!/")
    while stem.endswith(("/*", "/**", "/")):
        stem = stem.rsplit("/", 1)[0]
    if not stem or GLOB_CHARS.intersection(stem):
        return False
    covered = base / stem
    return covered == file_rel.parent or covered in file_rel.parent.parents


# ------------------------------------------------------------------- guides/shims


def is_shim(text: str) -> bool:
    """A CLAUDE.md whose whole content is the import line."""
    return text.strip() == SHIM_CONTENT


def _read(root: Path, rel: Path) -> str | None:
    try:
        return (root / rel).read_text("utf-8")
    except (FileNotFoundError, UnicodeDecodeError):
        return None


def lint_shims(root: Path, tracked: list[str]) -> list[Finding]:
    """Every AGENTS.md has a sibling CLAUDE.md that is exactly the shim, and no
    CLAUDE.md exists on its own or carries content of its own."""
    guides = {Path(p) for p in tracked if Path(p).name in GUIDE_FILE_NAMES}
    guides = {g for g in guides if not SKIPPED_PARTS.intersection(g.parts)}
    findings: list[Finding] = []
    for guide in sorted(guides):
        sibling = guide.with_name(
            "CLAUDE.md" if guide.name == "AGENTS.md" else "AGENTS.md"
        )
        if guide.name == "AGENTS.md":
            text = _read(root, sibling) if sibling in guides else None
            if sibling not in guides:
                msg = f"no {sibling.name} beside it; add one containing exactly `{SHIM_CONTENT}`"
            elif text is None or not is_shim(text):
                msg = f"{sibling.name} must contain exactly `{SHIM_CONTENT}`; move its content here"
            else:
                continue
        elif sibling not in guides:
            msg = "guides live in AGENTS.md; rename this file and leave a shim"
        else:
            continue
        findings.append(Finding(guide, 1, "shim", "", msg))
    return findings


def guide_chains(tracked: list[str]) -> dict[Path, list[Path]]:
    """Map each directory that holds a guide to the guides an agent loads there:
    every AGENTS.md from the repo root down to it."""
    guides_by_dir: dict[Path, list[Path]] = {}
    for posix in tracked:
        rel = Path(posix)
        if rel.name != "AGENTS.md" or SKIPPED_PARTS.intersection(rel.parts):
            continue
        guides_by_dir.setdefault(rel.parent, []).append(rel)

    chains: dict[Path, list[Path]] = {}
    for directory in guides_by_dir:
        chain: list[Path] = []
        for ancestor in [*reversed(directory.parents), directory]:
            chain.extend(sorted(guides_by_dir.get(ancestor, [])))
        chains[directory] = chain
    return chains


def lint_stacks(root: Path, tracked: list[str]) -> list[Finding]:
    findings: list[Finding] = []
    for directory, chain in sorted(guide_chains(tracked).items()):
        sizes = {g: len(_read(root, g) or "") for g in chain}
        total = sum(sizes.values())
        if total <= STACK_CHAR_BUDGET:
            continue
        detail = " + ".join(f"{g.as_posix()} ({n:,})" for g, n in sizes.items())
        findings.append(
            Finding(
                chain[-1],
                1,
                "stack",
                str(total),
                f"guide chain for {directory.as_posix() or '.'} is {total:,} chars, "
                f"over the {STACK_CHAR_BUDGET:,} budget an agent loads here: {detail}",
            )
        )
    return findings


# ------------------------------------------------------------------- references


def normalize_token(token: str) -> str:
    token = token.split("#", 1)[0].split("?", 1)[0]
    token = LINE_SUFFIX_RE.sub("", token)
    return unquote(token).rstrip("/")


def is_path_candidate(token: str) -> bool:
    """Whether a backticked token looks like a concrete repo path worth checking.

    Skipped: tokens without a slash, URLs, absolute/flag/import-like tokens,
    ellipses, single-segment directory names (``tests/``), and two-word phrases
    with no file extension (``try/finally``, ``origin/master``). A ``:line``
    suffix or ``#anchor`` is stripped first.
    """
    if "/" not in token or "://" in token or "..." in token:
        return False
    if token.startswith(("/", "-", "@", "~")):
        return False
    if not PATH_TOKEN_RE.match(normalize_token(token)):
        return False
    segments = [s for s in token.split("/") if s]
    if len(segments) < 2:
        return False
    return "." in segments[-1] or token.endswith("/") or len(segments) >= 3


def _collapse(posix: str) -> str:
    """Resolve ``..`` and ``.`` segments without touching the file system."""
    parts: list[str] = []
    for part in posix.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def _parent_candidates(token: str, file_rel: Path) -> list[Path]:
    """The token resolved against the file's directory and every parent up to root,
    with ``..`` segments collapsed so the result can be looked up in git ls-files."""
    return [
        Path(_collapse(f"{parent.as_posix()}/{token}"))
        for parent in [file_rel.parent, *file_rel.parent.parents]
    ]


def tracked_path_exists(candidate: Path, tracked_set: set[str]) -> bool:
    """Case-sensitive existence against ``git ls-files``: a tracked file, or a
    directory some tracked file lives under."""
    posix = candidate.as_posix()
    if posix in (".", ""):
        return True
    return posix in tracked_set or any(t.startswith(posix + "/") for t in tracked_set)


def tracked_tail_match(token: str, tracked: list[str]) -> bool:
    """Whether some tracked path equals ``token`` or ends with it at a segment
    boundary (docs often name files by their tail, e.g. ``source/sql/sql_config.py``)."""
    needle = "/" + token
    for path in tracked:
        haystack = "/" + path
        if (
            haystack == needle
            or haystack.endswith(needle)
            or (needle + "/") in haystack
        ):
            return True
    return False


def referenced_tokens(lines: list[str]) -> Iterable[tuple[int, str, bool]]:
    """Yield ``(line, token, is_link)``. A Markdown link target resolves only
    against the file's own directory (or the repo root when it starts with ``/``),
    as GitHub renders it; a backticked path may be written from the repo root or
    any parent, as these docs do."""
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
            if is_path_candidate(token):
                yield index, token, False
        targets = [m.group(1) for m in MARKDOWN_LINK_RE.finditer(line)]
        ref_def = REFERENCE_DEF_RE.match(line)
        if ref_def:
            targets.append(ref_def.group(1))
        for target in targets:
            if target.startswith(("#", "mailto:")) or "://" in target:
                continue
            yield index, target, True


def lint_file(path: Path, root: Path, tracked: list[str]) -> list[Finding]:
    rel = path.relative_to(root)
    text = _read(root, rel)
    if text is None:
        return [Finding(rel, 1, "ref", "", "Unable to read file as UTF-8")]
    lines = text.splitlines()
    tracked_set = set(tracked)

    findings: list[Finding] = []
    budget = LINE_BUDGETS.get(path.name)
    if budget is not None and len(lines) > budget:
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

    # First pass: everything resolvable from the tracked file list. Second pass:
    # one batched git check-ignore for what is left, so generated (gitignored)
    # paths count, unless the match is a blanket rule over the file's own folder.
    unresolved: dict[str, tuple[int, str, list[Path]]] = {}
    for line_no, token, is_link in referenced_tokens(lines):
        normalized = normalize_token(token)
        if not normalized or normalized in unresolved:
            continue
        if is_link:
            base = "" if normalized.startswith("/") else rel.parent.as_posix()
            candidates = [Path(_collapse(f"{base}/{normalized}"))]
        else:
            candidates = _parent_candidates(normalized, rel)
        if any(tracked_path_exists(c, tracked_set) for c in candidates):
            continue
        if not is_link and tracked_tail_match(normalized, tracked):
            continue
        unresolved[normalized] = (line_no, token, candidates)

    probes = [
        form
        for _, _, candidates in unresolved.values()
        for c in candidates
        for form in (c.as_posix(), c.as_posix() + "/")
    ]
    ignored = gitignored(probes, root)

    for normalized, (line_no, token, candidates) in unresolved.items():
        generated = any(
            form in ignored and not pattern_covers_file(rel, *ignored[form])
            for c in candidates
            for form in (c.as_posix(), c.as_posix() + "/")
        )
        if generated:
            continue
        where = (
            "relative to this file (Markdown links resolve against the file's directory)"
            if len(candidates) == 1
            else "relative to this file, its parents, or the repo root"
        )
        findings.append(
            Finding(
                rel, line_no, "ref", normalized, f"`{token}` does not exist {where}"
            )
        )
    return findings


# --------------------------------------------------------------------- baseline


def load_baseline(baseline_path: Path) -> set[str]:
    if not baseline_path.exists():
        return set()
    entries: set[str] = set()
    for raw in baseline_path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip("\r\n")
        if line.strip() and not line.lstrip().startswith("#"):
            entries.add(line)
    return entries


def parse_baseline(baseline: set[str]) -> tuple[dict[tuple[str, str], int], set[str]]:
    ceilings: dict[tuple[str, str], int] = {}
    exact: set[str] = set()
    for entry in baseline:
        parts = entry.split("\t")
        if len(parts) != 3:
            continue
        path, kind, key = parts
        if kind in CEILING_KINDS and key.isdigit():
            ceilings[(path, kind)] = int(key)
        elif kind in EXACT_KINDS:
            exact.add(entry)
    return ceilings, exact


def apply_baseline(
    findings: list[Finding], baseline: set[str]
) -> tuple[list[Finding], list[str]]:
    """Split findings into those not covered by the baseline, plus stale baseline entries.

    A stack or budget entry records a ceiling: the size may shrink but not grow.
    """
    ceilings, exact = parse_baseline(baseline)
    unexpected: list[Finding] = []
    used: set[str] = set()
    for finding in findings:
        posix = finding.path.as_posix()
        if finding.kind in CEILING_KINDS and (posix, finding.kind) in ceilings:
            ceiling = ceilings[(posix, finding.kind)]
            used.add(f"{posix}\t{finding.kind}\t{ceiling}")
            if int(finding.key) > ceiling:
                unexpected.append(finding)
            continue
        entry = finding.baseline_entry()
        if entry in exact:
            used.add(entry)
            continue
        unexpected.append(finding)

    stale = sorted(entry for entry in baseline if entry not in used)
    return unexpected, stale


def shrink_baseline(
    findings: list[Finding], baseline: set[str]
) -> tuple[set[str], list[str]]:
    """The new baseline: current findings, but never an entry the old baseline did
    not have and never a ceiling above the old one. Returns it with the list of
    findings that could not be admitted."""
    ceilings, exact = parse_baseline(baseline)
    admitted: set[str] = set()
    refused: list[str] = []
    for finding in findings:
        posix = finding.path.as_posix()
        if finding.kind in CEILING_KINDS:
            old = ceilings.get((posix, finding.kind))
            if old is None or int(finding.key) > old:
                refused.append(finding.baseline_entry())
            else:
                admitted.add(finding.baseline_entry())
        elif finding.baseline_entry() in exact:
            admitted.add(finding.baseline_entry())
        else:
            refused.append(finding.baseline_entry())
    return admitted, sorted(set(refused))


# ------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files to lint. Defaults to every tracked agent-instruction file. "
        "With explicit paths, stale baseline entries are not reported and only "
        "shim and chain findings attributed to a given file are checked.",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Rewrite the baseline from a run over every tracked file: drop stale "
        "entries and lower ceilings. New entries or raised ceilings are refused "
        "unless --allow-growth is given. Cannot be combined with explicit paths.",
    )
    parser.add_argument(
        "--allow-growth",
        action="store_true",
        help="With --update-baseline: also admit new findings and raised ceilings.",
    )
    args = parser.parse_args(argv)
    if args.update_baseline and args.paths:
        parser.error("--update-baseline lints every tracked file; do not pass paths")
    if args.allow_growth and not args.update_baseline:
        parser.error("--allow-growth only applies with --update-baseline")

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
    wanted = {p.resolve().relative_to(root) for p in paths if p.exists()}
    findings.extend(
        f
        for f in [*lint_shims(root, tracked), *lint_stacks(root, tracked)]
        if not args.paths or f.path in wanted
    )

    baseline = load_baseline(baseline_path)
    if args.update_baseline:
        if args.allow_growth:
            body, refused = {f.baseline_entry() for f in findings}, []
        else:
            body, refused = shrink_baseline(findings, baseline)
        header = [
            "# Known findings tolerated by .github/scripts/check_agent_docs.py.",
            "# Format: <path>\\t<stack|budget|shim|ref>\\t<chars or lines ceiling | dead token | empty>.",
            "# Fix the finding and remove its line; never add new entries by hand.",
        ]
        baseline_path.write_text(
            "\n".join(header + sorted(body)) + "\n", encoding="utf-8"
        )
        print(f"Wrote {len(body)} baseline entries to {BASELINE_FILE.as_posix()}")
        if refused:
            print(
                "Not admitted (fix them, or rerun with --allow-growth):",
                file=sys.stderr,
            )
            for entry in refused:
                print("  " + entry.replace("\t", " "), file=sys.stderr)
            return 1
        return 0

    unexpected, stale = apply_baseline(findings, baseline)
    if args.paths:
        stale = []  # a subset run cannot tell whether other entries still apply
    if unexpected or stale:
        print("Agent-instruction docs lint failed:", file=sys.stderr)
        for finding in unexpected:
            print(finding, file=sys.stderr)
        for entry in stale:
            print(
                f"{BASELINE_FILE.as_posix()}: entry no longer triggers; delete this line: "
                f"{entry.replace(chr(9), ' ')}",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
