#!/usr/bin/env python3
"""Lint agent-instruction files: AGENTS.md, CLAUDE.md, BUGBOT.md, Cursor rules, .claude docs.

Formatters already cover these files. This checks what they cannot:

- the size of the guide chain a coding agent loads for a directory: root
  ``AGENTS.md`` plus every nested ``AGENTS.md`` above it, capped in characters
  because that is what the agent pays, not file count;
- a per-file line budget for ``BUGBOT.md``, which a review bot loads separately;
- that every ``AGENTS.md`` has a sibling ``CLAUDE.md`` that is exactly ``@AGENTS.md``,
  and no ``CLAUDE.md`` carries content of its own;
- that every repo path written in backticks or as a relative Markdown link exists,
  and that no code fence is left open (it would hide every reference after it).

Findings recorded in the baseline file are tolerated so existing files can be
cleaned up incrementally. The baseline only shrinks: a ceiling that is no longer
reached is reported until lowered, and ``--update-baseline`` refuses to add
entries or raise a ceiling unless ``--allow-growth`` is given. A hand edit to the
baseline shows in the PR diff; review catches it.
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
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
EXACT_KINDS = {"ref", "shim", "fence"}


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    kind: str  # "stack", "budget", "shim", "fence" or "ref"
    key: str  # chars or lines for the size kinds, the dead token for ref, else ""
    message: str
    # Other files that share responsibility (the guide chain for a stack finding).
    related: tuple[Path, ...] = field(default=(), compare=False)

    def __str__(self) -> str:
        return f"{self.path.as_posix()}:{self.line}: {self.message}"

    def baseline_entry(self) -> str:
        return f"{self.path.as_posix()}\t{self.kind}\t{self.key}"


# --------------------------------------------------------------------------- git


def git(
    args: list[str], root: Path, stdin: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=root, input=stdin, capture_output=True, text=True
    )


def tracked_files(root: Path) -> list[str]:
    result = git(["ls-files"], root)
    result.check_returncode()
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

    One subprocess call. Negated patterns (``!foo``) are not matches. A directory
    pattern such as ``foo/`` only matches a path with a trailing slash, so callers
    pass both forms when they mean a directory.
    """
    if not paths:
        return {}
    result = git(
        ["check-ignore", "-v", "--no-index", "--stdin"], root, "\n".join(paths) + "\n"
    )
    # Exit code 1 means "nothing ignored"; 128 means "not a git repo".
    matches: dict[str, tuple[str, str]] = {}
    for line in result.stdout.splitlines():
        head, _, path = line.rpartition("\t")
        source_and_line, _, pattern = head.rpartition(":")
        source, _, _ = source_and_line.rpartition(":")
        if path and not pattern.startswith("!"):
            matches[path] = (source, pattern)
    return matches


def ignore_match_is_generated(file_rel: Path, source: str, pattern: str) -> bool:
    """Whether an ignore match says "this path is build output" rather than
    "this folder is ignored wholesale and tracked files are re-included".

    Rejected: negations, blanket rules (``.claude/*``, ``foo/**``), which exist to
    pair with negations, and any rule whose directory contains the linted file.
    """
    if pattern.startswith("!"):
        return False
    bare = pattern.rstrip("/")
    if bare.endswith(("/*", "/**")):
        return False
    base = Path(source).parent
    stem = bare.lstrip("/")
    if stem and not GLOB_CHARS.intersection(stem):
        covered = base / stem
        if covered == file_rel.parent or covered in file_rel.parent.parents:
            return False
    return True


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
                related=tuple(chain),
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


def collapse(posix: str) -> str | None:
    """Resolve ``..`` and ``.`` without the file system; None when it climbs above root."""
    parts: list[str] = []
    for part in posix.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def _candidates(token: str, file_rel: Path, is_link: bool) -> list[Path]:
    """Where a reference may point. A link resolves against the file's directory
    (or the repo root when it starts with ``/``); a backticked path may be written
    from the file's directory or any parent up to the repo root."""
    if is_link:
        bases = ["" if token.startswith("/") else file_rel.parent.as_posix()]
    else:
        bases = [p.as_posix() for p in [file_rel.parent, *file_rel.parent.parents]]
    resolved = [collapse(f"{b}/{token}") for b in bases]
    return [Path(r) for r in resolved if r is not None]


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


def referenced_tokens(
    lines: list[str],
) -> tuple[list[tuple[int, str, bool]], int | None]:
    """Return ``(tokens, open_fence_line)``: each token as ``(line, token, is_link)``,
    and the line of a code fence still open at end of file, if any."""
    tokens: list[tuple[int, str, bool]] = []
    in_fence = False
    fence_marker = ""
    fence_line = 0
    for index, line in enumerate(lines, start=1):
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)
            if not in_fence:
                in_fence, fence_marker, fence_line = True, marker, index
            elif marker[0] == fence_marker[0] and len(marker) >= len(fence_marker):
                in_fence, fence_marker = False, ""
            continue
        if in_fence:
            continue
        for match in BACKTICK_RE.finditer(line):
            token = match.group(1).strip()
            if is_path_candidate(token):
                tokens.append((index, token, False))
        targets = [m.group(1) for m in MARKDOWN_LINK_RE.finditer(line)]
        ref_def = REFERENCE_DEF_RE.match(line)
        if ref_def:
            targets.append(ref_def.group(1))
        for target in targets:
            if target.startswith(("#", "mailto:")) or "://" in target:
                continue
            tokens.append((index, target, True))
    return tokens, (fence_line if in_fence else None)


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

    tokens, open_fence = referenced_tokens(lines)
    if open_fence is not None:
        findings.append(
            Finding(
                rel,
                open_fence,
                "fence",
                "",
                "code fence opened here is never closed, so nothing after it is checked",
            )
        )

    # First pass: everything resolvable from the tracked file list. Second pass:
    # one batched git check-ignore for what is left, so generated (gitignored)
    # paths count, unless the match is a blanket rule rather than build output.
    unresolved: dict[str, tuple[int, str, bool, list[Path]]] = {}
    for line_no, token, is_link in tokens:
        normalized = normalize_token(token)
        if not normalized or normalized in unresolved:
            continue
        candidates = _candidates(normalized, rel, is_link)
        if any(tracked_path_exists(c, tracked_set) for c in candidates):
            continue
        if not is_link and tracked_tail_match(normalized, tracked):
            continue
        unresolved[normalized] = (line_no, token, is_link, candidates)

    probes = [
        form
        for _, _, _, candidates in unresolved.values()
        for c in candidates
        for form in (c.as_posix(), c.as_posix() + "/")
    ]
    ignored = gitignored(probes, root)

    for normalized, (line_no, token, is_link, candidates) in unresolved.items():
        generated = any(
            form in ignored and ignore_match_is_generated(rel, *ignored[form])
            for c in candidates
            for form in (c.as_posix(), c.as_posix() + "/")
        )
        if generated:
            continue
        where = (
            "relative to this file (Markdown links resolve against the file's directory)"
            if is_link
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
    """Split findings into those not covered by the baseline, plus stale entries.

    A stack or budget entry records a ceiling. It is stale when the size no longer
    reaches it, so a ceiling can only ever move down.
    """
    ceilings, exact = parse_baseline(baseline)
    unexpected: list[Finding] = []
    used: set[str] = set()
    for finding in findings:
        posix = finding.path.as_posix()
        if finding.kind in CEILING_KINDS and (posix, finding.kind) in ceilings:
            ceiling = ceilings[(posix, finding.kind)]
            if int(finding.key) >= ceiling:
                used.add(f"{posix}\t{finding.kind}\t{ceiling}")
            if int(finding.key) > ceiling:
                unexpected.append(finding)
            # below the ceiling: the entry is stale until lowered
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files to lint. Defaults to every tracked agent-instruction file. "
        "With explicit paths, stale baseline entries are not reported; shim and "
        "chain findings are reported when a given file is involved.",
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
    return parser


def main(argv: list[str] | None = None, root: Path | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.update_baseline and args.paths:
        parser.error("--update-baseline lints every tracked file; do not pass paths")
    if args.allow_growth and not args.update_baseline:
        parser.error("--allow-growth only applies with --update-baseline")

    root = (root or Path(__file__).resolve().parents[2]).resolve()
    baseline_path = root / BASELINE_FILE
    tracked = tracked_files(root)
    if args.paths:
        paths = [Path(p) if Path(p).is_absolute() else root / p for p in args.paths]
        missing = [p for p in paths if not p.exists()]
        if missing:
            parser.error("no such file: " + ", ".join(str(p) for p in missing))
    else:
        paths = tracked_agent_files(root, tracked)

    findings: list[Finding] = []
    for path in sorted(set(paths)):
        findings.extend(lint_file(path, root, tracked))
    wanted = {p.resolve().relative_to(root) for p in paths}
    for finding in [*lint_shims(root, tracked), *lint_stacks(root, tracked)]:
        involved = {finding.path, *finding.related}
        if not args.paths or involved & wanted:
            findings.append(finding)

    baseline = load_baseline(baseline_path)
    if args.update_baseline:
        if args.allow_growth:
            body, refused = {f.baseline_entry() for f in findings}, []
        else:
            body, refused = shrink_baseline(findings, baseline)
        header = [
            "# Known findings tolerated by .github/scripts/check_agent_docs.py.",
            "# Format: <path>\\t<stack|budget|shim|fence|ref>\\t<chars or lines ceiling | dead token | empty>.",
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
                f"{BASELINE_FILE.as_posix()}: entry no longer matches (fixed, or size now "
                f"below its ceiling); run --update-baseline: {entry.replace(chr(9), ' ')}",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
