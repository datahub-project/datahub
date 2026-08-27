#!/usr/bin/env python3
"""Lint Docusaurus-specific Markdown constructs that formatters do not validate."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ADMONITION_TYPES = {
    "caution",
    "danger",
    "important",
    "info",
    "note",
    "secondary",
    "success",
    "tip",
    "warning",
}

OPEN_DIRECTIVE_RE = re.compile(r"^([ \t]*)(:{3,})([A-Za-z][\w-]*)(?:\s+.*)?$")
CLOSE_DIRECTIVE_RE = re.compile(r"^([ \t]*)(:{3,})\s*$")
FENCE_RE = re.compile(r"^([ \t]*)(`{3,}|~{3,})")


@dataclass(frozen=True)
class LintError:
    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


@dataclass
class Directive:
    line: int
    indent: str
    marker: str
    kind: str


def _tracked_markdown_files(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "*.md", "*.mdx"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [root / line for line in result.stdout.splitlines() if line.strip()]


def _normalize_paths(paths: Iterable[str], root: Path) -> list[Path]:
    normalized: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.is_absolute():
            path = root / path
        if path.suffix not in {".md", ".mdx"}:
            continue
        if path.exists():
            normalized.append(path)
    return normalized


def lint_file(path: Path, root: Path) -> list[LintError]:
    errors: list[LintError] = []
    stack: list[Directive] = []
    in_fence = False
    fence_marker = ""
    in_html_comment = False

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return [
            LintError(
                path.relative_to(root),
                1,
                "Unable to decode Markdown file as UTF-8",
            )
        ]

    relative_path = path.relative_to(root)

    for index, line in enumerate(lines, start=1):
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(2)
            marker_char = marker[0]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker_char == fence_marker[0] and len(marker) >= len(fence_marker):
                in_fence = False
                fence_marker = ""
            continue

        if in_fence:
            continue

        if in_html_comment:
            if "-->" in line:
                in_html_comment = False
            continue

        if "<!--" in line:
            if "-->" not in line:
                in_html_comment = True
            continue

        close_match = CLOSE_DIRECTIVE_RE.match(line)
        if close_match:
            indent = close_match.group(1)
            marker = close_match.group(2)
            if not stack:
                errors.append(
                    LintError(
                        relative_path,
                        index,
                        "Docusaurus directive closing fence has no matching opening fence",
                    )
                )
                continue

            opening = stack.pop()
            if marker != opening.marker:
                errors.append(
                    LintError(
                        relative_path,
                        index,
                        f"Docusaurus directive closing fence uses {len(marker)} colons, "
                        f"but {opening.kind} opened on line {opening.line} with "
                        f"{len(opening.marker)} colons",
                    )
                )
            if indent != opening.indent:
                errors.append(
                    LintError(
                        relative_path,
                        index,
                        f"Docusaurus directive closing fence indentation does not match "
                        f"{opening.kind} opened on line {opening.line}",
                    )
                )
            continue

        open_match = OPEN_DIRECTIVE_RE.match(line)
        if open_match:
            indent = open_match.group(1)
            marker = open_match.group(2)
            kind = open_match.group(3)
            if kind not in ADMONITION_TYPES:
                errors.append(
                    LintError(
                        relative_path,
                        index,
                        f"Unknown Docusaurus admonition type '{kind}'",
                    )
                )
            stack.append(Directive(index, indent, marker, kind))

    for directive in stack:
        errors.append(
            LintError(
                relative_path,
                directive.line,
                f"Docusaurus directive '{directive.kind}' is missing a closing "
                f"{directive.marker} fence",
            )
        )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "paths",
        nargs="*",
        help="Markdown files to lint. Defaults to all tracked .md/.mdx files.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    paths = _normalize_paths(args.paths, root) if args.paths else _tracked_markdown_files(root)

    errors: list[LintError] = []
    for path in sorted(set(paths)):
        errors.extend(lint_file(path, root))

    if errors:
        print("Docusaurus Markdown lint failed:", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
