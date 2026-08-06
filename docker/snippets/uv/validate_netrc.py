#!/usr/bin/env python3
"""Validate a real .netrc used for uv / Docker private-index auth.

Catches a common footgun: a blank line followed by more #-comment lines makes
Python's netrc parser fail (uv then reports opaque "Missing credentials").

Usage:
  python3 docker/snippets/uv/validate_netrc.py [path]
  python3 docker/snippets/uv/validate_netrc.py --self-test
"""

from __future__ import annotations

import argparse
import netrc
import os
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

BLANK_THEN_COMMENT_HINT = (
    "A blank line appears before a #-comment line. Python's netrc parser treats "
    "that as a hard error (often 'bad toplevel token ...'), so credentials are "
    "never loaded and uv reports 'Missing credentials'.\n"
    "Fix: replace blank lines inside the comment header with a lone '#', and do "
    "not put #-comments after a blank line. See docker/snippets/uv/.netrc.example."
)


def _blank_then_comment_lines(text: str) -> List[int]:
    lines = text.splitlines()
    bad: List[int] = []
    prev_blank = False
    for i, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            prev_blank = True
            continue
        if prev_blank and stripped.startswith("#"):
            bad.append(i)
        prev_blank = False
    return bad


def validate(path: Path) -> Tuple[int, List[str]]:
    """Return (exit_code, errors)."""
    if not path.is_file():
        return 1, [f"netrc file not found: {path}"]

    text = path.read_text(encoding="utf-8")
    blank_comment_lines = _blank_then_comment_lines(text)

    try:
        netrc.netrc(str(path))
    except netrc.NetrcParseError as exc:
        errors = [f"failed to parse {path}: {exc}"]
        if blank_comment_lines:
            errors.append(
                f"likely cause: blank line(s) before #-comment at line(s) "
                f"{', '.join(map(str, blank_comment_lines))}.\n{BLANK_THEN_COMMENT_HINT}"
            )
        else:
            errors.append(
                "Tip: keep all #-comments contiguous at the top of the file "
                "(no blank lines before later comments). See "
                "docker/snippets/uv/.netrc.example."
            )
        return 1, errors

    if blank_comment_lines:
        return 1, [
            f"blank line(s) before #-comment at line(s) "
            f"{', '.join(map(str, blank_comment_lines))}.\n{BLANK_THEN_COMMENT_HINT}"
        ]

    return 0, []


def _self_test() -> int:
    cases = [
        (
            "ok_contiguous_comments",
            "# header\n# more\nmachine host.example login u password p\n",
            True,
        ),
        (
            "bad_blank_then_comment",
            "# header\n\n# Chainguard\nmachine host.example login u password p\n",
            False,
        ),
        (
            "ok_blank_between_machines",
            "machine a.example login u password p\n\nmachine b.example login u password p\n",
            True,
        ),
    ]
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        for name, content, expect_ok in cases:
            path = tmp_path / name
            path.write_text(content, encoding="utf-8")
            code, errors = validate(path)
            ok = code == 0
            if ok != expect_ok:
                failed += 1
                print(
                    f"FAIL {name}: expected_ok={expect_ok} got_ok={ok} errors={errors}",
                    file=sys.stderr,
                )
            else:
                print(f"ok: self-test {name}")
    if failed:
        print(f"error: {failed} self-test case(s) failed", file=sys.stderr)
        return 1
    print("ok: all self-tests passed")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        default=os.environ.get("DATAHUB_NETRC_PATH")
        or os.environ.get("NETRC")
        or str(Path(__file__).resolve().parent / ".netrc"),
        help="Path to .netrc (default: DATAHUB_NETRC_PATH / NETRC / docker/snippets/uv/.netrc)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print errors (no success line)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in regression checks and exit",
    )
    args = parser.parse_args(argv)

    if args.self_test:
        return _self_test()

    path = Path(args.path).expanduser()
    code, errors = validate(path)
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        print(
            f"error: invalid netrc at {path}\n"
            f"See docker/snippets/uv/.netrc.example, then re-run:\n"
            f"  python3 docker/snippets/uv/validate_netrc.py {path}",
            file=sys.stderr,
        )
    elif not args.quiet:
        print(f"ok: {path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
