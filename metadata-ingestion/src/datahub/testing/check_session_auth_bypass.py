"""Lint check: ban copying a requests session's headers to authenticate a request.

Copying ``session.headers`` into another request (``dict(graph._session.headers)``,
``{**session.headers}``, ``session.headers.copy()``) only carries credentials that
are baked into the headers — i.e. a static token. An OAuth token provider is
installed as ``session.auth`` and applies the Authorization header per request,
so any request built from copied headers goes out unauthenticated (401) the
moment a deployment switches to OAuth. Make the request through the session
itself instead (``session.get(...)``), or pass the session's ``auth`` object.

This has caused real production-shaped bugs (the DataHub Cloud events consumer
polled with copied headers and 401'd under OAuth), so it is enforced as a test.
"""

from __future__ import annotations

import ast
import pathlib
from typing import List, Union


def _mentions_session(node: ast.expr) -> bool:
    return "session" in ast.unparse(node).lower()


def _is_headers_attr_of_session(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "headers"
        and _mentions_session(node.value)
    )


def find_session_header_copies(
    paths: List[Union[str, pathlib.Path]],
) -> List[str]:
    """Return ``file:line: snippet`` for every session-header copy found."""
    findings: List[str] = []
    for path in paths:
        root = pathlib.Path(path)
        files = [root] if root.is_file() else sorted(root.rglob("*.py"))
        for file in files:
            if file.name == "check_session_auth_bypass.py":
                continue
            try:
                tree = ast.parse(file.read_text(), filename=str(file))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Call, ast.Dict)):
                    continue
                bad = False
                if isinstance(node, ast.Call):
                    func = node.func
                    # dict(session.headers)
                    if (
                        isinstance(func, ast.Name)
                        and func.id == "dict"
                        and len(node.args) == 1
                        and _is_headers_attr_of_session(node.args[0])
                    ):
                        bad = True
                    # session.headers.copy()
                    if (
                        isinstance(func, ast.Attribute)
                        and func.attr == "copy"
                        and _is_headers_attr_of_session(func.value)
                    ):
                        bad = True
                else:
                    # {**session.headers, ...}
                    for key, value in zip(node.keys, node.values, strict=True):
                        if key is None and _is_headers_attr_of_session(value):
                            bad = True
                if bad:
                    findings.append(f"{file}:{node.lineno}: {ast.unparse(node)}")
    return findings


def ensure_no_session_header_copies(dirs: List[pathlib.Path]) -> None:
    findings = find_session_header_copies(list(dirs))
    if findings:
        raise ValueError(
            "Copying session.headers to authenticate a request only works for "
            "static tokens baked into the headers; OAuth token providers live in "
            "session.auth and are silently bypassed (unauthenticated 401s). Make "
            "the request through the session itself instead. Found:\n"
            + "\n".join(findings)
        )


if __name__ == "__main__":
    import sys

    results = find_session_header_copies([pathlib.Path(p) for p in sys.argv[1:]])
    print("\n".join(results))
    sys.exit(1 if results else 0)
