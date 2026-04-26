"""Sigma formula bracket-reference extraction.

Sigma calculated columns carry formulas like `[Source/Column]` or `[Column]`.
This module extracts the bracket references for downstream lineage resolvers.

It does NOT parse operators, function calls, literals, or any non-bracket
syntax. The bracket refs alone are sufficient for column-level lineage
because every cross-column dependency in a Sigma formula appears inside
brackets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class BracketRef:
    """A single `[...]` reference inside a Sigma formula.

    `source` is the part before the first unescaped `/`. If the formula has
    no `/`, `source` is the whole bracket body and `column` is None (this
    is a sibling-column reference within the same element).

    `is_parameter` is True for `[P_*]` parameter refs, which lineage
    resolvers should skip.
    """

    raw: str
    source: str
    column: Optional[str]
    is_parameter: bool


_BRACKET_RE = re.compile(r"\[((?:[^\[\]\\]|\\.)+)\]")
_PARAM_RE = re.compile(r"^P_")
# Matches a double-quoted string literal, respecting `\"` escapes.
# Sigma uses double quotes for string literals; brackets inside them are not column refs.
_STRING_LITERAL_RE = re.compile(r'"(?:[^"\\]|\\.)*"')


def _strip_string_literals(formula: str) -> str:
    """Replace double-quoted string literals with spaces to neutralise bracket-like content.

    Preserves overall string length so any future positional debugging stays accurate.
    """
    return _STRING_LITERAL_RE.sub(lambda m: " " * len(m.group()), formula)


def _split_unescaped_slash(body: str) -> List[str]:
    """Split a bracket body on `/`, but treat `\\/` as a literal."""
    return re.split(r"(?<!\\)/", body)


def _unescape(s: str) -> str:
    """Convert `\\/` to `/` and `\\]` to `]` in a bracket body segment.

    Conservative: only unescapes the specific sequences observed in Sigma's
    formula language.
    """
    return s.replace(r"\/", "/").replace(r"\]", "]")


def extract_bracket_refs(formula: Optional[str]) -> List[BracketRef]:
    """Extract every bracket reference from a Sigma formula.

    Returns an empty list for None / empty / no-bracket formulas. Order
    matches appearance in the source string.
    """
    if not formula:
        return []
    out: List[BracketRef] = []
    for raw_body in _BRACKET_RE.findall(_strip_string_literals(formula)):
        parts = _split_unescaped_slash(raw_body)
        source_raw = parts[0]
        column_raw = "/".join(parts[1:]) if len(parts) > 1 else None
        source = _unescape(source_raw)
        column = _unescape(column_raw) if column_raw is not None else None
        out.append(
            BracketRef(
                raw=raw_body,
                source=source,
                column=column,
                is_parameter=bool(_PARAM_RE.match(source)) and column is None,
            )
        )
    return out
