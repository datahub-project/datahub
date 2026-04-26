"""Sigma formula bracket-reference extraction.

Sigma calculated columns carry formulas like `[Source/Column]` or `[Column]`.
This module extracts the bracket references for downstream lineage resolvers.

It does NOT parse operators, function calls, literals, or any non-bracket
syntax. Across the M0 probe of 127 DM-element columns, every observed
cross-column dependency appeared inside brackets; the remaining 2.4% are
sibling-column transforms that are also bracket-based.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class BracketRef:
    """A single `[...]` reference inside a Sigma formula.

    `raw` is the full bracket substring as it appears in the formula,
    including the surrounding ``[`` and ``]`` (e.g. ``[Source/col]``).

    `source` is the part before the first unescaped `/`, with leading/
    trailing whitespace stripped. If the formula has no `/`, `source` is
    the whole bracket body and `column` is None (sibling-column reference
    within the same element).

    `is_parameter` is True for `[P_*]` parameter refs (no column part),
    which lineage resolvers should skip.
    """

    raw: str
    source: str
    column: Optional[str]
    is_parameter: bool


_BRACKET_RE = re.compile(r"\[((?:[^\[\]\\]|\\.)+)\]")

# Matches double- or single-quoted string literals, respecting backslash escapes.
# Brackets inside string literals are not column refs and must be ignored.
# Known limitation: a column name that itself contains a quote character and
# happens to pair with a quote elsewhere in the formula can be silently
# mutilated. Sigma column names containing literal quotes are not observed in
# production; document the edge case rather than defend against it.
# TODO: replace with a left-to-right stateful scanner if quote-bearing column
# names are encountered in the wild.
_STRING_LITERAL_RE = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'')


def _strip_string_literals(formula: str) -> str:
    """Replace string literals with spaces to neutralise bracket-like content.

    Preserves overall string length so any future positional debugging stays
    accurate.
    """
    return _STRING_LITERAL_RE.sub(lambda m: " " * len(m.group()), formula)


def _split_unescaped_slash(body: str) -> List[str]:
    r"""Split a bracket body on `/`, but treat `\/` as a literal slash.

    Note: the single-character lookbehind does not handle a literal backslash
    immediately before `/` (e.g. ``A\\/B`` meaning source ``A\`` + column
    ``B``). Column names with a trailing literal ``\`` are not observed in
    production.
    # TODO: upgrade to a stateful scan if such names appear in the wild.
    """
    return re.split(r"(?<!\\)/", body)


def _unescape(s: str) -> str:
    r"""Convert ``\/`` to ``/`` and ``\]`` to ``]`` in a bracket body segment.

    Conservative: only unescapes the specific sequences observed in Sigma's
    formula language.
    """
    return s.replace(r"\/", "/").replace(r"\]", "]")


def extract_bracket_refs(formula: Optional[str]) -> List[BracketRef]:
    """Extract every bracket reference from a Sigma formula.

    Returns an empty list for None / empty / no-bracket formulas. Order
    matches appearance in the source string. Leading/trailing whitespace
    in source and column is stripped; ``raw`` preserves the original text.
    """
    if not formula:
        return []
    out: List[BracketRef] = []
    for raw_body in _BRACKET_RE.findall(_strip_string_literals(formula)):
        parts = _split_unescaped_slash(raw_body)
        source_raw = parts[0]
        column_raw = "/".join(parts[1:]) if len(parts) > 1 else None
        source = _unescape(source_raw).strip()
        column = _unescape(column_raw).strip() if column_raw is not None else None
        out.append(
            BracketRef(
                raw=f"[{raw_body}]",
                source=source,
                column=column,
                is_parameter=source.startswith("P_") and column is None,
            )
        )
    return out
