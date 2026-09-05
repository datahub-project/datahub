"""Sigma formula bracket-reference extraction.

Sigma calculated columns carry formulas like `[Source/Column]` or `[Column]`.
This module extracts the bracket references for downstream lineage resolvers.

It does NOT parse operators, function calls, literals, or any non-bracket
syntax. Across the M0 probe of 127 DM-element columns, every observed
cross-column dependency appeared inside brackets; the remaining 2.4% are
sibling-column transforms that are also bracket-based.

=============================================================================
State Transition Table
=============================================================================

States:
  NORMAL          — outside any bracket or string
  IN_BRACKET      — between [ and ]; accumulating into source_buf or column_buf
  IN_DOUBLE_STR   — between " and "; brackets here are NOT references
  IN_SINGLE_STR   — between ' and '; brackets here are NOT references

Within IN_BRACKET, an internal flag `seen_slash` tracks whether the first
unescaped `/` has been consumed yet. Before the first `/`, characters
accumulate into source_buf; after, into column_buf.

Backslash handling: in IN_BRACKET, IN_DOUBLE_STR, IN_SINGLE_STR, a `\\` consumes
the NEXT character verbatim (literal append; the `\\` itself is dropped
in IN_BRACKET to support `\\/` and `\\]` unescape semantics, and dropped
in strings to support `\\"` / `\\'` / `\\\\`). In NORMAL, `\\` has no
special meaning.

Transitions (rows = state, columns = next char OR EOF):

                     | [              | ]                  | "             | '             | \\                      | /                                           | other          | EOF
NORMAL               | enter IN_BRACKET; reset bufs | noop | enter IN_DBL_STR | enter IN_SGL_STR | noop      | noop                                        | noop           | done
IN_BRACKET           | append literal `[` to active buf | finish_bracket: emit_or_skip; -> NORMAL | append literal `"` | append literal `'` | escape_peek: append next raw char to active buf | first slash: set seen_slash, switch active buf to column_buf; subsequent: append literal `/` to column_buf | append to active buf | unterminated_bracket: emit nothing; done
IN_DOUBLE_STR        | noop           | noop               | -> NORMAL     | noop          | escape_peek: drop both  | noop                                        | noop           | unterminated_string: emit nothing; done
IN_SINGLE_STR        | noop           | noop               | noop          | -> NORMAL     | escape_peek: drop both  | noop                                        | noop           | unterminated_string: emit nothing; done

Invariants:
- **Quotes inside brackets are literal column-name characters.** This is the fix
  for the paired-quote bug: `[col"x"col]` in a formula that has another `"` elsewhere
  no longer gets mutilated by a pre-pass string stripper.
- **`\\X` inside a bracket appends `X` literally to the active buffer.** `\\/` becomes
  `/`, `\\]` becomes `]`, `\\\\` becomes `\\`. The fix for the `A\\\\/B` bug: the first
  `\\` is escape-peek, consuming the second `\\` literally; the next `/` is then a real
  (unescaped) slash separator.
- **`/` inside a bracket is the source/column separator on first occurrence only.**
  Subsequent `/` characters become literal members of the column buffer (so
  `[a/b/c]` → source=`a`, column=`b/c`).
- **Whitespace around source and column is stripped** when emitting
  `BracketRef.source` / `.column`. `BracketRef.raw` preserves the original text
  including whitespace.

=============================================================================
Empty-String and Degenerate-Input Decision Matrix
=============================================================================

Every row is an explicit decision. Rows marked "skip" emit no BracketRef and
increment no counter (a debug-level log line suffices; a dedicated counter is
deferred to a follow-up if production telemetry warrants it).

| Input                   | Old regex behavior                                        | Decision                                | Rationale                                                                  |
|-------------------------|-----------------------------------------------------------|-----------------------------------------|----------------------------------------------------------------------------|
| formula = None          | early-return []                                           | preserve ([])                           | defensive null-handling; part of the public contract                       |
| formula = ""            | return [] (no match)                                      | preserve ([])                           | empty input → empty output                                                 |
| []                      | no match (regex required `+`)                             | skip silently                           | empty body; no reference to resolve                                        |
| [ ]                     | source "" after strip                                     | skip silently                           | whitespace-only body → empty source after strip; same as empty             |
| [/col]                  | source "", column "col"                                   | skip                                    | empty source; resolver cannot look up a column with no DM name             |
| [source/]               | source "source", column ""                                | skip                                    | empty column; resolver cannot downstream to an empty column name           |
| [ / ]                   | source "", column "" after strip                          | skip                                    | both parts empty after strip; falls under empty-source rule                |
| [\\\\]                  | body `\\\\` → unescape produces `\\`                      | emit (source=`\\`)                      | single non-empty backslash; degenerate but resolver handles gracefully     |
| [\\] (lone escape)      | body `\\]` (the `]` consumed by escape) → no closing `]` | skip (unterminated bracket)             | escape_peek consumes `]`; scanner reaches EOF in IN_BRACKET → unterminated |
| Unterminated `[…`       | regex doesn't match                                       | skip; debug log only                    | no closing `]` before EOF; emit nothing                                    |
| Unterminated `"…`       | string regex doesn't match; brackets after may parse      | skip remaining refs inside the string   | treat as if string pairs at EOF; intentional behavior change from regex:   |
|                         |                                                           |                                         | once `"` opens IN_DOUBLE_STR, everything through EOF is inside-string;     |
|                         |                                                           |                                         | brackets inside are NOT parsed (previously they were, since regex left     |
|                         |                                                           |                                         | the unterminated string unstripped and the bracket regex grabbed them)     |
| [col"name]              | mutilated to `col   col` (literal-stripper BUG)           | emit col"name                           | quotes inside brackets are literal; reviewer-predicted fix                 |
| [A\\\\/B]               | source=`A\\/B`, column=None (lookbehind BUG)              | emit source=`A\\`, column=`B`           | escape_peek handles literal `\\` before separator; reviewer-predicted fix  |
| [a"b]+[innocent]+"hi"   | 0 refs (literal-stripper spans from `"` to `"`, BUG)      | emit 2 refs: `a"b`, `innocent`          | reviewer's strongest demo; state machine emits correctly                   |
| [a[b]c]                 | regex matches innermost [b] only                          | emit source=`a[b`                       | `[` in IN_BRACKET is literal; first `]` closes; behavior change vs regex   |
| [P_foo] (P_* heuristic) | is_parameter=True (source.startswith("P_") and no column) | preserve is_parameter=True              | ambiguity unresolvable from formula alone; keep heuristic for downstream  |
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

log = logging.getLogger(__name__)

_NORMAL = 0
_IN_BRACKET = 1
_IN_DOUBLE_STR = 2
_IN_SINGLE_STR = 3


@dataclass(frozen=True)
class BracketRef:
    """A single `[...]` reference inside a Sigma formula.

    `raw` is the full bracket substring as it appears in the formula,
    including the surrounding ``[`` and ``]`` (e.g. ``[Source/col]``).

    `source` is the part before the first unescaped `/`, with leading/
    trailing whitespace stripped. If the formula has no `/`, `source` is
    the whole bracket body and `column` is None (sibling-column reference
    within the same element).

    `column` is everything after the first unescaped `/`, with leading/
    trailing whitespace stripped. If the body contains multiple unescaped
    ``/`` characters, only the first separates source from column; the
    rest become literal ``/`` within ``column`` (e.g. ``[a/b/c]`` →
    source=``a``, column=``b/c``). ``column`` is None when no ``/`` is
    present.

    `is_parameter` is True when ``source`` starts with ``"P_"`` (case-sensitive)
    AND ``column`` is None. This is a heuristic for Sigma parameter refs, which
    lineage resolvers should skip. `[p_foo]` (lowercase) is NOT flagged;
    `[P_X/col]` (has column part) is NOT flagged.

    `segments` is every unescaped-``/``-delimited part of the body, in order and
    individually whitespace-stripped (e.g. ``[a/b/c]`` → ``["a", "b", "c"]``).
    ``source`` / ``column`` keep their first-slash split for backwards
    compatibility, but resolvers should prefer ``segments``: Sigma writes
    join-chain refs as ``[JoinElement/SourceElement/Column]``, where the owning
    element is the second-to-last segment, not the first. Only the scanner may
    produce this list -- an escaped ``\\/`` is a literal character inside one
    segment, so re-splitting ``column`` after the fact would corrupt it.
    """

    raw: str
    source: str
    column: Optional[str]
    is_parameter: bool
    # Defaults to the first-slash split so hand-built refs (tests, callers that
    # construct a ref directly) keep working. Deliberately does NOT split
    # ``column`` on "/" -- that would reintroduce the ambiguity the scanner
    # exists to resolve.
    segments: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.segments is None:
            default = (
                [self.source] if self.column is None else [self.source, self.column]
            )
            # Frozen dataclass: bypass the immutability guard once, at construction.
            object.__setattr__(self, "segments", default)

    @property
    def parts(self) -> List[str]:
        """``segments`` narrowed to a plain list for type-checked call sites."""
        return self.segments if self.segments else [self.source]


def candidate_source_column_splits(ref: BracketRef) -> List[Tuple[str, str]]:
    """Return (source, column) readings of a ref, most-likely first.

    Sigma writes a column reached through a join as
    ``[JoinElement/SourceElement/Column]`` (chaining further for nested joins),
    so the element that owns the column is the segment immediately before the
    column -- not the first segment, which is what ``ref.source`` holds.

    Candidates are ordered join-chain reading first, then the "element name
    itself contains a slash" reading, working right to left. Callers validate
    each candidate against a real element name *and* that element's schema, and
    take the first that holds; the final candidate is always the legacy
    first-slash split, so a single-slash ref resolves exactly as before.

    Returns an empty list for refs with no column part (bare/parameter refs).
    """
    segments = ref.parts
    if ref.column is None or len(segments) < 2:
        return []
    out: List[Tuple[str, str]] = []
    seen = set()

    def add(source: str, column: str) -> None:
        if not source or not column:
            return
        if (source, column) in seen:
            return
        seen.add((source, column))
        out.append((source, column))

    for i in range(len(segments) - 1, 0, -1):
        column = "/".join(segments[i:])
        # 1. join-chain: owning element is the segment before the column.
        add(segments[i - 1], column)
        # 2. element name containing an unescaped "/".
        add("/".join(segments[:i]), column)
    return out


def extract_bracket_refs(formula: Optional[str]) -> List[BracketRef]:
    """Extract every bracket reference from a Sigma formula.

    Returns an empty list for None / empty / no-bracket formulas. Order
    matches appearance in the source string. Leading/trailing whitespace
    in source and column is stripped; ``raw`` preserves the original text.

    Implemented as a single left-to-right stateful scanner driven by the
    transition table in the module docstring. No backtracking; no regex.
    """
    if not formula:
        return []
    out: List[BracketRef] = []
    state = _NORMAL
    bracket_start_idx = 0
    source_buf: List[str] = []
    column_buf: List[str] = []
    # One buffer per unescaped-"/"-delimited segment. Accumulated alongside
    # source_buf/column_buf so the legacy first-slash split is untouched.
    segment_bufs: List[List[str]] = [[]]
    seen_slash = False
    i = 0
    n = len(formula)
    while i < n:
        ch = formula[i]
        if state == _NORMAL:
            if ch == "[":
                state = _IN_BRACKET
                bracket_start_idx = i
                source_buf = []
                column_buf = []
                segment_bufs = [[]]
                seen_slash = False
            elif ch == '"':
                state = _IN_DOUBLE_STR
            elif ch == "'":
                state = _IN_SINGLE_STR
            # else: noop — NORMAL state, non-special char
        elif state == _IN_BRACKET:
            buf = column_buf if seen_slash else source_buf
            if ch == "]":
                # finish_bracket: emit_or_skip per decision matrix
                source = "".join(source_buf).strip()
                column = "".join(column_buf).strip() if seen_slash else None
                segments = ["".join(seg).strip() for seg in segment_bufs]
                if source and (column is None or column):
                    out.append(
                        BracketRef(
                            raw=formula[bracket_start_idx : i + 1],
                            source=source,
                            column=column,
                            is_parameter=source.startswith("P_") and column is None,
                            segments=segments,
                        )
                    )
                else:
                    log.debug(
                        "sigma formula_parser: skipping malformed bracket ref %r",
                        formula[bracket_start_idx : i + 1],
                    )
                state = _NORMAL
            elif ch == "\\":
                # escape_peek: consume next char literally into active buffer.
                # An escaped "/" is a literal character, so it must NOT open a
                # new segment -- this is why segments can only be built in the
                # scanner and never by re-splitting source/column afterwards.
                if i + 1 < n:
                    buf.append(formula[i + 1])
                    segment_bufs[-1].append(formula[i + 1])
                    i += 2
                    continue
                # lone backslash at EOF → unterminated bracket; loop ends naturally
            elif ch == "/":
                # Every unescaped slash opens a new segment. For source/column
                # only the first one separates; later ones stay literal in the
                # column buffer (unchanged legacy behaviour).
                segment_bufs.append([])
                if not seen_slash:
                    seen_slash = True
                else:
                    buf.append(ch)
            else:
                # covers '[', '"', "'", and all other chars
                buf.append(ch)
                segment_bufs[-1].append(ch)
        elif state == _IN_DOUBLE_STR:
            if ch == '"':
                state = _NORMAL
            elif ch == "\\":
                # escape_peek inside string: drop both chars
                i += 2
                continue
            # else: noop — inside string literal
        elif state == _IN_SINGLE_STR:
            if ch == "'":
                state = _NORMAL
            elif ch == "\\":
                # escape_peek inside string: drop both chars
                i += 2
                continue
            # else: noop — inside string literal
        i += 1
    # EOF: any open IN_BRACKET or IN_*_STR state is silently discarded
    return out
