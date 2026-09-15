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
  IN_BRACKET      — between [ and ]; accumulating into per-segment buffers
  IN_DOUBLE_STR   — between " and "; brackets here are NOT references
  IN_SINGLE_STR   — between ' and '; brackets here are NOT references

Within IN_BRACKET, characters accumulate into a list of segment buffers; each
unescaped `/` opens a new one. `source`, `column` and `segments` are all
derived from those buffers when the bracket closes, so there is one source of
truth: `source` is the first segment, `column` is the remaining segments
rejoined with `/`, and `segments` is every segment individually stripped.

Backslash handling: in IN_BRACKET, IN_DOUBLE_STR, IN_SINGLE_STR, a `\\` consumes
the NEXT character verbatim (literal append; the `\\` itself is dropped
in IN_BRACKET to support `\\/` and `\\]` unescape semantics, and dropped
in strings to support `\\"` / `\\'` / `\\\\`). In NORMAL, `\\` has no
special meaning.

Transitions (rows = state, columns = next char OR EOF):

                     | [              | ]                  | "             | '             | \\                      | /                                           | other          | EOF
NORMAL               | enter IN_BRACKET; reset bufs | noop | enter IN_DBL_STR | enter IN_SGL_STR | noop      | noop                                        | noop           | done
IN_BRACKET           | append literal `[` to current segment | finish_bracket: emit_or_skip; -> NORMAL | append literal `"` | append literal `'` | escape_peek: append next raw char to current segment | open a new segment buffer | append to current segment buffer | unterminated_bracket: emit nothing; done
IN_DOUBLE_STR        | noop           | noop               | -> NORMAL     | noop          | escape_peek: drop both  | noop                                        | noop           | unterminated_string: emit nothing; done
IN_SINGLE_STR        | noop           | noop               | noop          | -> NORMAL     | escape_peek: drop both  | noop                                        | noop           | unterminated_string: emit nothing; done

Invariants:
- **Quotes inside brackets are literal column-name characters.** This is the fix
  for the paired-quote bug: `[col"x"col]` in a formula that has another `"` elsewhere
  no longer gets mutilated by a pre-pass string stripper.
- **`\\X` inside a bracket appends `X` literally to the current segment.** `\\/` becomes
  `/`, `\\]` becomes `]`, `\\\\` becomes `\\`. The fix for the `A\\\\/B` bug: the first
  `\\` is escape-peek, consuming the second `\\` literally; the next `/` is then a real
  (unescaped) slash separator.
- **`/` inside a bracket is the source/column separator on first occurrence only.**
  `source` is the first segment and `column` is the remaining segments rejoined
  with `/`, so subsequent slashes survive inside `column` (`[a/b/c]` → source=`a`,
  column=`b/c`) while `segments` keeps them apart as `('a', 'b', 'c')`.
- **`segments[0] == source`, and `len(segments) == 1` exactly when `column is
  None`.** Enforced in `BracketRef.__post_init__` rather than only documented,
  because a ref whose `segments` contradict its `source`/`column` resolves to a
  different upstream depending on which field the caller happens to read.
- **`segments` MAY contain empty strings; resolvers must check.** The emit/skip
  rule below tests `source` and `column` only, so `[E1/E2/]` emits with
  `segments=('E1', 'E2', '')` -- its `column` is `'E2/'`, which is non-empty.
  Likewise `[a//b]` emits with `('a', '', 'b')`. Tightening the skip rule would
  change which refs are emitted at all, which is out of scope here.
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
| [E1/E2/]                | source "E1", column "E2/"                                 | emit; segments ('E1','E2','')           | column non-empty so the skip rule does not fire; LAST segment is empty     |
| [a//b]                  | source "a", column "/b"                                    | emit; segments ('a','','b')             | consecutive separators; MIDDLE segment is empty                            |
| [a/ /b]                 | source "a", column "/b"                                    | emit; segments ('a','','b')             | whitespace-only segment is empty after strip                               |
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
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

log = logging.getLogger(__name__)

_NORMAL = 0
_IN_BRACKET = 1
_IN_DOUBLE_STR = 2
_IN_SINGLE_STR = 3


_SEPARATOR_WITH_WS = re.compile(r"\s*/\s*")


def _normalize_separators(text: str) -> str:
    """Collapse whitespace around ``/`` so the two strip regimes compare equal."""
    return _SEPARATOR_WITH_WS.sub("/", text.strip())


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
    individually whitespace-stripped (e.g. ``[a/b/c]`` → ``("a", "b", "c")``).
    ``source`` / ``column`` keep their first-slash split for backwards
    compatibility, but resolvers should consider ``segments``: a reference with
    three or more parts carries structure the first-slash split discards.

    Sigma documents the three-part form as ``[Element/Relationship/Column]``,
    where the MIDDLE segment names a relationship defined in the data model --
    NOT a source element. Relationships are renamed independently of any
    element, so matching a middle segment against element names is unsound: it
    misses whenever a relationship has been renamed, and can match the wrong
    element when a relationship happens to share an element's name. For that
    documented shape the column's owner is the FIRST segment, reached through
    the relationship named in the middle.

    Deeper chains occur in real tenant data, and which segment owns the column
    there is not something this module can settle. Any positional reading also
    assumes Sigma escapes ``/`` inside a name. So this module exposes the parts
    and asserts NOTHING about their meaning -- every resolver must treat a
    split as a candidate to VALIDATE against real element names and schemas,
    never as a positional fact.

    Multi-segment tuples come from the scanner; the ``__post_init__`` default
    only ever builds the one- or two-element form. An escaped slash is a
    literal character inside one segment, so re-splitting ``column`` after the
    fact cannot reconstruct them.
    """

    raw: str
    source: str
    column: Optional[str]
    is_parameter: bool
    # A TUPLE, not a list: this dataclass is frozen, so a list field would make
    # ``__hash__`` raise TypeError and quietly turn a value type into something
    # that cannot go in a set or be a dict key.
    #
    # Defaults to the first-slash split so hand-built refs (tests, callers that
    # construct a ref directly) keep working. Deliberately does NOT split
    # ``column`` on "/" -- that would reintroduce the ambiguity the scanner
    # exists to resolve.
    segments: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.segments:
            # Frozen dataclass: bypass the immutability guard once, at construction.
            object.__setattr__(
                self,
                "segments",
                (self.source,) if self.column is None else (self.source, self.column),
            )
        elif not isinstance(self.segments, tuple):
            # A list here would make __hash__ raise TypeError -- the exact
            # failure the tuple annotation exists to prevent. mypy catches
            # typed callers; tests and untyped code would not be.
            object.__setattr__(self, "segments", tuple(self.segments))
        if self.segments[0] != self.source or (len(self.segments) == 1) != (
            self.column is None
        ):
            raise ValueError(
                f"inconsistent segments for {self.raw!r}: {self.segments!r} "
                f"against source={self.source!r} column={self.column!r}"
            )
        # Catch `column` CONTRADICTING the segments it is derived from, as
        # dataclasses.replace(ref, column=...) would leave a ref whose two
        # views name different upstreams. This detects contradictions; it
        # cannot prove agreement, because `column` does not record which of
        # its slashes were escaped -- ("a", "b/c") and ("a", "b", "c") both
        # rejoin to "b/c" and both pass. Compared after normalising whitespace
        # around separators, because `column` is stripped as a whole while
        # segments are stripped one by one: "[ a / b / c ]" gives
        # column="b / c" and segments ("a", "b", "c").
        if self.column is not None and _normalize_separators(
            self.column
        ) != _normalize_separators("/".join(self.segments[1:])):
            raise ValueError(
                f"column does not match segments for {self.raw!r}: "
                f"column={self.column!r} segments={self.segments!r}"
            )


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
    # One buffer per unescaped-"/"-delimited segment, and the ONLY accumulator:
    # source, column and segments are all derived from it when the bracket
    # closes. Keeping a second pair of source/column buffers in parallel would
    # be two sources of truth for the same characters.
    segment_bufs: List[List[str]] = [[]]
    i = 0
    n = len(formula)
    while i < n:
        ch = formula[i]
        if state == _NORMAL:
            if ch == "[":
                state = _IN_BRACKET
                bracket_start_idx = i
                segment_bufs = [[]]
            elif ch == '"':
                state = _IN_DOUBLE_STR
            elif ch == "'":
                state = _IN_SINGLE_STR
            # else: noop — NORMAL state, non-special char
        elif state == _IN_BRACKET:
            if ch == "]":
                # finish_bracket: emit_or_skip per decision matrix.
                # An escaped "\/" is already a literal "/" INSIDE a segment
                # buffer, so rejoining segments here reproduces the legacy
                # first-slash split exactly -- which is why segments can only
                # be built in the scanner, never by re-splitting column later.
                raw_segments = ["".join(seg) for seg in segment_bufs]
                source = raw_segments[0].strip()
                column = (
                    "/".join(raw_segments[1:]).strip()
                    if len(raw_segments) > 1
                    else None
                )
                segments = tuple(seg.strip() for seg in raw_segments)
                if source and (column is None or column):
                    try:
                        out.append(
                            BracketRef(
                                raw=formula[bracket_start_idx : i + 1],
                                source=source,
                                column=column,
                                is_parameter=source.startswith("P_") and column is None,
                                segments=segments,
                            )
                        )
                    except ValueError:
                        # The constructor's invariants cannot fire for scanner
                        # output today -- a property test asserts that. If a
                        # future change to this scanner breaks one, the module's
                        # policy for a malformed ref applies: drop THIS ref and
                        # log it. Raising would fail a whole ingestion run over
                        # one odd formula. The check stays strict for
                        # hand-built refs, where it catches real caller bugs.
                        log.warning(
                            "sigma formula_parser: scanner produced an "
                            "inconsistent ref for %r; skipping it",
                            formula[bracket_start_idx : i + 1],
                        )
                else:
                    log.debug(
                        "sigma formula_parser: skipping malformed bracket ref %r",
                        formula[bracket_start_idx : i + 1],
                    )
                state = _NORMAL
            elif ch == "\\":
                # escape_peek: consume next char literally into the current
                # segment. An escaped "/" is a literal character, so it must
                # NOT open a new segment.
                if i + 1 < n:
                    segment_bufs[-1].append(formula[i + 1])
                    i += 2
                    continue
                # lone backslash at EOF → unterminated bracket; loop ends naturally
            elif ch == "/":
                # Every unescaped slash opens a new segment.
                segment_bufs.append([])
            else:
                # covers '[', '"', "'", and all other chars
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
