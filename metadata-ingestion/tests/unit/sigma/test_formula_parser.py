"""Tests for the Sigma formula bracket-reference extractor."""

import dataclasses
import re
from typing import List, Optional, Tuple

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    extract_bracket_refs,
)


def _normalize(text: str) -> str:
    """Mirror of the parser's separator normalisation, for invariant checks."""
    return re.sub(r"\s*/\s*", "/", text.strip())


def test_none_formula_returns_empty() -> None:
    assert extract_bracket_refs(None) == []


def test_empty_formula_returns_empty() -> None:
    assert extract_bracket_refs("") == []


def test_literal_only_returns_empty() -> None:
    assert extract_bracket_refs("42") == []


def test_sibling_ref() -> None:
    result = extract_bracket_refs("[col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.raw == "[col]"
    assert ref.source == "col"
    assert ref.column is None
    assert ref.is_parameter is False


def test_cross_element_ref() -> None:
    result = extract_bracket_refs("[Source/col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.raw == "[Source/col]"
    assert ref.source == "Source"
    assert ref.column == "col"
    assert ref.is_parameter is False


def test_real_tenant_sample_m0() -> None:
    """Verifies the M0 probe sample: 97.6% of 127 observed formulas follow this pattern."""
    result = extract_bracket_refs("[Union of 1 Sources/win_by_runs]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Union of 1 Sources"
    assert ref.column == "win_by_runs"


def test_two_refs_in_order() -> None:
    result = extract_bracket_refs("[a] + [b]")
    assert len(result) == 2
    assert result[0].source == "a"
    assert result[1].source == "b"


def test_two_refs_around_literal_slash_operator() -> None:
    """The `/` between brackets is an arithmetic operator, not a source/column separator."""
    result = extract_bracket_refs("[Selected Metric] / [Successful Runs]")
    assert len(result) == 2
    assert result[0].source == "Selected Metric"
    assert result[0].column is None
    assert result[1].source == "Successful Runs"
    assert result[1].column is None


def test_escaped_slash_in_column_name_no_split() -> None:
    r"""A `\/` inside a bracket body is a literal `/`, not a source/column separator."""
    result = extract_bracket_refs(r"[Rank of MAR \/ Modified]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Rank of MAR / Modified"
    assert ref.column is None


def test_escaped_slash_only_in_column_part() -> None:
    r"""Source ends at the first unescaped `/`; the `\/` in the column part becomes a literal `/`."""
    result = extract_bracket_refs(r"[Source/with \/ slash]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Source"
    assert ref.column == "with / slash"


def test_parameter_ref() -> None:
    result = extract_bracket_refs("[P_MyParam]")
    assert len(result) == 1
    ref = result[0]
    assert ref.is_parameter is True


def test_p_prefixed_sibling_ref_is_classified_as_parameter() -> None:
    """The P_ heuristic assumes bare P_* refs are Sigma parameters, not sibling columns."""
    result = extract_bracket_refs("[P_revenue] * 2")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "P_revenue"
    assert ref.column is None
    assert ref.is_parameter is True


def test_parameter_with_slash_is_not_parameter() -> None:
    """A `P_*` ref with a column part is NOT treated as a parameter per our defensive rule."""
    result = extract_bracket_refs("[P_X/col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "P_X"
    assert ref.column == "col"
    assert ref.is_parameter is False


def test_parameter_heuristic_is_case_sensitive() -> None:
    """Lowercase `p_` prefix does NOT trigger the parameter heuristic."""
    result = extract_bracket_refs("[p_foo]")
    assert len(result) == 1
    assert result[0].is_parameter is False


def test_parameter_heuristic_bare_prefix() -> None:
    """[P_] (P_ with no suffix) still matches the heuristic — source starts with 'P_'."""
    result = extract_bracket_refs("[P_]")
    assert len(result) == 1
    assert result[0].source == "P_"
    assert result[0].is_parameter is True


def test_multiline_formula() -> None:
    formula = "If(\n  [a] > 0,\n  [b],\n  [c]\n)"
    result = extract_bracket_refs(formula)
    assert len(result) == 3
    assert result[0].source == "a"
    assert result[1].source == "b"
    assert result[2].source == "c"


def test_empty_bracket_skipped() -> None:
    """Empty brackets `[]` are silently skipped — source is empty after strip."""
    assert extract_bracket_refs("[]") == []


def test_empty_source_or_column_refs_skipped() -> None:
    assert extract_bracket_refs("[a/]") == []
    assert extract_bracket_refs("[/b]") == []
    assert extract_bracket_refs("[   ]") == []


def test_brackets_inside_string_literal_ignored() -> None:
    """Brackets inside double-quoted string literals must not produce lineage refs.

    `"[failed]"` is a string constant, not a column reference.
    Regression for the quote-awareness review finding.
    """
    result = extract_bracket_refs('If([status] = "FAILURE", "[failed]", [fallback])')
    assert len(result) == 2
    assert result[0].source == "status"
    assert result[1].source == "fallback"


def test_brackets_inside_single_quoted_string_literal_ignored() -> None:
    """Brackets inside single-quoted string literals are also ignored."""
    result = extract_bracket_refs("If([a], '[dummy]', [b])")
    assert len(result) == 2
    assert result[0].source == "a"
    assert result[1].source == "b"


def test_escaped_quote_inside_string_literal() -> None:
    r"""A `\"` escape inside a string literal should not prematurely close the string."""
    result = extract_bracket_refs(r'If([a] = "say \"hi\"", [b], [c])')
    assert len(result) == 3
    sources = [r.source for r in result]
    assert sources == ["a", "b", "c"]


def test_multi_slash_body() -> None:
    """A body with multiple `/` separators: source is first segment, column gets the rest."""
    result = extract_bracket_refs("[a/b/c]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "a"
    assert ref.column == "b/c"


def test_adjacent_brackets() -> None:
    """Two bracket refs with no whitespace between them both parse correctly."""
    result = extract_bracket_refs("[a][b]")
    assert len(result) == 2
    assert result[0].source == "a"
    assert result[1].source == "b"


def test_escaped_close_bracket_in_body() -> None:
    r"""A `\]` inside a body is unescaped to `]`; the bracket closes on the next unescaped `]`."""
    result = extract_bracket_refs(r"[col\]name]")
    assert len(result) == 1
    ref = result[0]
    assert ref.raw == r"[col\]name]"
    assert ref.source == "col]name"
    assert ref.column is None


def test_whitespace_inside_brackets_stripped() -> None:
    """Leading/trailing whitespace in source and column is stripped for exact-name lookup."""
    result = extract_bracket_refs("[ col ]")
    assert len(result) == 1
    ref = result[0]
    assert ref.raw == "[ col ]"  # raw preserves original
    assert ref.source == "col"  # source is normalized

    result2 = extract_bracket_refs("[ Source / col ]")
    assert len(result2) == 1
    assert result2[0].source == "Source"
    assert result2[0].column == "col"


def test_bracket_body_with_unpaired_quote() -> None:
    """A lone `"` inside a bracket body is a literal column-name character.

    The unbalanced quote can't form a string literal so the body is captured
    intact. This case was handled correctly by the old regex impl; the scanner
    preserves the behavior.
    """
    result = extract_bracket_refs('[col"name]')
    assert len(result) == 1
    assert result[0].source == 'col"name'


# --- Refactor regression tests (stateful scanner) ---


def test_bracket_body_with_paired_quote() -> None:
    """Regression for the layered-regex paired-quote bug.

    Before refactor, the literal-stripper turned `"x"` into `   `, mutilating
    the column name. The state machine treats the brackets as a single span;
    quote characters inside the span are literal column-name characters.
    """
    result = extract_bracket_refs('If([col"x"col] = "x", 1, 0)')
    assert len(result) == 1
    assert result[0].source == 'col"x"col'
    assert result[0].column is None


def test_literal_backslash_before_slash() -> None:
    r"""Regression for the lookbehind-cannot-distinguish-`\\/`-from-`\/` bug.

    `\\/` means: literal backslash, then real (unescaped) slash separator.
    The first `\` consumes the second `\` as a literal; the `/` is then
    the first unescaped slash.
    """
    result = extract_bracket_refs(r"[A\\/B]")
    assert len(result) == 1
    assert result[0].raw == r"[A\\/B]"
    assert result[0].source == "A\\"
    assert result[0].column == "B"


def test_balanced_quote_does_not_destroy_innocent_brackets() -> None:
    """Regression for the literal-stripper-spans-multiple-brackets bug.

    Layered regex: the string-literal pre-pass greedily matches from the
    first `"` (inside `[a"b]`) to the next `"` (in `"hello"`), replacing
    21 characters with spaces. Both `]`s and the entire `[innocent]` ref
    are destroyed; the bracket regex finds 0 matches.

    State machine: `"` inside IN_BRACKET is a literal column-name char,
    so `[a"b]` parses cleanly with source='a"b'; `[innocent]` is a
    separate untouched span; `"hello"` is a string-literal in NORMAL
    state with no brackets inside. Emit 2 refs.
    """
    result = extract_bracket_refs('[a"b] + [innocent] + "hello"')
    assert len(result) == 2
    assert result[0].source == 'a"b'
    assert result[0].column is None
    assert result[1].source == "innocent"
    assert result[1].column is None


def test_nested_bracket_in_body_treated_as_literal() -> None:
    """Decision: `[` inside IN_BRACKET is appended as a literal column-name
    character; the FIRST `]` closes the outer bracket. Trailing `c]` after
    the close becomes NORMAL-state noise.

    Behavior change vs regex (which matched the innermost `[b]` only and
    silently dropped `a` and `c]`). Documented in the module's decision
    matrix.
    """
    result = extract_bracket_refs("[a[b]c]")
    assert len(result) == 1
    assert result[0].source == "a[b"
    assert result[0].column is None


def test_p_star_sibling_treated_as_parameter() -> None:
    """Documented ambiguity: `[P_foo]` cannot be disambiguated from
    formula text alone. The parser flags `is_parameter=True` on the
    heuristic `source.startswith("P_") AND column is None`. The resolver
    will skip parameter refs; a real column literally named `P_foo` will
    be missed. Trade-off documented in the decision matrix; if a customer
    hits this, file a follow-up to make the heuristic configurable.
    """
    result = extract_bracket_refs("[P_foo]")
    assert len(result) == 1
    assert result[0].source == "P_foo"
    assert result[0].column is None
    assert result[0].is_parameter is True


# --- Decision matrix: explicit tests for every degenerate-input row ---


def test_whitespace_only_bracket_skipped() -> None:
    """Decision: whitespace-only bracket body is silently skipped (empty after strip)."""
    assert extract_bracket_refs("[ ]") == []


def test_empty_source_with_column_skipped() -> None:
    """Decision: `[/col]` has empty source; skip rather than emit unresolvable ref."""
    assert extract_bracket_refs("[/col]") == []


def test_empty_column_after_slash_skipped() -> None:
    """Decision: `[source/]` has empty column; skip rather than emit unresolvable ref."""
    assert extract_bracket_refs("[source/]") == []


def test_double_backslash_body_emits() -> None:
    r"""Decision: `[\\]` — escape_peek consumes the second `\` as literal; source is
    a single backslash character. Emitted since source is non-empty; the downstream
    resolver will not find a column named `\` and will skip gracefully.

    Behavior change from regex: old impl produced source='\\' (two backslashes,
    no escape processing); scanner produces source='\' (one backslash).
    """
    result = extract_bracket_refs(r"[\\]")
    assert len(result) == 1
    assert result[0].source == "\\"


def test_lone_escape_at_bracket_end_skipped() -> None:
    r"""Decision: `[\]` — the `\` escape_peeks the `]`, consuming it as literal;
    the scanner reaches EOF still in IN_BRACKET → unterminated bracket → skip.
    """
    assert extract_bracket_refs(r"[\]") == []


def test_unterminated_bracket_skipped() -> None:
    """Decision: unterminated `[` reaches EOF; emit nothing."""
    assert extract_bracket_refs("foo [bar baz") == []


def test_unterminated_string_continues_scanning() -> None:
    """Decision: unterminated `"` is treated as if quote pairs at EOF; brackets
    after the open-quote are inside-string and ignored.

    Note: pre-refactor regex behavior would parse `[not_a_ref]` because the
    literal-stripper non-greedy match required a closing quote. Document this
    as an intentional behavior change.
    """
    assert extract_bracket_refs('foo "still in string [not_a_ref]') == []


def test_balanced_quote_inside_bracket_body_preserves_later_refs() -> None:
    result = extract_bracket_refs('[col"name] + If([ok] = "yes", 1, 0)')
    assert len(result) == 2
    assert result[0].source == 'col"name'
    assert result[1].source == "ok"


# ---------------------------------------------------------------------------
# segments: every unescaped-"/" part, in order.
#
# Resolvers need this because Sigma APPEARS to write a column reached through a
# join as [JoinElement/SourceElement/Column], with the owning element the
# segment before the column rather than `source`. That reading is inferred from
# references resolving against real data, not documented by Sigma -- see the
# class docstring.
# ---------------------------------------------------------------------------


def test_join_chain_segments() -> None:
    (ref,) = extract_bracket_refs("[E1/E2/col]")
    assert ref.segments == ("E1", "E2", "col")
    # Legacy split must not move.
    assert ref.source == "E1"
    assert ref.column == "E2/col"


def test_nested_join_chain_segments() -> None:
    (ref,) = extract_bracket_refs("[E1/E2/E3/col]")
    assert ref.segments == ("E1", "E2", "E3", "col")


def test_single_slash_segments() -> None:
    (ref,) = extract_bracket_refs("[Element/col]")
    assert ref.segments == ("Element", "col")


def test_bare_ref_has_one_segment() -> None:
    (ref,) = extract_bracket_refs("[col]")
    assert ref.segments == ("col",)


def test_parameter_ref_has_one_segment() -> None:
    (ref,) = extract_bracket_refs("[P_x]")
    assert ref.is_parameter is True
    assert ref.segments == ("P_x",)


def test_escaped_slash_stays_inside_one_segment() -> None:
    (ref,) = extract_bracket_refs(r"[A\/B/col]")
    assert ref.segments == ("A/B", "col")


def test_escaped_backslash_before_a_separator_still_separates() -> None:
    r"""`\\` is a literal backslash, so the following `/` is a real separator."""
    (ref,) = extract_bracket_refs(r"[A\\/B/C]")
    assert ref.segments == ("A\\", "B", "C")


def test_escaped_brackets_stay_inside_one_segment() -> None:
    (ref,) = extract_bracket_refs(r"[\[Tag\] Element A/Element B/col_a]")
    assert ref.segments == ("[Tag] Element A", "Element B", "col_a")


def test_segments_are_whitespace_stripped() -> None:
    (ref,) = extract_bracket_refs("[ E1 / E2 / col ]")
    assert ref.segments == ("E1", "E2", "col")


def test_segment_buffers_reset_between_refs() -> None:
    """A missing per-bracket reset is the classic bug here."""
    first, second = extract_bracket_refs("[a/b/c] + [d]")
    assert first.segments == ("a", "b", "c")
    assert second.segments == ("d",)


def test_a_trailing_separator_leaves_the_last_segment_empty() -> None:
    """Documented contract: the skip rule tests source/column, not segments.

    `[E1/E2/]` has column "E2/", which is non-empty, so the ref is emitted and
    its last segment is "". Resolvers must check for empty segments.
    """
    (ref,) = extract_bracket_refs("[E1/E2/]")
    assert ref.column == "E2/"
    assert ref.segments == ("E1", "E2", "")


def test_consecutive_separators_leave_a_middle_segment_empty() -> None:
    (ref,) = extract_bracket_refs("[a//b]")
    assert ref.segments == ("a", "", "b")
    # Whitespace-only is empty after stripping, and behaves identically.
    (spaced,) = extract_bracket_refs("[a/ /b]")
    assert spaced.segments == ("a", "", "b")


def test_an_empty_column_is_still_skipped() -> None:
    """The single-slash empty-column row of the decision matrix is unchanged."""
    assert extract_bracket_refs("[E1/]") == []


def test_hand_built_ref_does_not_split_column() -> None:
    """Back-compat default must not re-split a column containing "/"."""
    ref = BracketRef(raw="[a/b/c]", source="a", column="b/c", is_parameter=False)
    assert ref.segments == ("a", "b/c")
    assert BracketRef(
        raw="[a]", source="a", column=None, is_parameter=False
    ).segments == ("a",)


def test_a_list_of_segments_is_coerced_to_a_tuple() -> None:
    """An untyped caller passing a list would otherwise make hash() raise."""
    ref = BracketRef(
        raw="[a/b]",
        source="a",
        column="b",
        is_parameter=False,
        segments=["a", "b"],  # type: ignore[arg-type]
    )
    assert ref.segments == ("a", "b")
    assert hash(ref) == hash(ref)


def test_segments_contradicting_source_or_column_are_rejected() -> None:
    """A self-inconsistent ref resolves differently per field read; reject it."""
    with pytest.raises(ValueError):
        BracketRef(
            raw="[a/b]",
            source="a",
            column="b",
            is_parameter=False,
            segments=("q", "r", "s"),
        )
    with pytest.raises(ValueError):
        # column present but only one segment
        BracketRef(
            raw="[a/b]",
            source="a",
            column="b",
            is_parameter=False,
            segments=("a",),
        )
    # Parse OUTSIDE the raises block: if parsing itself ever raised ValueError
    # the assertions below would pass without exercising replace() at all.
    (parsed,) = extract_bracket_refs("[E1/E2/col]")
    with pytest.raises(ValueError):
        dataclasses.replace(parsed, source="Z")
    with pytest.raises(ValueError):
        # Changing only the column leaves segments describing a different
        # upstream, so the two views of the ref disagree.
        dataclasses.replace(parsed, column="other")


@pytest.mark.parametrize(
    "formula",
    [
        "[a]",
        "[a/b]",
        "[a/b/c]",
        "[E1/E2/E3/col]",
        r"[A\/B/col]",
        r"[A\\/B/C]",
        "[ E1 / E2 / col ]",
        "[E1/E2/]",
        "[a//b]",
        "[P_x]",
        '[col"name] + [ok]',
        "[a[b]c]",
        "[a/b] + [c/d/e] + [f]",
    ],
)
def test_known_shapes_keep_source_and_column_derivable_from_segments(
    formula: str,
) -> None:
    """Regression corpus: the shapes that have actually broken before."""
    for ref in extract_bracket_refs(formula):
        assert ref.segments[0] == ref.source
        assert (len(ref.segments) == 1) == (ref.column is None)


# The characters that decide bracket parsing, plus a parameter prefix and
# several kinds of whitespace, so parameter refs and non-space separators are
# actually generated.
_FORMULA_CHARS = st.sampled_from(list("[]/\\\"' \t\n\u00a0abAB_P"))

# Free-form text alone almost never produces a bracket with two separators AND
# whitespace around them, which is where the source/column split is subtlest --
# a mutation that stripped each segment before rejoining survived 500 purely
# random examples. So generate bracket-SHAPED strings too, and draw from both.
_PADDING = st.sampled_from(["", " ", "  ", "\t"])
_SEGMENT_BODY = st.sampled_from(["a", "b", "A B", "P_x", "", "a\\/b", "a\\\\", 'q"r'])


@st.composite
def _bracket_shaped(draw: st.DrawFn) -> str:
    segments = draw(st.lists(_SEGMENT_BODY, min_size=1, max_size=4))
    body = "/".join(f"{draw(_PADDING)}{seg}{draw(_PADDING)}" for seg in segments)
    return f"{draw(_PADDING)}[{body}]{draw(_PADDING)}"


_FORMULAS = st.one_of(
    st.text(alphabet=_FORMULA_CHARS, max_size=24),
    _bracket_shaped(),
    st.lists(_bracket_shaped(), min_size=2, max_size=3).map(" + ".join),
)


@settings(max_examples=1500, suppress_health_check=[HealthCheck.too_slow])
@given(_FORMULAS)
def test_legacy_fields_are_unchanged_against_the_frozen_oracle(formula: str) -> None:
    """The actual "no behaviour change" guarantee, enforced in CI.

    Compares against an independent copy of the pre-PR scanner rather than
    against invariants derived from the new one, so a bug present in both
    cannot pass.
    """
    got = [
        (ref.raw, ref.source, ref.column, ref.is_parameter)
        for ref in extract_bracket_refs(formula)
    ]
    assert got == _legacy_extract(formula)


@settings(max_examples=1500, suppress_health_check=[HealthCheck.too_slow])
@given(_FORMULAS)
def test_every_parsed_ref_satisfies_its_own_invariants(formula: str) -> None:
    """The scanner never builds a ref that would trip its own check."""
    for ref in extract_bracket_refs(formula):
        assert ref.segments[0] == ref.source
        assert (len(ref.segments) == 1) == (ref.column is None)
        if ref.column is not None:
            assert _normalize(ref.column) == _normalize("/".join(ref.segments[1:]))
        # Reconstructing from the legacy pair must not raise either.
        BracketRef(
            raw=ref.raw,
            source=ref.source,
            column=ref.column,
            is_parameter=ref.is_parameter,
        )


def test_two_separately_parsed_refs_are_equal_and_hash_alike() -> None:
    """Frozen dataclass: a list field would make __hash__ raise TypeError."""
    (a,) = extract_bracket_refs("[a/b/c]")
    (b,) = extract_bracket_refs("[a/b/c]")
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b}) == 1


# --- frozen oracle -------------------------------------------------------
# The pre-PR first-slash scanner, copied VERBATIM from the parent commit and
# emitting plain tuples. It exists so "source/column/raw/is_parameter are
# unchanged" is enforced in CI against an independent implementation, not
# merely asserted in a commit message from a local fuzz run. Do NOT refactor
# it to share code with the parser -- a bug in both would then pass.
_ORACLE_NORMAL, _ORACLE_IN_BRACKET, _ORACLE_IN_DOUBLE_STR, _ORACLE_IN_SINGLE_STR = (
    range(4)
)


def _legacy_extract(
    formula: Optional[str],
) -> List[Tuple[str, str, Optional[str], bool]]:

    if not formula:
        return []
    out: List[Tuple[str, str, Optional[str], bool]] = []
    state = _ORACLE_NORMAL
    bracket_start_idx = 0
    source_buf: List[str] = []
    column_buf: List[str] = []
    seen_slash = False
    i = 0
    n = len(formula)
    while i < n:
        ch = formula[i]
        if state == _ORACLE_NORMAL:
            if ch == "[":
                state = _ORACLE_IN_BRACKET
                bracket_start_idx = i
                source_buf = []
                column_buf = []
                seen_slash = False
            elif ch == '"':
                state = _ORACLE_IN_DOUBLE_STR
            elif ch == "'":
                state = _ORACLE_IN_SINGLE_STR
            # else: noop — NORMAL state, non-special char
        elif state == _ORACLE_IN_BRACKET:
            buf = column_buf if seen_slash else source_buf
            if ch == "]":
                # finish_bracket: emit_or_skip per decision matrix
                source = "".join(source_buf).strip()
                column = "".join(column_buf).strip() if seen_slash else None
                if source and (column is None or column):
                    out.append(
                        (
                            formula[bracket_start_idx : i + 1],
                            source,
                            column,
                            source.startswith("P_") and column is None,
                        )
                    )
                else:
                    pass
                state = _ORACLE_NORMAL
            elif ch == "\\":
                # escape_peek: consume next char literally into active buffer
                if i + 1 < n:
                    buf.append(formula[i + 1])
                    i += 2
                    continue
                # lone backslash at EOF → unterminated bracket; loop ends naturally
            elif ch == "/" and not seen_slash:
                # first unescaped slash: switch accumulation to column_buf
                seen_slash = True
            else:
                # covers '[', '"', "'", subsequent '/', and all other chars
                buf.append(ch)
        elif state == _ORACLE_IN_DOUBLE_STR:
            if ch == '"':
                state = _ORACLE_NORMAL
            elif ch == "\\":
                # escape_peek inside string: drop both chars
                i += 2
                continue
            # else: noop — inside string literal
        elif state == _ORACLE_IN_SINGLE_STR:
            if ch == "'":
                state = _ORACLE_NORMAL
            elif ch == "\\":
                # escape_peek inside string: drop both chars
                i += 2
                continue
            # else: noop — inside string literal
        i += 1
    # EOF: any open IN_BRACKET or IN_*_STR state is silently discarded
    return out
