"""Tests for the Sigma formula bracket-reference extractor."""

from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    candidate_source_column_splits,
    extract_bracket_refs,
)


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


class TestSegments:
    """``segments`` exposes every unescaped-``/`` part, in order.

    Resolvers need this because Sigma writes a column reached through a join as
    ``[JoinElement/SourceElement/Column]``, where the owning element is the
    second-to-last segment rather than ``source``.
    """

    def test_join_chain_segments(self) -> None:
        (ref,) = extract_bracket_refs("[E1/E2/col]")
        assert ref.segments == ["E1", "E2", "col"]
        # Legacy split must not move.
        assert ref.source == "E1"
        assert ref.column == "E2/col"

    def test_nested_join_chain_segments(self) -> None:
        (ref,) = extract_bracket_refs("[E1/E2/E3/col]")
        assert ref.segments == ["E1", "E2", "E3", "col"]

    def test_single_slash_segments(self) -> None:
        (ref,) = extract_bracket_refs("[Element/col]")
        assert ref.segments == ["Element", "col"]

    def test_bare_ref_has_one_segment(self) -> None:
        (ref,) = extract_bracket_refs("[col]")
        assert ref.segments == ["col"]

    def test_escaped_slash_stays_inside_one_segment(self) -> None:
        (ref,) = extract_bracket_refs(r"[A\/B/col]")
        assert ref.segments == ["A/B", "col"]

    def test_escaped_brackets_stay_inside_one_segment(self) -> None:
        (ref,) = extract_bracket_refs(r"[\[Rel\] Originators/CA_DIM_DOCUMENTS/Name]")
        assert ref.segments == ["[Rel] Originators", "CA_DIM_DOCUMENTS", "Name"]

    def test_segments_are_whitespace_stripped(self) -> None:
        (ref,) = extract_bracket_refs("[ E1 / E2 / col ]")
        assert ref.segments == ["E1", "E2", "col"]

    def test_hand_built_ref_does_not_split_column(self) -> None:
        """Back-compat default must not re-split a column containing "/"."""
        ref = BracketRef(raw="[a/b/c]", source="a", column="b/c", is_parameter=False)
        assert ref.segments == ["a", "b/c"]
        assert BracketRef(
            raw="[a]", source="a", column=None, is_parameter=False
        ).segments == ["a"]


class TestCandidateSplits:
    def test_join_chain_tries_owning_element_first(self) -> None:
        (ref,) = extract_bracket_refs("[WRK/WRK CA_DIM_DOCUMENTS/Account Id]")
        candidates = candidate_source_column_splits(ref)
        assert candidates[0] == ("WRK CA_DIM_DOCUMENTS", "Account Id")
        # The legacy first-slash split stays available, last.
        assert candidates[-1] == ("WRK", "WRK CA_DIM_DOCUMENTS/Account Id")

    def test_nested_chain_tries_deepest_element_first(self) -> None:
        (ref,) = extract_bracket_refs("[E1/E2/E3/col]")
        assert candidate_source_column_splits(ref)[0] == ("E3", "col")

    def test_single_slash_yields_only_todays_split(self) -> None:
        (ref,) = extract_bracket_refs("[Element/col]")
        assert candidate_source_column_splits(ref) == [("Element", "col")]

    def test_bare_ref_yields_nothing(self) -> None:
        (ref,) = extract_bracket_refs("[col]")
        assert candidate_source_column_splits(ref) == []

    def test_slash_containing_element_name_is_offered(self) -> None:
        (ref,) = extract_bracket_refs("[a/b/c]")
        assert ("a/b", "c") in candidate_source_column_splits(ref)
