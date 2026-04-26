"""Tests for the Sigma formula bracket-reference extractor."""

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs


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


def test_ref_inside_function() -> None:
    result = extract_bracket_refs("Count([x])")
    assert len(result) == 1
    assert result[0].source == "x"
    assert result[0].column is None


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


def test_parameter_with_slash_is_not_parameter() -> None:
    """A `P_*` ref with a column part is NOT treated as a parameter per our defensive rule."""
    result = extract_bracket_refs("[P_X/col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "P_X"
    assert ref.column == "col"
    assert ref.is_parameter is False


def test_multiline_formula() -> None:
    formula = "If(\n  [a] > 0,\n  [b],\n  [c]\n)"
    result = extract_bracket_refs(formula)
    assert len(result) == 3
    assert result[0].source == "a"
    assert result[1].source == "b"
    assert result[2].source == "c"


def test_empty_bracket_skipped() -> None:
    """Empty brackets `[]` don't match — the body must be 1+ chars."""
    assert extract_bracket_refs("[]") == []


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


# --- Missing-test additions from review ---


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


def test_bracket_body_with_quote_pins_current_behavior() -> None:
    """A quote inside a bracket body may be mutilated if it pairs with a quote elsewhere.

    This test pins the CURRENT (imperfect) behavior. If this becomes a
    production issue, replace _strip_string_literals with a stateful scanner.
    """
    # Unbalanced quote: the lone `"` inside the bracket can't form a string
    # literal so the body is captured intact.
    result = extract_bracket_refs('[col"name]')
    assert len(result) == 1
    assert result[0].source == 'col"name'
