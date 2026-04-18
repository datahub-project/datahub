from datahub.ingestion.source.sigma.formula_parser import BracketRef, parse_formula


class TestParseFormulaBasicCases:
    def test_empty_formula(self) -> None:
        assert parse_formula("") == []

    def test_whitespace_only(self) -> None:
        assert parse_formula("   ") == []

    def test_no_brackets(self) -> None:
        assert parse_formula("1 + 2 / 3") == []

    def test_single_col_no_slash(self) -> None:
        # [Col] — same-element self-ref; caller drops, but parser returns it
        refs = parse_formula("[Status]")
        assert refs == [BracketRef(source=None, column="Status", raw="Status")]

    def test_namespaced_ref(self) -> None:
        # [Source/Column] — cross-element or warehouse ref
        refs = parse_formula("[T Log/status]")
        assert refs == [BracketRef(source="T Log", column="status", raw="T Log/status")]

    def test_warehouse_ref(self) -> None:
        refs = parse_formula("[FIVETRAN_LOG__AUDIT_TABLE/Connector Id]")
        assert refs == [
            BracketRef(
                source="FIVETRAN_LOG__AUDIT_TABLE",
                column="Connector Id",
                raw="FIVETRAN_LOG__AUDIT_TABLE/Connector Id",
            )
        ]

    def test_parameter_ref(self) -> None:
        # [P_*] — parameter; parser returns it; caller skips silently
        refs = parse_formula("[P_DateFilter]")
        assert refs == [
            BracketRef(source=None, column="P_DateFilter", raw="P_DateFilter")
        ]

    def test_element_self_ref(self) -> None:
        # DM-backed passthrough: element name == source name
        refs = parse_formula("[random data model/player_of_match]")
        assert refs == [
            BracketRef(
                source="random data model",
                column="player_of_match",
                raw="random data model/player_of_match",
            )
        ]


class TestParseFormulaEscapedSlash:
    def test_escaped_slash_in_column_name(self) -> None:
        # [Rank of MAR \/ Modified] — `\/` is a literal `/`; no source/column split
        refs = parse_formula("[Rank of MAR \\/ Modified]")
        assert refs == [
            BracketRef(
                source=None,
                column="Rank of MAR / Modified",
                raw="Rank of MAR / Modified",
            )
        ]

    def test_escaped_slash_in_namespaced_ref(self) -> None:
        # Source/Column where column name contains `\/`
        refs = parse_formula("[Source/Col \\/ Sub]")
        assert refs == [
            BracketRef(
                source="Source",
                column="Col / Sub",
                raw="Source/Col / Sub",
            )
        ]


class TestParseFormulaMultipleRefs:
    def test_intra_element_calc(self) -> None:
        # [Selected Metric] / [Successful Runs] — two no-slash refs
        refs = parse_formula("[Selected Metric] / [Successful Runs]")
        assert refs == [
            BracketRef(source=None, column="Selected Metric", raw="Selected Metric"),
            BracketRef(source=None, column="Successful Runs", raw="Successful Runs"),
        ]

    def test_intra_dm_calc(self) -> None:
        refs = parse_formula("[win_by_runs] + [win_by_wickets]")
        assert refs == [
            BracketRef(source=None, column="win_by_runs", raw="win_by_runs"),
            BracketRef(source=None, column="win_by_wickets", raw="win_by_wickets"),
        ]

    def test_sigma_dataset_passthrough(self) -> None:
        # Data Model passthrough: [dataset_name/col]
        refs = parse_formula("[data.csv/city]")
        assert refs == [
            BracketRef(source="data.csv", column="city", raw="data.csv/city")
        ]


class TestParseFormulaNestedFunctions:
    def test_rank_wrapping_col(self) -> None:
        # Function wrapper; only the bracket ref inside matters
        refs = parse_formula("Rank([Score])")
        assert refs == [BracketRef(source=None, column="Score", raw="Score")]

    def test_count_distinct_if(self) -> None:
        refs = parse_formula('CountDistinctIf([User Id], [Status] = "active")')
        assert len(refs) == 2
        assert refs[0] == BracketRef(source=None, column="User Id", raw="User Id")
        assert refs[1] == BracketRef(source=None, column="Status", raw="Status")

    def test_nested_function_calls(self) -> None:
        refs = parse_formula("Power(Count([Col A]), [Exponent])")
        assert len(refs) == 2
        columns = {r.column for r in refs}
        assert columns == {"Col A", "Exponent"}


class TestParseFormulaMalformedInput:
    def test_unclosed_bracket_returns_partial(self) -> None:
        # Parser returns what it found before the unclosed bracket
        refs = parse_formula("[Good/Col] + [Unclosed")
        assert refs == [BracketRef(source="Good", column="Col", raw="Good/Col")]

    def test_empty_bracket_ref(self) -> None:
        # [] — degenerate but shouldn't crash
        refs = parse_formula("[]")
        assert refs == [BracketRef(source=None, column="", raw="")]

    def test_no_text_inside_brackets(self) -> None:
        refs = parse_formula("[/]")
        assert refs == [BracketRef(source="", column="", raw="/")]
