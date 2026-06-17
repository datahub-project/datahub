"""Tests for Snowflake query performance optimizations.

Covers:
- _build_pattern_filter exact-FQN detection: RLIKE OR-chain → IN clause
- paginate_query_values_segment_by_byte_budget: byte-budget IN-clause paging
- columns_for_schema_in_template: IN-clause column query template
"""

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_query import (
    SnowflakeQuery,
    paginate_query_values_segment_by_byte_budget,
)

FQN_EXPR = "UPPER(CONCAT(table_catalog, '.', table_schema, '.', table_name))"


# ---------------------------------------------------------------------------
# _build_pattern_filter — exact-FQN optimization
# ---------------------------------------------------------------------------


class TestBuildPatternFilterExactFqn:
    def test_exact_patterns_produce_in_clause(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE_A$", "DB.SCHEMA.TABLE_B$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "RLIKE" not in result
        assert f"UPPER({FQN_EXPR})" in result
        assert "'DB.SCHEMA.TABLE_A'" in result
        assert "'DB.SCHEMA.TABLE_B'" in result

    def test_exact_patterns_case_sensitive(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$"], ignoreCase=False)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "RLIKE" not in result
        assert result.startswith(FQN_EXPR + " IN (")

    def test_exact_patterns_ignore_case_uppercases_values(self):
        pattern = AllowDenyPattern(allow=["db.schema.my_table$"], ignoreCase=True)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "'DB.SCHEMA.MY_TABLE'" in result

    def test_wildcard_pattern_falls_back_to_rlike(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.*$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "IN (" not in result

    def test_mixed_exact_and_wildcard_falls_back_to_rlike(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE_A$", "DB.SCHEMA.TABLE_.*"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "IN (" not in result

    def test_deny_pattern_prevents_in_clause_optimization(self):
        pattern = AllowDenyPattern(
            allow=["DB.SCHEMA.TABLE_A$"], deny=["DB.SCHEMA.SECRET.*"]
        )
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result

    def test_allow_all_returns_empty_string(self):
        pattern = AllowDenyPattern(allow=[".*"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert result == ""

    def test_single_exact_pattern_produces_in_clause(self):
        pattern = AllowDenyPattern(allow=["MYDB.MYSCHEMA.MYTABLE$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "RLIKE" not in result

    def test_dollar_sign_in_name_is_valid_exact_pattern(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$1$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "'DB.SCHEMA.TABLE$1'" in result

    def test_large_exact_pattern_list_produces_in_clause(self):
        names = [f"DB.SCHEMA.TABLE_{i}$" for i in range(500)]
        pattern = AllowDenyPattern(allow=names)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "RLIKE" not in result
        assert "'DB.SCHEMA.TABLE_0'" in result
        assert "'DB.SCHEMA.TABLE_499'" in result


# ---------------------------------------------------------------------------
# Security and correctness: exact-pattern boundary and SQL escaping
# ---------------------------------------------------------------------------


class TestBuildPatternFilterSecurity:
    """Verify that dangerous characters never reach the IN clause path, and that
    the RLIKE fallback path produces valid SQL regardless of pattern content."""

    @pytest.mark.parametrize(
        "pattern",
        [
            "DB.SCHEMA.TABLE'; DROP TABLE t; --$",
            "DB.SCHEMA.TABLE\\$",
            "DB.SCHEMA.TABLE;$",
            "DB.SCHEMA.TABLE NAME$",
            "DB.SCHEMA.TABLE*$",
            "DB.SCHEMA.TABLE+$",
            "DB.SCHEMA.TABLE?$",
            "(DB.SCHEMA.TABLE)$",
            "DB.SCHEMA.TABLE|OTHER$",
            "DB.SCHEMA.[TABLE]$",
            "^DB.SCHEMA.TABLE$",
            "DB.SCHEMA.TABLE{2}$",
        ],
    )
    def test_dangerous_patterns_fall_through_to_rlike(self, pattern):
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=[pattern]), FQN_EXPR
        )
        assert "RLIKE" in result, (
            f"Expected RLIKE for pattern {pattern!r}, got: {result}"
        )
        assert "IN (" not in result

    def test_single_quote_in_rlike_pattern_is_doubled(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.O'BRIEN_TABLE$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "O''BRIEN" in result
        stripped = result.replace("''", "")
        assert "'" not in stripped.split("RLIKE")[1].split("'")[1]

    def test_backslash_in_rlike_pattern_is_doubled_for_sql_and_regex(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE\\d$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "\\\\" in result

    def test_trailing_dollar_stripped_from_in_value(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "'DB.SCHEMA.TABLE'" in result
        assert "'DB.SCHEMA.TABLE$'" not in result

    def test_ignore_case_false_preserves_case_in_in_clause(self):
        pattern = AllowDenyPattern(allow=["Db.Schema.MyTable$"], ignoreCase=False)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "'Db.Schema.MyTable'" in result

    def test_ignore_case_true_uppercases_in_clause_values(self):
        pattern = AllowDenyPattern(allow=["Db.Schema.MyTable$"], ignoreCase=True)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "'DB.SCHEMA.MYTABLE'" in result
        assert "Db" not in result

    def test_dot_in_exact_pattern_used_literally_in_in_clause(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "IN (" in result
        assert "'DB.SCHEMA.TABLE'" in result

    def test_empty_allow_list_returns_false(self):
        pattern = AllowDenyPattern(allow=[])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert result == "FALSE"

    def test_none_pattern_returns_empty_string(self):
        result = SnowflakeQuery._build_pattern_filter(None, FQN_EXPR)
        assert result == ""


# ---------------------------------------------------------------------------
# paginate_query_values_segment_by_byte_budget
# ---------------------------------------------------------------------------

_TMPL = "SELECT x FROM t WHERE name IN ({in_values})"


class TestPaginateQueryValuesByByteBudget:
    def test_small_list_yields_single_query(self):
        names = ["TABLE_A", "TABLE_B", "TABLE_C"]
        pages = list(paginate_query_values_segment_by_byte_budget(_TMPL, names))
        assert len(pages) == 1
        assert "'TABLE_A'" in pages[0]
        assert "'TABLE_B'" in pages[0]
        assert "'TABLE_C'" in pages[0]

    def test_oversized_list_pages_correctly(self):
        # Budget so small that only one name fits per page.
        names = ["A", "B", "C"]
        tiny_budget = len(_TMPL.replace("{in_values}", "'A'").encode()) + 1
        pages = list(
            paginate_query_values_segment_by_byte_budget(_TMPL, names, tiny_budget)
        )
        assert len(pages) == 3
        for page, name in zip(pages, names, strict=False):
            assert f"'{name}'" in page

    def test_single_quote_escaped_in_output(self):
        names = ["O'BRIEN"]
        pages = list(paginate_query_values_segment_by_byte_budget(_TMPL, names))
        assert len(pages) == 1
        assert "O''BRIEN" in pages[0]

    def test_empty_names_yields_nothing(self):
        pages = list(paginate_query_values_segment_by_byte_budget(_TMPL, []))
        assert pages == []

    def test_each_page_within_budget(self):
        budget = 200
        names = [f"TABLE_{i:04d}" for i in range(50)]
        pages = list(paginate_query_values_segment_by_byte_budget(_TMPL, names, budget))
        for page in pages:
            assert len(page.encode()) <= budget


# ---------------------------------------------------------------------------
# columns_for_schema_in_template
# ---------------------------------------------------------------------------


class TestColumnsForSchemaInTemplate:
    def test_template_contains_in_values_placeholder(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("MY_SCHEMA", "MY_DB")
        assert "{in_values}" in tmpl

    def test_template_contains_schema_and_db(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("MY_SCHEMA", "MY_DB")
        assert "MY_SCHEMA" in tmpl
        assert "MY_DB" in tmpl

    def test_template_selects_expected_columns(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("S", "D")
        for col in (
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "DATA_TYPE",
        ):
            assert col in tmpl

    def test_substituted_template_is_valid_sql(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("MY_SCHEMA", "MY_DB")
        sql = tmpl.format(in_values="'TABLE_A', 'TABLE_B'")
        assert "IN ('TABLE_A', 'TABLE_B')" in sql
        assert "{in_values}" not in sql

    def test_schema_name_with_single_quote_is_escaped(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("O'BRIEN_SCHEMA", "MY_DB")
        assert "O''BRIEN_SCHEMA" in tmpl
        assert "O'BRIEN_SCHEMA'" not in tmpl.replace("O''BRIEN_SCHEMA", "")

    def test_db_name_with_single_quote_is_escaped(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("MY_SCHEMA", "O'BRIEN_DB")
        assert "O''BRIEN_DB" in tmpl

    def test_template_used_with_paginator_produces_valid_sql(self):
        tmpl = SnowflakeQuery.columns_for_schema_in_template("MY_SCHEMA", "MY_DB")
        names = [f"TABLE_{i}" for i in range(5)]
        pages = list(paginate_query_values_segment_by_byte_budget(tmpl, names))
        assert len(pages) == 1
        assert "table_name IN (" in pages[0]
        assert "'TABLE_0'" in pages[0]
        assert "'TABLE_4'" in pages[0]
