"""Tests for Snowflake query performance optimizations.

Covers:
- _build_pattern_filter exact-FQN detection: RLIKE OR-chain → IN clause
"""

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery

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
