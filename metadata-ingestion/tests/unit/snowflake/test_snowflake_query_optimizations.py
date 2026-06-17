"""Tests for Snowflake query performance optimizations.

Covers:
- _build_pattern_filter exact-FQN detection: RLIKE OR-chain → IN clause
- paginate_query_values_segment_by_byte_budget: byte-budget IN-clause paging
- columns_for_schema_in_template: IN-clause column query template
"""

import re as _re

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
# Composition: wildcard patterns → single RLIKE alternation
# ---------------------------------------------------------------------------


class TestBuildPatternFilterComposition:
    """Wildcard/regex patterns that can be safely wrapped in (?:...) should
    produce a single RLIKE alternation rather than an OR-chain."""

    def test_wildcard_patterns_compose_to_single_rlike(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.*_PROD$", "DB.SCHEMA.TABLE_\\d+$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert " OR " not in result
        assert "(?:" in result

    def test_single_wildcard_composes(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.*$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert " OR " not in result

    def test_unbalanced_open_paren_skipped_with_warning(self):
        # "TABLE(unclosed" has an unbalanced open paren — _make_composable returns None
        pattern = AllowDenyPattern(
            allow=["DB.SCHEMA.TABLE(unclosed", "DB.SCHEMA.OTHER.*$"]
        )
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # The bad pattern is skipped; the good one is still composed
        assert "(?:DB.SCHEMA.OTHER" in result
        assert "unclosed" not in result

    def test_all_unbalanced_skipped_yields_empty(self):
        pattern = AllowDenyPattern(allow=["TABLE(bad1", "TABLE(bad2"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # Both skipped → no allow condition; empty string means no SQL filter
        assert result == ""

    def test_bare_open_paren_at_end_is_noncomposable(self):
        # "TABLE(" — the inner ( is closed by our wrapper's ), leaving (?:... unclosed
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE("])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert result == ""

    def test_backslash_escaped_paren_is_composable(self):
        # \) in regex means literal ')' (escaped), so (?:TABLE\)) is a valid closed group
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE\\)$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        # Backslash doubled in SQL literal; the regex seen by Snowflake is TABLE\)
        assert "\\\\" in result

    def test_composable_backslash_pattern_doubles_in_sql(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE_\\d+$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        # One backslash in the regex must become two in the SQL string literal
        assert "\\\\" in result

    def test_composable_pattern_with_ignorecase(self):
        pattern = AllowDenyPattern(allow=["db.schema.*_prod$"], ignoreCase=True)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "DB.SCHEMA.*_PROD" in result


# ---------------------------------------------------------------------------
# Adversarial: SQL injection, escape abuse, and regex breakout attempts
# ---------------------------------------------------------------------------


def _rlike_string_properly_escaped(sql: str) -> bool:
    """After collapsing '' (escaped SQL quotes), the RLIKE argument should have
    exactly one opening and one closing quote with no bare quote inside."""
    rlike_idx = sql.find("RLIKE ")
    if rlike_idx == -1:
        return True  # no RLIKE — trivially safe
    rlike_part = sql[rlike_idx + 6 :]
    collapsed = rlike_part.replace("''", "\x00")  # neutralise escaped quotes
    return collapsed.count("'") == 2  # exactly the opening and closing delimiters


class TestBuildPatternFilterAdversarial:
    """
    Adversarial inputs designed to break SQL quoting, inject statements,
    or exploit double/triple backslash escaping.

    The invariant: regardless of pattern content, the SQL output must either
    (a) contain no RLIKE (pattern was skipped), or (b) have a properly delimited
    RLIKE '...' string with no unescaped quotes that could terminate the literal.
    """

    @pytest.mark.parametrize(
        "attack",
        [
            # ---- SQL injection via single quote ----
            "DB.SCHEMA.TABLE'; DROP TABLE t; --",
            "DB.SCHEMA.TABLE' OR '1'='1",
            "DB.SCHEMA.TABLE'; SELECT * FROM sensitive; --$",
            "DB.SCHEMA.TABLE' UNION SELECT password FROM users --",
            # ---- Backslash + quote escape abuse ----
            # If escaping is naive (one pass), \' could become \\' → unescaped quote
            "DB.SCHEMA.TABLE\\'",
            "DB.SCHEMA.TABLE\\\\'",
            "DB.SCHEMA.TABLE\\\\\\'",
            # ---- Semicolons ----
            # Semicolons terminate SQL statements; must stay inside the literal
            "DB.SCHEMA.TABLE; DELETE FROM metadata; --$",
            "DB.SCHEMA.TABLE$; exec xp_cmdshell('bad'); --",
            # ---- Snowflake comment injection ----
            "DB.SCHEMA.TABLE/**/UNION SELECT",
            "DB.SCHEMA.TABLE--$",
            # ---- Regex composition breakout via )| ----
            # If (?:...) wrapper can be escaped, arbitrary alternation is added
            "DB.SCHEMA.TABLE)|(evil_table",
            ")|(DB.SCHEMA.TABLE",
            "DB.SCHEMA.TABLE)||(extra",
            # ---- Null byte ----
            "DB.SCHEMA.TABLE\x00EVIL",
            # ---- Unicode single quote (U+0027 == standard ', but belt-and-suspenders) ----
            "DB.SCHEMA.TABLE'EVIL",
            # ---- Double-percent / format-string attempts ----
            "DB.SCHEMA.TABLE%(injection)s",
            "DB.SCHEMA.TABLE{injection}",
        ],
    )
    def test_sql_injection_attempt_stays_in_string_literal(self, attack):
        pattern = AllowDenyPattern(allow=[attack])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)

        # Must produce a valid SQL fragment — never an empty-string exception
        assert isinstance(result, str)

        # The pattern either contributed no RLIKE (skipped as non-composable)
        # or the RLIKE string is properly quoted with no injection path
        assert _rlike_string_properly_escaped(result), (
            f"Possible SQL injection with pattern {attack!r}: {result!r}"
        )

    def test_backslash_quote_two_pass_correctness(self):
        # Naive single-pass escaping of \' produces \'' which is still an
        # unescaped backslash before a doubled quote. Correct order:
        # escape backslashes FIRST, then escape single quotes.
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.O\\'BRIEN$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # Should be safe regardless of whether it composed or was skipped
        assert _rlike_string_properly_escaped(result)

    def test_regex_composition_breakout_does_not_inject_sql(self):
        # "TABLE)|(evil" composes as (?:TABLE)|(evil) — a valid regex alternation
        # that matches 'evil' too. This widens what gets ingested but is NOT SQL
        # injection: it stays within the RLIKE string literal.
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE)|(evil_table"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert _rlike_string_properly_escaped(result)
        # Confirm no SQL keywords leaked outside the literal
        if "RLIKE" in result:
            after_rlike = result.split("RLIKE")[1]
            assert "DROP" not in after_rlike or "'" in after_rlike.split("DROP")[0]

    def test_triple_backslash_before_quote_is_safe(self):
        # \\\ before a ' — after proper two-pass escaping:
        # step 1: \\\' → \\\\\' (backslashes doubled)
        # step 2: ...\'' (quotes doubled)
        # No bare quote should escape the literal
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE\\\\'"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert _rlike_string_properly_escaped(result)

    def test_curly_brace_format_string_does_not_interpolate(self):
        # A pattern containing {in_values} or similar should not accidentally
        # trigger Python str.format() substitution in any code path
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.{in_values}"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert isinstance(result, str)
        assert _rlike_string_properly_escaped(result)


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


# ---------------------------------------------------------------------------
# Helpers for IN-clause adversarial tests
# ---------------------------------------------------------------------------

_IN_VALUE_RE = _re.compile(r"'((?:[^']|'')*)'")


def _parse_in_values(sql: str) -> list:
    """Extract SQL-unescaped values from the IN (...) clause.

    Uses the quoted-value regex rather than searching for ')' — names may
    contain ')' themselves (e.g. ``%(injection)s``), which would fool a naive
    find(')') search.
    """
    idx = sql.find("IN (")
    assert idx != -1, f"No IN clause in: {sql!r}"
    after_in = sql[idx + 4 :]
    return [m.group(1).replace("''", "'") for m in _IN_VALUE_RE.finditer(after_in)]


def _in_clause_has_no_bare_quotes(sql: str) -> bool:
    """After collapsing '' pairs, no bare single quote should remain inside
    any IN value — which would indicate a string literal that wasn't closed."""
    idx = sql.find("IN (")
    if idx == -1:
        return True
    after_in = sql[idx + 4 :]
    # Collapse escaped '' pairs; remaining ' are only structural delimiters.
    # Each value contributes exactly 2 structural quotes (open + close).
    collapsed = after_in.replace("''", "\x00")
    # Count bare quotes: should be exactly 2 per value (even total).
    return collapsed.count("'") % 2 == 0


# ---------------------------------------------------------------------------
# Adversarial table / column names — paginator IN-clause path
# ---------------------------------------------------------------------------


_PAGINATOR_TMPL = "SELECT * FROM t WHERE name IN ({in_values})"

# Ground-truth adversarial names: the paginator must round-trip these exactly.
_ADVERSARIAL_NAMES = [
    # SQL injection via quote termination
    "'; DROP TABLE t; --",
    "' OR '1'='1",
    "'; SELECT * FROM passwords; --",
    "' UNION SELECT null,null,null --",
    # Backslash + quote combinations (multi-pass escape abuse)
    "TABLE\\'",  # backslash then quote
    "TABLE\\\\'",  # two backslashes then quote
    "TABLE\\\\\\'",  # three backslashes then quote
    # Quote-only names
    "'",
    "''",
    "'''",
    # SQL comment injection
    "TABLE--COMMENT",
    "TABLE/*BLOCK*/NAME",
    # Semicolons
    "TABLE; DELETE FROM t; --",
    "TABLE; exec xp_cmdshell('cmd'); --",
    # Null byte (edge case for C-string parsers)
    "TABLE\x00EVIL",
    # Format string / template injection
    "{in_values}",
    "%(injection)s",
    # Unicode single quote (U+2019 RIGHT SINGLE QUOTATION MARK — not SQL-special)
    "TABLE’NAME",
    # Standard names that must still work
    "NORMAL_TABLE",
    "MY_DB.MY_SCHEMA.MY_TABLE",
]


class TestPaginatorAdversarialNames:
    """Verify the IN-clause paginator round-trips adversarial table names
    without SQL injection, escape abuse, or data corruption."""

    def test_adversarial_names_round_trip(self):
        """SQL-unescaping the paginator output must recover every original name."""
        pages = list(
            paginate_query_values_segment_by_byte_budget(
                _PAGINATOR_TMPL, _ADVERSARIAL_NAMES
            )
        )
        recovered = []
        for page in pages:
            recovered.extend(_parse_in_values(page))
        assert recovered == _ADVERSARIAL_NAMES

    def test_adversarial_names_no_bare_quotes(self):
        """No bare single quote should appear inside any IN value after escaping."""
        pages = list(
            paginate_query_values_segment_by_byte_budget(
                _PAGINATOR_TMPL, _ADVERSARIAL_NAMES
            )
        )
        for page in pages:
            assert _in_clause_has_no_bare_quotes(page), (
                f"Bare quote found in IN clause: {page!r}"
            )

    @pytest.mark.parametrize("attack", _ADVERSARIAL_NAMES)
    def test_single_adversarial_name_round_trips(self, attack):
        """Each adversarial name in isolation must round-trip correctly."""
        pages = list(
            paginate_query_values_segment_by_byte_budget(_PAGINATOR_TMPL, [attack])
        )
        assert len(pages) == 1
        recovered = _parse_in_values(pages[0])
        assert recovered == [attack], (
            f"Round-trip failed for {attack!r}: got {recovered!r}\nSQL: {pages[0]!r}"
        )

    def test_name_that_is_only_quotes_round_trips(self):
        # A table named literally "'''" (three single quotes) via Snowflake quoting
        name = "'''"
        pages = list(
            paginate_query_values_segment_by_byte_budget(_PAGINATOR_TMPL, [name])
        )
        recovered = _parse_in_values(pages[0])
        assert recovered == [name]

    def test_format_string_placeholder_does_not_interpolate(self):
        # If {in_values} appears in a table name, str.format() in the template
        # must not try to substitute it — the name is data, not a format key.
        name = "{in_values}"
        pages = list(
            paginate_query_values_segment_by_byte_budget(_PAGINATOR_TMPL, [name])
        )
        assert len(pages) == 1
        recovered = _parse_in_values(pages[0])
        assert recovered == [name]


# ---------------------------------------------------------------------------
# Adversarial schema / DB names — columns_for_schema_in_template
# ---------------------------------------------------------------------------


_SCHEMA_IN_WHERE_RE = _re.compile(r"WHERE table_schema='((?:[^']|'')*)'")


def _extract_schema_from_template(tmpl: str) -> str:
    """SQL-unescape the schema name embedded in the WHERE clause."""
    m = _SCHEMA_IN_WHERE_RE.search(tmpl)
    assert m, f"No schema WHERE clause in: {tmpl!r}"
    return m.group(1).replace("''", "'")


class TestColumnsTemplateAdversarialNames:
    """Verify that adversarial schema and DB names are safely embedded in the
    SQL template produced by columns_for_schema_in_template."""

    @pytest.mark.parametrize(
        "schema,db",
        [
            ("'; DROP TABLE t; --", "MY_DB"),
            ("MY_SCHEMA", "'; DROP TABLE t; --"),
            ("O'BRIEN_SCHEMA", "O'BRIEN_DB"),
            ("SCHEMA' OR '1'='1", "DB"),
            ("SCHEMA; DELETE FROM t --", "DB"),
            ("SCHEMA\\'", "DB"),
            ("{in_values}", "DB"),
            ("SCHEMA", "{in_values}"),
        ],
    )
    def test_adversarial_schema_names_round_trip(self, schema, db):
        tmpl = SnowflakeQuery.columns_for_schema_in_template(schema, db)
        # Placeholder must survive intact — if it were missing the paginator
        # would raise KeyError on str.format().
        assert "{in_values}" in tmpl, (
            f"Template placeholder was corrupted for schema={schema!r} db={db!r}"
        )
        # Round-trip the schema name: SQL-unescaping it must recover the original.
        # If a quote escaped the SQL string, the recovered value would differ.
        recovered = _extract_schema_from_template(tmpl)
        assert recovered == schema, (
            f"Schema name did not round-trip: expected {schema!r}, got {recovered!r}"
        )

    def test_adversarial_schema_round_trips_via_paginator(self):
        """End-to-end: adversarial schema name → template → paginate → valid SQL."""
        schema, db = "O'BRIEN; DROP TABLE t; --", "MY_DB"
        tmpl = SnowflakeQuery.columns_for_schema_in_template(schema, db)
        names = ["ORDERS", "CUSTOMERS"]
        pages = list(paginate_query_values_segment_by_byte_budget(tmpl, names))
        assert len(pages) == 1
        sql = pages[0]
        # Schema name must be safely embedded — round-trip proves no escape
        recovered_schema = _extract_schema_from_template(sql)
        assert recovered_schema == schema
        # Table names in the IN clause must be unaffected
        recovered_names = _parse_in_values(sql)
        assert recovered_names == names
