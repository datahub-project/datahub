"""Tests for Snowflake query performance optimizations.

Covers:
- _build_pattern_filter: composition of allow patterns into a single RLIKE alternation
- paginate_query_values_segment_by_byte_budget: byte-budget IN-clause paging
- columns_for_schema_in_template: IN-clause column query template
"""

import itertools
import re

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_query import (
    _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE,
    SnowflakeQuery,
    _make_composable,
    paginate_query_values_segment_by_byte_budget,
)

FQN_EXPR = "UPPER(CONCAT(table_catalog, '.', table_schema, '.', table_name))"


# ---------------------------------------------------------------------------
# Security and correctness: SQL escaping and RLIKE quoting
# ---------------------------------------------------------------------------


class TestBuildPatternFilterSecurity:
    """Verify that all allow patterns go through RLIKE and produce correctly
    quoted SQL regardless of pattern content — no injection path exists."""

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
    def test_all_patterns_use_rlike(self, pattern):
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=[pattern]), FQN_EXPR
        )
        assert "RLIKE" in result, (
            f"Expected RLIKE for pattern {pattern!r}, got: {result}"
        )

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

    def test_exact_literal_dollar_anchor_kept_in_rlike(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "DB.SCHEMA.TABLE$" in result

    def test_ignore_case_false_preserves_case_in_rlike(self):
        pattern = AllowDenyPattern(allow=["Db.Schema.MyTable$"], ignoreCase=False)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "Db.Schema.MyTable" in result

    def test_ignore_case_true_uppercases_rlike_pattern(self):
        pattern = AllowDenyPattern(allow=["Db.Schema.MyTable$"], ignoreCase=True)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "DB.SCHEMA.MYTABLE" in result
        assert "Db" not in result

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

    def test_all_unbalanced_skipped_fails_closed(self):
        pattern = AllowDenyPattern(allow=["TABLE(bad1", "TABLE(bad2"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # All allow patterns dropped → must fail CLOSED (match nothing), not emit
        # an empty filter that would allow everything.
        assert result == "FALSE"

    def test_bare_open_paren_at_end_is_noncomposable(self):
        # "TABLE(" — the inner ( is closed by our wrapper's ), leaving (?:... unclosed
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE("])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # Sole allow pattern dropped → fail closed.
        assert result == "FALSE"

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

    def test_exact_literal_patterns_compose_to_single_rlike(self):
        # Exact-literal patterns (no metacharacters beyond '$') go through the
        # fast path in _make_composable and emerge as a single RLIKE alternation.
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE_A$", "DB.SCHEMA.TABLE_B$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert " OR " not in result
        assert "(?:DB.SCHEMA.TABLE_A$)" in result
        assert "(?:DB.SCHEMA.TABLE_B$)" in result

    def test_exact_literal_with_deny_still_composes(self):
        # Adding a deny pattern no longer gates the IN-clause path (removed);
        # allow patterns still compose into a single RLIKE alternation.
        pattern = AllowDenyPattern(
            allow=["DB.SCHEMA.TABLE_A$"], deny=["DB.SCHEMA.SECRET.*"]
        )
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert " OR " not in result

    def test_large_exact_literal_list_is_single_rlike(self):
        names = [f"DB.SCHEMA.TABLE_{i}$" for i in range(500)]
        pattern = AllowDenyPattern(allow=names)
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert " OR " not in result
        assert "(?:DB.SCHEMA.TABLE_0$)" in result
        assert "(?:DB.SCHEMA.TABLE_499$)" in result

    def test_dollar_sign_in_name_is_composable(self):
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.TABLE$1$"])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert "RLIKE" in result
        assert "(?:DB.SCHEMA.TABLE$1$)" in result


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


def _parse_snowflake_string_literals(body: str) -> list:
    """Parse comma-separated single-quoted literals using Snowflake's REAL
    escaping rules — BOTH ``''`` and backslash escapes.

    A ``''``-only oracle (the obvious-looking regex ``'((?:[^']|'')*)'``) is
    WRONG for Snowflake: by default ``\\'`` is an escaped quote and a trailing
    backslash consumes the closing quote. Using the wrong oracle makes a
    backslash injection appear to "round-trip" cleanly. Raises ``ValueError``
    on an unterminated literal, which is the signature of an injection breakout.
    """
    values: list = []
    i, n = 0, len(body)
    while i < n:
        while i < n and body[i] in ", \t\n":
            i += 1
        if i >= n:
            break
        assert body[i] == "'", f"expected opening quote at {i}: {body[i:]!r}"
        i += 1
        chars: list = []
        terminated = False
        while i < n:
            c = body[i]
            if c == "\\":
                if i + 1 >= n:
                    raise ValueError("dangling backslash escaped the closing quote")
                chars.append(body[i + 1])
                i += 2
                continue
            if c == "'":
                if i + 1 < n and body[i + 1] == "'":
                    chars.append("'")
                    i += 2
                    continue
                i += 1
                terminated = True
                break
            chars.append(c)
            i += 1
        if not terminated:
            raise ValueError(f"unterminated literal (recovered {''.join(chars)!r})")
        values.append("".join(chars))
    return values


def test_snowflake_oracle_rejects_backslash_breakout():
    """Guard the test oracle itself. A ``''``-only parser would happily "recover"
    a value from ``'TABLE\\'`` (the trailing backslash eats the closing quote),
    giving a false all-clear. The Snowflake-accurate oracle must reject it. This
    keeps the oracle these tests rely on from silently regressing to ``''``-only,
    which is what hid the backslash injection in the first place."""
    with pytest.raises(ValueError):
        _parse_snowflake_string_literals(r"'TABLE\'")


def _parse_in_values(sql: str) -> list:
    """Extract Snowflake-unescaped values from the IN (...) clause."""
    idx = sql.find("IN (")
    assert idx != -1, f"No IN clause in: {sql!r}"
    body = sql[idx + 4 : sql.rfind(")")]
    return _parse_snowflake_string_literals(body)


def _in_clause_is_well_formed(sql: str) -> bool:
    """True iff every IN-clause literal is properly terminated under Snowflake's
    real escaping rules (no backslash breakout)."""
    idx = sql.find("IN (")
    if idx == -1:
        return True
    body = sql[idx + 4 : sql.rfind(")")]
    try:
        _parse_snowflake_string_literals(body)
        return True
    except (ValueError, AssertionError):
        return False


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
    "TABLE\\",  # trailing backslash — most direct breakout (eats closing quote)
    "TABLE\\'",  # backslash then quote
    "TABLE\\\\'",  # two backslashes then quote
    "TABLE\\\\\\'",  # three backslashes then quote
    "ORDERS\\' UNION SELECT current_user() --",  # backslash breakout + payload
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
        """Every IN-clause literal must be properly terminated under Snowflake's
        real escaping rules (backslash + quote)."""
        pages = list(
            paginate_query_values_segment_by_byte_budget(
                _PAGINATOR_TMPL, _ADVERSARIAL_NAMES
            )
        )
        for page in pages:
            assert _in_clause_is_well_formed(page), (
                f"Malformed/unterminated literal in IN clause: {page!r}"
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


def _extract_schema_from_sql(sql: str) -> str:
    """Snowflake-unescape the schema name embedded in the WHERE clause of a
    final (post-format) query. Backslash-aware, so a backslash that escaped the
    closing quote surfaces as a parse error rather than a silently-wrong value.
    """
    idx = sql.find("table_schema=")
    assert idx != -1, f"No schema WHERE clause in: {sql!r}"
    body = sql[idx + len("table_schema=") :].split("\n", 1)[0].strip()
    return _parse_snowflake_string_literals(body)[0]


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
            ("PUBLIC\\", "DB"),  # trailing backslash in schema name
            ("{in_values}", "DB"),
            ("SCHEMA", "{in_values}"),
            ("S{0}CHEMA", "MY_DB"),  # positional brace — would crash naive .format()
            ("SCHEMA{", "DB"),  # lone brace — would raise ValueError in .format()
        ],
    )
    def test_adversarial_schema_names_round_trip(self, schema, db):
        tmpl = SnowflakeQuery.columns_for_schema_in_template(schema, db)
        # Brace chars in the identifier must not break the later str.format()
        # (no KeyError/IndexError/ValueError) and the real placeholder must
        # resolve to exactly the supplied IN list, not the identifier.
        sql = tmpl.format(in_values="'PLACEHOLDER'")
        assert _parse_in_values(sql) == ["PLACEHOLDER"], (
            f"IN placeholder corrupted for schema={schema!r} db={db!r}: {sql!r}"
        )
        # Round-trip the schema name under Snowflake's real escaping rules.
        recovered = _extract_schema_from_sql(sql)
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
        recovered_schema = _extract_schema_from_sql(sql)
        assert recovered_schema == schema
        # Table names in the IN clause must be unaffected
        recovered_names = _parse_in_values(sql)
        assert recovered_names == names


# ---------------------------------------------------------------------------
# _make_composable fast path: the re.compile-skip safety contract
# ---------------------------------------------------------------------------
#
# _make_composable wraps a pattern in (?:...) and, for patterns matching
# _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE, returns it WITHOUT the re.compile validation
# every other pattern goes through. Skipping validation is only safe because that
# subset admits no SQL-quote, backslash, or regex group/alternation metacharacter.
# These tests pin that contract so any future loosening of the subset (or of the
# escaping) trips a red test instead of shipping a scope-widening / literal-
# breaking pattern straight into SQL.


def _extract_rlike_literal(sql: str) -> str:
    """Return the FIRST ``RLIKE '...'`` literal in *sql*, parsed with Snowflake's
    real escaping rules. Raises ``ValueError`` if the literal is unterminated
    (a backslash/quote broke out of it)."""
    marker = "RLIKE '"
    idx = sql.find(marker)
    assert idx != -1, f"no RLIKE literal in: {sql!r}"
    i = idx + len(marker)
    chars: list = []
    n = len(sql)
    while i < n:
        c = sql[i]
        if c == "\\":
            if i + 1 >= n:
                raise ValueError(
                    "backslash escaped the closing quote of the RLIKE literal"
                )
            chars.append(sql[i + 1])
            i += 2
            continue
        if c == "'":
            if i + 1 < n and sql[i + 1] == "'":
                chars.append("'")
                i += 2
                continue
            return "".join(chars)
        chars.append(c)
        i += 1
    raise ValueError(f"unterminated RLIKE literal (recovered {''.join(chars)!r})")


# SQL-literal breakers (quote, backslash) and regex group/alternation breakers
# that the validation-skipping subset must never admit.
_FORBIDDEN_IN_FAST_PATH = ["'", "\\", "(", ")", "|", "[", "]", "{", "}", "*", "+", "?"]


class TestMakeComposableFastPath:
    """Lock the safety contract of the re.compile-skipping fast path."""

    @pytest.mark.parametrize("ch", _FORBIDDEN_IN_FAST_PATH)
    def test_subset_regex_rejects_dangerous_characters(self, ch):
        """If the subset ever admits a SQL-literal breaker or a regex
        group/alternation metacharacter, skipping re.compile becomes unsafe."""
        assert not _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE.match(f"DB.T{ch}ABLE$"), (
            f"subset regex now admits {ch!r} — re.compile skip is unsafe"
        )

    def test_every_subset_pattern_is_a_valid_wrapped_regex(self):
        """Skipping re.compile is sound only if EVERY subset-matching pattern is a
        valid regex once wrapped. Fuzz the alphabet to confirm there is no input
        the slow path would have rejected."""
        offenders = []
        for length in range(1, 5):
            for combo in itertools.product("AZ09_$.", repeat=length):
                p = "".join(combo) + "$"
                if not _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE.match(p):
                    continue
                wrapped = _make_composable(p)
                assert wrapped is not None
                try:
                    re.compile(wrapped)
                except re.error:
                    offenders.append(p)
        assert not offenders, (
            f"fast path accepts but re.compile rejects: {offenders[:10]}"
        )

    def test_fast_path_output_equals_validated_path(self):
        """The optimization must be behavior-preserving: for subset patterns the
        fast path returns exactly what the validate-then-wrap path would."""
        for p in ["A.B.C$", "PROD_DB.PUBLIC.T$", "9X$", "A$B$", "X...$", "T_1$"]:
            wrapped = f"(?:{p})"
            re.compile(wrapped)  # the slow path would accept it
            assert _make_composable(p) == wrapped

    @pytest.mark.parametrize(
        "allow",
        [
            ["DB.SCHEMA.TABLE'; DROP TABLE t; --"],
            ["DB.SCHEMA.TABLE\\"],  # trailing backslash
            ["DB.SCHEMA.O\\'BRIEN"],  # backslash + quote
            ["DB.SCHEMA.T\\\\"],  # double backslash
            ["DB.SCHEMA.TABLE)|(.*"],  # regex-breakout attempt via )|(
            ["A'$", "B\\$"],  # quote/backslash patterns that LOOK subset-ish
            ["NORMAL.TABLE$"],  # benign control
        ],
    )
    def test_allow_pattern_rlike_literal_is_always_well_formed(self, allow):
        """Whatever the allow pattern, the emitted RLIKE literal must be a single,
        properly-terminated Snowflake literal — no quote/backslash may break out.
        (Patterns may still WIDEN regex matching, but that is within the recipe
        author's existing authority; SQL breakout is not.)"""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=allow), FQN_EXPR
        )
        if "RLIKE '" in result:
            _extract_rlike_literal(result)  # raises on breakout

    def test_quotey_subsetlike_pattern_routes_through_escaping(self):
        """``A'$`` resembles a safe literal but contains a quote, so it must NOT
        take the validation-skip fast path, and its quote must be doubled."""
        assert not _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE.match("A'$")
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=["A'$"]), FQN_EXPR
        )
        assert "''" in result
        _extract_rlike_literal(result)


# ---------------------------------------------------------------------------
# Adversarial deny patterns (NOT RLIKE)
# ---------------------------------------------------------------------------


def _all_rlike_literals(sql: str) -> list:
    """Return every ``RLIKE '...'`` / ``NOT RLIKE '...'`` literal in *sql*, parsed
    with Snowflake's real escaping rules. Raises ``ValueError`` if any literal is
    left unterminated (backslash/quote breakout)."""
    results: list = []
    marker = "RLIKE '"
    i, n = 0, len(sql)
    while True:
        idx = sql.find(marker, i)
        if idx == -1:
            return results
        j = idx + len(marker)
        chars: list = []
        terminated = False
        while j < n:
            c = sql[j]
            if c == "\\":
                if j + 1 >= n:
                    raise ValueError(
                        "backslash consumed the closing quote of a RLIKE literal"
                    )
                chars.append(sql[j + 1])
                j += 2
                continue
            if c == "'":
                if j + 1 < n and sql[j + 1] == "'":
                    chars.append("'")
                    j += 2
                    continue
                j += 1
                terminated = True
                break
            chars.append(c)
            j += 1
        if not terminated:
            raise ValueError(
                f"unterminated RLIKE literal (recovered {''.join(chars)!r})"
            )
        results.append("".join(chars))
        i = j


class TestDenyPatternInjection:
    """Deny patterns are recipe-controlled (untrusted). Each NOT RLIKE literal
    must stay self-contained, and deny patterns must never be silently dropped."""

    @pytest.mark.parametrize(
        "deny",
        [
            "SECRET.*'; DROP TABLE t; --",
            "SECRET\\",  # trailing backslash — would eat the closing quote if unescaped
            "O\\'BRIEN_SECRET",  # backslash + quote
            "X\\\\",  # double backslash
            ".*_TEMP' OR '1'='1",
            "SECRET)|(.*",  # regex widen attempt (must stay inside the literal)
        ],
    )
    def test_single_deny_literal_is_well_formed(self, deny):
        # ignoreCase=False keeps the round-trip exact; escaping is case-independent.
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(deny=[deny], ignoreCase=False), FQN_EXPR
        )
        literals = _all_rlike_literals(result)  # raises on breakout
        assert deny in literals, (
            f"deny pattern {deny!r} not recovered intact from: {result!r}"
        )

    def test_backslash_deny_is_escaped(self):
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(deny=["SECRET\\"]), FQN_EXPR
        )
        assert "\\\\" in result  # backslash doubled for the SQL literal
        _all_rlike_literals(result)  # well-formed

    def test_allow_and_deny_both_literals_well_formed(self):
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=["DB.GOOD.*$"], deny=["DB.SECRET\\", "X'Y"]),
            FQN_EXPR,
        )
        literals = _all_rlike_literals(result)
        assert any("GOOD" in lit for lit in literals)
        assert "NOT RLIKE" in result and " AND " in result

    def test_uncomposable_deny_is_not_silently_dropped(self):
        """A deny pattern an allow pattern would drop (unbalanced paren) must NOT
        vanish from the deny path — it should still be emitted (failing loudly at
        Snowflake), never silently widening collection to excluded objects."""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(deny=["SECRET("]), FQN_EXPR
        )
        assert "NOT RLIKE" in result, f"deny pattern was silently dropped: {result!r}"
        assert "SECRET(" in _all_rlike_literals(result)
