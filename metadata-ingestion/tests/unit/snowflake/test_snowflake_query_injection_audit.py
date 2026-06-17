"""Adversarial SQL-injection / exfiltration audit for the Snowflake query
optimizations introduced after commit 4088474a4974d8784591a2254ef3204c1893a4ae.

WHY THIS FILE EXISTS
--------------------
The optimization commits (RLIKE-OR-chain -> IN clause, byte-budget paging,
columns_for_schema_in_template) ship with a large companion test file,
``test_snowflake_query_optimizations.py``. Several of those tests *look*
adversarial ("SQL injection", "escape abuse", "round-trip") but validate the
output with an oracle that is WEAKER than the SQL engine the queries actually
run against. Specifically, the existing oracle treats ``''`` as the only quote
escape and ignores BACKSLASH escaping. Snowflake string literals, by default,
treat backslash as an escape character (``\\'`` == a literal quote,
``\\\\`` == a literal backslash). A test that unescapes with the wrong rules
will "pass" on SQL that Snowflake would reject or mis-parse.

THREAT MODEL (per the operator running this audit)
--------------------------------------------------
The deployments running this connector are assumed to be reachable by hostile
end-users who can:
  (a) author ingestion recipes  -> ``allow``/``deny`` / db / schema PATTERNS
      are attacker-controlled, and
  (b) create databases / schemas / tables / columns in a crawled account ->
      OBJECT NAMES returned by Snowflake are attacker-controlled.

Therefore BOTH the recipe-supplied patterns and the source-supplied object
names must be treated as untrusted. The tests below encode that invariant
using a Snowflake-accurate string-literal parser as the oracle.

HOW TO READ FAILURES
--------------------
A failing test here is not flaky — it is a *finding*. Each test docstring names
the vector. These tests are written to PASS only if the producing code escapes
backslashes (and quotes) correctly and fails closed on unusable patterns. They
will FAIL against the current code, which:
  * escapes only ``'`` (not ``\\``) when building IN-clause values and the
    columns template, and
  * silently drops un-composable allow patterns, collapsing the WHERE clause to
    an empty string (allow-everything) instead of failing closed.
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

FQN_EXPR = "CONCAT(table_catalog, '.', table_schema, '.', table_name)"
_TMPL = "SELECT x FROM t WHERE name IN ({in_values})"


# ---------------------------------------------------------------------------
# Snowflake-accurate string-literal oracle
# ---------------------------------------------------------------------------
#
# Snowflake single-quoted string constants (default session settings) accept
# TWO escape mechanisms inside a literal:
#   * ``''``  -> one literal single quote
#   * ``\X``  -> the character X taken literally (so ``\'`` is a quote and
#                ``\\`` is a backslash); a backslash thus *consumes the next
#                character*, including a quote that would otherwise close the
#                literal.
# A single ``'`` that is neither doubled nor backslash-escaped terminates the
# literal. Anything after an unterminated literal is live SQL -> injection.
#
# This parser raises if a literal is left unterminated or the value list is
# structurally malformed. That raise IS the injection detector.


def parse_snowflake_string_list(body: str) -> list:
    """Parse a comma-separated list of Snowflake single-quoted string literals.

    Honors both ``''`` and backslash escapes, exactly as a default Snowflake
    session would. Raises ``ValueError`` on an unterminated literal or
    unexpected structure — i.e. when the surrounding query has been broken out
    of, which is the signature of a successful injection.
    """
    values: list = []
    i = 0
    n = len(body)
    while i < n:
        while i < n and body[i] in ", \t\n":
            i += 1
        if i >= n:
            break
        if body[i] != "'":
            raise ValueError(
                f"Expected an opening quote at offset {i}; found {body[i:]!r}. "
                "A bare token outside a string literal means the literal above "
                "was closed early (injection breakout)."
            )
        i += 1  # consume opening quote
        chars: list = []
        terminated = False
        while i < n:
            c = body[i]
            if c == "\\":
                if i + 1 >= n:
                    raise ValueError(
                        "Dangling backslash escape at end of input — the "
                        "backslash consumed the closing quote (injection)."
                    )
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
            raise ValueError(
                f"Unterminated string literal (recovered so far: "
                f"{''.join(chars)!r}). The closing quote was escaped away — "
                "subsequent characters are interpreted as live SQL."
            )
        values.append("".join(chars))
    return values


def _in_body(sql: str) -> str:
    """Return the raw text between ``IN (`` and the LAST ``)`` in the query.

    Using the last paren is deliberate: a naive 'first )' scan can be fooled by
    an injected ``)`` inside a broken literal, hiding the breakout from the
    oracle. We hand the parser the widest plausible region so an injection
    cannot duck out of the analysis window.
    """
    idx = sql.find("IN (")
    assert idx != -1, f"no IN clause: {sql!r}"
    start = idx + 4
    end = sql.rfind(")")
    return sql[start:end]


# ---------------------------------------------------------------------------
# Vector 1: attacker-controlled TABLE NAMES -> paginator IN-clause injection
# ---------------------------------------------------------------------------


class TestSourceTableNameInjectionViaPaginator:
    """``paginate_query_values_segment_by_byte_budget`` receives object names
    discovered from the account (``prefix_group.names`` in snowflake_schema.py).
    Under this threat model those names are attacker-chosen. The function escapes
    only ``'`` and NOT ``\\``, so a name ending in a backslash escapes the
    closing quote of its own literal.
    """

    # Each of these is a plausible Snowflake object name an attacker could
    # create. The literal each produces must remain self-contained.
    BACKSLASH_BREAKOUTS = [
        "TABLE\\",  # trailing backslash -> eats the closing quote
        "TABLE\\'",  # backslash + quote
        "ORDERS\\' UNION SELECT current_user(),current_role() --",
        "X\\\\'",  # two backslashes then a quote
        "A\\' OR 1=1 --",
    ]

    @pytest.mark.parametrize("name", BACKSLASH_BREAKOUTS)
    def test_backslash_name_does_not_break_out_of_literal(self, name):
        """FINDING: a backslash in a table name terminates the SQL string
        literal early because the producer never doubles backslashes. The
        Snowflake-accurate oracle detects the unterminated literal."""
        (page,) = list(paginate_query_values_segment_by_byte_budget(_TMPL, [name]))
        recovered = parse_snowflake_string_list(_in_body(page))
        assert recovered == [name], (
            f"Table name {name!r} did not round-trip under Snowflake quoting; "
            f"emitted SQL: {page!r}"
        )

    def test_mixed_batch_round_trips_under_snowflake_rules(self):
        """A realistic mix: benign names plus one hostile backslash name. If the
        hostile name breaks out, the whole batched query is compromised."""
        names = ["ORDERS", "CUSTOMERS", "EVIL\\", "INVOICES"]
        pages = list(paginate_query_values_segment_by_byte_budget(_TMPL, names))
        recovered: list = []
        for page in pages:
            recovered.extend(parse_snowflake_string_list(_in_body(page)))
        assert recovered == names

    def test_quote_only_names_still_safe(self):
        """Quote-only names ARE handled (only '' escaping is needed); included
        as a control so a failure clearly isolates the backslash gap, not quote
        handling."""
        names = ["'", "''", "O'BRIEN"]
        (page,) = list(paginate_query_values_segment_by_byte_budget(_TMPL, names))
        assert parse_snowflake_string_list(_in_body(page)) == names


# ---------------------------------------------------------------------------
# Vector 2: existing suite's oracle is weaker than Snowflake (false assurance)
# ---------------------------------------------------------------------------


class TestExistingOracleIsTooWeak:
    """Demonstrates *why* the companion suite's injection tests pass on
    vulnerable output: its unescaper ignores backslash escaping. This is the
    mechanism by which a real backslash injection is hidden behind green tests.
    """

    def test_naive_oracle_accepts_what_snowflake_rejects(self):
        """On a literal where a trailing backslash escapes the closing quote, the
        ``''``-only oracle happily "recovers" a value while a Snowflake-accurate
        parser correctly flags the unterminated literal. This is the mechanism
        by which the original suite's green tests hid the backslash injection —
        it is a property of the oracles, independent of the (now-fixed) code."""
        import re

        naive_re = re.compile(r"'((?:[^']|'')*)'")
        # Hand-crafted: a single value whose trailing backslash eats the quote.
        vulnerable_body = r"'TABLE\'"

        assert naive_re.search(vulnerable_body) is not None, (
            "control: the weak ''-only oracle accepts the malformed literal"
        )
        with pytest.raises(ValueError):
            parse_snowflake_string_list(vulnerable_body)


# ---------------------------------------------------------------------------
# Vector 3: attacker-controlled SCHEMA / DB NAMES -> columns template
# ---------------------------------------------------------------------------


class TestSchemaDbNameInjectionViaTemplate:
    """``columns_for_schema_in_template`` interpolates schema/db names with an
    f-string (escaping only ``'``) and leaves a literal ``{in_values}`` for a
    later ``str.format()``. This two-phase build introduces two problems when
    schema/db names are attacker-controlled.
    """

    def test_backslash_in_schema_name_does_not_break_where_clause(self):
        """FINDING: a backslash in a schema name escapes the closing quote of
        the ``table_schema='...'`` predicate (only quotes are doubled)."""
        schema = "PUBLIC\\"
        tmpl = SnowflakeQuery.columns_for_schema_in_template(schema, "MY_DB")
        sql = tmpl.format(in_values="'ORDERS'")
        where_line = next(line for line in sql.splitlines() if "table_schema=" in line)
        # Extract the literal after table_schema= and parse it Snowflake-style.
        literal = where_line.split("table_schema=", 1)[1].strip()
        assert parse_snowflake_string_list(literal) == [schema], (
            f"schema name {schema!r} broke out of its literal: {where_line!r}"
        )

    @pytest.mark.parametrize(
        "schema",
        [
            "S{0}CHEMA",  # positional field -> IndexError at format() time
            "{in_values}",  # collides with the real placeholder -> displacement
            "SCHEMA{",  # lone brace -> ValueError at format() time
        ],
    )
    def test_brace_in_schema_name_does_not_crash_or_displace(self, schema):
        """FINDING: because the template is finished with ``str.format()``, any
        ``{``/``}`` in an attacker-chosen schema name either crashes ingestion
        (DoS) or, for ``{in_values}``, makes the IN-list get substituted into
        the schema predicate position (structure corruption)."""
        tmpl = SnowflakeQuery.columns_for_schema_in_template(schema, "MY_DB")
        try:
            sql = tmpl.format(in_values="'ORDERS'")
        except (IndexError, KeyError, ValueError) as e:
            pytest.fail(
                f"schema name {schema!r} crashed str.format(): {type(e).__name__}: {e}"
            )
        # If it did not crash, the table-name list must NOT have leaked into the
        # schema predicate, and the schema must round-trip in its own literal.
        where_line = next(line for line in sql.splitlines() if "table_schema=" in line)
        literal = where_line.split("table_schema=", 1)[1].strip()
        assert parse_snowflake_string_list(literal) == [schema], (
            f"schema {schema!r} displaced/corrupted the WHERE clause: {where_line!r}"
        )


# ---------------------------------------------------------------------------
# Vector 4: attacker-controlled RECIPE PATTERNS -> fail-open scope broadening
# ---------------------------------------------------------------------------


class TestRecipePatternFailOpen:
    """When every ``allow`` pattern is un-composable (e.g. unbalanced parens),
    the new RLIKE-composition path drops them all and appends NO condition. With
    no deny patterns, the whole filter collapses to ``""`` — meaning *no SQL
    filter*, i.e. ingest everything. For an operator who used an allow-list to
    scope a crawl away from sensitive schemas, a single malformed pattern
    silently widens collection to the entire account (metadata exfiltration).
    """

    def test_all_uncomposable_allow_patterns_must_not_fail_open(self):
        """FINDING: all-invalid allow list -> '' (allow everything) instead of
        failing closed."""
        pattern = AllowDenyPattern(allow=["SENSITIVE(", "ALSO_BAD("])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        assert result not in ("", None), (
            "all-invalid allow-list collapsed to an empty (allow-everything) "
            f"filter; got {result!r}. An allow-list that silently matches "
            "everything is a scope-broadening / exfiltration risk."
        )

    def test_partial_invalid_allow_does_not_silently_shrink_scope(self):
        """A mix of valid and invalid allow patterns: the invalid one is dropped
        with only a log warning. Whether dropping is acceptable is a policy
        call, but it must be observable, not silent. This test documents the
        current silent-drop behavior so a regression in either direction is
        visible."""
        pattern = AllowDenyPattern(allow=["DB.SCHEMA.GOOD.*$", "DB.SCHEMA.BAD("])
        result = SnowflakeQuery._build_pattern_filter(pattern, FQN_EXPR)
        # The valid pattern still applies...
        assert "RLIKE" in result
        # ...but the dropped pattern means the emitted filter is NOT equivalent
        # to applying both. Flag that the bad pattern simply vanished.
        assert "BAD" not in result, (
            "sanity: malformed pattern text should not leak into SQL"
        )


# ---------------------------------------------------------------------------
# Vector 5: the RLIKE-only refactor's re.compile-SKIP fast path
# (commit 2d39aa1e478 "refactor(snowflake): go RLIKE-only for allow patterns")
# ---------------------------------------------------------------------------
#
# That commit removed the IN-clause tier and added a fast path to
# ``_make_composable``: if a pattern matches ``_SNOWFLAKE_SAFE_LITERAL_SUBSET_RE``
# it is wrapped in ``(?:...)`` and returned WITHOUT the ``re.compile`` validation
# that every other pattern goes through. The safety of skipping validation rests
# entirely on a claim about that regex's character set. If the regex is ever
# broadened to admit a SQL-quote, a backslash, or a regex group/alternation
# metacharacter, the skip would emit an unvalidated — and potentially
# literal-breaking or scope-widening — pattern straight into SQL.
#
# These tests pin the invariants that make the skip safe, so any future loosening
# of the subset (or of the escaping) trips a red test instead of shipping quietly.


def _extract_rlike_literal(sql: str) -> str:
    """Return the raw text of the FIRST ``RLIKE '...'`` string literal in *sql*,
    parsed with Snowflake's real escaping rules. Raises ``ValueError`` if the
    literal is unterminated (a backslash/quote broke out of it)."""
    marker = "RLIKE '"
    idx = sql.find(marker)
    assert idx != -1, f"no RLIKE literal in: {sql!r}"
    i = idx + len(marker)  # first char inside the literal
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
            return "".join(chars)  # properly closed
        chars.append(c)
        i += 1
    raise ValueError(f"unterminated RLIKE literal (recovered {''.join(chars)!r})")


# Characters that must NEVER be admitted by the validation-skipping subset:
# SQL-literal breakers (quote, backslash) and regex-group/alternation breakers.
_FORBIDDEN_IN_FAST_PATH = ["'", "\\", "(", ")", "|", "[", "]", "{", "}", "*", "+", "?"]


class TestRlikeOnlyFastPathInvariants:
    """Lock the safety contract of the ``re.compile``-skipping fast path."""

    @pytest.mark.parametrize("ch", _FORBIDDEN_IN_FAST_PATH)
    def test_subset_regex_rejects_dangerous_characters(self, ch):
        """The validation-skip subset must reject any pattern containing a
        SQL-literal breaker or a regex group/alternation metacharacter. If this
        fails, the subset was broadened and the skip is no longer safe."""
        # Place the char in a pattern that otherwise looks like a safe literal.
        assert not _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE.match(f"DB.T{ch}ABLE$"), (
            f"subset regex now admits {ch!r} — re.compile skip is unsafe"
        )

    def test_every_subset_pattern_is_a_valid_wrapped_regex(self):
        """Skipping ``re.compile`` is only sound if EVERY subset-matching pattern
        is in fact a valid regex once wrapped. Fuzz the subset alphabet and
        confirm there is no input the slow path would have rejected."""
        alphabet = "AZ09_$."
        offenders = []
        for length in range(1, 5):
            for combo in itertools.product(alphabet, repeat=length):
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
            f"subset patterns the fast path accepts but re.compile rejects: "
            f"{offenders[:10]}"
        )

    def test_fast_path_output_equals_validated_path(self):
        """The optimization must be behavior-preserving: for subset patterns the
        fast path must return exactly what the validate-then-wrap path would."""
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
        """Whatever the allow pattern, the emitted RLIKE string literal must be a
        single, properly-terminated Snowflake literal — no quote/backslash may
        break out of it. (Patterns may still WIDEN regex matching, but that is
        within the recipe author's existing authority; SQL breakout is not.)"""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=allow), FQN_EXPR
        )
        if "RLIKE '" in result:
            # Must parse as one well-formed literal (raises on breakout).
            _extract_rlike_literal(result)

    def test_quotey_subsetlike_pattern_routes_through_escaping(self):
        """``A'$`` superficially resembles a safe literal but contains a quote,
        so it must NOT take the validation-skip fast path, and its quote must be
        doubled in the final SQL literal."""
        assert not _SNOWFLAKE_SAFE_LITERAL_SUBSET_RE.match("A'$")
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=["A'$"]), FQN_EXPR
        )
        assert "''" in result  # quote doubled
        _extract_rlike_literal(result)  # well-formed

    def test_exact_fqn_allow_is_now_a_regex_not_an_exact_match(self):
        """Behavioral lock: removing the IN tier means an 'exact' FQN allow
        pattern is now a REGEX where '.' matches any char. Documented so a future
        silent re-introduction of an exact/IN path (or further widening) is
        visible. This is not an injection — it restores DataHub's long-standing
        regex AllowDenyPattern semantics."""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=["DB.SCHEMA.TABLE$"]), FQN_EXPR
        )
        assert "RLIKE" in result and "IN (" not in result
        # The dot is emitted literally into the regex (a wildcard at match time),
        # not escaped to a literal dot — proving exactness was traded for regex.
        assert "(?:DB.SCHEMA.TABLE$)" in result


# ---------------------------------------------------------------------------
# Vector 6: the DENY path (NOT RLIKE)
# ---------------------------------------------------------------------------
#
# The deny branch of _build_pattern_filter was NOT touched by the recent
# optimization commits, but it is the other half of the filter and is equally
# attacker-reachable (deny patterns come from the recipe). Two properties matter:
#
#   1. SQL-literal integrity: each ``NOT RLIKE '...'`` literal must stay closed
#      regardless of pattern content (it currently escapes backslash THEN quote
#      inline — correct, but duplicated rather than using the shared helper, so a
#      future "cleanup" could silently drop the backslash pass).
#   2. NO silent drop: unlike the allow path, deny patterns are never run through
#      _make_composable, so an unusable deny pattern reaches Snowflake and errors
#      LOUDLY instead of vanishing. A vanished deny = objects meant to be EXCLUDED
#      get ingested (exfiltration of intended-excluded metadata). If someone ever
#      adds drop-on-invalid to the deny path, test_uncomposable_deny... must fail.


def _all_rlike_literals(sql: str) -> list:
    """Return every ``RLIKE '...'`` / ``NOT RLIKE '...'`` literal in *sql*,
    parsed with Snowflake's real escaping rules. Raises ``ValueError`` if any
    literal is left unterminated (backslash/quote breakout)."""
    results: list = []
    marker = "RLIKE '"
    i = 0
    n = len(sql)
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
    must stay self-contained under Snowflake's real escaping rules."""

    DENY_ATTACKS = [
        "SECRET.*'; DROP TABLE t; --",
        "SECRET\\",  # trailing backslash -> would eat the closing quote if unescaped
        "O\\'BRIEN_SECRET",  # backslash + quote
        "X\\\\",  # double backslash
        ".*_TEMP' OR '1'='1",
        "SECRET)|(.*",  # regex widen attempt (stays in literal)
    ]

    @pytest.mark.parametrize("deny", DENY_ATTACKS)
    def test_single_deny_literal_is_well_formed(self, deny):
        # ignoreCase=False so the pattern is not uppercased by transform(), making
        # the round-trip exact. Escaping is independent of case, so this still
        # fully exercises the SQL-literal escape path.
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
        """Combined allow+deny: every literal in the AND-joined filter must be
        well-formed, and the structure must apply both."""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(allow=["DB.GOOD.*$"], deny=["DB.SECRET\\", "X'Y"]),
            FQN_EXPR,
        )
        literals = _all_rlike_literals(result)
        assert any("GOOD" in lit for lit in literals)
        assert "NOT RLIKE" in result and " AND " in result

    def test_uncomposable_deny_is_not_silently_dropped(self):
        """A deny pattern that an allow pattern would drop (e.g. unbalanced
        paren) must NOT vanish from the deny path — it should still be emitted
        (and fail loudly at Snowflake), never silently widening collection."""
        result = SnowflakeQuery._build_pattern_filter(
            AllowDenyPattern(deny=["SECRET("]), FQN_EXPR
        )
        assert "NOT RLIKE" in result, (
            "deny pattern was silently dropped — objects meant to be excluded "
            f"would be ingested. Got: {result!r}"
        )
        assert "SECRET(" in _all_rlike_literals(result)
