import pytest

from datahub.sdk.search_filters import FilterDsl as F
from datahub_agent_context.mcp_tools.search_filter_parser import parse_filter_string

# ---------------------------------------------------------------------------
# Basic parsing (carried over from original tests)
# ---------------------------------------------------------------------------


def test_simple_equals():
    result = parse_filter_string("platform = snowflake")
    assert result == F.platform("snowflake")


def test_simple_equals_with_urn():
    result = parse_filter_string("domain = urn:li:domain:marketing")
    assert result == F.domain("urn:li:domain:marketing")


def test_in_operator():
    result = parse_filter_string("platform IN (snowflake, bigquery)")
    assert result == F.platform(["snowflake", "bigquery"])


def test_in_operator_single_value():
    result = parse_filter_string("entity_type IN (dataset)")
    assert result == F.entity_type("dataset")


def test_and():
    result = parse_filter_string("platform = snowflake AND env = PROD")
    assert result == F.and_(F.platform("snowflake"), F.env("PROD"))


def test_or():
    result = parse_filter_string("platform = snowflake OR platform = bigquery")
    assert result == F.or_(F.platform("snowflake"), F.platform("bigquery"))


def test_not():
    result = parse_filter_string("NOT platform = snowflake")
    assert result == F.not_(F.platform("snowflake"))


def test_not_equals():
    result = parse_filter_string("platform != snowflake")
    assert result == F.not_(F.platform("snowflake"))


def test_parentheses():
    result = parse_filter_string(
        "entity_type = dataset AND (platform = snowflake OR platform = bigquery)"
    )
    assert result == F.and_(
        F.entity_type("dataset"),
        F.or_(F.platform("snowflake"), F.platform("bigquery")),
    )


def test_complex_expression():
    result = parse_filter_string(
        "entity_type = dataset AND env = PROD AND (platform = snowflake OR platform = bigquery)"
    )
    assert result == F.and_(
        F.entity_type("dataset"),
        F.env("PROD"),
        F.or_(F.platform("snowflake"), F.platform("bigquery")),
    )


def test_and_with_not():
    result = parse_filter_string("entity_type = dataset AND NOT platform = looker")
    assert result == F.and_(
        F.entity_type("dataset"),
        F.not_(F.platform("looker")),
    )


def test_in_with_and():
    result = parse_filter_string(
        "entity_type = dataset AND platform IN (snowflake, bigquery, redshift)"
    )
    assert result == F.and_(
        F.entity_type("dataset"),
        F.platform(["snowflake", "bigquery", "redshift"]),
    )


def test_entity_type_filter():
    assert parse_filter_string("entity_type = dataset") == F.entity_type("dataset")
    assert parse_filter_string("type = chart") == F.entity_type("chart")


def test_entity_subtype_filter():
    assert parse_filter_string("entity_subtype = Table") == F.entity_subtype("Table")
    assert parse_filter_string("subtype = View") == F.entity_subtype("View")


def test_tag_filter():
    result = parse_filter_string("tag = urn:li:tag:PII")
    assert result == F.tag("urn:li:tag:PII")


def test_owner_filter():
    result = parse_filter_string("owner = urn:li:corpuser:alice")
    assert result == F.owner("urn:li:corpuser:alice")


def test_glossary_term_filter():
    result = parse_filter_string("glossary_term = urn:li:glossaryTerm:data-quality")
    assert result == F.glossary_term("urn:li:glossaryTerm:data-quality")


def test_container_filter():
    result = parse_filter_string(
        "container = urn:li:container:f784c48c306ba1c775ef917e2f8c1560"
    )
    assert result == F.container("urn:li:container:f784c48c306ba1c775ef917e2f8c1560")


def test_env_filter():
    result = parse_filter_string("env = PROD")
    assert result == F.env("PROD")
    result = parse_filter_string("environment = DEV")
    assert result == F.env("DEV")


def test_unknown_field_becomes_custom():
    result = parse_filter_string('customProperties = "key=value"')
    assert result == F.custom_filter("customProperties", "EQUAL", ["key=value"])


def test_unknown_field_simple_value():
    result = parse_filter_string("customProperties = somevalue")
    assert result == F.custom_filter("customProperties", "EQUAL", ["somevalue"])


def test_quoted_values():
    result = parse_filter_string('platform = "snowflake"')
    assert result == F.platform("snowflake")


def test_quoted_values_with_spaces():
    result = parse_filter_string('tag = "urn:li:tag:my tag"')
    assert result == F.tag("urn:li:tag:my tag")


def test_single_quoted_values():
    result = parse_filter_string("platform = 'snowflake'")
    assert result == F.platform("snowflake")


def test_operator_precedence():
    """AND binds tighter than OR: a OR b AND c == a OR (b AND c)."""
    result = parse_filter_string(
        "platform = snowflake OR platform = bigquery AND env = PROD"
    )
    assert result == F.or_(
        F.platform("snowflake"),
        F.and_(F.platform("bigquery"), F.env("PROD")),
    )


def test_whitespace_tolerance():
    result = parse_filter_string("  platform  =  snowflake  AND  env  =  PROD  ")
    assert result == F.and_(F.platform("snowflake"), F.env("PROD"))


def test_case_insensitive_keywords():
    result = parse_filter_string("platform = snowflake and env = PROD")
    assert result == F.and_(F.platform("snowflake"), F.env("PROD"))

    result = parse_filter_string("platform = snowflake or platform = bigquery")
    assert result == F.or_(F.platform("snowflake"), F.platform("bigquery"))

    result = parse_filter_string("not platform = snowflake")
    assert result == F.not_(F.platform("snowflake"))

    result = parse_filter_string("platform in (snowflake, bigquery)")
    assert result == F.platform(["snowflake", "bigquery"])


def test_case_insensitive_field_names():
    assert parse_filter_string("Platform = snowflake") == F.platform("snowflake")
    assert parse_filter_string("ENTITY_TYPE = dataset") == F.entity_type("dataset")
    assert parse_filter_string("ENV = PROD") == F.env("PROD")


def test_empty_string_raises():
    with pytest.raises(ValueError, match="cannot be empty"):
        parse_filter_string("")

    with pytest.raises(ValueError, match="cannot be empty"):
        parse_filter_string("   ")


def test_unterminated_string_raises():
    with pytest.raises(ValueError, match="Unterminated string"):
        parse_filter_string('platform = "snowflake')


def test_missing_operator_raises():
    with pytest.raises(ValueError, match="Expected ="):
        parse_filter_string("platform snowflake")


def test_unexpected_token_raises():
    with pytest.raises(ValueError, match="Unexpected token"):
        parse_filter_string("platform = snowflake bigquery")


def test_nested_parens():
    result = parse_filter_string(
        "(platform = snowflake OR platform = bigquery) AND (env = PROD OR env = STAGING)"
    )
    assert result == F.and_(
        F.or_(F.platform("snowflake"), F.platform("bigquery")),
        F.or_(F.env("PROD"), F.env("STAGING")),
    )


# ---------------------------------------------------------------------------
# Bug fix: lone `!` no longer causes infinite loop
# ---------------------------------------------------------------------------


def test_bare_exclamation_raises():
    with pytest.raises(ValueError, match="Unexpected character"):
        parse_filter_string("!")


def test_exclamation_not_followed_by_equals_raises():
    with pytest.raises(ValueError, match="Unexpected character"):
        parse_filter_string("platform ! snowflake")


def test_trailing_exclamation_raises():
    with pytest.raises(ValueError, match="Unexpected character"):
        parse_filter_string("platform = foo!")


# ---------------------------------------------------------------------------
# Bug fix: backslash escapes in quoted strings are now unescaped
# ---------------------------------------------------------------------------


def test_escaped_double_quote_in_double_quoted_string():
    result = parse_filter_string(r'custom_field = "snow\"flake"')
    assert result == F.custom_filter("custom_field", "EQUAL", ['snow"flake'])


def test_escaped_single_quote_in_single_quoted_string():
    result = parse_filter_string(r"custom_field = 'it\'s'")
    assert result == F.custom_filter("custom_field", "EQUAL", ["it's"])


def test_escaped_backslash_in_quoted_string():
    result = parse_filter_string(r'custom_field = "back\\slash"')
    assert result == F.custom_filter("custom_field", "EQUAL", ["back\\slash"])


def test_no_escape_sequences_unchanged():
    result = parse_filter_string('custom_field = "no escape"')
    assert result == F.custom_filter("custom_field", "EQUAL", ["no escape"])


# ---------------------------------------------------------------------------
# Bug fix: keywords (AND, OR, NOT, IN) accepted as values
# ---------------------------------------------------------------------------


def test_keyword_as_quoted_value():
    assert parse_filter_string('platform = "AND"') == F.platform("AND")
    assert parse_filter_string('platform = "NOT"') == F.platform("NOT")
    assert parse_filter_string('platform = "IN"') == F.platform("IN")
    assert parse_filter_string('platform = "OR"') == F.platform("OR")


def test_keyword_in_urn_value():
    """Keywords embedded in URN-like values are part of a single unquoted token."""
    result = parse_filter_string("tag = urn:li:tag:NOT")
    assert result == F.tag("urn:li:tag:NOT")


def test_keyword_in_in_list_quoted():
    result = parse_filter_string('platform IN ("AND", "OR")')
    assert result == F.platform(["AND", "OR"])


# ---------------------------------------------------------------------------
# Bug fix: NOT env / env != no longer crashes at compile time
# ---------------------------------------------------------------------------


def test_not_env_parses_and_compiles():
    result = parse_filter_string("NOT env = DEV")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert len(rules) == 2
    fields = {r.field for r in rules}
    assert fields == {"origin", "env"}
    for rule in rules:
        assert rule.negated is True
        assert rule.condition == "EQUAL"
        assert rule.values == ["DEV"]


def test_not_environment_alias_parses_and_compiles():
    result = parse_filter_string("NOT environment = STAGING")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    fields = {r.field for r in rules}
    assert fields == {"origin", "env"}
    for rule in rules:
        assert rule.negated is True
        assert rule.values == ["STAGING"]


def test_env_not_equals_parses_and_compiles():
    result = parse_filter_string("env != DEV")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert len(rules) == 2
    fields = {r.field for r in rules}
    assert fields == {"origin", "env"}
    for rule in rules:
        assert rule.negated is True


def test_not_env_same_as_env_neq():
    """NOT env = DEV and env != DEV should produce the same compiled output."""
    not_env = parse_filter_string("NOT env = DEV")
    env_neq = parse_filter_string("env != DEV")
    assert not_env.compile() == env_neq.compile()


def test_not_env_combined_with_and():
    result = parse_filter_string("entity_type = dataset AND NOT env = DEV")
    compiled = result.compile()
    assert len(compiled) == 1
    and_rules = compiled[0]["and"]
    entity_type_rules = [r for r in and_rules if r.field == "_entityType"]
    env_rules = [r for r in and_rules if r.field in ("origin", "env")]
    assert len(entity_type_rules) == 1
    assert entity_type_rules[0].negated is False
    assert len(env_rules) == 2
    for r in env_rules:
        assert r.negated is True


# ---------------------------------------------------------------------------
# Status filter tests
# ---------------------------------------------------------------------------


def test_status_not_soft_deleted():
    from datahub.ingestion.graph.filters import RemovedStatusFilter

    result = parse_filter_string("status = NOT_SOFT_DELETED")
    assert result == F.soft_deleted(RemovedStatusFilter.NOT_SOFT_DELETED)


def test_status_all():
    from datahub.ingestion.graph.filters import RemovedStatusFilter

    result = parse_filter_string("status = ALL")
    assert result == F.soft_deleted(RemovedStatusFilter.ALL)


def test_status_only_soft_deleted():
    from datahub.ingestion.graph.filters import RemovedStatusFilter

    result = parse_filter_string("status = ONLY_SOFT_DELETED")
    assert result == F.soft_deleted(RemovedStatusFilter.ONLY_SOFT_DELETED)


def test_status_multiple_values_raises():
    with pytest.raises(ValueError, match="exactly one value"):
        parse_filter_string("status IN (NOT_SOFT_DELETED, ALL)")


# ---------------------------------------------------------------------------
# Error message quality tests
# ---------------------------------------------------------------------------


def test_empty_in_list_gives_parse_error():
    with pytest.raises(ValueError, match="Expected value"):
        parse_filter_string("platform IN ()")


def test_trailing_equals_gives_parse_error():
    with pytest.raises(ValueError, match="Expected value"):
        parse_filter_string("platform = ")


def test_missing_field_gives_parse_error():
    with pytest.raises(ValueError, match="Expected STRING"):
        parse_filter_string("= snowflake")


# ---------------------------------------------------------------------------
# Compilation round-trip tests
# ---------------------------------------------------------------------------


def test_compile_platform_filter():
    result = parse_filter_string("platform = snowflake")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert any(
        r.field == "platform.keyword" and "snowflake" in str(r.values) for r in rules
    )


def test_compile_not_platform():
    result = parse_filter_string("NOT platform = snowflake")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert len(rules) == 1
    assert rules[0].negated is True
    assert rules[0].field == "platform.keyword"


def test_compile_and_filters():
    result = parse_filter_string("entity_type = dataset AND platform = snowflake")
    compiled = result.compile()
    assert len(compiled) == 1
    and_rules = compiled[0]["and"]
    fields = {r.field for r in and_rules}
    assert "_entityType" in fields
    assert "platform.keyword" in fields


def test_compile_env_positive():
    """Positive env filter produces two OR clauses (origin and env fields)."""
    result = parse_filter_string("env = PROD")
    compiled = result.compile()
    assert len(compiled) == 2
    fields = {compiled[0]["and"][0].field, compiled[1]["and"][0].field}
    assert fields == {"origin", "env"}


# ---------------------------------------------------------------------------
# IS NULL / IS NOT NULL
# ---------------------------------------------------------------------------


def test_is_not_null_glossary_term():
    result = parse_filter_string("glossary_term IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert len(rules) == 1
    assert rules[0].field == "glossaryTerms"
    assert rules[0].condition == "EXISTS"
    assert rules[0].negated is False


def test_is_null_glossary_term():
    result = parse_filter_string("glossary_term IS NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    assert len(rules) == 1
    assert rules[0].field == "glossaryTerms"
    assert rules[0].condition == "EXISTS"
    assert rules[0].negated is True


def test_is_not_null_tag():
    result = parse_filter_string("tag IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "tags"
    assert compiled[0]["and"][0].condition == "EXISTS"


def test_is_not_null_owner():
    result = parse_filter_string("owner IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "owners"
    assert compiled[0]["and"][0].condition == "EXISTS"


def test_is_not_null_platform():
    result = parse_filter_string("platform IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "platform.keyword"
    assert compiled[0]["and"][0].condition == "EXISTS"


def test_is_not_null_domain():
    result = parse_filter_string("domain IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "domains"
    assert compiled[0]["and"][0].condition == "EXISTS"


def test_is_not_null_case_insensitive():
    result = parse_filter_string("tag is not null")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "tags"
    assert compiled[0]["and"][0].condition == "EXISTS"


def test_is_not_null_combined_with_and():
    result = parse_filter_string("entity_type = dataset AND glossary_term IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    fields = {r.field for r in rules}
    assert "_entityType" in fields
    assert "glossaryTerms" in fields


def test_is_null_env():
    """IS NULL on env checks both origin and env fields."""
    result = parse_filter_string("env IS NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rules = compiled[0]["and"]
    fields = {r.field for r in rules}
    assert fields == {"origin", "env"}
    for rule in rules:
        assert rule.condition == "EXISTS"
        assert rule.negated is True


def test_is_not_null_env():
    """IS NOT NULL on env checks either origin or env field."""
    result = parse_filter_string("env IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 2
    fields = {compiled[0]["and"][0].field, compiled[1]["and"][0].field}
    assert fields == {"origin", "env"}
    for clause in compiled:
        assert clause["and"][0].condition == "EXISTS"
        assert clause["and"][0].negated is False


def test_is_null_custom_field():
    """IS NULL on unrecognized fields uses the field name as-is."""
    result = parse_filter_string("customField IS NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    assert compiled[0]["and"][0].field == "customField"
    assert compiled[0]["and"][0].condition == "EXISTS"
    assert compiled[0]["and"][0].negated is True


def test_is_null_status_raises():
    with pytest.raises(ValueError, match="not supported for the status field"):
        parse_filter_string("status IS NOT NULL")


def test_is_without_null_raises():
    with pytest.raises(ValueError, match="Expected NULL"):
        parse_filter_string("tag IS something")


# ---------------------------------------------------------------------------
# Boolean filter fields (deprecated, hasActiveIncidents, hasFailingAssertions)
# ---------------------------------------------------------------------------


def test_deprecated_true():
    result = parse_filter_string("deprecated = true")
    assert result == F.custom_filter("deprecated", "EQUAL", ["true"])


def test_deprecated_false():
    result = parse_filter_string("deprecated = false")
    assert result == F.custom_filter("deprecated", "EQUAL", ["false"])


def test_has_active_incidents_true():
    result = parse_filter_string("hasActiveIncidents = true")
    assert result == F.custom_filter("hasActiveIncidents", "EQUAL", ["true"])


def test_has_failing_assertions_true():
    result = parse_filter_string("hasFailingAssertions = true")
    assert result == F.custom_filter("hasFailingAssertions", "EQUAL", ["true"])


def test_deprecated_is_not_null():
    result = parse_filter_string("deprecated IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rule = compiled[0]["and"][0]
    assert rule.field == "deprecated"
    assert rule.condition == "EXISTS"
    assert rule.negated is False


def test_deprecated_is_null():
    result = parse_filter_string("deprecated IS NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rule = compiled[0]["and"][0]
    assert rule.field == "deprecated"
    assert rule.condition == "EXISTS"
    assert rule.negated is True


def test_has_active_incidents_is_not_null():
    result = parse_filter_string("hasActiveIncidents IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rule = compiled[0]["and"][0]
    assert rule.field == "hasActiveIncidents"
    assert rule.condition == "EXISTS"
    assert rule.negated is False


def test_column_count_equals():
    result = parse_filter_string("columnCount = 10")
    assert result == F.custom_filter("columnCount", "EQUAL", ["10"])


def test_column_count_is_not_null():
    result = parse_filter_string("columnCount IS NOT NULL")
    compiled = result.compile()
    assert len(compiled) == 1
    rule = compiled[0]["and"][0]
    assert rule.field == "columnCount"
    assert rule.condition == "EXISTS"
    assert rule.negated is False


# ---------------------------------------------------------------------------
# Comparison operators (>, >=, <, <=)
# ---------------------------------------------------------------------------


def test_greater_than():
    result = parse_filter_string("columnCount > 10")
    assert result == F.custom_filter("columnCount", "GREATER_THAN", ["10"])


def test_greater_than_or_equal():
    result = parse_filter_string("columnCount >= 5")
    assert result == F.custom_filter("columnCount", "GREATER_THAN_OR_EQUAL_TO", ["5"])


def test_less_than():
    result = parse_filter_string("columnCount < 100")
    assert result == F.custom_filter("columnCount", "LESS_THAN", ["100"])


def test_less_than_or_equal():
    result = parse_filter_string("columnCount <= 50")
    assert result == F.custom_filter("columnCount", "LESS_THAN_OR_EQUAL_TO", ["50"])


def test_comparison_combined_with_and():
    result = parse_filter_string("entity_type = dataset AND columnCount > 0")
    compiled = result.compile()
    assert len(compiled) == 1
    fields = {r.field for r in compiled[0]["and"]}
    assert "_entityType" in fields
    assert "columnCount" in fields
    col_rule = next(r for r in compiled[0]["and"] if r.field == "columnCount")
    assert col_rule.condition == "GREATER_THAN"
    assert col_rule.values == ["0"]


def test_boolean_filter_combined_with_and():
    result = parse_filter_string(
        "entity_type = dataset AND deprecated = true AND hasActiveIncidents = true"
    )
    compiled = result.compile()
    assert len(compiled) == 1
    fields = {r.field for r in compiled[0]["and"]}
    assert "_entityType" in fields
    assert "deprecated" in fields
    assert "hasActiveIncidents" in fields
