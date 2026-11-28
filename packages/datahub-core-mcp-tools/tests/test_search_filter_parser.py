"""The SQL-like filter language the search tools accept.

The caller is usually a model that cannot ask a follow-up question: a parse error is
its only feedback, so errors say what to do, and shapes models actually send parse.
"""

import pytest
from datahub.sdk.search_filters import FilterDsl as F

from datahub_core_mcp_tools.graphql_helpers import (
    parenthesized_filter,
    parse_filter_input,
)
from datahub_core_mcp_tools.search_filter_parser import (
    _FIELD_MAP,
    FILTER_DOCS,
    parse_filter_string,
)

_DOMAIN = "urn:li:domain:engineering"
_DOMAIN_CHILD = "urn:li:domain:platform"
_CONTAINER = "urn:li:container:f784c48c306ba1c775ef917e2f8c1560"
_PARENT_DOC = "urn:li:document:runbooks"


def _compiled_rule(filter_str: str, field: str):
    compiled = parse_filter_string(filter_str).compile()
    assert len(compiled) == 1
    matches = [rule for rule in compiled[0]["and"] if rule.field == field]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("platform = snowflake", F.platform("snowflake")),
        ("platform IN (snowflake, bigquery)", F.platform(["snowflake", "bigquery"])),
        ("platform != snowflake", F.not_(F.platform("snowflake"))),
        (
            "entity_type = dataset AND env = PROD AND (platform = a OR platform = b)",
            F.and_(
                F.entity_type("dataset"),
                F.env("PROD"),
                F.or_(F.platform("a"), F.platform("b")),
            ),
        ),
        # AND binds tighter than OR.
        (
            "platform = a OR platform = b AND env = PROD",
            F.or_(F.platform("a"), F.and_(F.platform("b"), F.env("PROD"))),
        ),
        ("TypeNames = View", F.entity_subtype("View")),
        ("glossaryTerms = urn:li:glossaryTerm:x", F.glossary_term("urn:li:glossaryTerm:x")),
        ("tag = urn:li:tag:NOT", F.tag("urn:li:tag:NOT")),
        ('platform IN ("AND", "OR")', F.platform(["AND", "OR"])),
        ('customProperties = "key=value"', F.custom_filter("customProperties", "EQUAL", ["key=value"])),
        ("deprecated = TRUE", F.custom_filter("deprecated", "EQUAL", ["true"])),
        ("columnCount > 10", F.custom_filter("columnCount", "GREATER_THAN", ["10"])),
    ],
)  # fmt: skip
def test_parses_the_documented_syntax(text, expected):
    assert parse_filter_string(text) == expected


def test_not_env_compiles_against_both_env_fields():
    # env is stored in two fields; a negation has to exclude both.
    (clause,) = parse_filter_string("NOT env = DEV").compile()
    assert {rule.field for rule in clause["and"]} == {"origin", "env"}
    assert all(rule.negated for rule in clause["and"])
    assert (
        parse_filter_string("NOT env = DEV").compile()
        == parse_filter_string("env != DEV").compile()
    )


def _clause_rules(filter_str: str) -> list:
    return [
        sorted((rule.field, tuple(rule.values), rule.negated) for rule in clause["and"])
        for clause in parse_filter_string(filter_str).compile()
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # NOT (a OR b) = NOT a AND NOT b: one clause excluding both.
        (
            "NOT (platform = snowflake OR platform = bigquery)",
            [[("platform.keyword", ("urn:li:dataPlatform:bigquery",), True),
              ("platform.keyword", ("urn:li:dataPlatform:snowflake",), True)]],
        ),
        # NOT (a AND (b OR c)) = NOT a OR (NOT b AND NOT c).
        (
            "NOT (tag = urn:li:tag:a AND (env = PROD OR env = DEV))",
            [[("tags", ("urn:li:tag:a",), True)],
             [("env", ("DEV",), True), ("env", ("PROD",), True),
              ("origin", ("DEV",), True), ("origin", ("PROD",), True)]],
        ),
        # Double negation cancels.
        (
            "NOT NOT (platform = snowflake OR platform = bigquery)",
            [[("platform.keyword", ("urn:li:dataPlatform:snowflake",), False)],
             [("platform.keyword", ("urn:li:dataPlatform:bigquery",), False)]],
        ),
    ],
)  # fmt: skip
def test_not_negates_grouped_conditions(text, expected):
    assert _clause_rules(text) == expected


def test_is_not_null_and_is_null_check_existence():
    present = _compiled_rule("glossary_term IS NOT NULL", "glossaryTerms")
    assert (present.condition, present.negated) == ("EXISTS", False)
    absent = _compiled_rule("glossary_term IS NULL", "glossaryTerms")
    assert (absent.condition, absent.negated) == ("EXISTS", True)


def test_structured_property_urn_is_corrected_to_its_qualified_name():
    rule = _compiled_rule(
        "structuredProperties.urn:li:structuredProperty:io.acryl.retention >= 30",
        "structuredProperties.io.acryl.retention",
    )
    assert rule.condition == "GREATER_THAN_OR_EQUAL_TO"
    assert rule.values == ["30"]


def test_unquoted_multi_word_values_are_joined():
    assert parse_filter_string("subtype = Fact Sheet") == F.entity_subtype("Fact Sheet")
    assert parse_filter_string(
        "subtype IN (Fact Sheet, Terms AND Conditions)"
    ) == F.entity_subtype(["Fact Sheet", "Terms AND Conditions"])
    # Outside an IN list, a conjunction still ends the value.
    assert parse_filter_string("subtype = Runbook AND platform = notion") == F.and_(
        F.entity_subtype("Runbook"), F.platform("notion")
    )
    assert parse_filter_string('subtype IN ("Redwood, CA")') == F.entity_subtype(
        "Redwood, CA"
    )


def test_over_escaped_quotes_read_as_ordinary_quotes():
    # Sent by agents that escaped the quotes again while building a JSON string.
    assert parse_filter_string(
        r"subtype IN (\"Known Issues\", Insight, \"Vendor Q&A\")"
    ) == F.entity_subtype(["Known Issues", "Insight", "Vendor Q&A"])
    with pytest.raises(ValueError, match="Unterminated string"):
        parse_filter_string(r"subtype = \"Fact Sheet")


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("", "cannot be empty"),
        ('platform = "snowflake', "Unterminated string"),
        ("platform snowflake", "Expected ="),
        ("subtype = Fact Sheet platform = notion", "Unexpected token"),
        ("deprecated = yes", "expected 'true' or 'false'"),
        ("status IN (ALL, ONLY_SOFT_DELETED)", "exactly one value"),
        ("platform IN (snowflake,)", "Expected value"),
    ],
)
def test_malformed_filters_raise(text, error):
    with pytest.raises(ValueError, match=error):
        parse_filter_string(text)


@pytest.mark.parametrize(
    "text", ["platform IN ()", "entity_type IN ( )", "domain WITHIN ()"]
)
def test_an_empty_list_says_to_drop_the_condition(text):
    with pytest.raises(ValueError, match="drop the condition") as exc:
        parse_filter_string(text)
    field = text.split()[0]
    assert repr(field) in str(exc.value)


def test_equals_stays_exact_and_within_includes_descendants():
    assert _compiled_rule(f"domain = {_DOMAIN}", "domains").condition == "EQUAL"
    assert _compiled_rule(f"container = {_CONTAINER}", "browsePathV2").condition == (
        "CONTAIN"
    )
    assert _compiled_rule(
        f"parent_document = {_PARENT_DOC}", "parentDocument"
    ).condition == ("EQUAL")

    domains = _compiled_rule(f"domains within ({_DOMAIN}, {_DOMAIN_CHILD})", "domains")
    assert domains.condition == "DESCENDANTS_INCL"
    assert domains.values == [_DOMAIN, _DOMAIN_CHILD]
    container = _compiled_rule(f"container WITHIN {_CONTAINER}", "container")
    assert container.condition == "DESCENDANTS_INCL"
    document = _compiled_rule(f"parentDocument WITHIN {_PARENT_DOC}", "parentDocument")
    assert document.condition == "DESCENDANTS_INCL"
    negated = _compiled_rule(f"NOT domain WITHIN {_DOMAIN}", "domains")
    assert (negated.condition, negated.negated) == ("DESCENDANTS_INCL", True)


def test_within_needs_a_hierarchy_field_and_a_full_urn():
    with pytest.raises(ValueError, match="only supported for domain, container"):
        parse_filter_string("platform WITHIN snowflake")
    with pytest.raises(ValueError, match="full URN"):
        parse_filter_string("domain WITHIN engineering")
    with pytest.raises(ValueError, match="full URN"):
        parse_filter_string("parent_document WITHIN runbooks")


def test_filter_docs_cover_every_field_and_their_examples_parse():
    # FILTER_DOCS is the model's only description of what it can filter on.
    documented = FILTER_DOCS.lower()
    missing = {f for f in set(_FIELD_MAP.values()) if f.lower() not in documented}
    assert not missing
    parse_filter_string('subtype IN ("Materialized View", Table)')
    parse_filter_string(
        "entity_type = dataset AND env = PROD AND (platform = snowflake OR platform = bigquery)"
    )


def test_blank_tool_filters_mean_no_filter():
    assert parse_filter_input(None) is None
    assert parse_filter_input("  ") is None
    assert parse_filter_input(" platform = x ") == F.platform("x")
    assert parenthesized_filter("") is None
    assert parenthesized_filter("a = 1 OR b = 2") == "(a = 1 OR b = 2)"
    # Wrapped, this would parse, and ANDed clauses would then bind its last branch.
    with pytest.raises(ValueError):
        parenthesized_filter("a = 1) OR (b = 2")
