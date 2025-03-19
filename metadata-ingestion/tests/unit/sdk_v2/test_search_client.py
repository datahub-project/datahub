from io import StringIO

import pytest
import yaml
from pydantic import ValidationError

from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl as F, load_filters
from datahub.utilities.urns.error import InvalidUrnError


def test_filters_simple() -> None:
    yaml_dict = {"platform": ["snowflake", "bigquery"]}
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.platform(["snowflake", "bigquery"])
    assert filter_obj.compile() == [
        {
            "and": [
                SearchFilterRule(
                    field="platform.keyword",
                    condition="EQUAL",
                    values=[
                        "urn:li:dataPlatform:snowflake",
                        "urn:li:dataPlatform:bigquery",
                    ],
                )
            ]
        }
    ]


def test_filters_and() -> None:
    yaml_dict = {
        "and": [
            {"env": ["PROD"]},
            {"platform": ["snowflake", "bigquery"]},
        ]
    }
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.and_(
        F.env("PROD"),
        F.platform(["snowflake", "bigquery"]),
    )
    platform_rule = SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=[
            "urn:li:dataPlatform:snowflake",
            "urn:li:dataPlatform:bigquery",
        ],
    )
    assert filter_obj.compile() == [
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                platform_rule,
            ]
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                platform_rule,
            ]
        },
    ]


def test_filters_complex() -> None:
    yaml_dict = yaml.safe_load(
        StringIO("""\
and:
  - env: [PROD]
  - or:
    - platform: [ snowflake, bigquery ]
    - and:
      - platform: [postgres]
      - not:
            domain: [urn:li:domain:analytics]
    - field: customProperties
      condition: EQUAL
      values: ["dbt_unique_id=source.project.name"]
""")
    )
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.and_(
        F.env("PROD"),
        F.or_(
            F.platform(["snowflake", "bigquery"]),
            F.and_(
                F.platform("postgres"),
                F.not_(F.domain("urn:li:domain:analytics")),
            ),
            F.has_custom_property("dbt_unique_id", "source.project.name"),
        ),
    )
    warehouse_rule = SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=["urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:bigquery"],
    )
    postgres_rule = SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=["urn:li:dataPlatform:postgres"],
    )
    domain_rule = SearchFilterRule(
        field="domains",
        condition="EQUAL",
        values=["urn:li:domain:analytics"],
        negated=True,
    )
    custom_property_rule = SearchFilterRule(
        field="customProperties",
        condition="EQUAL",
        values=["dbt_unique_id=source.project.name"],
    )

    # There's one OR clause in the original filter with 3 clauses,
    # and one hidden in the env filter with 2 clauses.
    # The final result should have 3 * 2 = 6 OR clauses.
    assert filter_obj.compile() == [
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                warehouse_rule,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                postgres_rule,
                domain_rule,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                custom_property_rule,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                warehouse_rule,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                postgres_rule,
                domain_rule,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                custom_property_rule,
            ],
        },
    ]


def test_invalid_filter() -> None:
    with pytest.raises(InvalidUrnError):
        F.domain("marketing")


def test_unsupported_not() -> None:
    env_filter = F.env("PROD")
    with pytest.raises(
        ValidationError,
        match="Cannot negate a filter with multiple OR clauses",
    ):
        F.not_(env_filter)


def test_compile_filters() -> None:
    filter = F.and_(F.env("PROD"), F.platform("snowflake"))
    expected_filters = [
        {
            "and": [
                {
                    "field": "origin",
                    "condition": "EQUAL",
                    "values": ["PROD"],
                    "negated": False,
                },
                {
                    "field": "platform.keyword",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                    "negated": False,
                },
            ]
        },
        {
            "and": [
                {
                    "field": "env",
                    "condition": "EQUAL",
                    "values": ["PROD"],
                    "negated": False,
                },
                {
                    "field": "platform.keyword",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                    "negated": False,
                },
            ]
        },
    ]
    assert compile_filters(filter) == expected_filters
