from io import StringIO
from unittest.mock import Mock

import pytest
import yaml
from pydantic import ValidationError

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import (
    RemovedStatusFilter,
    SearchFilterRule,
    generate_filter,
)
from datahub.metadata.urns import DataPlatformUrn, QueryUrn
from datahub.sdk.main_client import DataHubClient
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
                },
                {
                    "field": "platform.keyword",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
            ]
        },
        {
            "and": [
                {
                    "field": "env",
                    "condition": "EQUAL",
                    "values": ["PROD"],
                },
                {
                    "field": "platform.keyword",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
            ]
        },
    ]
    assert compile_filters(filter) == expected_filters


def test_generate_filters() -> None:
    compiled = compile_filters(
        F.and_(
            F.entity_type(QueryUrn.ENTITY_TYPE),
            F.custom_filter("origin", "EQUAL", [DataPlatformUrn("snowflake").urn()]),
        )
    )
    assert compiled == [
        {
            "and": [
                {"field": "_entityType", "condition": "EQUAL", "values": ["QUERY"]},
                {
                    "field": "origin",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
            ]
        }
    ]

    assert generate_filter(
        platform=None,
        platform_instance=None,
        env=None,
        container=None,
        status=RemovedStatusFilter.NOT_SOFT_DELETED,
        extra_filters=None,
        extra_or_filters=compiled,
    ) == [
        {
            "and": [
                {
                    "field": "removed",
                    "condition": "EQUAL",
                    "negated": True,
                    "values": ["true"],
                },
                {
                    "field": "_entityType",
                    "condition": "EQUAL",
                    "values": ["QUERY"],
                },
                {
                    "field": "origin",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
            ]
        }
    ]


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


"""
def test_get_urns(client: DataHubClient, mock_graph: Mock) -> None:
    result_urns = ["urn:li:corpuser:datahub"]
    mock_graph.get_urns_by_filter.return_value = result_urns

    urns = client.search.get_urns(
        filter=F.and_(
            F.entity_type("corpuser"),
        )
    )
    assert list(urns) == [Urn.from_string(urn) for urn in result_urns]

    assert mock_graph.get_urns_by_filter.call_count == 1
    assert mock_graph.get_urns_by_filter.call_args.kwargs == dict(
        query=None,
        entity_types=["corpuser"],
        extra_or_filters=[
            {
                "and": [
                    {
                        "field": "_entityType",
                        "condition": "EQUAL",
                        "values": ["CORP_USER"],
                    }
                ]
            },
        ],
    )
"""
