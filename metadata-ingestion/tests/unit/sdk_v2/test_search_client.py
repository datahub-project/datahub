import unittest
import unittest.mock
from io import StringIO

import pytest
import yaml
from pydantic import ValidationError

from datahub.ingestion.graph.filters import (
    RemovedStatusFilter,
    SearchFilterRule,
    generate_filter,
)
from datahub.metadata.urns import DataPlatformUrn, QueryUrn, Urn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import compile_filters, compute_entity_types
from datahub.sdk.search_filters import Filter, FilterDsl as F, load_filters
from datahub.utilities.urns.error import InvalidUrnError
from tests.test_helpers.graph_helpers import MockDataHubGraph


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


_default_status_filter = {
    "field": "removed",
    "condition": "EQUAL",
    "values": ["true"],
    "negated": True,
}


def test_compute_entity_types() -> None:
    assert compute_entity_types(
        [
            {
                "and": [
                    SearchFilterRule(
                        field="_entityType",
                        condition="EQUAL",
                        values=["DATASET"],
                    )
                ]
            },
            {
                "and": [
                    SearchFilterRule(
                        field="_entityType",
                        condition="EQUAL",
                        values=["CHART"],
                    )
                ]
            },
        ]
    ) == ["DATASET", "CHART"]


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
                _default_status_filter,
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
                _default_status_filter,
            ]
        },
    ]
    types, compiled = compile_filters(filter)
    assert types is None
    assert compiled == expected_filters


def test_compile_no_default_status() -> None:
    filter = F.and_(
        F.platform("snowflake"), F.soft_deleted(RemovedStatusFilter.ONLY_SOFT_DELETED)
    )

    _, compiled = compile_filters(filter)

    # Check that no status filter was added.
    assert compiled == [
        {
            "and": [
                {
                    "condition": "EQUAL",
                    "field": "platform.keyword",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
                {
                    "condition": "EQUAL",
                    "field": "removed",
                    "values": ["true"],
                },
            ],
        },
    ]


def test_generate_filters() -> None:
    types, compiled = compile_filters(
        F.and_(
            F.entity_type(QueryUrn.ENTITY_TYPE),
            F.custom_filter("origin", "EQUAL", [DataPlatformUrn("snowflake").urn()]),
        )
    )
    assert types == ["QUERY"]
    assert compiled == [
        {
            "and": [
                {"field": "_entityType", "condition": "EQUAL", "values": ["QUERY"]},
                {
                    "field": "origin",
                    "condition": "EQUAL",
                    "values": ["urn:li:dataPlatform:snowflake"],
                },
                _default_status_filter,
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
                # This filter appears twice - once from the compiled filters, and once
                # from the status arg to generate_filter.
                _default_status_filter,
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
                _default_status_filter,
            ]
        }
    ]


def test_get_urns() -> None:
    graph = MockDataHubGraph()

    with unittest.mock.patch.object(graph, "execute_graphql") as mock_execute_graphql:
        mock_execute_graphql.return_value = {
            "scrollAcrossEntities": {
                "nextScrollId": None,
                "searchResults": [{"entity": {"urn": "urn:li:corpuser:datahub"}}],
            }
        }

        result_urns = ["urn:li:corpuser:datahub"]
        mock_execute_graphql.return_value = {
            "scrollAcrossEntities": {
                "nextScrollId": None,
                "searchResults": [{"entity": {"urn": urn}} for urn in result_urns],
            }
        }

        client = DataHubClient(graph=graph)
        urns = client.search.get_urns(
            filter=F.and_(
                F.entity_type("corpuser"),
            )
        )
        assert list(urns) == [Urn.from_string(urn) for urn in result_urns]

        assert mock_execute_graphql.call_count == 1
        assert "scrollAcrossEntities" in mock_execute_graphql.call_args.args[0]
        mock_execute_graphql.assert_called_once_with(
            unittest.mock.ANY,
            variables={
                "types": ["CORP_USER"],
                "query": "*",
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "_entityType",
                                "condition": "EQUAL",
                                "values": ["CORP_USER"],
                            },
                            {
                                "field": "removed",
                                "condition": "EQUAL",
                                "values": ["true"],
                                "negated": True,
                            },
                        ]
                    }
                ],
                "batchSize": unittest.mock.ANY,
                "scrollId": None,
            },
        )
