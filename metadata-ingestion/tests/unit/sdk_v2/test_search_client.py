from io import StringIO

import yaml

from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.sdk.search_filters import Filter, FilterDsl as F, load_filters


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
    platform_check = SearchFilterRule(
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
                platform_check,
            ]
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                platform_check,
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
      - domain: [urn:li:domain:analytics]
""")
    )
    filter_obj: Filter = load_filters(yaml_dict)
    assert filter_obj == F.and_(
        F.env("PROD"),
        F.or_(
            F.platform(["snowflake", "bigquery"]),
            F.and_(
                F.platform("postgres"),
                F.domain("urn:li:domain:analytics"),
            ),
        ),
    )
    warehouse_check = SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=["urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:bigquery"],
    )
    postgres_check = SearchFilterRule(
        field="platform.keyword",
        condition="EQUAL",
        values=["urn:li:dataPlatform:postgres"],
    )
    domain_check = SearchFilterRule(
        field="domains",
        condition="EQUAL",
        values=["urn:li:domain:analytics"],
    )

    # There's one OR clause in the original filter, and one hidden in the env filter.
    # The final result should have 2 * 2 = 4 OR clauses.
    assert filter_obj.compile() == [
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                warehouse_check,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="origin", condition="EQUAL", values=["PROD"]),
                postgres_check,
                domain_check,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                warehouse_check,
            ],
        },
        {
            "and": [
                SearchFilterRule(field="env", condition="EQUAL", values=["PROD"]),
                postgres_check,
                domain_check,
            ],
        },
    ]
