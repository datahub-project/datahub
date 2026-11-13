from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.resolver import HightouchResolver, match_field


def make_resolver(
    graph: Optional[MagicMock] = None, **config: Any
) -> HightouchResolver:
    return HightouchResolver(
        HightouchSourceConfig.model_validate(
            {"api_config": {"api_key": "k"}, **config}
        ),
        HightouchSourceReport(),
        graph,
    )


def make_source(**configuration: str) -> HightouchSourceConnection:
    return HightouchSourceConnection.model_validate(
        {
            "id": 101,
            "name": "Warehouse",
            "slug": "warehouse",
            "type": "snowflake",
            "workspaceId": 1,
            "configuration": configuration,
        }
    )


def make_model(**query: Dict[str, Any]) -> HightouchModel:
    return HightouchModel.model_validate(
        {
            "id": 201,
            "name": "Display Name",
            "slug": "display-name",
            "workspaceId": 1,
            "sourceId": 101,
            "queryType": "raw_sql" if "raw" in query else "table",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z",
            **query,
        }
    )


@pytest.mark.parametrize(
    "table_name, expected_name",
    [
        ("ORDERS", "analytics.public.orders"),
        ("SALES.ORDERS", "analytics.sales.orders"),
        ("OTHER_DB.SALES.ORDERS", "other_db.sales.orders"),
    ],
)
def test_table_model_urn_uses_table_name_not_display_name(table_name, expected_name):
    resolver = make_resolver()
    urn = resolver.source_table_urn(
        make_model(table={"name": table_name}),
        make_source(database="ANALYTICS", schema="PUBLIC"),
    )
    assert urn == f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{expected_name},PROD)"


def test_table_model_urn_follows_platform_detail():
    resolver = make_resolver(
        sources_to_platform_instance={
            "101": {
                "platform_instance": "prod",
                "env": "DEV",
                "database": "Main",
                "include_schema_in_urn": False,
                "convert_urns_to_lowercase": False,
            }
        }
    )
    urn = resolver.source_table_urn(
        make_model(table={"name": "PUBLIC.Orders"}), make_source(database="ANALYTICS")
    )
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.Main.Orders,DEV)"


def test_query_models_have_no_source_table():
    resolver = make_resolver()
    model = make_model(raw={"sql": "SELECT 1"})
    assert resolver.source_table_urn(model, make_source()) is None


def test_dbt_model_reads_its_qualified_table():
    resolver = make_resolver()
    model = make_model(dbt={"database": "DB", "schema": "MARTS", "name": "ORDERS"})
    model.query_type = "dbt"
    assert (
        resolver.source_table_urn(model, make_source())
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.marts.orders,PROD)"
    )


def test_platform_detail_is_resolved_on_a_copy():
    resolver = make_resolver(sources_to_platform_instance={"101": {"database": "X"}})
    details = resolver.source_platform(make_source())
    assert details.platform == "snowflake"
    assert resolver.config.sources_to_platform_instance["101"].platform is None


def test_unknown_platform_type_is_used_as_is_and_reported_once():
    resolver = make_resolver()
    destination = HightouchDestination.model_validate(
        {
            "id": 301,
            "name": "Custom",
            "slug": "custom",
            "type": "Some_Tool",
            "workspaceId": 1,
        }
    )
    resolver.destination_urn("Object", destination)
    urn = resolver.destination_urn("Object", destination)
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:some_tool,Object,PROD)"
    assert len(resolver.report.infos) == 1


def test_parsed_sql_upstreams_follow_platform_detail():
    resolver = make_resolver(
        sources_to_platform_instance={"101": {"platform_instance": "prod"}}
    )
    model = make_model(
        raw={"sql": "SELECT o.id, c.name FROM orders o JOIN crm.customers c ON 1 = 1"}
    )
    parsed = resolver.parse_model_sql(
        model, make_source(database="ANALYTICS", schema="PUBLIC")
    )
    assert parsed is not None
    assert sorted(parsed.upstream_urns) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.crm.customers,PROD)",
        # Unqualified tables take the connection's database and schema.
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.public.orders,PROD)",
    ]
    assert [f.fieldPath for f in parsed.output_fields] == ["id", "name"]
    assert parsed.column_upstreams["name"][0].table.endswith(
        "prod.analytics.crm.customers,PROD)"
    )


def test_parsed_sql_without_schema_in_urn_keeps_database_and_table():
    resolver = make_resolver(
        sources_to_platform_instance={
            "101": {"database": "ANALYTICS", "include_schema_in_urn": False}
        }
    )
    model = make_model(
        raw={"sql": "SELECT * FROM orders JOIN other_db.sales.customers USING (id)"}
    )
    parsed = resolver.parse_model_sql(model, make_source())
    assert parsed is not None
    assert sorted(parsed.upstream_urns) == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,other_db.customers,PROD)",
    ]


def test_unparseable_sql_is_reported_and_parsed_once():
    resolver = make_resolver()
    model = make_model(raw={"sql": "SELEC FROM WHERE"})
    source = make_source()
    assert resolver.parse_model_sql(model, source) is None
    assert resolver.parse_model_sql(model, source) is None
    assert resolver.report.sql_parsing_failures == 1


def test_sql_parsing_can_be_disabled():
    resolver = make_resolver(parse_model_sql=False)
    model = make_model(raw={"sql": "SELECT * FROM orders"})
    assert resolver.parse_model_sql(model, make_source()) is None


def test_datahub_lookup_failures_degrade_and_are_cached():
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = ConnectionError("down")
    graph.exists.side_effect = ConnectionError("down")
    resolver = make_resolver(graph)
    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"

    assert resolver.schema_fields(urn) is None
    assert resolver.schema_fields(urn) is None
    assert resolver.exists(urn) is False
    assert graph.get_schema_metadata.call_count == 1
    assert resolver.report.datahub_lookup_failures == {
        "dataset schema": 1,
        "dataset existence": 1,
    }


@pytest.mark.parametrize(
    "name, candidates, expected",
    [
        ("user_email", ["USER_EMAIL", "ID"], "USER_EMAIL"),
        ("first_name", ["first_name", "firstname"], "first_name"),
        # Only a unique loose match is trusted.
        ("first-name", ["FIRST_NAME", "ID"], "FIRST_NAME"),
        ("first_name", ["firstname", "first-name"], None),
        ("Name", ["NAME", "name"], None),
        ("missing", ["ID"], None),
    ],
)
def test_match_field_prefers_exact_and_refuses_ambiguity(name, candidates, expected):
    assert match_field(name, candidates) == expected
