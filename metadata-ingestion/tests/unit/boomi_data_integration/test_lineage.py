from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.lineage import (
    TargetLineageResolver,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    LogicNode,
    SourceToTargetProperties,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)


def _resolver(**overrides: object) -> TargetLineageResolver:
    config: Dict[str, object] = {"account_id": "acct", "api_token": "t"}
    config.update(overrides)
    return TargetLineageResolver(
        BoomiDataIntegrationSourceConfig.model_validate(config),
        BoomiDataIntegrationSourceReport(),
    )


def _props(
    target: Dict[str, object], tables: Optional[List[Dict[str, object]]] = None
) -> SourceToTargetProperties:
    return SourceToTargetProperties.model_validate(
        {
            "source": {"name": "mysql", "connection_id": "conn-src"},
            "target": {"connection_id": "conn-tgt", **target},
            "schemas": [{"name": "app", "tables": tables or []}],
        }
    )


def _table(
    name: str, selected: bool = True, target_table: Optional[str] = None
) -> Dict[str, object]:
    return {
        "details": {"name": name, "is_selected": selected, "target_table": target_table}
    }


BQ_PROJECT = {"conn-tgt": {"platform": "bigquery", "database": "my-project"}}


@pytest.mark.parametrize(
    "target, mapping, expected",
    [
        (
            {"name": "snowflake", "database_name": "DB", "schema_name": "RAW"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.raw.orders,PROD)",
        ),
        (
            {"name": "bigquery", "dataset_id": "raw"},
            BQ_PROJECT,
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.raw.orders,PROD)",
        ),
        (
            {"name": "redshift", "schema_name": "staging"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:redshift,staging.orders,PROD)",
        ),
        (
            {"name": "redshift", "schema_name": "staging"},
            {"conn-tgt": {"platform": "redshift", "database": "dev"}},
            "urn:li:dataset:(urn:li:dataPlatform:redshift,dev.staging.orders,PROD)",
        ),
        (
            {"name": "postgres_rds", "schema_name": "public"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)",
        ),
        (
            {"name": "azure_synapse_analytics", "schema_name": "dbo"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:mssql,dbo.orders,PROD)",
        ),
        (
            {"name": "athena", "schema_name": "lake", "bucket_name": "b"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:athena,lake.orders,PROD)",
        ),
        (
            {"name": "databricks", "catalog_name": "main", "schema_name": "ops"},
            {},
            "urn:li:dataset:(urn:li:dataPlatform:databricks,main.ops.orders,PROD)",
        ),
        (
            {"name": "databricks", "schema_name": "ops"},
            {"conn-tgt": {"platform": "databricks", "database": "hive"}},
            "urn:li:dataset:(urn:li:dataPlatform:databricks,hive.ops.orders,PROD)",
        ),
    ],
)
def test_single_table_target_naming(
    target: Dict[str, object], mapping: Dict[str, object], expected: str
) -> None:
    resolver = _resolver(connection_to_platform=mapping)
    props = _props({**target, "table_name": "Orders"})
    assert resolver.river_targets(props, "r") == [expected]


def test_bigquery_without_project_is_skipped_and_reported() -> None:
    resolver = _resolver()
    props = _props({"name": "bigquery", "dataset_id": "raw", "table_name": "t"})

    assert resolver.river_targets(props, "r") == []
    assert len(resolver.report.targets_missing_bigquery_project) == 1
    assert [w.title for w in resolver.report.warnings] == [
        "BigQuery target without a project"
    ]


def test_missing_schema_is_reported() -> None:
    resolver = _resolver()
    props = _props({"name": "redshift", "table_name": "t"})

    assert resolver.river_targets(props, "r") == []
    assert list(resolver.report.targets_missing_location) == ["r"]


def test_multi_table_uses_selected_tables_and_target_override() -> None:
    resolver = _resolver()
    props = _props(
        {"name": "snowflake", "database_name": "db", "schema_name": "raw"},
        tables=[
            _table("orders"),
            _table("items", target_table="ORDER_ITEMS"),
            _table("audit", selected=False),
        ],
    )
    assert resolver.river_targets(props, "r") == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.raw.order_items,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.raw.orders,PROD)",
    ]


def test_multi_table_with_prefix_is_skipped() -> None:
    resolver = _resolver()
    props = _props(
        {
            "name": "snowflake",
            "database_name": "db",
            "schema_name": "raw",
            "target_prefix": "app_",
        },
        tables=[_table("orders")],
    )
    assert resolver.river_targets(props, "r") == []
    assert list(resolver.report.targets_prefix_skipped) == ["r"]


def test_multi_table_with_nothing_selected_ignores_table_name() -> None:
    resolver = _resolver()
    props = _props(
        {
            "name": "snowflake",
            "database_name": "db",
            "schema_name": "raw",
            "table_name": "leftover",
        },
        tables=[_table("orders", selected=False)],
    )
    assert resolver.river_targets(props, "r") == []


def test_object_storage_and_unknown_targets_are_skipped() -> None:
    resolver = _resolver()
    s3 = _props({"name": "s3", "bucket_name": "b", "path": "p"})
    email = _props({"name": "target_email", "table_name": "t"})

    assert resolver.river_targets(s3, "r1") == []
    assert resolver.river_targets(email, "r2") == []
    assert resolver.report.targets_object_storage_skipped == 1
    assert list(resolver.report.targets_unsupported) == ["r2: target_email"]


def test_platform_map_override_and_connection_details() -> None:
    resolver = _resolver(
        connection_type_platform_map={"azure_sql": "azure-sql"},
        convert_lineage_urns_to_lowercase=False,
    )
    props = _props({"name": "azure_sql", "schema_name": "dbo", "table_name": "Orders"})
    assert resolver.river_targets(props, "r") == [
        "urn:li:dataset:(urn:li:dataPlatform:azure-sql,dbo.Orders,PROD)"
    ]

    mapped = _resolver(
        connection_type_platform_map={"azure_sql": "azure-sql"},
        connection_to_platform={
            "conn-tgt": {
                "platform": "mssql",
                "platform_instance": "erp",
                "env": "DEV",
                "database": "Sales",
                "convert_urns_to_lowercase": False,
            }
        },
    )
    assert mapped.river_targets(props, "r") == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,erp.Sales.dbo.Orders,DEV)"
    ]


def _node(
    step_type: str, target: Dict[str, object], connection_id: str = "conn-wh"
) -> LogicNode:
    return LogicNode.model_validate(
        {
            "type": step_type,
            "step_id": "s",
            "connection_id": connection_id,
            "target_settings": target,
        }
    )


def test_logic_step_targets() -> None:
    resolver = _resolver(connection_to_platform=BQ_PROJECT)
    table: Dict[str, object] = {"target_type": "table", "table_name": "Daily"}

    assert resolver.step_target(
        _node("databricks_sql_query", {**table, "catalog": "main", "schema_name": "g"}),
        "s",
    ) == ("urn:li:dataset:(urn:li:dataPlatform:databricks,main.g.daily,PROD)")
    assert resolver.step_target(
        _node("bigquery_sql_query", {**table, "dataset_id": "rep"}, "conn-tgt"), "s"
    ) == ("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.rep.daily,PROD)")
    assert (
        resolver.step_target(
            _node("azure_synapse_sql_query", {**table, "schema_name": "dbo"}), "s"
        )
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,dbo.daily,PROD)"
    )
    assert (
        resolver.step_target(
            _node("snowflake_sql_query", {"target_type": "variable"}), "s"
        )
        is None
    )
    assert resolver.step_target(_node("snowflake_sql_script", table), "s1") is None
    assert (
        resolver.step_target(
            _node("snowflake_sql_query", {"target_type": "table"}), "s2"
        )
        is None
    )
    assert list(resolver.report.targets_unsupported) == ["s1: snowflake_sql_script"]
    assert list(resolver.report.targets_missing_location) == ["s2"]
