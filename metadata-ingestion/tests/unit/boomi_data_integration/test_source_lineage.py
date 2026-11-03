from typing import Dict, List, Optional
from unittest.mock import MagicMock

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
from datahub.ingestion.source.boomi_data_integration.source_lineage import (
    JobLineage,
    SourceLineageResolver,
)
from datahub.ingestion.source.boomi_data_integration.sql_columns import (
    output_columns,
)
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

SF_TARGET: Dict[str, object] = {
    "name": "snowflake",
    "connection_id": "conn-sf",
    "database_name": "DW",
    "schema_name": "RAW",
}
PG_SOURCE = {"conn-pg": {"platform": "postgres", "database": "appdb"}}
BQ_TARGET = {"conn-bq": {"platform": "bigquery", "database": "my-project"}}


def _resolver(
    graph: Optional[MagicMock] = None, **overrides: object
) -> SourceLineageResolver:
    config_dict: Dict[str, object] = {"account_id": "acct", "api_token": "t"}
    config_dict.update(overrides)
    config = BoomiDataIntegrationSourceConfig.model_validate(config_dict)
    report = BoomiDataIntegrationSourceReport()
    return SourceLineageResolver(
        config, report, TargetLineageResolver(config, report), graph
    )


def _table(
    name: str,
    target_table: Optional[str] = None,
    columns: Optional[List[Optional[Dict[str, object]]]] = None,
    selected: bool = True,
) -> Dict[str, object]:
    return {
        "details": {
            "name": name,
            "is_selected": selected,
            "target_table": target_table,
            "modified_columns": columns or [],
        }
    }


def _multi(
    source_name: str,
    schema: str,
    tables: List[Dict[str, object]],
    target: Optional[Dict[str, object]] = None,
) -> SourceToTargetProperties:
    return SourceToTargetProperties.model_validate(
        {
            "source": {
                "name": source_name,
                "connection_id": "conn-pg",
                "run_type": "multi_tables",
            },
            "target": target or SF_TARGET,
            "schemas": [{"name": schema, "tables": tables}],
        }
    )


def _custom_query(
    source_name: str,
    query: Optional[str],
    target: Dict[str, object],
    fields: Optional[List[Dict[str, object]]] = None,
) -> SourceToTargetProperties:
    return SourceToTargetProperties.model_validate(
        {
            "source": {
                "name": source_name,
                "connection_id": "conn-pg",
                "run_type": "custom_query",
                "custom_query_source_settings": {"query": query, "fields": fields},
            },
            "target": target,
            "schemas": [],
        }
    )


def _step(
    step_type: str,
    sql: str,
    target: Optional[Dict[str, object]] = None,
    connection_id: str = "conn-sf",
) -> LogicNode:
    return LogicNode.model_validate(
        {
            "type": step_type,
            "step_id": "s",
            "connection_id": connection_id,
            "sql_query": sql,
            "target_settings": target,
        }
    )


def _edges(lineage: JobLineage) -> Dict[str, List[str]]:
    return {
        downstream.split(",")[-1].rstrip(")"): sorted(
            upstream.split(",")[-1].rstrip(")") for upstream in edge.upstreams
        )
        for downstream, edge in lineage.columns.items()
    }


def _sf(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{name},PROD)"


def test_multi_table_sources_use_the_mapped_database() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = _multi(
        "postgresql",
        "public",
        [_table("Orders"), _table("items"), _table("audit", selected=False)],
    )

    lineage = resolver.river_lineage(props, "r")

    assert sorted(lineage.inputs) == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.items,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)",
    ]
    assert sorted(lineage.outputs) == [_sf("dw.raw.items"), _sf("dw.raw.orders")]
    assert resolver.report.source_tables_emitted == 2


def test_multi_table_without_source_database_is_skipped_with_one_warning() -> None:
    resolver = _resolver()
    props = _multi("postgresql", "public", [_table("orders"), _table("items")])

    first = resolver.river_lineage(props, "r1")
    resolver.river_lineage(props, "r2")

    assert first.inputs == set()
    assert first.outputs == {_sf("dw.raw.items"), _sf("dw.raw.orders")}
    assert resolver.report.source_lineage_skipped["missing_source_database"] == 4
    assert list(resolver.report.sources_missing_database) == ["r1", "r2"]
    assert [w.title for w in resolver.report.warnings] == ["Source database unknown"]


def test_mysql_sources_are_two_tier_without_a_database() -> None:
    resolver = _resolver()
    lineage = resolver.river_lineage(_multi("mysql", "shop", [_table("orders")]), "r")

    assert lineage.inputs == {
        "urn:li:dataset:(urn:li:dataPlatform:mysql,shop.orders,PROD)"
    }


@pytest.mark.parametrize(
    "mapping, expected",
    [
        ({}, "ops.tickets"),
        ({"conn-pg": {"platform": "oracle", "database": "ORCL"}}, "orcl.ops.tickets"),
    ],
)
def test_oracle_source_prefixes_the_database_only_when_mapped(
    mapping: Dict[str, object], expected: str
) -> None:
    resolver = _resolver(connection_to_platform=mapping)
    lineage = resolver.river_lineage(_multi("oracle", "OPS", [_table("TICKETS")]), "r")

    assert lineage.inputs == {
        f"urn:li:dataset:(urn:li:dataPlatform:oracle,{expected},PROD)"
    }


def test_prefixed_targets_still_get_source_lineage() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    target = {**SF_TARGET, "target_prefix": "app_"}
    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders")], target), "r"
    )

    assert lineage.outputs == set()
    assert len(lineage.inputs) == 1


def test_unsupported_source_and_run_type_are_counted() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    hubspot = SourceToTargetProperties.model_validate(
        {
            "source": {"name": "hubspot", "run_type": "predefined_report"},
            "target": {**SF_TARGET, "table_name": "leads"},
        }
    )
    legacy = SourceToTargetProperties.model_validate(
        {
            "source": {
                "name": "postgresql",
                "connection_id": "conn-pg",
                "run_type": "legacy",
            },
            "target": {**SF_TARGET, "table_name": "leads"},
        }
    )

    assert resolver.river_lineage(hubspot, "r1").outputs == {_sf("dw.raw.leads")}
    assert resolver.river_lineage(legacy, "r2").inputs == set()
    assert dict(resolver.report.sources_unsupported) == {"hubspot": 1}
    assert dict(resolver.report.source_lineage_skipped) == {
        "unsupported_source": 1,
        "unsupported_run_type": 1,
    }


def test_modified_columns_become_column_lineage() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    columns: List[Optional[Dict[str, object]]] = [
        {"name": "ID", "alias": "ORDER_ID", "is_selected": True},
        {"name": "status", "is_selected": True},
        {"name": "secret", "is_selected": False},
        {"name": "loaded_at", "is_selected": True, "expression": "now()"},
        {"name": "__rivery_run", "is_selected": True, "is_rivery_metadata": True},
        {"alias": "nameless", "is_selected": True},
        {"name": "broken", "is_selected": None},
        None,
    ]
    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders", columns=columns)]), "r"
    )

    assert _edges(lineage) == {"order_id": ["id"], "status": ["status"]}
    assert dict(resolver.report.column_lineage_skipped) == {
        "expression": 1,
        "metadata_column": 1,
        "missing_name": 1,
        "invalid_column": 1,
    }


def test_modified_columns_skip_when_target_is_unresolved() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    target = {**SF_TARGET, "target_prefix": "app_"}
    columns: List[Optional[Dict[str, object]]] = [{"name": "id", "is_selected": True}]
    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders", columns=columns)], target),
        "r",
    )

    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["missing_target"] == 1


def test_column_lineage_can_be_turned_off() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE, include_column_lineage=False)
    columns: List[Optional[Dict[str, object]]] = [{"name": "id", "is_selected": True}]
    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders", columns=columns)]), "r"
    )

    assert lineage.inputs
    assert lineage.columns == {}


def test_custom_query_inputs_and_select_column_lineage() -> None:
    resolver = _resolver(connection_to_platform=BQ_TARGET)
    props = _custom_query(
        "mysql",
        "SELECT g.stable_id, UPPER(g.biotype) AS kind, COUNT(*) FROM core.gene g",
        {
            "name": "bigquery",
            "connection_id": "conn-bq",
            "dataset_id": "raw",
            "table_name": "genes",
        },
    )

    lineage = resolver.river_lineage(props, "r")

    assert lineage.inputs == {
        "urn:li:dataset:(urn:li:dataPlatform:mysql,core.gene,PROD)"
    }
    assert lineage.outputs == {
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.raw.genes,PROD)"
    }
    assert _edges(lineage) == {"kind": ["biotype"], "stable_id": ["stable_id"]}
    transforms = {
        downstream.split(",")[-1].rstrip(")"): edge.transform
        for downstream, edge in lineage.columns.items()
    }
    assert transforms == {"kind": "SQL", "stable_id": "IDENTITY"}


def test_custom_query_uses_default_database_and_schema() -> None:
    resolver = _resolver(
        connection_to_platform={
            "conn-pg": {
                "platform": "postgres",
                "database": "appdb",
                "default_schema": "sales",
            }
        }
    )
    props = _custom_query(
        "postgresql", "SELECT id FROM orders", {**SF_TARGET, "table_name": "orders"}
    )

    assert resolver.river_lineage(props, "r").inputs == {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.sales.orders,PROD)"
    }


def test_custom_query_unqualified_tables_are_skipped_with_one_warning() -> None:
    resolver = _resolver()
    props = _custom_query(
        "postgresql",
        "SELECT o.id FROM sales.orders o JOIN sales.items i ON o.id = i.order_id",
        {**SF_TARGET, "table_name": "orders"},
    )

    lineage = resolver.river_lineage(props, "r")

    assert lineage.inputs == set()
    assert lineage.columns == {}
    assert resolver.report.source_lineage_skipped["unqualified_sql_table"] == 2
    assert [w.title for w in resolver.report.warnings] == [
        "SQL table not fully qualified"
    ]


def test_custom_query_with_field_mapping_has_no_column_lineage() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = _custom_query(
        "postgresql",
        "SELECT id FROM public.orders",
        {**SF_TARGET, "table_name": "orders"},
        fields=[{"name": "id", "target_column": "order_id"}],
    )

    lineage = resolver.river_lineage(props, "r")

    assert len(lineage.inputs) == 1
    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["custom_field_mapping"] == 1


def test_custom_query_missing_and_unparsable() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    target = {**SF_TARGET, "table_name": "orders"}

    missing = resolver.river_lineage(_custom_query("postgresql", None, target), "r1")
    lineage = resolver.river_lineage(
        _custom_query("postgresql", "SELEC nothing useful", target), "r2"
    )
    resolver.river_lineage(_custom_query("postgresql", "SELECT 1", target), "r3")

    assert (missing.inputs, missing.outputs) == (set(), {_sf("dw.raw.orders")})
    assert lineage.inputs == set()
    assert lineage.outputs == {_sf("dw.raw.orders")}
    assert resolver.report.source_lineage_skipped["missing_query"] == 1
    assert resolver.report.source_lineage_skipped["sql_parse_failed"] == 2
    assert list(resolver.report.sql_parse_failures) == ["r2", "r3"]
    assert [w.title for w in resolver.report.warnings] == ["Failed to parse SQL"]


def test_malformed_custom_query_settings_keep_target_lineage() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = SourceToTargetProperties.model_validate(
        {
            "source": {
                "name": "postgresql",
                "connection_id": "conn-pg",
                "run_type": "custom_query",
                "custom_query_source_settings": {"query": ["not", "a", "string"]},
            },
            "target": {**SF_TARGET, "table_name": "orders"},
        }
    )

    lineage = resolver.river_lineage(props, "r")

    assert (lineage.inputs, lineage.outputs) == (set(), {_sf("dw.raw.orders")})
    assert resolver.report.source_lineage_skipped["invalid_query_settings"] == 1


def test_custom_query_outputs_are_not_counted() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = _custom_query(
        "postgresql",
        "INSERT INTO appdb.public.audit SELECT id FROM appdb.public.orders",
        {**SF_TARGET, "table_name": "orders"},
    )

    lineage = resolver.river_lineage(props, "r")

    assert lineage.outputs == {_sf("dw.raw.orders")}
    assert resolver.report.sql_tables_emitted == 1


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO appdb.public.audit (order_id) SELECT id FROM appdb.public.orders",
        "CREATE TABLE appdb.public.audit AS SELECT id FROM appdb.public.orders",
    ],
)
def test_custom_query_writing_tables_has_no_column_lineage(query: str) -> None:
    # The river loads the query's result into its own target, so columns of a
    # table the SQL writes must not become column edges.
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = _custom_query("postgresql", query, {**SF_TARGET, "table_name": "orders"})

    lineage = resolver.river_lineage(props, "r")

    assert lineage.inputs == {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)"
    }
    assert lineage.outputs == {_sf("dw.raw.orders")}
    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["custom_query_not_select"] == 1


def test_custom_query_columns_follow_target_aliases() -> None:
    resolver = _resolver(connection_to_platform=PG_SOURCE)
    props = SourceToTargetProperties.model_validate(
        {
            "source": {
                "name": "postgresql",
                "connection_id": "conn-pg",
                "run_type": "custom_query",
                "custom_query_source_settings": {
                    "query": "SELECT id, status, note FROM appdb.public.orders"
                },
            },
            "target": SF_TARGET,
            "schemas": [
                {
                    "name": "public",
                    "tables": [
                        _table(
                            "orders",
                            target_table="orders",
                            columns=[
                                {"name": "id", "alias": "order_id"},
                                {"name": "note", "is_selected": False},
                            ],
                        )
                    ],
                }
            ],
        }
    )

    lineage = resolver.river_lineage(props, "r")

    assert lineage.outputs == {_sf("dw.raw.orders")}
    assert _edges(lineage) == {"order_id": ["id"], "status": ["status"]}
    assert all(
        d.startswith("urn:li:schemaField:(" + _sf("dw.raw.orders"))
        for d in lineage.columns
    )
    assert resolver.report.column_lineage_skipped["unmapped_sql_column"] == 1


def test_logic_select_step_reads_tables_and_maps_columns_to_target() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "SELECT DBID, COUNT(ID) AS N FROM DW.RAW.RNA GROUP BY DBID",
        {
            "target_type": "table",
            "database_name": "DW",
            "schema_name": "MART",
            "table_name": "ROLLUP",
        },
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.rna")}
    assert lineage.outputs == {_sf("dw.mart.rollup")}
    assert _edges(lineage) == {"dbid": ["dbid"], "n": ["id"]}


def test_logic_select_step_into_a_variable_has_inputs_only() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "SELECT MAX(ID) FROM DW.RAW.RNA",
        {"target_type": "variable"},
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.rna")}
    assert lineage.outputs == set()
    assert lineage.columns == {}


def test_logic_select_step_with_field_mapping_has_no_column_lineage() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "SELECT DBID FROM DW.RAW.RNA",
        {
            "target_type": "table",
            "database_name": "DW",
            "schema_name": "MART",
            "table_name": "ROLLUP",
            "fields": [{"name": "DBID", "target": "SOURCE_ID"}],
        },
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["custom_field_mapping"] == 1


def test_insert_select_maps_columns_by_position() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "INSERT INTO DW.MART.SUMMARY (LABEL, TOTAL) "
        "SELECT DESCR, AMOUNT FROM DW.RAW.ORDERS",
        {
            "target_type": "table",
            "database_name": "DW",
            "schema_name": "MART",
            "table_name": "SUMMARY",
        },
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.orders")}
    assert lineage.outputs == {_sf("dw.mart.summary")}
    assert _edges(lineage) == {"label": ["descr"], "total": ["amount"]}


def test_insert_without_column_list_has_no_column_lineage() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "INSERT INTO DW.MART.SUMMARY SELECT DESCR FROM DW.RAW.ORDERS",
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.orders")}
    assert lineage.outputs == {_sf("dw.mart.summary")}
    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["insert_without_column_list"] == 1


MERGE_SQL = (
    "MERGE INTO DW.MART.SUMMARY t USING DW.RAW.ORDERS o ON t.ID = o.ID "
    "WHEN MATCHED THEN UPDATE SET TOTAL = o.AMOUNT"
)
INSERT_SQL = (
    "INSERT INTO DW.MART.SUMMARY (LABEL, TOTAL) SELECT DESCR, AMOUNT FROM DW.RAW.ORDERS"
)


@pytest.mark.parametrize("include_column_lineage", [True, False])
@pytest.mark.parametrize("sql", [MERGE_SQL, INSERT_SQL])
def test_write_statements_read_their_source_tables(
    sql: str, include_column_lineage: bool
) -> None:
    resolver = _resolver(include_column_lineage=include_column_lineage)

    lineage = resolver.step_lineage(_step("snowflake_sql_query", sql), "s")

    assert lineage.inputs == {_sf("dw.raw.orders")}
    assert lineage.outputs == {_sf("dw.mart.summary")}
    if not include_column_lineage:
        assert lineage.columns == {}


def test_update_target_is_not_an_input() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_query",
        "UPDATE DW.MART.SUMMARY s SET TOTAL = o.AMOUNT FROM DW.RAW.ORDERS o "
        "WHERE s.ID = o.ID",
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.orders")}
    assert lineage.outputs == {_sf("dw.mart.summary")}


def test_script_keeps_tables_written_then_read_as_inputs() -> None:
    resolver = _resolver()
    node = _step(
        "snowflake_sql_script",
        "INSERT INTO DW.MART.A SELECT ID FROM DW.RAW.ORDERS; "
        "INSERT INTO DW.MART.B SELECT ID FROM DW.MART.A; "
        "UPDATE DW.MART.C SET X = 1",
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.orders"), _sf("dw.mart.a")}
    assert lineage.outputs == {_sf("dw.mart.a"), _sf("dw.mart.b"), _sf("dw.mart.c")}


def test_script_sql_is_read_from_sql_script_too() -> None:
    resolver = _resolver()
    node = LogicNode.model_validate(
        {
            "type": "snowflake_sql_script",
            "step_id": "s",
            "connection_id": "conn-sf",
            "sql_script": "INSERT INTO DW.MART.A SELECT ID FROM DW.RAW.ORDERS",
        }
    )

    lineage = resolver.step_lineage(node, "s")

    assert lineage.inputs == {_sf("dw.raw.orders")}
    assert lineage.outputs == {_sf("dw.mart.a")}


def test_script_steps_take_outputs_from_the_sql() -> None:
    resolver = _resolver()
    script = _step(
        "redshift_sql_script",
        "CREATE TEMP TABLE daily AS SELECT id, amount FROM raw.payments; "
        "INSERT INTO reporting.daily_totals SELECT id, SUM(amount) FROM daily GROUP BY id",
        connection_id="conn-rs",
    )
    single = _step(
        "redshift_sql_script",
        "INSERT INTO reporting.totals (payment_id) SELECT id FROM raw.payments",
        connection_id="conn-rs",
    )

    lineage = resolver.step_lineage(script, "s1")
    single_lineage = resolver.step_lineage(single, "s2")

    redshift = "urn:li:dataset:(urn:li:dataPlatform:redshift,{},PROD)"
    assert lineage.inputs == {redshift.format("raw.payments")}
    assert lineage.outputs == {redshift.format("reporting.daily_totals")}
    assert lineage.columns == {}
    assert resolver.report.column_lineage_skipped["multi_statement_script"] == 1
    assert single_lineage.outputs == {redshift.format("reporting.totals")}
    assert _edges(single_lineage) == {"payment_id": ["id"]}


def test_fully_qualified_sql_resolves_without_a_mapped_database() -> None:
    # Redshift targets drop the database when connection_to_platform has none,
    # but SQL that names it is fully qualified, as the redshift source names it.
    resolver = _resolver()
    node = _step(
        "redshift_sql_query",
        "INSERT INTO dev.reporting.totals (payment_id) SELECT id FROM dev.raw.payments",
        connection_id="conn-rs",
    )

    lineage = resolver.step_lineage(node, "s")

    redshift = "urn:li:dataset:(urn:li:dataPlatform:redshift,{},PROD)"
    assert lineage.inputs == {redshift.format("dev.raw.payments")}
    assert lineage.outputs == {redshift.format("dev.reporting.totals")}


def test_non_sql_and_unknown_platform_steps() -> None:
    resolver = _resolver()
    river_step = LogicNode.model_validate(
        {"type": "river", "step_id": "s", "river_id": "r"}
    )
    unknown = _step("oracle_sql_query", "SELECT 1 FROM a.b")

    assert resolver.step_lineage(river_step, "s1") == JobLineage()
    assert resolver.step_lineage(unknown, "s2").inputs == set()
    assert resolver.report.source_lineage_skipped["unsupported_step_platform"] == 1


def _schema(*paths: str) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="t",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=path,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="varchar",
            )
            for path in paths
        ],
    )


def test_schema_lookup_keeps_stored_field_paths_and_drops_unknown_columns() -> None:
    graph = MagicMock()
    graph.get_aspect.side_effect = lambda urn, _: (
        _schema("[version=2.0].[type=string].Status")
        if "postgres" in urn
        else _schema("status")
    )
    resolver = _resolver(graph=graph, connection_to_platform=PG_SOURCE)
    columns: List[Optional[Dict[str, object]]] = [
        {"name": "status", "is_selected": True},
        {"name": "missing", "is_selected": True},
    ]

    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders", columns=columns)]), "r"
    )

    assert list(lineage.columns.values())[0].upstreams == {
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,"
        "appdb.public.orders,PROD),[version=2.0].[type=string].Status)"
    }
    assert len(lineage.columns) == 1
    assert resolver.report.column_lineage_skipped["not_in_schema"] == 1
    assert graph.get_aspect.call_count == 2


def test_schema_lookup_failure_warns_once_and_uses_column_names() -> None:
    graph = MagicMock()
    graph.get_aspect.side_effect = RuntimeError("boom")
    resolver = _resolver(graph=graph, connection_to_platform=PG_SOURCE)
    columns: List[Optional[Dict[str, object]]] = [{"name": "id", "is_selected": True}]

    lineage = resolver.river_lineage(
        _multi("postgresql", "public", [_table("orders", columns=columns)]), "r"
    )

    assert _edges(lineage) == {"id": ["id"]}
    assert [w.title for w in resolver.report.warnings] == [
        "Failed to read dataset schemas from DataHub"
    ]


@pytest.mark.parametrize(
    "sql, is_insert, by_output",
    [
        ("SELECT a, b AS c FROM t", False, {"a": "a", "c": "c"}),
        ("SELECT * FROM t", False, {}),
        ("SELECT a, a FROM t", False, {}),
        ("INSERT INTO x (p, q) SELECT a, b FROM t", True, {"a": "p", "b": "q"}),
        ("INSERT INTO x (p) SELECT a, b FROM t", True, {}),
        ("INSERT INTO x SELECT a FROM t", True, None),
    ],
)
def test_output_columns(
    sql: str, is_insert: bool, by_output: Optional[Dict[str, str]]
) -> None:
    result = output_columns(sql, "postgres")

    assert result.parsed
    assert result.is_insert == is_insert
    assert result.by_output == by_output


def test_output_columns_of_other_statements() -> None:
    assert not output_columns("SELEC broken FROM", "postgres").parsed
    update = output_columns("UPDATE t SET a = 1", "postgres")
    assert update.parsed and not update.is_insert and not update.is_select
