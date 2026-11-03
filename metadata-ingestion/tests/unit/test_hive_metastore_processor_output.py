import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import patch

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hive.hive_metastore_source import (
    HiveMetastore,
    HiveMetastoreSource,
)
from datahub.metadata.schema_classes import MetadataChangeProposalClass

TABLE_URN = make_dataset_urn("hive", "db1.orders")
VIEW_URN = make_dataset_urn("hive", "db1.orders_view")
TABLE_LOCATION = "s3://bucket/warehouse/orders"


def _table_rows(duplicate_column: bool) -> List[Dict[str, Any]]:
    def col(name: str, order: int, col_type: str, partition: int) -> Dict[str, Any]:
        return {
            "schema_name": "db1",
            "table_name": "orders",
            "table_type": "EXTERNAL_TABLE",
            "create_date": "2024-01-01",
            "col_name": name,
            "col_sort_order": order,
            "col_description": f"{name} column",
            "col_type": col_type,
            "is_partition_col": partition,
            "table_location": TABLE_LOCATION,
        }

    rows = [
        col("id", 0, "bigint", 0),
        col("amount", 1, "double", 0),
        col("dt", 0, "string", 1),
    ]
    if duplicate_column:
        rows.insert(2, col("amount", 2, "double", 0))
    return rows


VIEW_ROWS: List[Dict[str, Any]] = [
    {
        "schema_name": "db1",
        "table_name": "orders_view",
        "table_type": "VIRTUAL_VIEW",
        "view_expanded_text": "SELECT `orders`.`id` FROM `db1`.`orders`",
        "description": None,
        "create_date": "2024-01-02",
        "col_name": "id",
        "col_sort_order": 0,
        "col_description": None,
        "col_type": "bigint",
    }
]


class _StubFetcher:
    def __init__(self, duplicate_column: bool) -> None:
        self._duplicate_column = duplicate_column

    def fetch_table_rows(self) -> Iterable[Dict[str, Any]]:
        return _table_rows(self._duplicate_column)

    def fetch_view_rows(self) -> Iterable[Dict[str, Any]]:
        return VIEW_ROWS

    def fetch_schema_rows(self) -> Iterable[Dict[str, Any]]:
        return [{"schema": "db1"}]

    def fetch_table_properties_rows(self) -> Iterable[Dict[str, Any]]:
        return [
            {
                "schema_name": "db1",
                "table_name": "orders",
                "PARAM_KEY": "comment",
                "PARAM_VALUE": "orders table",
            }
        ]

    def close(self) -> None:
        pass


Row = Tuple[str, str, str, Dict[str, Any]]


def _rows(wus: Iterable[MetadataWorkUnit]) -> List[Row]:
    """Flatten work units to (urn, aspect, changeType, aspect JSON)."""
    rows: List[Row] = []
    for wu in wus:
        mcp = wu.metadata
        if isinstance(mcp, MetadataChangeProposalWrapper):
            assert mcp.aspect is not None
            aspect_json = mcp.aspect.to_obj()
        else:
            # Raw MCPs come from the patch builder (enable_properties_merge).
            assert isinstance(mcp, MetadataChangeProposalClass), wu.id
            assert mcp.aspect is not None
            aspect_json = json.loads(mcp.aspect.value)
        rows.append(
            (
                str(mcp.entityUrn),
                str(mcp.aspectName),
                str(mcp.changeType),
                aspect_json,
            )
        )
    return rows


def _run(extra_config: Dict[str, Any], duplicate_column: bool = False) -> List[Row]:
    config = HiveMetastore.model_validate(
        {
            "host_port": "localhost:3306",
            "database": "db1",
            "mode": "hive",
            **extra_config,
        }
    )
    with patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient"):
        source = HiveMetastoreSource(config, PipelineContext(run_id="hms-char"))
    source._fetcher = _StubFetcher(duplicate_column)  # type: ignore[assignment]
    rows = _rows(source.get_workunits())
    report = source.get_report()
    assert not report.failures, report.failures
    assert not report.warnings, report.warnings
    return rows


def _index(rows: List[Row], urn: str, aspect: str) -> int:
    matches = [i for i, r in enumerate(rows) if r[0] == urn and r[1] == aspect]
    assert len(matches) == 1, (urn, aspect, matches)
    return matches[0]


def _entity_rows(rows: List[Row], urn: str) -> List[Tuple[str, str]]:
    return [(r[1], r[2]) for r in rows if r[0] == urn]


def _aspect(rows: List[Row], urn: str, aspect: str) -> Any:
    return rows[_index(rows, urn, aspect)][3]


def _field(
    name: str,
    avro_type: str,
    native_type: str,
    type_class: str,
    description: Optional[str],
    partition: bool = False,
) -> Dict[str, Any]:
    field: Dict[str, Any] = {
        "fieldPath": f"[version=2.0].[type={avro_type}].{name}",
        "nullable": True,
        "type": {"type": {f"com.linkedin.pegasus2avro.schema.{type_class}": {}}},
        "nativeDataType": native_type,
        "recursive": False,
        "isPartOfKey": False,
    }
    if description is not None:
        field["description"] = description
    if partition:
        field["isPartitioningKey"] = True
    return field


def _schema(name: str, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "schemaName": name,
        "platform": "urn:li:dataPlatform:hive",
        "version": 0,
        "created": {"time": 0, "actor": "urn:li:corpuser:unknown"},
        "lastModified": {"time": 0, "actor": "urn:li:corpuser:unknown"},
        "hash": "",
        "platformSchema": {
            "com.linkedin.pegasus2avro.schema.MySqlDDL": {"tableSchema": ""}
        },
        "fields": fields,
    }


TABLE_SCHEMA = _schema(
    "db1.orders",
    [
        _field("id", "long", "bigint", "NumberType", "id column"),
        _field("amount", "double", "double", "NumberType", "amount column"),
        _field("dt", "string", "string", "StringType", "dt column", partition=True),
    ],
)
VIEW_SCHEMA = _schema(
    "db1.orders_view", [_field("id", "long", "bigint", "NumberType", None)]
)
VIEW_PROPERTIES = {
    "materialized": False,
    "viewLogic": "SELECT `orders`.`id` FROM `db1`.`orders`",
    "viewLanguage": "SQL",
}
TABLE_CUSTOM_PROPERTIES = {
    "comment": "orders table",
    "table_type": "EXTERNAL_TABLE",
    "table_location": TABLE_LOCATION,
    "create_date": "2024-01-01",
    "partitioned_columns": "dt",
}

# The view's rows are the same in every config.
VIEW_ROWS_EXPECTED = [
    ("container", "UPSERT"),
    ("status", "UPSERT"),
    ("schemaMetadata", "UPSERT"),
    ("datasetProperties", "UPSERT"),
    ("viewProperties", "UPSERT"),
    ("subTypes", "UPSERT"),
    ("upstreamLineage", "UPSERT"),
    ("browsePathsV2", "UPSERT"),
]


def _assert_view(rows: List[Row]) -> None:
    assert _entity_rows(rows, VIEW_URN) == VIEW_ROWS_EXPECTED
    assert _aspect(rows, VIEW_URN, "status") == {"removed": False}
    assert _aspect(rows, VIEW_URN, "schemaMetadata") == VIEW_SCHEMA
    assert _aspect(rows, VIEW_URN, "datasetProperties") == {
        "customProperties": {"is_view": "True"},
        "name": "orders_view",
        "tags": [],
    }
    assert _aspect(rows, VIEW_URN, "viewProperties") == VIEW_PROPERTIES
    table_positions = [i for i, r in enumerate(rows) if r[0] == TABLE_URN]
    view_positions = [i for i, r in enumerate(rows) if r[0] == VIEW_URN]
    assert max(table_positions) < min(view_positions)


def test_default_config_rows_in_order() -> None:
    rows = _run({})

    # Properties go out as this connector's own PATCH, before status and schema.
    assert _entity_rows(rows, TABLE_URN) == [
        ("container", "UPSERT"),
        ("datasetProperties", "PATCH"),
        ("status", "UPSERT"),
        ("schemaMetadata", "UPSERT"),
        ("subTypes", "UPSERT"),
        ("browsePathsV2", "UPSERT"),
    ]
    assert _aspect(rows, TABLE_URN, "status") == {"removed": False}
    assert _aspect(rows, TABLE_URN, "schemaMetadata") == TABLE_SCHEMA
    assert _aspect(rows, TABLE_URN, "datasetProperties") == [
        {"op": "add", "path": "/name", "value": "orders"},
        {"op": "add", "path": "/description", "value": "orders table"},
    ] + [
        {"op": "add", "path": f"/customProperties/{k}", "value": v}
        for k, v in TABLE_CUSTOM_PROPERTIES.items()
    ]
    _assert_view(rows)


def test_properties_merge_off_emits_upsert_after_schema() -> None:
    rows = _run({"enable_properties_merge": False})

    assert _entity_rows(rows, TABLE_URN) == [
        ("container", "UPSERT"),
        ("status", "UPSERT"),
        ("schemaMetadata", "UPSERT"),
        ("datasetProperties", "UPSERT"),
        ("subTypes", "UPSERT"),
        ("browsePathsV2", "UPSERT"),
    ]
    assert _aspect(rows, TABLE_URN, "schemaMetadata") == TABLE_SCHEMA
    assert _aspect(rows, TABLE_URN, "datasetProperties") == {
        "customProperties": TABLE_CUSTOM_PROPERTIES,
        "name": "orders",
        "description": "orders table",
        "tags": [],
    }
    _assert_view(rows)


def test_storage_lineage_sees_deduplicated_schema() -> None:
    rows = _run(
        {"emit_storage_lineage": True, "include_column_lineage": True},
        duplicate_column=True,
    )

    # The duplicate `amount` row is dropped by the schema field path validator,
    # which edits the emitted SchemaMetadata in place; storage lineage reads that
    # same object afterwards, so it sees three fields, not four.
    assert _aspect(rows, TABLE_URN, "schemaMetadata") == TABLE_SCHEMA
    storage_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/warehouse/orders,PROD)"
    assert _entity_rows(rows, TABLE_URN) == [
        ("container", "UPSERT"),
        ("datasetProperties", "PATCH"),
        ("status", "UPSERT"),
        ("schemaMetadata", "UPSERT"),
        ("subTypes", "UPSERT"),
        ("browsePathsV2", "UPSERT"),
        ("upstreamLineage", "UPSERT"),
    ]
    storage_schema = _aspect(rows, storage_urn, "schemaMetadata")
    assert [f["fieldPath"] for f in storage_schema["fields"]] == [
        f["fieldPath"] for f in TABLE_SCHEMA["fields"]
    ]
    lineage = _aspect(rows, TABLE_URN, "upstreamLineage")
    assert [u["dataset"] for u in lineage["upstreams"]] == [storage_urn]
    assert len(lineage["fineGrainedLineages"]) == 3
    _assert_view(rows)
