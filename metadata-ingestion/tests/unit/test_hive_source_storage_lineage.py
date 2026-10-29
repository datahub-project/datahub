from typing import Any, Dict, Iterable, List, Optional, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import types

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hive.hive_source import HiveSource
from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage
from datahub.ingestion.source.sql.two_tier_sql_source import TwoTierSQLAlchemySource
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    UpstreamLineageClass,
    _Aspect,
)

# Every aspect this scenario emits. rows() expands an MCE in this order, so Status,
# DatasetProperties, SchemaMetadata must stay in snapshot order for MCE and MCP output to match.
_ASPECT_CLASSES: List[Type[_Aspect]] = [
    StatusClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    DataPlatformInstanceClass,
    SubTypesClass,
    ContainerClass,
    UpstreamLineageClass,
]

HIVE_TABLE = "urn:li:dataset:(urn:li:dataPlatform:hive,db1.located,PROD)"
HIVE_TABLE_NO_LOCATION = "urn:li:dataset:(urn:li:dataPlatform:hive,db1.unlocated,PROD)"
STORAGE = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/located,PROD)"


def _inspector() -> MagicMock:
    inspector = MagicMock()
    inspector.engine.url.database = "db1"
    inspector.get_columns.return_value = [
        {"name": "id", "type": types.INTEGER(), "full_type": "int", "nullable": False},
        {
            "name": "name",
            "type": types.VARCHAR(),
            "full_type": "string",
            "nullable": True,
        },
        {
            "name": "ts",
            "type": types.TIMESTAMP(),
            "full_type": "timestamp",
            "nullable": True,
        },
    ]
    inspector.get_pk_constraint.return_value = {}
    inspector.get_foreign_keys.return_value = []
    inspector.get_indexes.return_value = []

    def table_comment(table: str, schema: str) -> Dict[str, Any]:
        properties = (
            {"Location": "s3://my-bucket/data/located"} if table == "located" else {}
        )
        return {"text": None, "properties": properties}

    inspector.get_table_comment.side_effect = table_comment
    return inspector


def _base_scan(self: HiveSource) -> Iterable[MetadataWorkUnit]:
    inspector = _inspector()
    for table in ("located", "unlocated"):
        yield from self._process_table(
            f"db1.{table}", inspector, "db1", table, self.config, None
        )


def run_scenario(direction: str) -> Tuple[HiveSource, List[MetadataWorkUnit]]:
    source = HiveSource.create(
        {
            "host_port": "localhost:10000",
            "emit_storage_lineage": True,
            "include_column_lineage": True,
            "hive_storage_lineage_direction": direction,
        },
        PipelineContext(run_id="hive-storage-lineage-test"),
    )
    with patch.object(TwoTierSQLAlchemySource, "get_workunits_internal", _base_scan):
        workunits = list(source.get_workunits_internal())
    return source, workunits


def rows(workunits: List[MetadataWorkUnit]) -> List[Tuple[str, str, _Aspect]]:
    """(urn, aspect name, aspect) per aspect in stream order, the same for an MCE or its MCPs."""
    result: List[Tuple[str, str, _Aspect]] = []
    for wu in workunits:
        found = [
            aspect
            for cls in _ASPECT_CLASSES
            if (aspect := wu.get_aspect_of_type(cls)) is not None
        ]
        assert found, f"work unit {wu.id} carries an aspect this test does not list"
        result += [(wu.get_urn(), a.get_aspect_name(), a) for a in found]
    return result


def _find(
    found: List[Tuple[str, str, _Aspect]], urn: str, name: str
) -> Optional[_Aspect]:
    matches = [a for u, n, a in found if u == urn and n == name]
    assert len(matches) <= 1
    return matches[0] if matches else None


def assert_storage_lineage(
    source: HiveSource, workunits: List[MetadataWorkUnit], direction: str
) -> None:
    found = rows(workunits)
    lineage_urn = HIVE_TABLE if direction == "upstream" else STORAGE

    assert not source.report.failures
    assert not source.report.warnings
    assert [(u, n) for u, n, _ in found if u in (HIVE_TABLE, STORAGE)] == [
        (HIVE_TABLE, "container"),
        (HIVE_TABLE, "status"),
        (HIVE_TABLE, "datasetProperties"),
        (HIVE_TABLE, "schemaMetadata"),
        (STORAGE, "datasetProperties"),
        (STORAGE, "dataPlatformInstance"),
        (STORAGE, "schemaMetadata"),
        (lineage_urn, "upstreamLineage"),
        (HIVE_TABLE, "subTypes"),
    ]
    # A table without a Location gets no storage lineage.
    assert [n for u, n, _ in found if u == HIVE_TABLE_NO_LOCATION] == [
        "container",
        "status",
        "datasetProperties",
        "schemaMetadata",
        "subTypes",
    ]

    storage_schema = _find(found, STORAGE, "schemaMetadata")
    assert isinstance(storage_schema, SchemaMetadataClass)
    assert [f.fieldPath for f in storage_schema.fields] == ["id", "name", "ts"]

    lineage = _find(found, lineage_urn, "upstreamLineage")
    assert isinstance(lineage, UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [
        STORAGE if direction == "upstream" else HIVE_TABLE
    ]
    assert lineage.fineGrainedLineages is not None
    assert len(lineage.fineGrainedLineages) == 3


@pytest.mark.parametrize("direction", ["upstream", "downstream"])
def test_storage_lineage_follows_hive_table_schema(direction: str) -> None:
    source, workunits = run_scenario(direction)
    assert_storage_lineage(source, workunits, direction)


def test_storage_lineage_error_is_reported_and_table_still_emitted() -> None:
    with patch.object(
        HiveStorageLineage, "get_lineage_mcp", side_effect=ValueError("bad location")
    ):
        source, workunits = run_scenario("upstream")
    found = rows(workunits)

    assert not source.report.failures
    assert len(source.report.warnings) == 1
    assert not [n for u, n, _ in found if u == STORAGE]
    assert [n for u, n, _ in found if u == HIVE_TABLE] == [
        "container",
        "status",
        "datasetProperties",
        "schemaMetadata",
        "subTypes",
    ]
