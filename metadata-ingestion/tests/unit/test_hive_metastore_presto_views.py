import base64
import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.hive.hive_metadata_processor import (
    HiveMetadataProcessor,
)
from datahub.ingestion.source.sql.hive.hive_metastore_config import HiveMetastore
from datahub.ingestion.source.sql.hive.hive_metastore_report import (
    HiveMetastoreSourceReport,
)
from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyDataFetcher
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    NullTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)

VIEW_SQL = "SELECT * FROM db1.events"


def _view_text(payload: Dict[str, Any]) -> str:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return f"/* Presto View: {encoded} */"


def _processor(
    view_text: str,
) -> HiveMetadataProcessor:
    config = HiveMetastore.model_validate(
        {
            "host_port": "localhost:3306",
            "database": "db1",
            "mode": "presto-on-hive",
        }
    )
    fetcher = MagicMock(spec=SQLAlchemyDataFetcher)
    fetcher.fetch_presto_view_rows.return_value = [
        {"schema": "db1", "name": "events_view", "view_original_text": view_text}
    ]
    return HiveMetadataProcessor(
        config=config,
        fetcher=fetcher,
        report=HiveMetastoreSourceReport(),
        platform="presto-on-hive",
    )


def _view_schema(processor: HiveMetadataProcessor) -> Optional[SchemaMetadataClass]:
    for wu in processor.process_views("db1"):
        mcp = wu.metadata
        if isinstance(mcp, MetadataChangeProposalWrapper) and isinstance(
            mcp.aspect, SchemaMetadataClass
        ):
            return mcp.aspect
    return None


def _field_by_suffix(schema: SchemaMetadataClass, suffix: str) -> SchemaFieldClass:
    matches = [f for f in schema.fields if f.fieldPath.endswith(suffix)]
    assert len(matches) == 1, [f.fieldPath for f in schema.fields]
    return matches[0]


def _warning_contexts(report: HiveMetastoreSourceReport) -> List[str]:
    return [context for warning in report.warnings for context in warning.context]


def test_presto_view_columns_typed_with_original_native_type() -> None:
    processor = _processor(
        _view_text(
            {
                "originalSql": VIEW_SQL,
                "columns": [
                    {"name": "payload", "type": "row(a bigint, b varchar)"},
                    {"name": "tags", "type": "array(varchar)"},
                    {"name": "created_at", "type": "timestamp(3) with time zone"},
                ],
            }
        )
    )

    schema = _view_schema(processor)

    assert schema is not None
    payload = _field_by_suffix(schema, "].payload")
    assert isinstance(payload.type.type, RecordTypeClass)
    assert payload.nativeDataType == "row(a bigint, b varchar)"
    assert _field_by_suffix(schema, "].payload.[type=long].a")
    assert _field_by_suffix(schema, "].payload.[type=string].b")

    tags = _field_by_suffix(schema, "].tags")
    assert isinstance(tags.type.type, ArrayTypeClass)
    assert tags.nativeDataType == "array(varchar)"

    created_at = _field_by_suffix(schema, "].created_at")
    assert isinstance(created_at.type.type, TimeTypeClass)
    assert created_at.nativeDataType == "timestamp(3) with time zone"
    assert not processor.report.warnings


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"originalSql": VIEW_SQL}, id="missing_columns"),
        pytest.param(
            {"originalSql": VIEW_SQL, "columns": [{"name": "id"}]},
            id="column_without_type",
        ),
        pytest.param(
            {"originalSql": VIEW_SQL, "columns": "id bigint"}, id="columns_not_list"
        ),
        pytest.param({"columns": [{"name": "id", "type": "bigint"}]}, id="missing_sql"),
        pytest.param(
            {"originalSql": "", "columns": [{"name": "id", "type": "bigint"}]},
            id="empty_sql",
        ),
        pytest.param(
            {"originalSql": "  \n ", "columns": [{"name": "id", "type": "bigint"}]},
            id="whitespace_sql",
        ),
    ],
)
def test_malformed_presto_view_is_skipped_with_warning(
    payload: Dict[str, Any],
) -> None:
    processor = _processor(_view_text(payload))

    assert _view_schema(processor) is None
    assert any("db1.events_view" in c for c in _warning_contexts(processor.report))


def test_presto_view_with_empty_columns_is_emitted_with_warning() -> None:
    processor = _processor(_view_text({"originalSql": VIEW_SQL, "columns": []}))

    schema = _view_schema(processor)

    assert schema is not None
    assert schema.fields == []
    assert any("db1.events_view" in c for c in _warning_contexts(processor.report))


def test_presto_column_too_nested_to_convert_falls_back_to_string() -> None:
    nested_type = "array(" * 70 + "bigint" + ")" * 70
    processor = _processor(
        _view_text(
            {
                "originalSql": VIEW_SQL,
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "deep", "type": nested_type},
                ],
            }
        )
    )

    schema = _view_schema(processor)

    assert schema is not None
    deep = _field_by_suffix(schema, "].deep")
    assert isinstance(deep.type.type, StringTypeClass)
    assert deep.nativeDataType == nested_type
    assert _field_by_suffix(schema, "].id").nativeDataType == "bigint"
    contexts = _warning_contexts(processor.report)
    assert any("db1.events_view" in c and "deep" in c for c in contexts)


def test_presto_column_hive_parser_cannot_read_is_reported() -> None:
    # The quoted row field name is copied into the Hive struct string as-is
    # (`struct<a:b:bigint>`), which the Hive parser leaves untyped.
    processor = _processor(
        _view_text(
            {
                "originalSql": VIEW_SQL,
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "payload", "type": 'row("a:b" bigint)'},
                ],
            }
        )
    )

    schema = _view_schema(processor)

    assert schema is not None
    # An unparsed column gets a bare (non-v2) field path.
    payload = _field_by_suffix(schema, "payload")
    assert isinstance(payload.type.type, NullTypeClass)
    assert payload.nativeDataType == 'row("a:b" bigint)'
    contexts = _warning_contexts(processor.report)
    assert any("db1.events_view" in c and "payload" in c for c in contexts)
    assert not any("id (bigint)" in c for c in contexts)
