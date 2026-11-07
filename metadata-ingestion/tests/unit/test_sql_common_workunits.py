"""Pins the table and view work unit stream of the SQLAlchemy base on a real sqlite catalog.

Rows are compared with MCEs expanded, so the pins hold for one MCE or one MCP per aspect.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest import mock

import pytest
from sqlalchemy import create_engine, inspect, text

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.testing.compare_metadata_json import expand_mces, load_json_file

TABLE = "urn:li:dataset:(urn:li:dataPlatform:sqlite,inst.main.t,PROD)"
VIEW = "urn:li:dataset:(urn:li:dataPlatform:sqlite,inst.main.v,PROD)"
LOCATION = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/t,PROD)"


class _SQLiteConfig(SQLCommonConfig):
    def get_sql_alchemy_url(self) -> str:
        return "sqlite://"


def _source() -> SQLAlchemySource:
    config = _SQLiteConfig.model_validate(
        {
            "platform_instance": "inst",
            "include_table_location_lineage": True,
            "include_view_lineage": False,
        }
    )
    return SQLAlchemySource(config, PipelineContext(run_id="test_ctx"), "sqlite")


def _rows(workunits: List[MetadataWorkUnit], tmp_path: Path) -> List[Dict[str, Any]]:
    out = tmp_path / "out.json"
    write_metadata_file(out, [wu.metadata for wu in workunits])
    return expand_mces(load_json_file(out))


def _scan(
    tmp_path: Path, location_urn: str = ""
) -> Tuple[SQLAlchemySource, List[Dict[str, Any]]]:
    source = _source()
    engine = create_engine("sqlite://")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)"))
        conn.execute(text("CREATE VIEW v AS SELECT a, b FROM t"))
        inspector = inspect(conn)
        real_properties = source.get_table_properties

        def table_properties(*args: Any, **kwargs: Any) -> Any:
            description, properties, _ = real_properties(*args, **kwargs)
            return description, properties, location_urn or None

        with mock.patch.object(
            source, "get_table_properties", side_effect=table_properties
        ):
            workunits = list(source.loop_tables(inspector, "main", source.config))
            workunits += list(source.loop_views(inspector, "main", source.config))
    return source, _rows(workunits, tmp_path)


def _aspect(rows: List[Dict[str, Any]], urn: str, name: str) -> Dict[str, Any]:
    (found,) = [
        r["aspect"]["json"]
        for r in rows
        if r["entityUrn"] == urn and r["aspectName"] == name
    ]
    return found


TABLE_ORDER = [
    (TABLE, "container"),
    (TABLE, "status"),
    (TABLE, "datasetProperties"),
    (TABLE, "schemaMetadata"),
    (TABLE, "dataPlatformInstance"),
    (TABLE, "subTypes"),
]
VIEW_ORDER = [
    (VIEW, "container"),
    (VIEW, "status"),
    (VIEW, "datasetProperties"),
    (VIEW, "schemaMetadata"),
    (VIEW, "dataPlatformInstance"),
    (VIEW, "subTypes"),
    (VIEW, "viewProperties"),
]


@pytest.mark.parametrize("location_urn", ["", LOCATION])
def test_table_and_view_stream_order(tmp_path: Path, location_urn: str) -> None:
    source, rows = _scan(tmp_path, location_urn)

    lineage = [(TABLE, "upstreamLineage")] if location_urn else []
    assert [(r["entityUrn"], r["aspectName"]) for r in rows] == (
        lineage + TABLE_ORDER + VIEW_ORDER
    )
    assert all(r["changeType"] == "UPSERT" for r in rows)
    assert not source.report.failures
    assert not source.report.warnings


def test_table_and_view_aspect_values(tmp_path: Path) -> None:
    _, rows = _scan(tmp_path, LOCATION)

    assert _aspect(rows, TABLE, "status") == {"removed": False}
    assert _aspect(rows, TABLE, "datasetProperties") == {
        "customProperties": {},
        "name": "t",
        "tags": [],
    }
    table_schema = _aspect(rows, TABLE, "schemaMetadata")
    assert table_schema["schemaName"] == "main.t"
    assert table_schema["platform"] == "urn:li:dataPlatform:sqlite"
    assert [f["fieldPath"] for f in table_schema["fields"]] == ["a", "b"]
    assert _aspect(rows, TABLE, "subTypes") == {"typeNames": ["Table"]}
    assert _aspect(rows, TABLE, "upstreamLineage")["upstreams"][0]["dataset"] == (
        LOCATION
    )

    assert _aspect(rows, VIEW, "status") == {"removed": False}
    view_properties = _aspect(rows, VIEW, "datasetProperties")
    assert view_properties["name"] == "v"
    assert view_properties["customProperties"] == {
        "is_view": "True",
        "view_definition": "CREATE VIEW v AS SELECT a, b FROM t",
    }
    assert [
        f["fieldPath"] for f in _aspect(rows, VIEW, "schemaMetadata")["fields"]
    ] == [
        "a",
        "b",
    ]
    assert _aspect(rows, VIEW, "subTypes") == {"typeNames": ["View"]}
    assert _aspect(rows, VIEW, "viewProperties")["viewLogic"] == (
        "CREATE VIEW v AS SELECT a, b FROM t"
    )


def test_view_without_columns_has_no_schema_metadata(tmp_path: Path) -> None:
    source = _source()
    engine = create_engine("sqlite://")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.execute(text("CREATE VIEW v AS SELECT a FROM t"))
        inspector = inspect(conn)
        with mock.patch.object(inspector, "get_columns", side_effect=KeyError("v")):
            workunits = list(source.loop_views(inspector, "main", source.config))
    rows = _rows(workunits, tmp_path)

    assert [(r["entityUrn"], r["aspectName"]) for r in rows] == [
        row for row in VIEW_ORDER if row[1] != "schemaMetadata"
    ]
    assert not source.report.failures
