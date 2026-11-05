"""ClickHouse definition lineage for each table and view.

Only the base scan, the inspector, the lineage map and the system.tables fetch are faked;
the real ``_process_table``/``_process_view`` build each dataset.
"""

from typing import Any, Dict, Iterable, List, Tuple
from unittest import mock

import pytest
from sqlalchemy import types as sqltypes

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    ClickHouseSource,
    LineageCollectorType,
    LineageDataset,
    LineageDatasetPlatform,
    LineageItem,
)
from datahub.ingestion.source.sql.two_tier_sql_source import TwoTierSQLAlchemySource
from datahub.metadata.schema_classes import UpstreamLineageClass

TABLE = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,db1.events,PROD)"
VIEW = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,db1.events_v,PROD)"
RAW = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,db1.raw,PROD)"

COLUMNS = [
    {"name": "col_a", "type": sqltypes.Integer(), "nullable": True, "comment": ""},
    {"name": "col_b", "type": sqltypes.String(), "nullable": True, "comment": ""},
]


def _inspector(comment: Dict[str, Any], view_definition: str = "") -> mock.MagicMock:
    inspector = mock.MagicMock()
    inspector.get_columns.return_value = COLUMNS
    inspector.get_pk_constraint.return_value = {}
    inspector.get_foreign_keys.return_value = []
    inspector.get_indexes.return_value = []
    inspector.get_table_comment.return_value = comment
    inspector.get_view_definition.return_value = view_definition
    inspector.engine.url.database = "db1"
    inspector.dialect.type_compiler_instance.process.side_effect = lambda t, **kw: type(
        t
    ).__name__.upper()
    return inspector


def _base_scan(self: ClickHouseSource) -> Iterable[MetadataWorkUnit]:
    yield from self._process_table(
        "db1.events",
        _inspector({"text": "events", "properties": {"engine": "MergeTree"}}),
        "db1",
        "events",
        self.config,
        None,
    )
    yield from self._process_view(
        "db1.events_v",
        _inspector({"text": None, "properties": {}}, "SELECT col_a FROM db1.events"),
        "db1",
        "events_v",
        self.config,
    )
    yield from self._generate_aggregator_workunits()


def _lineage_item(path: str, upstream: str, kind: LineageCollectorType) -> LineageItem:
    return LineageItem(
        LineageDataset(LineageDatasetPlatform.CLICKHOUSE, path),
        {LineageDataset(LineageDatasetPlatform.CLICKHOUSE, upstream)},
        kind,
    )


def run_scenario(
    include_table_lineage: bool = True,
) -> Tuple[ClickHouseSource, List[Tuple[str, UpstreamLineageClass]]]:
    """Run ClickHouseSource.get_workunits_internal; return the source and its lineage writes in order."""
    source = ClickHouseSource(
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "include_table_lineage": include_table_lineage,
            }
        ),
        PipelineContext(run_id="clickhouse-definition-lineage"),
    )
    source._lineage_map = {
        "db1.events": _lineage_item(
            "db1.events", "db1.raw", LineageCollectorType.TABLE
        ),
        "db1.events_v": _lineage_item(
            "db1.events_v", "db1.events", LineageCollectorType.VIEW
        ),
    }
    with (
        mock.patch.object(
            TwoTierSQLAlchemySource, "get_workunits_internal", _base_scan
        ),
        mock.patch.object(
            ClickHouseSource, "_get_all_tables", lambda self: {"db1.raw", "db1.events"}
        ),
    ):
        workunits = list(source.get_workunits_internal())
    lineage = [
        (wu.get_urn(), aspect)
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(UpstreamLineageClass)) is not None
    ]
    return source, lineage


def _upstreams(lineage: UpstreamLineageClass) -> List[Tuple[str, str]]:
    return [(u.dataset, str(u.type)) for u in lineage.upstreams]


def test_definition_lineage_for_each_table_and_view() -> None:
    source, lineage = run_scenario()
    assert not source.report.failures
    assert [(urn, _upstreams(aspect)) for urn, aspect in lineage] == [
        (TABLE, [(RAW, "COPY")]),
        (VIEW, [(TABLE, "VIEW")]),
        (VIEW, [(TABLE, "VIEW")]),
    ]
    # The view's lineage-map write must come before its SQL-parsed write, which carries
    # column lineage and must stay the last write.
    assert not lineage[1][1].fineGrainedLineages
    assert lineage[2][1].fineGrainedLineages


def test_no_definition_lineage_when_disabled() -> None:
    source, lineage = run_scenario(include_table_lineage=False)
    assert not source.report.failures
    # Only the view's SQL-parsed lineage stays.
    assert [(urn, _upstreams(aspect)) for urn, aspect in lineage] == [
        (VIEW, [(TABLE, "VIEW")]),
    ]
    assert lineage[0][1].fineGrainedLineages


def test_lineage_lookup_failure_fails_the_run() -> None:
    """A failing _get_all_tables must fail the run, not become a table warning."""
    source = ClickHouseSource(
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "include_table_lineage": True}
        ),
        PipelineContext(run_id="clickhouse-definition-lineage"),
    )
    inspector = _inspector({"text": "events", "properties": {}})
    inspector.get_table_names.return_value = ["events"]

    def scan(self: ClickHouseSource) -> Iterable[MetadataWorkUnit]:
        yield from self.loop_tables(inspector, "db1", self.config)

    def unreachable(self: ClickHouseSource) -> None:
        raise ConnectionError("_get_all_tables failed")

    with (
        mock.patch.object(TwoTierSQLAlchemySource, "get_workunits_internal", scan),
        mock.patch.object(ClickHouseSource, "_get_all_tables", unreachable),
        pytest.raises(ConnectionError),
    ):
        list(source.get_workunits_internal())


def test_empty_lineage_map_loads_once() -> None:
    source = ClickHouseSource(
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "include_table_lineage": True}
        ),
        PipelineContext(run_id="clickhouse-definition-lineage"),
    )

    def load_nothing(self: ClickHouseSource) -> None:
        self._lineage_map = {}

    with (
        mock.patch.object(
            TwoTierSQLAlchemySource, "get_workunits_internal", _base_scan
        ),
        mock.patch.object(
            ClickHouseSource,
            "_populate_lineage",
            autospec=True,
            side_effect=load_nothing,
        ) as populate,
    ):
        list(source.get_workunits_internal())
    assert populate.call_count == 1


def test_no_lineage_load_when_nothing_is_scanned() -> None:
    source = ClickHouseSource(
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "include_table_lineage": True,
                "include_tables": False,
                "include_views": False,
            }
        ),
        PipelineContext(run_id="clickhouse-definition-lineage"),
    )

    def unreachable(self: ClickHouseSource) -> None:
        raise ConnectionError("_get_all_tables failed")

    with (
        mock.patch.object(
            TwoTierSQLAlchemySource, "get_workunits_internal", lambda self: iter([])
        ),
        mock.patch.object(ClickHouseSource, "_get_all_tables", unreachable),
    ):
        assert list(source.get_workunits_internal()) == []
    assert not source.report.failures
