"""Stateful-window handling and edge mapping in SnowflakeLineageExtractor."""

from datetime import datetime, timezone
from typing import List, Tuple
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
    UpstreamTableNode,
)
from datahub.metadata.schema_classes import DatasetLineageTypeClass
from datahub.utilities.time import ts_millis_to_datetime
from tests.unit.snowflake.test_snowflake_lineage_extraction import (  # type: ignore[import-untyped]
    END,
    START,
    _make_extractor,
)

EPOCH = ts_millis_to_datetime(0)


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _with_handler(
    **config_overrides: object,
) -> Tuple[SnowflakeLineageExtractor, MagicMock]:
    extractor = _make_extractor(lambda _sql: [], **config_overrides)
    handler = MagicMock()
    handler.suggest_run_time_window.return_value = (
        datetime(2024, 1, 1, 6, tzinfo=timezone.utc),
        END,
    )
    handler.should_skip_this_run.return_value = False
    extractor.redundant_run_skip_handler = handler
    extractor.start_time, extractor.end_time = extractor.get_time_window()
    return extractor, handler


def test_window_comes_from_the_skip_handler_and_state_is_updated_after_a_run() -> None:
    extractor, handler = _with_handler()

    assert extractor.start_time == datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
    handler.suggest_run_time_window.assert_called_with(START, END)

    extractor.add_time_based_lineage_to_aggregator(
        discovered_tables=[], discovered_views=[]
    )
    extractor.update_state()

    handler.update_state.assert_called_once_with(START, END)


def test_ignore_start_time_widens_the_window_to_the_epoch() -> None:
    extractor, handler = _with_handler(ignore_start_time_lineage=True)

    handler.suggest_run_time_window.assert_called_with(EPOCH, END)
    extractor.update_state()
    handler.update_state.assert_called_once_with(EPOCH, END)


def test_redundant_window_skips_lineage_with_a_warning() -> None:
    extractor, handler = _with_handler()
    handler.should_skip_this_run.return_value = True

    extractor.add_time_based_lineage_to_aggregator(
        discovered_tables=["db.s.t"], discovered_views=[]
    )

    _as_mock(extractor.connection).query.assert_not_called()
    assert len(extractor.report.warnings) == 1


def test_permission_errors_are_warnings_only_under_stateful_ingestion() -> None:
    plain = _make_extractor(lambda _sql: [])
    plain.warn_if_stateful_else_error("key", "reason")
    assert len(plain.report.failures) == 1

    stateful = _make_extractor(lambda _sql: [], stateful_ingestion={"enabled": True})
    stateful.warn_if_stateful_else_error("key", "reason")
    assert stateful.report.failures == []
    assert len(stateful.report.warnings) == 1


def test_external_upstreams_are_limited_to_s3_locations() -> None:
    extractor = _make_extractor(lambda _sql: [])

    upstreams = extractor.get_external_upstreams(
        {"s3://bucket/b", "gcs://bucket/a", "s3://bucket/a"}
    )

    assert [u.type for u in upstreams] == [DatasetLineageTypeClass.COPY] * 2
    assert [u.dataset for u in upstreams] == sorted(u.dataset for u in upstreams)
    assert all("s3" in u.dataset for u in upstreams)


def test_upstreams_from_other_queries_or_unparseable_names_are_dropped() -> None:
    extractor = _make_extractor(lambda _sql: [])
    nodes: List[UpstreamTableNode] = [
        UpstreamTableNode(
            upstream_object_domain="TABLE",
            upstream_object_name="DB.S.A",
            query_id="q1",
        ),
        UpstreamTableNode(
            upstream_object_domain="TABLE",
            upstream_object_name="DB.S.B",
            query_id="other",
        ),
    ]
    extractor.identifiers = MagicMock()
    extractor.identifiers.get_dataset_identifier_from_qualified_name.side_effect = (
        ValueError("bad name")
    )

    assert extractor.map_query_result_upstreams(nodes, "q1") == []
    assert extractor.map_query_result_upstreams(None, "q1") == []
