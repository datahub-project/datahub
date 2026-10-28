"""RedshiftUsageExtractor: reading access events from the query-history cursor,
operational-stat emission and stateful-run gating."""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import MagicMock, patch

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.query import RedshiftServerlessQuery
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDataDictionary,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import (
    RedshiftAccessEvent,
    RedshiftUsageExtractor,
)
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass

ALL_TABLES: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
    "dev": {
        "public": [
            RedshiftTable(
                name="users",
                schema="public",
                type="BASE TABLE",
                created=None,
                comment="",
            )
        ]
    }
}

T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

EVENT_COLUMNS = [
    "userid",
    "username",
    "query",
    "querytxt",
    "tbl",
    "database",
    "schema",
    "table",
    "starttime",
    "endtime",
    "operation_type",
]


def _event_row(
    table: str = "users",
    schema: str = "public",
    operation_type: Optional[str] = "insert",
    querytxt: Optional[str] = "insert into public.users values (1)",
    endtime: datetime = T0,
    userid: object = 1,
) -> Tuple:
    return (
        userid,
        "alice",
        1,
        querytxt,
        1,
        "dev",
        schema,
        table,
        T0,
        endtime,
        operation_type,
    )


def _cursor(rows: List[Tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.description = [(c,) for c in EVENT_COLUMNS]
    cursor.fetchmany.side_effect = [rows, []]
    return cursor


def _extractor(
    *,
    include_operational_stats: bool = True,
    is_serverless: bool = False,
    redundant_run_skip_handler: Optional[MagicMock] = None,
) -> RedshiftUsageExtractor:
    config = RedshiftConfig(
        host_port="test:1234",
        database="dev",
        email_domain="example.com",
        include_usage_statistics=True,
        include_operational_stats=include_operational_stats,
        is_serverless=is_serverless,
        start_time="2024-01-01T00:00:00Z",
        end_time="2024-01-02T00:00:00Z",
    )
    return RedshiftUsageExtractor(
        config=config,
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda table: make_dataset_urn("redshift", table),
        redundant_run_skip_handler=redundant_run_skip_handler,
    )


def test_access_events_from_cursor_skip_invalid_and_unknown_tables() -> None:
    extractor = _extractor()
    rows = [
        _event_row(),
        _event_row(userid="not-an-int"),  # fails validation
        _event_row(table="not_ingested"),  # not in all_tables
        _event_row(querytxt=None),
    ]
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor(rows)
    ):
        events = list(
            extractor._gen_access_events_from_history_query(
                "q", MagicMock(), all_tables=ALL_TABLES
            )
        )

    # Rows without query text are dropped too (text is a required field).
    assert [e.table for e in events] == ["users"]
    assert events[0].operation_type == "insert"
    assert extractor.report.num_usage_stat_skipped == 3


def test_operation_aspects_are_emitted_and_near_duplicates_dropped() -> None:
    extractor = _extractor()
    rows = [
        # Newest first, as the operation query orders them.
        _event_row(endtime=datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)),
        _event_row(endtime=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        _event_row(
            operation_type="delete",
            endtime=datetime(2024, 1, 1, 11, 59, 58, tzinfo=timezone.utc),
        ),
        _event_row(operation_type=None),  # no operation -> ignored
    ]
    # Both the operation query and the usage query read through this boundary.
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        side_effect=lambda *args, **kwargs: _cursor(rows),
    ):
        workunits = list(extractor.get_usage_workunits(all_tables=ALL_TABLES))

    operations = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, OperationClass)
    ]
    assert [op.operationType for op in operations] == [
        OperationTypeClass.INSERT,
        OperationTypeClass.DELETE,
    ]
    assert operations[0].actor == "urn:li:corpuser:alice"
    assert extractor.report.num_repeated_operations_dropped == 1
    assert extractor.report.num_operational_stats_workunits_emitted == 3
    assert "dev" in extractor.report.operational_metadata_extraction_sec


def test_operation_aspects_filtered_for_tables_outside_scope() -> None:
    extractor = _extractor()
    event = RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=1,
        querytxt="insert into other.t values (1)",
        tbl=1,
        database="dev",
        schema="other",
        table="t",
        operation_type="insert",
        starttime=T0,
        endtime=T0,
    )
    mcps = list(
        extractor._gen_operation_aspect_workunits_from_access_events(
            iter([event]), all_tables=ALL_TABLES
        )
    )
    assert mcps == []
    assert extractor.report.num_operational_stats_filtered == 1


def test_stateful_usage_run_is_skipped_when_window_already_covered() -> None:
    handler = MagicMock()
    handler.suggest_run_time_window.return_value = (
        datetime(2024, 1, 1, 6, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    handler.should_skip_this_run.return_value = True
    extractor = _extractor(redundant_run_skip_handler=handler)

    assert extractor.start_time == datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
    assert list(extractor.get_usage_workunits(all_tables=ALL_TABLES)) == []
    assert extractor.report.warnings
    handler.update_state.assert_not_called()

    extractor.report_status("usage", True)
    handler.report_current_run_status.assert_called_once_with("usage", True)


def test_stateful_usage_run_updates_checkpoint_after_success() -> None:
    handler = MagicMock()
    handler.suggest_run_time_window.return_value = (
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    handler.should_skip_this_run.return_value = False
    extractor = _extractor(
        include_operational_stats=False, redundant_run_skip_handler=handler
    )
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_cursor([])
    ):
        list(extractor.get_usage_workunits(all_tables=ALL_TABLES))
    handler.update_state.assert_called_once()


def test_serverless_extractor_uses_serverless_queries() -> None:
    assert isinstance(_extractor(is_serverless=True).queries, RedshiftServerlessQuery)
