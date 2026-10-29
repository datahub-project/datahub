"""RedshiftUsageExtractor: reading access events from the query-history cursor,
operational-stat emission and stateful-run gating."""

from datetime import datetime, timezone
from typing import Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.redshift.query import RedshiftServerlessQuery
from datahub.ingestion.source.redshift.redshift_schema import RedshiftDataDictionary
from datahub.ingestion.source.redshift.usage import RedshiftAccessEvent
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
    OperationClass,
    OperationTypeClass,
)
from tests.unit.redshift.conftest import (
    DB,
    AllTables,
    aspects_of,
    make_cursor,
    make_table,
    make_usage_extractor,
)

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
        DB,
        schema,
        table,
        T0,
        endtime,
        operation_type,
    )


def _event_cursor(rows: Tuple[Tuple, ...]) -> MagicMock:
    return make_cursor(EVENT_COLUMNS, rows)


def test_access_events_from_cursor_skip_invalid_and_unknown_tables(
    all_tables: AllTables,
) -> None:
    extractor = make_usage_extractor()
    rows = (
        _event_row(),
        _event_row(userid="not-an-int"),  # fails validation
        _event_row(table="not_ingested"),  # not in all_tables
    )
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_event_cursor(rows)
    ):
        events = list(
            extractor._gen_access_events_from_history_query(
                "q", MagicMock(), all_tables=all_tables
            )
        )

    assert [e.table for e in events] == ["users"]
    assert events[0].operation_type == "insert"
    assert extractor.report.num_usage_stat_skipped == 2


@pytest.mark.xfail(
    strict=True,
    reason="RedshiftAccessEvent.text is a required str, but "
    "_gen_access_events_from_history_query passes None for an empty querytxt, "
    "so the row fails validation and is dropped (counted as skipped) instead of "
    "being attributed to the table.",
)
def test_access_event_without_query_text_is_still_attributed(
    all_tables: AllTables,
) -> None:
    extractor = make_usage_extractor()
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        return_value=_event_cursor((_event_row(querytxt=None),)),
    ):
        events = list(
            extractor._gen_access_events_from_history_query(
                "q", MagicMock(), all_tables=all_tables
            )
        )
    assert [e.table for e in events] == ["users"]
    assert extractor.report.num_usage_stat_skipped == 0


def test_operation_aspects_are_emitted_and_near_duplicates_dropped(
    all_tables: AllTables,
) -> None:
    """Operation rows arrive newest first; a second insert on the same table
    within the dedupe window is dropped, a delete is kept, and a row with no
    operation type is ignored."""
    extractor = make_usage_extractor(include_operational_stats=True)
    rows = (
        _event_row(endtime=datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)),
        _event_row(endtime=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        _event_row(
            operation_type="delete",
            endtime=datetime(2024, 1, 1, 11, 59, 58, tzinfo=timezone.utc),
        ),
        _event_row(operation_type=None),
    )
    # Both the operation query and the usage query read through this boundary.
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        side_effect=lambda *args, **kwargs: _event_cursor(rows),
    ):
        workunits = list(extractor.get_usage_workunits(all_tables=all_tables))

    operations = aspects_of(workunits, OperationClass)
    assert [op.operationType for op in operations] == [
        OperationTypeClass.INSERT,
        OperationTypeClass.DELETE,
    ]
    assert operations[0].actor == "urn:li:corpuser:alice"
    assert extractor.report.num_repeated_operations_dropped == 1
    assert extractor.report.num_operational_stats_workunits_emitted == 3
    assert DB in extractor.report.operational_metadata_extraction_sec


def test_operation_aspects_filtered_for_tables_outside_scope(
    all_tables: AllTables,
) -> None:
    extractor = make_usage_extractor(include_operational_stats=True)
    event = RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=1,
        querytxt="insert into other.t values (1)",
        tbl=1,
        database=DB,
        schema="other",
        table="t",
        operation_type="insert",
        starttime=T0,
        endtime=T0,
    )
    mcps = list(
        extractor._gen_operation_aspect_workunits_from_access_events(
            iter([event]), all_tables=all_tables
        )
    )
    assert mcps == []
    assert extractor.report.num_operational_stats_filtered == 1


@pytest.mark.xfail(
    strict=True,
    reason="RedshiftUsageExtractor lowercases `db.schema.table` before building "
    "the usage and operation URNs, so for mixed-case names they land on a "
    "different dataset than the schemaMetadata emitted by RedshiftSource.",
)
def test_usage_and_operations_land_on_the_dataset_urn_for_mixed_case_names() -> None:
    extractor = make_usage_extractor(include_operational_stats=True)
    mixed_case_tables: AllTables = {
        DB: {"Sales": [make_table("Orders", schema="Sales")]}
    }
    rows = (_event_row(table="Orders", schema="Sales"),)
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        side_effect=lambda *args, **kwargs: _event_cursor(rows),
    ):
        workunits = list(extractor.get_usage_workunits(all_tables=mixed_case_tables))

    urns = {
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(
            wu.metadata.aspect, (DatasetUsageStatisticsClass, OperationClass)
        )
    }
    assert urns == {make_dataset_urn("redshift", f"{DB}.Sales.Orders")}


def test_stateful_usage_run_is_skipped_when_window_already_covered(
    all_tables: AllTables, usage_skip_handler: MagicMock
) -> None:
    usage_skip_handler.suggest_run_time_window.return_value = (
        datetime(2024, 1, 1, 6, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    usage_skip_handler.should_skip_this_run.return_value = True
    extractor = make_usage_extractor(skip_handler=usage_skip_handler)

    assert extractor.start_time == datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
    assert list(extractor.get_usage_workunits(all_tables=all_tables)) == []
    assert any("usage-extraction" in w.context for w in extractor.report.warnings)
    usage_skip_handler.update_state.assert_not_called()

    extractor.report_status("usage", True)
    usage_skip_handler.report_current_run_status.assert_called_once_with("usage", True)


def test_stateful_usage_run_updates_checkpoint_after_success(
    all_tables: AllTables, usage_skip_handler: MagicMock
) -> None:
    usage_skip_handler.suggest_run_time_window.return_value = (
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    usage_skip_handler.should_skip_this_run.return_value = False
    extractor = make_usage_extractor(skip_handler=usage_skip_handler)
    with patch.object(
        RedshiftDataDictionary, "get_query_result", return_value=_event_cursor(())
    ):
        list(extractor.get_usage_workunits(all_tables=all_tables))
    usage_skip_handler.update_state.assert_called_once()


def test_serverless_extractor_uses_serverless_queries() -> None:
    assert isinstance(
        make_usage_extractor(is_serverless=True).queries, RedshiftServerlessQuery
    )
