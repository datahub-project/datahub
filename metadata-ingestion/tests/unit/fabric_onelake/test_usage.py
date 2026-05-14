from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fabric.onelake.config import FabricUsageConfig
from datahub.ingestion.source.fabric.onelake.models import FabricQueryInsightsRow
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.fabric.onelake.usage import FabricUsageExtractor
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery

WORKSPACE_ID = "ws-123"
ITEM_ID = "lh-456"
ITEM_DISPLAY_NAME = "Test Lakehouse"
QUERY_TS = datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone.utc)


def make_row(
    command: str = "SELECT * FROM dbo.customers",
    login_name: str | None = "alice@example.com",
    start_time: datetime | None = None,
    status: str = "Succeeded",
) -> FabricQueryInsightsRow:
    return FabricQueryInsightsRow(
        start_time=start_time or QUERY_TS,
        statement_type="SELECT",
        status=status,
        command=command,
        login_name=login_name,
        row_count=1,
    )


def make_extractor(
    config: FabricUsageConfig | None = None,
    skip_handler: MagicMock | None = None,
) -> tuple[FabricUsageExtractor, MagicMock, FabricOneLakeSourceReport]:
    """Build an extractor with a mock aggregator and real report."""
    config = config or FabricUsageConfig()
    aggregator = MagicMock()
    report = FabricOneLakeSourceReport()
    extractor = FabricUsageExtractor(
        config=config,
        aggregator=aggregator,
        report=report,
        redundant_run_skip_handler=skip_handler,
    )
    return extractor, aggregator, report


def test_resolve_time_window_no_skip_handler_uses_config_window() -> None:
    extractor, _, report = make_extractor()
    assert extractor.start_time == extractor.config.start_time
    assert extractor.end_time == extractor.config.end_time
    assert report.usage_start_time == extractor.start_time
    assert report.usage_end_time == extractor.end_time


def test_resolve_time_window_skip_handler_narrows_window() -> None:
    narrowed_start = QUERY_TS - timedelta(hours=2)
    narrowed_end = QUERY_TS
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (narrowed_start, narrowed_end)

    extractor, _, report = make_extractor(skip_handler=skip_handler)

    assert extractor.start_time == narrowed_start
    assert extractor.end_time == narrowed_end
    assert report.usage_start_time == narrowed_start
    assert report.usage_end_time == narrowed_end
    skip_handler.suggest_run_time_window.assert_called_once()


def test_should_skip_run_returns_false_without_handler() -> None:
    extractor, _, report = make_extractor()
    assert extractor.should_skip_run() is False
    assert report.usage_run_skipped is False


def test_should_skip_run_returns_false_when_handler_says_run() -> None:
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (
        FabricUsageConfig().start_time,
        FabricUsageConfig().end_time,
    )
    skip_handler.should_skip_this_run.return_value = False

    extractor, _, report = make_extractor(skip_handler=skip_handler)
    assert extractor.should_skip_run() is False
    assert report.usage_run_skipped is False


def test_should_skip_run_returns_true_when_handler_says_skip() -> None:
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (
        FabricUsageConfig().start_time,
        FabricUsageConfig().end_time,
    )
    skip_handler.should_skip_this_run.return_value = True

    extractor, _, report = make_extractor(skip_handler=skip_handler)
    assert extractor.should_skip_run() is True
    assert report.usage_run_skipped is True


def test_update_state_on_success_persists_resolved_window() -> None:
    narrowed_start = QUERY_TS - timedelta(hours=2)
    narrowed_end = QUERY_TS
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (narrowed_start, narrowed_end)

    extractor, _, _ = make_extractor(skip_handler=skip_handler)
    extractor.update_state_on_success()

    skip_handler.update_state.assert_called_once_with(
        narrowed_start,
        narrowed_end,
        extractor.config.bucket_duration,
    )


def test_extract_happy_path_feeds_aggregator() -> None:
    extractor, aggregator, report = make_extractor()
    rows = [make_row(), make_row(command="SELECT 1")]

    schema_client = MagicMock()
    schema_client.stream_usage_history.return_value = iter(rows)

    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    assert aggregator.add_observed_query.call_count == 2
    assert report.num_usage_queries_fetched == 2
    assert f"{WORKSPACE_ID}/{ITEM_ID}" in report.usage_extraction_per_item_sec


def test_extract_passes_resolved_window_and_skip_flag_to_client() -> None:
    config = FabricUsageConfig(skip_failed_queries=False)
    extractor, _, _ = make_extractor(config=config)

    schema_client = MagicMock()
    schema_client.stream_usage_history.return_value = iter([])

    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    schema_client.stream_usage_history.assert_called_once_with(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        start_time=extractor.start_time,
        end_time=extractor.end_time,
        skip_failed_queries=False,
    )


def test_extract_swallows_stream_exception_and_warns() -> None:
    extractor, aggregator, report = make_extractor()

    schema_client = MagicMock()
    schema_client.stream_usage_history.side_effect = RuntimeError("connection lost")

    # Must not raise — per-item failures don't poison the run.
    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    aggregator.add_observed_query.assert_not_called()
    assert any(w.title == "Failed to Extract Usage" for w in report.warnings)
    # Timing is still recorded even when extraction fails.
    assert f"{WORKSPACE_ID}/{ITEM_ID}" in report.usage_extraction_per_item_sec


def test_extract_failure_marks_skip_handler_step_failed() -> None:
    """Per-item failures must call report_current_run_status(False) so the skip
    handler refuses to advance the usage checkpoint — otherwise a window where
    every item failed would be permanently marked as covered.
    """
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (
        FabricUsageConfig().start_time,
        FabricUsageConfig().end_time,
    )
    extractor, _, _ = make_extractor(skip_handler=skip_handler)

    schema_client = MagicMock()
    schema_client.stream_usage_history.side_effect = RuntimeError("endpoint down")

    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    skip_handler.report_current_run_status.assert_called_once_with(
        f"usage-extraction-{WORKSPACE_ID}/{ITEM_ID}", False
    )


def test_extract_happy_path_does_not_mark_skip_handler_step_failed() -> None:
    skip_handler = MagicMock()
    skip_handler.suggest_run_time_window.return_value = (
        FabricUsageConfig().start_time,
        FabricUsageConfig().end_time,
    )
    extractor, _, _ = make_extractor(skip_handler=skip_handler)

    schema_client = MagicMock()
    schema_client.stream_usage_history.return_value = iter([make_row()])

    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    skip_handler.report_current_run_status.assert_not_called()


def test_extract_partial_failure_keeps_already_processed_rows() -> None:
    """A mid-stream failure should still leave prior rows in the aggregator."""
    extractor, aggregator, report = make_extractor()

    def failing_iter():
        yield make_row(command="SELECT 1")
        yield make_row(command="SELECT 2")
        raise RuntimeError("stream broke")

    schema_client = MagicMock()
    schema_client.stream_usage_history.return_value = failing_iter()

    extractor.extract(
        workspace_id=WORKSPACE_ID,
        item_id=ITEM_ID,
        item_display_name=ITEM_DISPLAY_NAME,
        schema_client=schema_client,
    )

    assert aggregator.add_observed_query.call_count == 2
    assert report.num_usage_queries_fetched == 2
    assert any(w.title == "Failed to Extract Usage" for w in report.warnings)


@pytest.mark.parametrize("command", ["", "   ", "\n\t"])
def test_handle_row_skips_empty_command(command: str) -> None:
    extractor, aggregator, report = make_extractor()
    extractor._handle_row(
        make_row(command=command), WORKSPACE_ID, ITEM_ID, ITEM_DISPLAY_NAME
    )

    aggregator.add_observed_query.assert_not_called()
    assert report.num_usage_queries_skipped.get("empty_command") == 1


def test_handle_row_skips_user_filtered_by_email_pattern() -> None:
    config = FabricUsageConfig(
        user_email_pattern=AllowDenyPattern(deny=["bot@example.com"]),
    )
    extractor, aggregator, report = make_extractor(config=config)

    extractor._handle_row(
        make_row(login_name="bot@example.com"),
        WORKSPACE_ID,
        ITEM_ID,
        ITEM_DISPLAY_NAME,
    )

    aggregator.add_observed_query.assert_not_called()
    assert report.num_usage_queries_skipped.get("user_filtered") == 1


def test_handle_row_lowercases_login_name_in_user_urn() -> None:
    extractor, aggregator, _ = make_extractor()
    extractor._handle_row(
        make_row(login_name="Alice@Example.COM"),
        WORKSPACE_ID,
        ITEM_ID,
        ITEM_DISPLAY_NAME,
    )

    aggregator.add_observed_query.assert_called_once()
    observed: ObservedQuery = aggregator.add_observed_query.call_args[0][0]
    assert observed.user is not None
    assert "alice@example.com" in str(observed.user)


def test_handle_row_no_login_name_emits_query_without_user() -> None:
    extractor, aggregator, _ = make_extractor()
    extractor._handle_row(
        make_row(login_name=None),
        WORKSPACE_ID,
        ITEM_ID,
        ITEM_DISPLAY_NAME,
    )

    aggregator.add_observed_query.assert_called_once()
    observed: ObservedQuery = aggregator.add_observed_query.call_args[0][0]
    assert observed.user is None


def test_handle_row_sets_default_db_to_workspace_item() -> None:
    """default_db must match the URN scheme `<workspace_id>.<item_id>` so
    sqlglot resolves `<schema>.<table>` references to the dataset URNs we emit.
    """
    extractor, aggregator, _ = make_extractor()
    extractor._handle_row(make_row(), WORKSPACE_ID, ITEM_ID, ITEM_DISPLAY_NAME)

    observed: ObservedQuery = aggregator.add_observed_query.call_args[0][0]
    assert observed.default_db == f"{WORKSPACE_ID}.{ITEM_ID}"
    assert observed.default_schema == "dbo"


def test_normalize_timestamp_naive_treated_as_utc() -> None:
    naive = datetime(2026, 5, 11, 10, 0, 0)
    result = FabricUsageExtractor._normalize_timestamp(naive)
    assert result.tzinfo is timezone.utc
    assert result == datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone.utc)


def test_normalize_timestamp_non_utc_converted_to_utc() -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    aware = datetime(2026, 5, 11, 15, 30, 0, tzinfo=ist)
    result = FabricUsageExtractor._normalize_timestamp(aware)
    assert result.tzinfo == timezone.utc
    # 15:30 IST == 10:00 UTC
    assert result == datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone.utc)


def test_normalize_timestamp_already_utc_returns_utc() -> None:
    aware = datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone.utc)
    assert FabricUsageExtractor._normalize_timestamp(aware) == aware
