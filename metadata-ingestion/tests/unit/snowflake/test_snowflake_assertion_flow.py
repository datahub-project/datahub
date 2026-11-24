"""SnowflakeAssertionsHandler: result rows to assertion run events."""

from datetime import datetime, timezone
from typing import List

from datahub.ingestion.source.snowflake.snowflake_assertion import (
    DataQualityMonitoringResult,
    SnowflakeAssertionsHandler,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
)
from tests.unit.snowflake.conftest import (
    as_mock,
    fake_connection,
    log_keys,
    make_v2_config,
)

MEASURED = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)


def _row(metric: str, table: str = "ORDERS", value: int = 1) -> dict:
    return {
        "MEASUREMENT_TIME": MEASURED,
        "METRIC_NAME": metric,
        "TABLE_NAME": table,
        "TABLE_SCHEMA": "PUBLIC",
        "TABLE_DATABASE": "DB",
        "VALUE": value,
        "REFERENCE_ID": "ref-1",
        "ARGUMENT_NAMES": '["amount"]',
    }


def _make_handler(
    rows: List[dict], **config_overrides: object
) -> SnowflakeAssertionsHandler:
    config = make_v2_config(**config_overrides)
    report = SnowflakeV2Report()
    return SnowflakeAssertionsHandler(
        config,
        report,
        fake_connection(lambda _sql: rows),
        SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
    )


def test_results_for_discovered_datasets_become_run_events() -> None:
    """Result rows for discovered tables become run events keyed by the DMF's
    assertion URN (VALUE 1 is SUCCESS, 0 is FAILURE); rows for undiscovered
    tables are dropped."""
    rows = [
        _row("datahub__abc", value=1),
        _row("datahub__abc", value=0),
        _row("null_check", table="UNDISCOVERED"),
    ]
    handler = _make_handler(
        rows, include_assertion_results=True, include_externally_managed_dmfs=True
    )

    workunits = list(handler.get_assertion_workunits(["db.public.orders"]))

    query = as_mock(handler.connection).query.call_args.args[0]
    assert "ilike" not in query  # external DMFs included
    events = [
        wu.get_aspect_of_type(AssertionRunEventClass)
        for wu in workunits
        if wu.get_aspect_of_type(AssertionRunEventClass)
    ]
    assert [e.result.type for e in events if e and e.result] == [
        AssertionResultTypeClass.SUCCESS,
        AssertionResultTypeClass.FAILURE,
    ]
    assert all(e.assertionUrn == "urn:li:assertion:abc" for e in events if e)
    # DataHub-managed DMFs carry no AssertionInfo (DataHub already owns it) and
    # the platform-instance aspect is emitted once per assertion, not per run.
    assert not any(wu.get_aspect_of_type(AssertionInfoClass) for wu in workunits)
    assert handler._urns_processed == ["urn:li:assertion:abc"]


def test_malformed_result_row_is_reported_and_skipped() -> None:
    handler = _make_handler([{"METRIC_NAME": "datahub__abc"}])

    assert list(handler.get_assertion_workunits(["db.public.orders"])) == []
    assert "Failed to parse assertion result" in log_keys(handler.report.warnings)


def test_argument_names_that_are_not_a_json_list_are_dropped() -> None:
    assert (
        DataQualityMonitoringResult.model_validate(
            {**_row("m"), "ARGUMENT_NAMES": "not json"}
        ).ARGUMENT_NAMES
        == []
    )
    assert (
        DataQualityMonitoringResult.model_validate(
            {**_row("m"), "ARGUMENT_NAMES": '{"a": 1}'}
        ).ARGUMENT_NAMES
        == []
    )
