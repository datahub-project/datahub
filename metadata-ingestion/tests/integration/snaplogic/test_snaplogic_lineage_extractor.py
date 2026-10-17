import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snaplogic.snaplogic_config import SnaplogicConfig
from datahub.ingestion.source.snaplogic.snaplogic_lineage_extractor import (
    SnaplogicLineageExtractor,
)
from datahub.ingestion.source.snaplogic.snaplogic_parser import (
    ColumnMapping,
    Dataset,
    SnapLogicParser,
)


@pytest.fixture
def lineage_data():
    # Path to your local test_data folder
    test_resources_dir = Path(__file__).parent / "test_data"

    # Load the mock response from snaplogic_base_response.json
    with open(test_resources_dir / "snaplogic_simple_response.json", "r") as f:
        snaplogic_response = json.load(f)

    # Return it so tests can use it
    return snaplogic_response


def test_extract_columns_mapping_from_lineage(lineage_data):
    instance = SnapLogicParser([], {})

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result


def test_extract_columns_mapping_from_lineage_case_insensitive_namespace(lineage_data):
    instance = SnapLogicParser(
        ["sqlserver://snaplogic-test.database.windows.net:1433"], {}
    )

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result


def test_extract_columns_mapping_from_lineage_case_namespace_mapping(lineage_data):
    instance = SnapLogicParser(
        [], {"sqlserver://snaplogic-test.database.windows.net:1433": "prod-sqlserver"}
    )

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform_instance="prod-sqlserver",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform_instance="prod-sqlserver",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result


class _FakeSkipHandler:
    """Captures the (start, end) window passed to update_state."""

    def __init__(self):
        self.updated_start = None
        self.updated_end = None

    def suggest_run_time_window(self, start_time, end_time):
        return start_time, end_time

    def update_state(self, start_time, end_time):
        self.updated_start = start_time
        self.updated_end = end_time


def _make_extractor(skip_handler):
    config = SnaplogicConfig(
        username="user",
        password="pass",
        org_name="org",
        start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    return SnaplogicLineageExtractor(
        config=config,
        redundant_run_skip_handler=skip_handler,
        report=SourceReport(),
    )


def test_update_stats_commits_latest_received_event_time():
    # The checkpoint end should be the max eventTime actually received, not the
    # configured end_time, so lagging (not-yet-proccessed) runs are not skipped.
    extractor = _make_extractor(_FakeSkipHandler())
    extractor._track_event_time({"eventTime": "2025-01-01T10:00:00.000Z"})
    extractor._track_event_time({"eventTime": "2025-01-01T12:30:00.000Z"})
    extractor._track_event_time({"eventTime": "2025-01-01T09:00:00.000Z"})

    extractor.update_stats()

    assert extractor.redundant_run_skip_handler.updated_end == datetime(
        2025, 1, 1, 12, 30, tzinfo=timezone.utc
    )


def test_update_stats_holds_window_when_no_records_received():
    # With no records, the checkpoint end must stay at start_time so the window
    # does not advance.
    extractor = _make_extractor(_FakeSkipHandler())

    extractor.update_stats()

    assert (
        extractor.redundant_run_skip_handler.updated_end == extractor.config.start_time
    )


def test_track_event_time_ignores_unparseable_event_time():
    extractor = _make_extractor(_FakeSkipHandler())
    extractor._track_event_time({"eventTime": "not-a-timestamp"})
    extractor._track_event_time({})

    assert extractor.latest_event_time is None
