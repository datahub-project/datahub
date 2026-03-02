from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.datahub_database_reader import (
    DATETIME_FORMAT,
    DataHubDatabaseReader,
    VersionOrderer,
)
from datahub.ingestion.source.datahub.datahub_source import DataHubSource


@pytest.fixture
def rows():
    return [
        {"createdon": 0, "version": 0, "urn": "one"},
        {"createdon": 0, "version": 1, "urn": "one"},
        {"createdon": 0, "version": 0, "urn": "two"},
        {"createdon": 0, "version": 0, "urn": "three"},
        {"createdon": 0, "version": 1, "urn": "three"},
        {"createdon": 0, "version": 2, "urn": "three"},
        {"createdon": 0, "version": 1, "urn": "two"},
        {"createdon": 0, "version": 4, "urn": "three"},
        {"createdon": 0, "version": 5, "urn": "three"},
        {"createdon": 1, "version": 6, "urn": "three"},
        {"createdon": 1, "version": 0, "urn": "four"},
        {"createdon": 2, "version": 0, "urn": "five"},
        {"createdon": 2, "version": 1, "urn": "six"},
        {"createdon": 2, "version": 0, "urn": "six"},
        {"createdon": 3, "version": 0, "urn": "seven"},
        {"createdon": 3, "version": 0, "urn": "eight"},
    ]


def test_version_orderer(rows):
    orderer = VersionOrderer[Dict[str, Any]](enabled=True)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == sorted(
        ordered_rows, key=lambda x: (x["createdon"], x["version"] == 0)
    )


def test_version_orderer_disabled(rows):
    orderer = VersionOrderer[Dict[str, Any]](enabled=False)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == rows


@pytest.fixture
def mock_reader():
    with patch(
        "datahub.ingestion.source.datahub.datahub_database_reader.create_engine"
    ) as mock_create_engine:
        config = MagicMock()
        connection_config = MagicMock()
        report = MagicMock()
        mock_engine = MagicMock()
        mock_dialect = MagicMock()
        mock_identifier_preparer = MagicMock()
        mock_dialect.identifier_preparer = mock_identifier_preparer
        mock_identifier_preparer.quote = lambda x: f'"{x}"'
        mock_engine.dialect = mock_dialect
        mock_create_engine.return_value = mock_engine
        reader = DataHubDatabaseReader(config, connection_config, report)
        reader.query = MagicMock(side_effect=reader.query)  # type: ignore
        reader.execute_server_cursor = MagicMock()  # type: ignore
        return reader


def test_get_rows_for_date_range_no_rows(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    mock_reader.execute_server_cursor.return_value = []

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 50))

    # Assert
    assert len(result) == 0
    mock_reader.query.assert_called_once_with(False)
    mock_reader.execute_server_cursor.assert_called_once()


def test_get_rows_for_date_range_with_rows(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    mock_rows = [
        {"urn": "urn1", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn2", "metadata": "data2", "createdon": datetime(2023, 1, 1, 13, 0)},
    ]
    mock_reader.execute_server_cursor.return_value = mock_rows

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 50))

    # Assert
    assert result == mock_rows
    mock_reader.query.assert_called_once_with(False)
    assert mock_reader.execute_server_cursor.call_count == 1


def test_get_rows_for_date_range_pagination_same_timestamp(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    batch1 = [
        {"urn": "urn1", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn2", "metadata": "data2", "createdon": datetime(2023, 1, 1, 12, 0)},
    ]
    batch2 = [
        {"urn": "urn3", "metadata": "data3", "createdon": datetime(2023, 1, 1, 12, 0)},
    ]
    batch3: List[Dict] = []

    mock_reader.execute_server_cursor.side_effect = [batch1, batch2, batch3]

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 2))

    # Assert
    assert len(result) == 3
    assert result[0]["urn"] == "urn1"
    assert result[1]["urn"] == "urn2"
    assert result[2]["urn"] == "urn3"
    assert mock_reader.execute_server_cursor.call_count == 2


def test_get_rows_for_date_range_pagination_different_timestamp(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    batch1 = [
        {"urn": "urn1", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn2", "metadata": "data2", "createdon": datetime(2023, 1, 1, 13, 0)},
    ]
    batch2 = [
        {"urn": "urn3", "metadata": "data3", "createdon": datetime(2023, 1, 1, 14, 0)},
    ]
    batch3: List[Dict] = []

    mock_reader.execute_server_cursor.side_effect = [batch1, batch2, batch3]

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 2))

    # Assert
    assert len(result) == 3
    assert result[0]["urn"] == "urn1"
    assert result[1]["urn"] == "urn2"
    assert result[2]["urn"] == "urn3"
    assert mock_reader.execute_server_cursor.call_count == 2


def test_get_rows_for_date_range_duplicate_data_handling(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    batch1 = [
        {"urn": "urn1", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
    ]
    batch2 = [
        {"urn": "urn2", "metadata": "data2", "createdon": datetime(2023, 1, 1, 13, 0)},
    ]
    batch3: List[Dict] = []

    mock_reader.execute_server_cursor.side_effect = [batch1, batch2, batch3]

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 1))

    # Assert
    assert len(result) == 2
    assert result[0]["urn"] == "urn1"
    assert result[1]["urn"] == "urn2"

    # Check call parameters for each iteration
    calls = mock_reader.execute_server_cursor.call_args_list
    assert len(calls) == 3

    # First call: initial parameters
    first_call_params = calls[0][0][1]
    assert first_call_params["since_createdon"] == start_date.strftime(DATETIME_FORMAT)
    assert first_call_params["end_createdon"] == end_date.strftime(DATETIME_FORMAT)
    assert first_call_params["limit"] == 1
    assert first_call_params["offset"] == 0

    # Second call: duplicate detected, same createdon so offset increased
    second_call_params = calls[1][0][1]
    assert second_call_params["offset"] == 1
    assert second_call_params["since_createdon"] == datetime(
        2023, 1, 1, 12, 0
    ).strftime(DATETIME_FORMAT)

    # Third call: successful fetch after duplicate with new timestamp
    third_call_params = calls[2][0][1]
    # After a duplicate with no last_createdon, offset should increase
    assert third_call_params["offset"] == 0


def test_get_rows_multiple_paging(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    batch1 = [
        {"urn": "urn1", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn2", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn3", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
    ]
    batch2 = [
        {"urn": "urn4", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn5", "metadata": "data1", "createdon": datetime(2023, 1, 1, 12, 0)},
        {"urn": "urn6", "metadata": "data1", "createdon": datetime(2023, 1, 1, 13, 0)},
    ]
    batch3 = [
        {"urn": "urn7", "metadata": "data1", "createdon": datetime(2023, 1, 1, 14, 0)},
        {"urn": "urn8", "metadata": "data1", "createdon": datetime(2023, 1, 1, 14, 0)},
        {"urn": "urn9", "metadata": "data1", "createdon": datetime(2023, 1, 1, 15, 0)},
    ]
    batch4 = [
        {"urn": "urn10", "metadata": "data1", "createdon": datetime(2023, 1, 1, 16, 0)},
    ]

    mock_reader.execute_server_cursor.side_effect = [batch1, batch2, batch3, batch4]

    # Execute
    result = list(mock_reader._get_rows(start_date, end_date, False, 3))

    # Assert
    # In this case duplicate items are expected
    assert len(result) == 10
    assert result[0]["urn"] == "urn1"
    assert result[1]["urn"] == "urn2"
    assert result[2]["urn"] == "urn3"
    assert result[3]["urn"] == "urn4"
    assert result[4]["urn"] == "urn5"
    assert result[5]["urn"] == "urn6"
    assert result[6]["urn"] == "urn7"
    assert result[7]["urn"] == "urn8"
    assert result[8]["urn"] == "urn9"
    assert result[9]["urn"] == "urn10"

    # Check call parameters for each iteration
    calls = mock_reader.execute_server_cursor.call_args_list
    assert len(calls) == 4

    # First call: initial parameters
    first_call_params = calls[0][0][1]
    assert first_call_params["since_createdon"] == start_date.strftime(DATETIME_FORMAT)
    assert first_call_params["end_createdon"] == end_date.strftime(DATETIME_FORMAT)
    assert first_call_params["limit"] == 3
    assert first_call_params["offset"] == 0

    # Second call: duplicate detected, same createdon so offset increased
    second_call_params = calls[1][0][1]
    assert second_call_params["offset"] == 3
    assert second_call_params["limit"] == 3
    assert second_call_params["since_createdon"] == datetime(
        2023, 1, 1, 12, 0
    ).strftime(DATETIME_FORMAT)
    assert first_call_params["end_createdon"] == end_date.strftime(DATETIME_FORMAT)

    # Third call: successful fetch after duplicate with new timestamp
    third_call_params = calls[2][0][1]
    # After a duplicate with no last_createdon, offset should increase
    assert third_call_params["offset"] == 0
    assert third_call_params["since_createdon"] == datetime(2023, 1, 1, 13, 0).strftime(
        DATETIME_FORMAT
    )

    # Third call: successful fetch after duplicate with new timestamp
    fourth_call_params = calls[3][0][1]
    # After a duplicate with no last_createdon, offset should increase
    assert fourth_call_params["offset"] == 0
    assert fourth_call_params["since_createdon"] == datetime(
        2023, 1, 1, 15, 0
    ).strftime(DATETIME_FORMAT)
    assert fourth_call_params["limit"] == 3


def test_get_rows_for_date_range_exception_handling(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    mock_reader.execute_server_cursor.side_effect = Exception("Test exception")

    # Execute and Assert
    with pytest.raises(Exception, match="Test exception"):
        list(mock_reader._get_rows(start_date, end_date, False, 50))


def test_get_rows_for_date_range_exclude_aspects(mock_reader):
    # Setup
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 2)
    mock_reader.config.exclude_aspects = ["aspect1", "aspect2"]
    mock_reader.execute_server_cursor.return_value = []

    # Execute
    list(mock_reader._get_rows(start_date, end_date, False, 50))

    # Assert
    called_params = mock_reader.execute_server_cursor.call_args[0][1]
    assert "exclude_aspects" in called_params
    assert isinstance(called_params["exclude_aspects"], tuple)
    assert called_params["exclude_aspects"] == ("aspect1", "aspect2")


def test_datahub_source_urn_pattern_warning_when_customized():
    """Test that warning is emitted when user customizes urn_pattern."""
    config_dict = {
        "pull_from_datahub_api": True,
        "urn_pattern": {
            "allow": ["urn:li:dataset:.*"],
            "deny": ["urn:li:chart:.*"],
        },
    }
    config = DataHubSourceConfig.model_validate(config_dict)

    ctx = PipelineContext(run_id="test-run", pipeline_name="test-pipeline")
    ctx.graph = MagicMock()

    source = DataHubSource(config, ctx)

    assert len(source.report.warnings) > 0
    warning_found = any(
        "urn_pattern_override" in str(w) for w in source.report.warnings
    )
    assert warning_found, "Expected urn_pattern_override warning not found"


def test_datahub_source_no_warning_with_default_urn_pattern():
    """Test that no warning is emitted when using default urn_pattern."""
    config_dict = {
        "pull_from_datahub_api": True,
    }
    config = DataHubSourceConfig.model_validate(config_dict)

    ctx = PipelineContext(run_id="test-run", pipeline_name="test-pipeline")
    ctx.graph = MagicMock()

    source = DataHubSource(config, ctx)

    warning_found = any(
        "urn_pattern_override" in str(w) for w in source.report.warnings
    )
    assert not warning_found, "Unexpected urn_pattern_override warning found"
