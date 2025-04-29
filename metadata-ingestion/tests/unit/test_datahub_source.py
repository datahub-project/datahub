from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple
from unittest.mock import Mock, call

import pytest

from datahub.ingestion.source.datahub.datahub_database_reader import (
    DATETIME_FORMAT,
    DataHubDatabaseReader,
    VersionOrderer,
)


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


# Create a simple mock class to test the method
class Config:
    def __init__(self, days_per_query: int):
        self.days_per_query = days_per_query


class DateBatchesTest:
    def __init__(self, config: Config):
        self.config = config

    def get_date_batches(
        self, dates: List[datetime]
    ) -> Iterable[Tuple[datetime, datetime]]:
        """
        Group consecutive dates into batches based on the days_per_query config.
        Returns a list of (start_date, end_date) tuples, where end_date is exclusive.
        Assumes dates list is sorted.
        """
        i = 0

        end_date: datetime = datetime.min

        while i < len(dates):
            end_idx = min(i + self.config.days_per_query, len(dates))
            if end_idx - 1 > i:  # Ensure we have at least two items to create a range
                start_date = dates[i]
                end_date = dates[end_idx - 1]
                yield (start_date, end_date)
            else:
                # If there's only one date left, yield it with itself
                yield (end_date, dates[i])
            i += self.config.days_per_query


def test_dataset_batch_sliding_window():
    """Test using a pytest fixture to provide sorted test data"""
    config = Config(days_per_query=2)
    test_instance = DateBatchesTest(config)

    sorted_sample_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 2),
        datetime(2023, 1, 3),
        datetime(2023, 1, 4),
        datetime(2023, 1, 5),
    ]

    result = list(test_instance.get_date_batches(sorted_sample_dates))

    assert len(result) == 3
    assert result[0] == (sorted_sample_dates[0], sorted_sample_dates[1])
    assert result[1] == (sorted_sample_dates[2], sorted_sample_dates[3])
    assert result[2] == (sorted_sample_dates[3], sorted_sample_dates[4])


def test_dataset_batch_sliding_window_with_only_two_item():
    """Test using a pytest fixture to provide sorted test data"""
    config = Config(days_per_query=2)
    test_instance = DateBatchesTest(config)

    sorted_sample_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 2),
    ]

    result = list(test_instance.get_date_batches(sorted_sample_dates))

    assert len(result) == 1
    assert result[0] == (sorted_sample_dates[0], sorted_sample_dates[1])


@pytest.fixture
def mock_data_fetcher():
    """Create a mock data fetcher with necessary attributes and methods"""
    data_fetcher = Mock(spec=DataHubDatabaseReader)
    data_fetcher.available_dates_cache = None
    data_fetcher.config = Mock()
    data_fetcher.config.exclude_aspects = None

    # Attach the actual method to the mock
    data_fetcher.get_available_dates = (
        DataHubDatabaseReader.get_available_dates.__get__(data_fetcher)
    )

    # Setup mock for execute_with_params method
    data_fetcher.execute_with_params = Mock()

    # Setup mock for get_available_dates_query method
    data_fetcher.get_available_dates_query = Mock(return_value="mock query")

    return data_fetcher


def test_returns_cache_if_available(mock_data_fetcher):
    """Test that the method returns cached dates if available"""
    # Setup
    mock_data_fetcher.available_dates_cache = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 2),
    ]
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert
    assert result == mock_data_fetcher.available_dates_cache
    mock_data_fetcher.execute_with_params.assert_not_called()


def test_handles_datetime_results(mock_data_fetcher):
    """Test handling of datetime objects in query results"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_result = [
        {"created_date": datetime(2023, 1, 2)},
        {"created_date": datetime(2023, 1, 5)},
        {"created_date": datetime(2023, 1, 8)},
    ]
    mock_data_fetcher.execute_with_params.return_value = mock_result

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert
    expected = [
        datetime(2023, 1, 2),
        datetime(2023, 1, 5),
        datetime(2023, 1, 8),
        stop_date,
    ]
    assert result == expected
    assert mock_data_fetcher.available_dates_cache == expected


def test_handles_string_results(mock_data_fetcher):
    """Test handling of string dates in query results"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_result = [
        {"created_date": "2023-01-02"},
        {"created_date": "2023-01-05"},
        {"created_date": "2023-01-08"},
    ]
    mock_data_fetcher.execute_with_params.return_value = mock_result

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert
    expected = [
        datetime(2023, 1, 2),
        datetime(2023, 1, 5),
        datetime(2023, 1, 8),
        stop_date,
    ]
    assert result == expected
    assert mock_data_fetcher.available_dates_cache == expected


def test_respects_from_date_boundary(mock_data_fetcher):
    """Test that dates earlier than from_date are replaced with from_date"""
    # Setup
    from_date = datetime(2023, 1, 5, 10, 12)
    stop_date = datetime(2023, 1, 10)

    mock_result = [
        {"created_date": datetime(2023, 1, 5)},  # Earlier than from_date
        {"created_date": datetime(2023, 1, 8)},
    ]
    mock_data_fetcher.execute_with_params.return_value = mock_result

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert - should include from_date instead of 2023-01-02
    expected = [from_date, datetime(2023, 1, 8), stop_date]
    assert result == expected


def test_empty_result_handling(mock_data_fetcher):
    """Test handling of empty query results"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_data_fetcher.execute_with_params.return_value = []

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert - should return empty list
    assert result == []
    assert mock_data_fetcher.available_dates_cache == []  # Cache should be empty


def test_exception_handling(mock_data_fetcher):
    """Test handling of exceptions during query execution"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_data_fetcher.execute_with_params.side_effect = Exception("Database error")

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert - should return empty list on error
    assert result == []
    assert (
        mock_data_fetcher.available_dates_cache is None
    )  # Cache should not be updated


def test_with_exclude_aspects(mock_data_fetcher):
    """Test that exclude_aspects configuration is included in params when available"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_data_fetcher.config.exclude_aspects = ["aspect1", "aspect2"]
    mock_data_fetcher.execute_with_params.return_value = [
        {"created_date": datetime(2023, 1, 5)}
    ]

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert
    expected_params = {
        "since_createdon": from_date.strftime(DATETIME_FORMAT),
        "end_createdon": stop_date.strftime(DATETIME_FORMAT),
        "exclude_aspects": tuple(mock_data_fetcher.config.exclude_aspects),
    }

    mock_data_fetcher.execute_with_params.assert_called_once_with(
        "mock query", expected_params
    )
    expected_dates = [datetime(2023, 1, 5), stop_date]
    assert result == expected_dates


def test_without_exclude_aspects(mock_data_fetcher):
    """Test params when exclude_aspects is not set"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    # Remove exclude_aspects attribute
    del mock_data_fetcher.config.exclude_aspects
    mock_data_fetcher.execute_with_params.return_value = [
        {"created_date": datetime(2023, 1, 5)}
    ]

    # Execute
    mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert
    expected_params = {
        "since_createdon": from_date.strftime(DATETIME_FORMAT),
        "end_createdon": stop_date.strftime(DATETIME_FORMAT),
    }

    mock_data_fetcher.execute_with_params.assert_called_once_with(
        "mock query", expected_params
    )


def test_missing_created_date_in_results(mock_data_fetcher):
    """Test handling of results without created_date field"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_result = [
        {"some_other_field": "value"},
        {"created_date": datetime(2023, 1, 5)},
        {"created_date": None},
    ]
    mock_data_fetcher.execute_with_params.return_value = mock_result

    # Execute
    result = mock_data_fetcher.get_available_dates(from_date, stop_date)

    # Assert - should only include valid dates
    expected = [datetime(2023, 1, 5), stop_date]
    assert result == expected


@pytest.fixture
def mock_get_rows_fetcher():
    """Create a mock data fetcher with necessary attributes and methods"""
    data_fetcher = Mock(spec=DataHubDatabaseReader)
    data_fetcher.config = Mock()
    data_fetcher.config.days_per_query = 2

    # Attach the actual method to the mock
    data_fetcher._get_rows = DataHubDatabaseReader._get_rows.__get__(data_fetcher)

    # Setup mock for dependencies
    data_fetcher.get_available_dates = Mock()
    data_fetcher.get_date_batches = Mock()
    data_fetcher._get_rows_for_date_range = Mock()

    return data_fetcher


def test_no_available_dates(mock_get_rows_fetcher):
    """Test behavior when no available dates are found"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    mock_get_rows_fetcher.get_available_dates.return_value = []

    # Execute
    result = list(mock_get_rows_fetcher._get_rows(from_date, stop_date))

    # Assert
    assert result == []
    mock_get_rows_fetcher.get_available_dates.assert_called_once_with(
        from_date, stop_date
    )
    mock_get_rows_fetcher.get_date_batches.assert_not_called()
    mock_get_rows_fetcher._get_rows_for_date_range.assert_not_called()


def test_structured_properties_filter_enabled(mock_get_rows_fetcher):
    """Test behavior when set_structured_properties_filter is True"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    available_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 5),
        datetime(2023, 1, 10),
    ]
    mock_get_rows_fetcher.get_available_dates.return_value = available_dates

    # When set_structured_properties_filter is True, we expect one batch covering the entire range
    mock_rows = [{"id": 1}, {"id": 2}, {"id": 3}]
    mock_get_rows_fetcher._get_rows_for_date_range.return_value = mock_rows

    # Execute
    result = list(
        mock_get_rows_fetcher._get_rows(
            from_date, stop_date, set_structured_properties_filter=True
        )
    )

    # Assert
    assert result == mock_rows
    mock_get_rows_fetcher.get_available_dates.assert_called_once_with(
        from_date, stop_date
    )
    mock_get_rows_fetcher.get_date_batches.assert_not_called()
    mock_get_rows_fetcher._get_rows_for_date_range.assert_called_once_with(
        from_date, stop_date, True
    )


def test_normal_batch_processing(mock_get_rows_fetcher):
    """Test normal batch processing with multiple date batches"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    available_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 3),
        datetime(2023, 1, 5),
        datetime(2023, 1, 7),
        datetime(2023, 1, 10),
    ]
    mock_get_rows_fetcher.get_available_dates.return_value = available_dates

    # Define batches for testing
    batches = [
        (datetime(2023, 1, 1), datetime(2023, 1, 5)),
        (datetime(2023, 1, 5), datetime(2023, 1, 10)),
    ]
    mock_get_rows_fetcher.get_date_batches.return_value = batches

    # Define return values for each batch
    batch_1_rows = [{"id": 1}, {"id": 2}]
    batch_2_rows = [{"id": 3}, {"id": 4}]
    mock_get_rows_fetcher._get_rows_for_date_range.side_effect = [
        batch_1_rows,
        batch_2_rows,
    ]

    # Execute
    result = list(mock_get_rows_fetcher._get_rows(from_date, stop_date))

    # Assert
    assert result == batch_1_rows + batch_2_rows
    mock_get_rows_fetcher.get_available_dates.assert_called_once_with(
        from_date, stop_date
    )
    mock_get_rows_fetcher.get_date_batches.assert_called_once_with(available_dates)

    # Check that _get_rows_for_date_range was called for each batch with correct parameters
    assert mock_get_rows_fetcher._get_rows_for_date_range.call_count == 2
    mock_get_rows_fetcher._get_rows_for_date_range.assert_has_calls(
        [
            call(batches[0][0], batches[0][1], False),
            call(batches[1][0], batches[1][1], False),
        ]
    )


def test_empty_batches(mock_get_rows_fetcher):
    """Test behavior when batches don't return any rows"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 10)

    available_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 5),
        datetime(2023, 1, 10),
    ]
    mock_get_rows_fetcher.get_available_dates.return_value = available_dates

    batches = [(datetime(2023, 1, 1), datetime(2023, 1, 10))]
    mock_get_rows_fetcher.get_date_batches.return_value = batches

    # Return empty list for the batch
    mock_get_rows_fetcher._get_rows_for_date_range.return_value = []

    # Execute
    result = list(mock_get_rows_fetcher._get_rows(from_date, stop_date))

    # Assert
    assert result == []
    mock_get_rows_fetcher.get_available_dates.assert_called_once_with(
        from_date, stop_date
    )
    mock_get_rows_fetcher.get_date_batches.assert_called_once_with(available_dates)
    mock_get_rows_fetcher._get_rows_for_date_range.assert_called_once_with(
        batches[0][0], batches[0][1], False
    )


def test_multiple_batches_with_empty_results(mock_get_rows_fetcher):
    """Test handling of multiple batches where some return empty results"""
    # Setup
    from_date = datetime(2023, 1, 1)
    stop_date = datetime(2023, 1, 20)

    available_dates = [
        datetime(2023, 1, 1),
        datetime(2023, 1, 5),
        datetime(2023, 1, 10),
        datetime(2023, 1, 15),
        datetime(2023, 1, 20),
    ]
    mock_get_rows_fetcher.get_available_dates.return_value = available_dates

    batches = [
        (datetime(2023, 1, 1), datetime(2023, 1, 7)),
        (datetime(2023, 1, 7), datetime(2023, 1, 14)),
        (datetime(2023, 1, 14), datetime(2023, 1, 20)),
    ]
    mock_get_rows_fetcher.get_date_batches.return_value = batches

    # First batch has data, second is empty, third has data
    batch_1_rows = [{"id": 1}, {"id": 2}]
    batch_2_rows: List[Dict] = []
    batch_3_rows = [{"id": 5}, {"id": 6}]

    mock_get_rows_fetcher._get_rows_for_date_range.side_effect = [
        batch_1_rows,
        batch_2_rows,
        batch_3_rows,
    ]

    # Execute
    result = list(mock_get_rows_fetcher._get_rows(from_date, stop_date))

    # Assert
    assert result == batch_1_rows + batch_3_rows
    mock_get_rows_fetcher.get_available_dates.assert_called_once_with(
        from_date, stop_date
    )
    mock_get_rows_fetcher.get_date_batches.assert_called_once_with(available_dates)

    # Check that _get_rows_for_date_range was called for each batch
    assert mock_get_rows_fetcher._get_rows_for_date_range.call_count == 3
    mock_get_rows_fetcher._get_rows_for_date_range.assert_has_calls(
        [
            call(batches[0][0], batches[0][1], False),
            call(batches[1][0], batches[1][1], False),
            call(batches[2][0], batches[2][1], False),
        ]
    )
