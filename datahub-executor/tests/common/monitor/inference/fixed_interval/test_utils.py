import datetime
import unittest
from typing import List
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd


# Use unittest.mock to patch the config module and avoid circular imports
@patch(
    "datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_inference.config"
)
class TestPreprocessData(unittest.TestCase):
    """Tests for the preprocess_data function."""

    def test_preprocess_integer_timestamps(self, mock_config: MagicMock) -> None:
        """Test preprocessing with integer timestamps (milliseconds)."""
        # Import the function within the test to avoid circular imports
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        # Test data: three consecutive days at noon
        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672660800000,  # 2023-01-02 12:00:00
            1672747200000,  # 2023-01-03 12:00:00
        ]

        result = preprocess_data(timestamps, [])

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        # Check columns exist
        self.assertIn("ds", result.columns)
        self.assertIn("time_delta", result.columns)
        self.assertIn("date", result.columns)

        # Check time deltas (should be 24 hours = 1440 minutes)
        # First row's time_delta will be NaN, so we check the rest
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))
        self.assertAlmostEqual(result.iloc[1]["time_delta"], 1440.0)
        self.assertAlmostEqual(result.iloc[2]["time_delta"], 1440.0)

        # Check dates
        self.assertEqual(result.iloc[0]["date"], datetime.date(2023, 1, 1))
        self.assertEqual(result.iloc[1]["date"], datetime.date(2023, 1, 2))
        self.assertEqual(result.iloc[2]["date"], datetime.date(2023, 1, 3))

    def test_preprocess_datetime_timestamps(self, mock_config: MagicMock) -> None:
        """Test preprocessing with datetime objects."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        timestamps: List[datetime.datetime] = [
            datetime.datetime(2023, 1, 1, 12, 0),
            datetime.datetime(2023, 1, 2, 12, 0),
            datetime.datetime(2023, 1, 3, 12, 0),
        ]

        result = preprocess_data(timestamps, [])

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        # Check time deltas (should be 24 hours = 1440 minutes)
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))
        self.assertAlmostEqual(result.iloc[1]["time_delta"], 1440.0)
        self.assertAlmostEqual(result.iloc[2]["time_delta"], 1440.0)

    def test_preprocess_pandas_timestamps(self, mock_config: MagicMock) -> None:
        """Test preprocessing with pandas Timestamp objects."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        timestamps: List[pd.Timestamp] = [
            pd.Timestamp("2023-01-01 12:00:00"),
            pd.Timestamp("2023-01-02 12:00:00"),
            pd.Timestamp("2023-01-03 12:00:00"),
        ]

        result = preprocess_data(timestamps, [])

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        # Check time deltas
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))
        self.assertAlmostEqual(result.iloc[1]["time_delta"], 1440.0)
        self.assertAlmostEqual(result.iloc[2]["time_delta"], 1440.0)

    def test_preprocess_with_duplicates(self, mock_config: MagicMock) -> None:
        """Test preprocessing handles duplicates correctly."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672574400000,  # 2023-01-01 12:00:00 (duplicate)
            1672660800000,  # 2023-01-02 12:00:00
        ]

        result = preprocess_data(timestamps, [])

        # Should remove duplicates
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["date"], datetime.date(2023, 1, 1))
        self.assertEqual(result.iloc[1]["date"], datetime.date(2023, 1, 2))

    def test_preprocess_unsorted_timestamps(self, mock_config: MagicMock) -> None:
        """Test preprocessing sorts timestamps correctly."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        timestamps: List[int] = [
            1672747200000,  # 2023-01-03 12:00:00
            1672574400000,  # 2023-01-01 12:00:00
            1672660800000,  # 2023-01-02 12:00:00
        ]

        result = preprocess_data(timestamps, [])

        # Should sort by date
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]["date"], datetime.date(2023, 1, 1))
        self.assertEqual(result.iloc[1]["date"], datetime.date(2023, 1, 2))
        self.assertEqual(result.iloc[2]["date"], datetime.date(2023, 1, 3))

    def test_preprocess_with_known_anomalies(self, mock_config: MagicMock) -> None:
        """Test that intervals ending with known anomalies are filtered correctly."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        # Test data: timestamps at hourly intervals
        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672578000000,  # 2023-01-01 13:00:00
            1672581600000,  # 2023-01-01 14:00:00 - known anomaly
            1672585200000,  # 2023-01-01 15:00:00
            1672588800000,  # 2023-01-01 16:00:00
        ]

        # Mark one timestamp as an anomaly
        known_anomaly_timestamps = [1672581600000]  # 2023-01-01 14:00:00

        result = preprocess_data(timestamps, known_anomaly_timestamps)

        # Validate that:
        # 1. All timestamps are still in the dataframe
        self.assertEqual(len(result), 5)

        # 2. The time_delta for the anomalous timestamp is NaN
        self.assertTrue(np.isnan(result.iloc[2]["time_delta"]))

        # 4. Other time_deltas are not NaN
        self.assertTrue(
            np.isnan(result.iloc[0]["time_delta"])
        )  # First row is always NaN
        self.assertFalse(np.isnan(result.iloc[1]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[3]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[4]["time_delta"]))

    def test_preprocess_with_multiple_known_anomalies(
        self, mock_config: MagicMock
    ) -> None:
        """Test filtering with multiple known anomalies."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        # Test data: timestamps at 30-minute intervals
        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672576200000,  # 2023-01-01 12:30:00
            1672578000000,  # 2023-01-01 13:00:00 - known anomaly
            1672579800000,  # 2023-01-01 13:30:00
            1672581600000,  # 2023-01-01 14:00:00 - known anomaly
            1672583400000,  # 2023-01-01 14:30:00
        ]

        # Mark two timestamps as anomalies
        known_anomaly_timestamps = [1672578000000, 1672581600000]

        result = preprocess_data(timestamps, known_anomaly_timestamps)

        # Validate results
        self.assertEqual(len(result), 6)

        # Time deltas that should be NaN:
        # - First row (always NaN)
        # - Anomalous rows themselves (idx 2, 4)
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))  # First row
        self.assertTrue(np.isnan(result.iloc[2]["time_delta"]))  # Anomaly itself
        self.assertTrue(np.isnan(result.iloc[4]["time_delta"]))  # Anomaly itself

        # Other time deltas should be preserved
        self.assertFalse(np.isnan(result.iloc[1]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[3]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[5]["time_delta"]))

    def test_preprocess_with_consecutive_anomalies(
        self, mock_config: MagicMock
    ) -> None:
        """Test filtering with consecutive known anomalies."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        # Test data: timestamps at hourly intervals with consecutive anomalies
        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672578000000,  # 2023-01-01 13:00:00
            1672581600000,  # 2023-01-01 14:00:00 - known anomaly
            1672585200000,  # 2023-01-01 15:00:00 - known anomaly (consecutive)
            1672588800000,  # 2023-01-01 16:00:00
        ]

        known_anomaly_timestamps = [1672581600000, 1672585200000]

        result = preprocess_data(timestamps, known_anomaly_timestamps)

        # Validate that the right intervals are filtered
        self.assertEqual(len(result), 5)

        # Should have NaN time_deltas for:
        # - First row (always NaN)
        # - Row leading to first anomaly (idx 1)
        # - Both anomalous rows (idx 2, 3)
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))  # First row
        self.assertTrue(np.isnan(result.iloc[2]["time_delta"]))  # First anomaly
        self.assertTrue(np.isnan(result.iloc[3]["time_delta"]))  # Second anomaly

        # Other time deltas should be preserved
        self.assertFalse(np.isnan(result.iloc[1]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[4]["time_delta"]))

    def test_preprocess_with_empty_known_anomalies(
        self, mock_config: MagicMock
    ) -> None:
        """Test that function works correctly with empty known_anomaly_timestamps."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        # Test data: three consecutive days at noon
        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672660800000,  # 2023-01-02 12:00:00
            1672747200000,  # 2023-01-03 12:00:00
        ]

        # Empty list of known anomalies
        known_anomaly_timestamps: List[int] = []

        result = preprocess_data(timestamps, known_anomaly_timestamps)

        # Should behave like the original function
        self.assertEqual(len(result), 3)
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))
        self.assertAlmostEqual(result.iloc[1]["time_delta"], 1440.0)
        self.assertAlmostEqual(result.iloc[2]["time_delta"], 1440.0)

    def test_preprocess_with_non_matching_anomalies(
        self, mock_config: MagicMock
    ) -> None:
        """Test behavior when known anomalies don't match any timestamps."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            preprocess_data,
        )

        timestamps: List[int] = [
            1672574400000,  # 2023-01-01 12:00:00
            1672578000000,  # 2023-01-01 13:00:00
            1672581600000,  # 2023-01-01 14:00:00
        ]

        # Anomaly timestamp that doesn't match any in the list
        known_anomaly_timestamps = [1672592400000]  # 2023-01-01 17:00:00

        result = preprocess_data(timestamps, known_anomaly_timestamps)

        # Should not filter any intervals since the anomaly doesn't match
        self.assertEqual(len(result), 3)
        self.assertTrue(np.isnan(result.iloc[0]["time_delta"]))  # First row always NaN
        self.assertFalse(np.isnan(result.iloc[1]["time_delta"]))
        self.assertFalse(np.isnan(result.iloc[2]["time_delta"]))


@patch(
    "datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_inference.config"
)
class TestAnomalyDetection(unittest.TestCase):
    """Tests for anomaly detection functions."""

    def setUp(self) -> None:
        """Set up test data for anomaly detection."""
        # Normal data with one outlier
        self.test_data = np.array([10, 12, 11, 9, 13, 50, 11, 10, 12])

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    def test_detect_anomaly_z_score(
        self, mock_utils_config: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test z-score anomaly detection."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            detect_anomaly_z_score,
        )

        # Set config for the test
        mock_utils_config.DEVIATION_THRESHOLD = 2.0

        result = detect_anomaly_z_score(self.test_data)

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            self.assertEqual(len(result), len(self.test_data))
            # The outlier (50) should be flagged as anomaly
            self.assertTrue(result[5])
            # Others should be normal
            self.assertFalse(result[0])
            self.assertFalse(result[1])

            # Count anomalies - should be just one
            self.assertEqual(sum(result), 1)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    @patch("sklearn.neighbors.LocalOutlierFactor")
    def test_detect_anomaly_lof(
        self,
        mock_lof_class: MagicMock,
        mock_utils_config: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test Local Outlier Factor anomaly detection."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            detect_anomaly_lof,
        )

        # Set config for the test
        mock_utils_config.N_NEIGHBORS = 3
        mock_utils_config.LOF_CONTAMINATION = 0.1

        # Mock the LOF predictor to ensure consistent test results
        mock_lof = MagicMock()
        mock_lof.predict.return_value = [
            -1 if i == 5 else 1 for i in range(len(self.test_data))
        ]
        mock_lof_class.return_value = mock_lof

        result = detect_anomaly_lof(self.test_data)

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            self.assertEqual(len(result), len(self.test_data))
            # Check that the mocked outlier is flagged
            self.assertTrue(result[5])
            # Others should be normal
            self.assertFalse(result[0])

            # Count anomalies - should be just one
            self.assertEqual(sum(result), 1)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.detect_anomaly_z_score"
    )
    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.detect_anomaly_lof"
    )
    def test_predict_anomaly_auto(
        self,
        mock_lof: MagicMock,
        mock_z_score: MagicMock,
        mock_utils_config: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test the combined anomaly prediction with 'auto' method."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            predict_anomaly,
        )

        # Set config for the test
        mock_utils_config.DEVIATION_THRESHOLD = 2.0
        mock_utils_config.N_NEIGHBORS = 3
        mock_utils_config.LOF_CONTAMINATION = 0.1
        mock_utils_config.ANOMALY_CONFIDENCE_PENALTY_WEIGHT = 0.5
        mock_utils_config.MISSING_DAYS_PENALTY_THRESHOLD = 0.3

        # Both methods agree on the anomaly
        mock_z_score.return_value = [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
        ]
        mock_lof.return_value = [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
        ]

        # Case 1: Both methods agree, no missing days penalty
        result, penalty = predict_anomaly(
            self.test_data, "auto", missing_days_penalty=0.1
        )

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            self.assertEqual(len(result), len(self.test_data))
            self.assertTrue(result[5])
            self.assertEqual(sum(result), 1)

        # No disagreements, so penalty should be 0
        self.assertEqual(penalty, 0.0)

        # Case 2: Methods disagree on one point
        mock_z_score.return_value = [
            False,
            False,
            False,
            False,
            True,
            True,
            False,
            False,
            False,
        ]
        mock_lof.return_value = [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
        ]

        result, penalty = predict_anomaly(
            self.test_data, "auto", missing_days_penalty=0.1
        )

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            # Should only flag where both agree
            self.assertTrue(result[5])
            self.assertFalse(result[4])

        # Should have a non-zero penalty due to disagreement
        self.assertGreater(penalty, 0.0)

        # Case 3: With high missing days penalty, should use z_score only
        result, penalty = predict_anomaly(
            self.test_data, "auto", missing_days_penalty=0.5
        )

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            # Should use z_score results
            self.assertTrue(result[4])
            self.assertTrue(result[5])

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.detect_anomaly_z_score"
    )
    def test_predict_anomaly_specific_method(
        self,
        mock_z_score: MagicMock,
        mock_utils_config: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test the anomaly prediction with a specific method."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            predict_anomaly,
        )

        # Make sure to set the ANOMALY_CONFIDENCE_PENALTY_WEIGHT to a specific value
        # so the test doesn't use the mock object itself
        mock_utils_config.ANOMALY_CONFIDENCE_PENALTY_WEIGHT = 0.5

        mock_z_score.return_value = [
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
        ]

        result, penalty = predict_anomaly(self.test_data, "z_score")

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            self.assertEqual(len(result), len(self.test_data))
            self.assertTrue(result[5])

        # Should have zero penalty when using a specific method
        self.assertEqual(penalty, 0.0)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    def test_predict_anomaly_invalid_method(
        self, mock_utils_config: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test the anomaly prediction with an invalid method."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            predict_anomaly,
        )

        result, penalty = predict_anomaly(self.test_data, "invalid_method")

        # Should return None and negative penalty on error
        self.assertIsNone(result)
        self.assertEqual(penalty, -1.0)


@patch(
    "datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_inference.config"
)
class TestMostCommonFunction(unittest.TestCase):
    """Tests for the find_most_common function."""

    def test_find_most_common_single_mode(self, mock_config: MagicMock) -> None:
        """Test finding most common value with a single mode."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            find_most_common,
        )

        numbers: List[int] = [1, 2, 3, 2, 4, 2, 5]

        result = find_most_common(numbers)

        self.assertEqual(result, 2)

    def test_find_most_common_multiple_modes_maximum(
        self, mock_config: MagicMock
    ) -> None:
        """Test finding most common with multiple modes, using maximum preference."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            find_most_common,
        )

        numbers: List[int] = [1, 2, 3, 2, 3, 4, 5]

        result = find_most_common(numbers, preference="maximum")

        # Both 2 and 3 appear twice, should return the larger one (3)
        self.assertEqual(result, 3)

    def test_find_most_common_multiple_modes_most_recent(
        self, mock_config: MagicMock
    ) -> None:
        """Test finding most common with multiple modes, using most_recent preference."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            find_most_common,
        )

        numbers: List[int] = [1, 2, 3, 2, 4, 3, 5]

        result = find_most_common(numbers, preference="most_recent")

        # Both 2 and 3 appear twice, should return the most recent (3)
        self.assertEqual(result, 3)

    def test_find_most_common_empty_list(self, mock_config: MagicMock) -> None:
        """Test handling of empty list."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            find_most_common,
        )

        numbers: List[int] = []

        result = find_most_common(numbers)

        # Should handle empty list gracefully
        self.assertIsNone(result)

    def test_find_most_common_all_same(self, mock_config: MagicMock) -> None:
        """Test with all elements being the same."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            find_most_common,
        )

        numbers: List[int] = [7, 7, 7, 7]

        result = find_most_common(numbers)

        self.assertEqual(result, 7)


@patch(
    "datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_inference.config"
)
class TestPenaltyFunctions(unittest.TestCase):
    """Tests for various penalty calculation functions."""

    def test_get_missing_days_penalty_empty(self, mock_config: MagicMock) -> None:
        """Test missing days penalty with empty array."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_missing_days_penalty,
        )

        data: np.ndarray = np.array([])

        result = get_missing_days_penalty(data)

        # Should handle empty array gracefully
        self.assertEqual(result, 0)

    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.find_most_common"
    )
    def test_get_missing_days_penalty_uniform(
        self, mock_find_most_common: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test missing days penalty with uniform data."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_missing_days_penalty,
        )

        data: np.ndarray = np.array([5, 5, 5, 5, 5])
        mock_find_most_common.return_value = 5

        result = get_missing_days_penalty(data)

        # All values same, should have zero penalty
        self.assertEqual(result, 0)

    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.find_most_common"
    )
    def test_get_missing_days_penalty_varied(
        self, mock_find_most_common: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test missing days penalty with varied data."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_missing_days_penalty,
        )

        data: np.ndarray = np.array([5, 5, 5, 7, 8])
        mock_find_most_common.return_value = 5

        result = get_missing_days_penalty(data)

        # Some values different, should have non-zero penalty
        self.assertGreater(result, 0)
        # Penalty should be less than max_weight (default 1.0)
        self.assertLess(result, 1.0)

    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.find_most_common"
    )
    def test_get_missing_days_penalty_with_nans(
        self, mock_find_most_common: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test missing days penalty with NaN values."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_missing_days_penalty,
        )

        data: np.ndarray = np.array([5, 5, np.nan, 5, np.nan])
        mock_find_most_common.return_value = 5

        result = get_missing_days_penalty(data)

        # Should ignore NaNs
        self.assertEqual(result, 0)  # All non-NaN values are the same

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    def test_get_time_span_penalty_short_span(
        self, mock_utils_config: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test time span penalty with short time span."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_time_span_penalty,
        )

        # Test with 7 days (less than 14)
        result = get_time_span_penalty(7)

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            # Should have non-zero penalty
            self.assertGreater(result, 0)
            # Formula: max_weight * (14 - time_span) / 14
            expected = 0.4 * (14 - 7) / 14
            self.assertAlmostEqual(result, expected)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    def test_get_time_span_penalty_long_span(
        self, mock_utils_config: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test time span penalty with long time span."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_time_span_penalty,
        )

        # Test with 20 days (more than 14)
        result = get_time_span_penalty(20)

        self.assertIsNotNone(result)
        if result is not None:  # Add null check for mypy
            # Should have zero penalty
            self.assertEqual(result, 0)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    def test_get_cluster_penalties_empty_data(
        self, mock_utils_config: MagicMock, mock_config: MagicMock
    ) -> None:
        """Test cluster penalties with empty data."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_cluster_penalties,
        )

        data: np.ndarray = np.array([])
        recent_data: np.ndarray = np.array([10])
        max_normal: float = 10.0

        inter_cluster, cluster_index, new_max_normal = get_cluster_penalties(
            data, recent_data, max_normal
        )

        # Should handle empty data gracefully
        self.assertEqual(inter_cluster, 0.0)
        self.assertEqual(cluster_index, 0.0)
        self.assertEqual(new_max_normal, max_normal)

    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.config")
    @patch(
        "datahub_executor.common.monitor.inference.fixed_interval.utils.gaussian_kde"
    )
    @patch("datahub_executor.common.monitor.inference.fixed_interval.utils.find_peaks")
    def test_get_cluster_penalties_no_peaks(
        self,
        mock_find_peaks: MagicMock,
        mock_kde: MagicMock,
        mock_utils_config: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test cluster penalties with no peaks in KDE."""
        from datahub_executor.common.monitor.inference.fixed_interval.utils import (
            get_cluster_penalties,
        )

        data: np.ndarray = np.array([10, 11, 12, 13])
        recent_data: np.ndarray = np.array([10, 11, 12, 13])
        max_normal: float = 13.0

        # Mock KDE and find_peaks to return values that result in no peaks
        mock_kde.return_value = lambda x: np.ones_like(x)
        mock_find_peaks.return_value = (np.array([]), None)

        # Set INTER_CLUSTER_PENALTY_WEIGHT for testing
        mock_utils_config.INTER_CLUSTER_PENALTY_WEIGHT = 0.5

        inter_cluster, cluster_index, new_max_normal = get_cluster_penalties(
            data, recent_data, max_normal
        )

        # Should calculate penalty based on standard deviation
        expected_penalty = mock_utils_config.INTER_CLUSTER_PENALTY_WEIGHT * (
            np.std(data) / (max(data) - min(data))
        )
        self.assertAlmostEqual(inter_cluster, expected_penalty)
        self.assertEqual(cluster_index, 0.0)
        self.assertEqual(new_max_normal, max_normal)


if __name__ == "__main__":
    unittest.main()
