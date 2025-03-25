import logging
import os
import pickle
from typing import Any, List, Tuple

import numpy as np
import pytest

CURRENT_WDR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(CURRENT_WDR, "test_input")
INPUT_DATASET_PATH = os.path.join(INPUT_DIR, "test_data.pkl")

EXPECTED_OUTCOMES_PATH = os.path.join(
    INPUT_DIR, "prophet_metric_value_forecasting_expected_outputs.pkl"
)
NUM_PREDICTIONS = 7
ACCEPTED_CHANGE_RATIO = 0.15
np.random.seed(100)

logger = logging.getLogger(__name__)


def get_test_datasets() -> List[Tuple[str, list, list]]:
    with open(INPUT_DATASET_PATH, "rb") as fp:
        dataset = pickle.load(fp)
        fp.close()
    return dataset


@pytest.fixture(scope="module")
def fixture_load_expected_outcomes() -> Any:
    with open(EXPECTED_OUTCOMES_PATH, "rb") as file_:
        return pickle.load(file_)


############################
# Start unit testing #
############################
# Unit Test for Metric Change Assertion #


# # TODO: This was copied from datahub-inference-engine. This is a temporary unit test.
# # These tests should be updated with more readable unit test, currently this is ONLY used for regressive testing.
# @pytest.mark.parametrize(
#     "dataset_name, ts, metric_values",
#     [(a, b, c) for a, b, c in get_test_datasets()],
# )
# def test_metric_change_public_datasets(
#     fixture_load_expected_outcomes: Any,
#     dataset_name: Any,
#     ts: Any,
#     metric_values: Any,
# ) -> None:
#     start_time = (
#         pd.to_datetime(max(ts), unit="ms") + pd.Timedelta(days=1)
#     ).timestamp() * 1000

#     forecaster = MetricForecaster(
#         timestamps=ts,
#         metric_values=metric_values,
#         start_time=start_time,
#         prediction_interval_size=timedelta(hours=1),
#         num_intervals=7,
#         config=MetricProjectorConfig(
#             TUNE_HYPERPARAMETERS=True,
#             REMOVE_ANOMALIES=True,
#             USE_PARALLELIZATION=False,
#         ),
#     )
#     buckets, metric_forecast_info = forecaster.forecast_metric()
#     predicted_interval = forecaster.data_interval
#     data_standard_deviation = metric_forecast_info.data_standard_deviation
#     if data_standard_deviation is not None:
#         data_standard_deviation = np.round(data_standard_deviation, 3)
#     predicted_min_values = [bucket.min_value for bucket in buckets]
#     predicted_max_values = [bucket.max_value for bucket in buckets]
#     predicted_min_values_with_buffer = [
#         bucket.min_value_with_buffer for bucket in buckets
#     ]
#     predicted_max_values_with_buffer = [
#         bucket.max_value_with_buffer for bucket in buckets
#     ]
#     predicted_buckets_end_times = [bucket.bucket_end_time for bucket in buckets]
#     predicted_buckets_start_times = [bucket.bucket_start_time for bucket in buckets]

#     expected_results = fixture_load_expected_outcomes[dataset_name]
#     expected_interval = expected_results["data_interval"]
#     expected_min_values = expected_results["min_values"]
#     expected_max_values = expected_results["max_values"]
#     expected_min_values_with_buffer = expected_results["min_values_with_buffer"]
#     expected_max_values_with_buffer = expected_results["max_values_with_buffer"]
#     expected_buckets_end_times = expected_results["buckets_end_times"]
#     expected_buckets_start_times = expected_results["buckets_start_times"]
#     expected_data_standard_deviation = expected_results["data_standard_deviation"]

#     if expected_data_standard_deviation is not None:
#         expected_data_standard_deviation = float(
#             np.round(expected_data_standard_deviation, 3)
#         )

#     # Assertion on predicted interval:
#     assert predicted_interval == expected_interval, (
#         f"Data predicted interval equality test failed for : '{dataset_name}'"
#     )

#     # Assertion on data_standard_deviation:
#     assert data_standard_deviation == expected_data_standard_deviation, (
#         f"data standard deviation equality test failed for : '{dataset_name}'"
#     )

#     # Assertion on best parameters:
#     for min_pred, min_exp in zip(
#         predicted_min_values, expected_min_values, strict=False
#     ):
#         if min_pred is not None and min_exp is not None:
#             assert (min_pred == 0 and min_exp == 0) or abs(min_pred - min_exp) / abs(
#                 min_exp
#             ) <= ACCEPTED_CHANGE_RATIO, (
#                 f"Buckets min_values equality test failed for : '{dataset_name}'"
#             )
#         else:
#             assert min_pred is None and min_exp is None, (
#                 f"Buckets min_values equality test failed for : '{dataset_name}'"
#             )

#     for max_pred, max_exp in zip(
#         predicted_max_values, expected_max_values, strict=False
#     ):
#         if max_pred is not None and max_exp is not None:
#             assert (max_pred == 0 and max_exp == 0) or abs(max_pred - max_exp) / abs(
#                 max_exp
#             ) <= ACCEPTED_CHANGE_RATIO, (
#                 f"Buckets max_values equality test failed for : '{dataset_name}'"
#             )
#         else:
#             assert max_pred is None and max_exp is None, (
#                 f"Buckets max_values equality test failed for : '{dataset_name}'"
#             )

#     # Assertion on min and max prediction with buffer:
#     for min_pred_with_buffer, min_exp_with_buffer in zip(
#         predicted_min_values_with_buffer, expected_min_values_with_buffer, strict=False
#     ):
#         if min_pred_with_buffer is not None and min_exp_with_buffer is not None:
#             assert (min_pred_with_buffer == 0 and min_exp_with_buffer == 0) or abs(
#                 min_pred_with_buffer - min_exp_with_buffer
#             ) / abs(min_exp_with_buffer) <= ACCEPTED_CHANGE_RATIO, (
#                 f"Buckets min_values with buffer equality test failed for : '{dataset_name}'"
#             )
#         else:
#             assert min_pred_with_buffer is None and min_exp_with_buffer is None, (
#                 f"Buckets min_values with buffer equality test failed for : '{dataset_name}': '{dataset_name}'"
#             )

#     for max_pred_with_buffer, max_exp_with_buffer in zip(
#         predicted_max_values_with_buffer, expected_max_values_with_buffer, strict=False
#     ):
#         if max_pred_with_buffer is not None and max_exp_with_buffer is not None:
#             assert (max_pred_with_buffer == 0 and max_exp_with_buffer == 0) or abs(
#                 max_pred_with_buffer - max_exp_with_buffer
#             ) / abs(max_exp_with_buffer) <= ACCEPTED_CHANGE_RATIO, (
#                 f"Buckets max_values with buffer equality test failed for : '{dataset_name}'"
#             )
#         else:
#             assert max_pred_with_buffer is None and max_exp_with_buffer is None, (
#                 f"Buckets max_values with buffer equality test failed for : '{dataset_name}'"
#             )

#     # Test for buckets start and end time:
#     assert predicted_buckets_start_times == expected_buckets_start_times, (
#         f"Buckets start time equality test failed for : '{dataset_name}'"
#     )

#     assert predicted_buckets_end_times == expected_buckets_end_times, (
#         f"Buckets end time equality test failed for : '{dataset_name}'"
#     )
