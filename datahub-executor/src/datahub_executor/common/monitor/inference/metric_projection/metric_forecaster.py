"""
## API "MetricForecaster.forecast_metric()"
This API predicts the future ranges of metric values of a table from the historical event information.


## API Contract:
- Input
    1. timestamps - This is a list of timestamps
    2. metric_values - This is a list of respective metric values for timestamps mentioned in above list
    3. start_time (default: None) - Starting timestamp of the projection window
    4. prediction_interval_size (default: None) - Interval granularity for which user wants to have projections (e.g. "hour", "day", "week", "month" case-insensitive)
    5. num_intervals (default: None) - Number of intervals
    6. input_prediction_timestamps (default: None) - Timestamps for projection ranges - point prediction
    7. user_config (default: None) - A dictionary containing config parameter values. User can enable or disable the hyperparameter tuning using boolean 'TUNE_HYPERPARAMETERS' flag.

    NOTE: User will provide either the ["start_time", "prediction_interval_size", "num_intervals"] or ["input_prediction_timestamps"]

- Output
    1. List of Bucket class instances
        - Bucket class:
            Each bucket class instance has the following attributes:
            - `bucket_number` - An integer that indicates the bucket number in order, beginning at zero
            - `bucket_size` - It is the size of each bucket.
            - `bucket_start_time` - The bucket start time is the first millisecond of the bucket window.
            - `bucket_end_time` - The bucket end time is the last millisecond of the bucket window.
            - `bucket_id` - Unique identifier of the bucket. Created by concatenation of bucket_number, bucket_start_time and bucket_end_time
            - `min_value` - Predicted minimum metric value for the bucket.
            - `max_value` - Predicted maximum metric value for the bucket.
            - `min_value_with_buffer` - Predicted minimum metric value with some added buffer for the bucket.
            - `max_value_with_buffer` - Predicted maximum metric value with some added buffer for the bucket.
    2. Instance of "MetricForecastInfo" class, it contains following information
        confidence_score_info: Object of 'ConfidenceScoreInfo' class which stores the confidence score of the predictions.
        best_params: A dictionary of the hyperparameters used in the final model.
        data_standard_deviation: Standard deviation in the training data.


## Steps
Following steps are involved in predicting min-max ranges of a metric for user specified timestamps
1. Data Preprocessing
2. Qualification
3. Timeseries Model Training and Forecast
4. Output Preparation
5. Confidence Score Computation

# Step1: Data Preprocessing
    - Prepare the required datastructure (dataframe) from user inputs
    - Removes any duplicate values
    - Infer the user provided data interval
        - The interval of data is the data resampling time interval for which the data has consistent representation.
        - The dataframe with columns 'delta' and 'ds' are resampled iteratively on every time interval stored in the 'VALID_INTERVALS' attribute of the config file which is ordered from smaller to larger intervals.
        - In each iteration the api checks for the percentage of missing values after resampling, if the percentage of missing values after resampling is less than the configured threshold for that interval, the particular interval is inferred as the final interval.

# Step2: Qualification
    - Basic checks to make sure to have sufficient datapoints to process

# Step3: Timeseries Model Training and Forecast
    - Check the applicability of hyperparameter tuning, if we have less data points then hyperparameter tuning is not possible
    - If hyperparameter tuning is enabled,
        - split the data into train & test split with 70% for train and 30% for test
        - Train a time series model (Prophet Model) on train data with hyperparameter combinations
        - For each combination, get projections on the test data and record the MAPE (mean absolute percentage error)
        - Select the best hyperparameter combination with lesser MAPE value
        - Train a time series model on all data (train + test) with the best hyperparameter combination
    - Else use the default model parameters and train a time series model on all data
    - Prepare prediction timestamps
        - Determine the higher of the user-specified interval (prediction_interval_size) and the inferred interval from the data to determine the round_off_level.
        - round_off_level will be used to determine the inference timestamps. The model predictions will be generated for these inference_timestamps.
        - The validity interval for the model's min-max range prediction is determined by the inference timestamp, which is made up of the floor and ceiling timestamps.
        - For more explanation, refer following doc
        https://www.notion.so/acryldata/Modes-of-operation-f6a54ca631874de49131244724a808c8
    - Get the model predictions on inference timestamps (floor prediction timestamps and ceil prediction timestamps)
    - Verify the need of postprocessing to remove certain anomalous points (spikes) from the predictions when operating on less number of data points
    - Post processing steps
        - Combine the user provided data and projections, compute the delta
        - Locate the abnormal pair of deltas in the projected values (identify spike)
        - Remove the associated projection point and interpolate it.

Step4: Output Preparation
    - Get the min and max values for the window by considering the min and max ranges of both end as mentioned in the following document
    https://www.notion.so/acryldata/Approach-to-identify-min-and-max-values-for-given-timestamp-4300b2f7280d423e8f20ca51278ff7f1
    - Minimum value: Select the minimum from minimum of lower and upper boundary
    - Maximum value: Select the maximum from minimum of lower and upper boundary

Step5: Confidence Score Computation
    - Using cross validation function, we compute forecast error which is being used to calculate confidence of the model (lesser the error, higher the confidence)
    https://facebook.github.io/prophet/docs/diagnostics.html#cross-validation
"""

import itertools
import logging
from datetime import timedelta
from typing import List, Optional, Sequence, Tuple, Union

import pandas as pd

from datahub_executor.common.monitor.inference.metric_projection.config import (
    MetricProjectorConfig,
)
from datahub_executor.common.monitor.inference.metric_projection.types import (
    Bucket,
    ConfidenceScoreInfo,
    MetricForecastInfo,
    TimePeriod,
)
from datahub_executor.common.monitor.inference.metric_projection.utils import (
    create_dataset,
    create_test_train_splits,
    fit_model,
    get_confidence_score,
    get_floor_and_ceil_dataframes,
    get_prediction_timestamps,
    get_round_off_level,
    get_validity_time_ranges,
    infer_frequency,
    perform_basic_checks,
    post_process_predictions,
)

logger = logging.getLogger(__name__)


class MetricForecaster:
    def __init__(
        self,
        timestamps: Sequence[Union[float, int]],
        metric_values: List[Union[float, int]],
        start_time: Optional[Union[int, float]] = None,
        prediction_interval_size: Optional[timedelta] = None,
        num_intervals: Optional[int] = None,
        input_prediction_timestamps: Optional[List[Union[int, float]]] = None,
        config: Optional[MetricProjectorConfig] = None,
    ):
        assert not (
            input_prediction_timestamps is None
            and any(
                [
                    x is None
                    for x in [
                        prediction_interval_size,
                        num_intervals,
                        start_time,
                    ]
                ]
            )
        ), (
            "User must provide either a list of prediction timestamps or start_time, along with "
            "prediction_interval_size and num_intervals!!"
        )

        self.timestamps: List[Union[float, int]] = list(timestamps)
        self.metric_values: List[Union[float, int]] = metric_values
        self.start_time: Optional[Union[int, float]] = start_time
        if prediction_interval_size is not None:
            self.prediction_interval_size: Optional[TimePeriod] = (
                TimePeriod.from_timedelta(prediction_interval_size)
            )

        self.num_intervals: Optional[int] = num_intervals
        if input_prediction_timestamps is not None:
            self.input_prediction_timestamps: Optional[List[pd.Timestamp]] = [
                pd.to_datetime(timestamp, unit="ms")
                for timestamp in input_prediction_timestamps
            ]
        else:
            self.input_prediction_timestamps = None

        self.config = config or MetricProjectorConfig()

        # Preprocess Data
        self.data = create_dataset(self.config, self.timestamps, self.metric_values)
        data_interval = infer_frequency(self.config, self.data)
        if data_interval is not None:
            self.data_interval: TimePeriod = TimePeriod(data_interval)
        else:
            raise Exception("Unable to infer data cadence")

        # Perform Data Sufficiency Checks
        if not perform_basic_checks(self.config, self.data):
            raise Exception("Data is not sufficient!!!")

        self._data_standard_deviation: float = self.data.y.std()

    def _make_prediction_dataframe(
        self, data_interval: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[pd.Timestamp]]:
        """
        This function creates floor and ceil prediction timestamps which represent the
        bucket start and end timestamps respectively.
        It also returns the bucket_timestamps which are generated considering the number of desired buckets
        and prediction interval size.

        :return:
            pd.DataFrame, dataframe with floor timestamps of the buckets.
            pd.DataFrame, dataframe with ceil timestamps of the buckets.
            list of Timestamps representing the
        """
        if (
            self.input_prediction_timestamps is None
            and self.prediction_interval_size is not None
            and self.num_intervals is not None
            and self.start_time is not None
        ):
            bucket_timestamps = get_prediction_timestamps(
                start_timestamp=self.start_time,
                prediction_interval_size=self.prediction_interval_size.value,
                num_intervals=self.num_intervals,
            )
            round_off_level = get_round_off_level(
                prediction_interval_size=self.prediction_interval_size.value,
                data_interval=data_interval,
            )
        elif self.input_prediction_timestamps is not None:
            bucket_timestamps = self.input_prediction_timestamps
            round_off_level = data_interval
        else:
            raise Exception(
                "User must provide either a list of prediction timestamps or start_time, "
                "along with prediction_interval_size and num_intervals!!"
            )

        (floor_future_dataframe, ceil_future_dataframe) = get_floor_and_ceil_dataframes(
            round_off_level=round_off_level, bucket_timestamps=bucket_timestamps
        )

        return floor_future_dataframe, ceil_future_dataframe, bucket_timestamps

    def forecast_metric(self) -> Tuple[List[Bucket], MetricForecastInfo]:
        try:
            # Create test and train splits
            param_grid = self.config.PARAM_GRID
            if self.config.TUNE_HYPERPARAMETERS:
                if len(self.data) >= self.config.MIN_SAMPLES_FOR_TUNING:
                    train_data, test_data = create_test_train_splits(
                        self.config, self.data
                    )
                    if (
                        self.data_interval.value == "W"
                        or len(self.data)
                        < self.config.WEEKLY_SEASONALITY_SAMPLE_COUNT_THRESHOLD
                    ):
                        param_grid.update({"weekly_seasonality": [False]})

                    all_params = [
                        dict(zip(param_grid.keys(), v, strict=False))
                        for v in itertools.product(*param_grid.values())
                    ]
                else:
                    train_data, test_data = None, None
                    all_params = []

            else:
                train_data, test_data = None, None
                all_params = []

            # Fit Prophet model:
            data_forecast, model, best_params = fit_model(
                config=self.config,
                all_params=all_params,
                data=self.data,
                train_data=train_data,
                test_data=test_data,
            )

            # Get Prediction TimeStamps and predict
            (
                floor_future_dataframe,
                ceil_future_dataframe,
                bucket_timestamps,
            ) = self._make_prediction_dataframe(data_interval=self.data_interval.value)

            floor_forecast = model.predict(floor_future_dataframe)
            ceil_forecast = model.predict(ceil_future_dataframe)

            # Post process results
            if (
                self.prediction_interval_size is not None
                and self.data_interval is not None
            ):
                interval_pair = (
                    f"{self.prediction_interval_size.value}{self.data_interval.value}"
                )
            else:
                interval_pair = None

            if (
                self.config.POST_PROCESSING
                and len(self.data) < self.config.POST_PROCESSING_SAMPLE_COUNT_THRESHOLD
                and self.prediction_interval_size is not None
                and interval_pair in self.config.POST_PROCESS_INTERVAL_PAIRS
                and data_forecast is not None
            ):
                floor_forecast, ceil_forecast = post_process_predictions(
                    self.config, self.data, data_forecast, floor_forecast, ceil_forecast
                )

            # Get min and max values
            min_values = [
                min(
                    floor_forecast["yhat_lower"].tolist()[i],
                    ceil_forecast["yhat_lower"].tolist()[i],
                )
                for i in range(len(floor_forecast))
            ]
            max_values = [
                max(
                    floor_forecast["yhat_upper"].tolist()[i],
                    ceil_forecast["yhat_upper"].tolist()[i],
                )
                for i in range(len(floor_forecast))
            ]

            min_values_with_buffer = [
                value - self.config.BUFFER_FACTOR * self._data_standard_deviation
                for value in min_values
            ]
            max_values_with_buffer = [
                value + self.config.BUFFER_FACTOR * self._data_standard_deviation
                for value in max_values
            ]

            # Get Validity Time Ranges
            if self.prediction_interval_size is not None:
                validity_time_ranges = get_validity_time_ranges(
                    prediction_interval_size=self.prediction_interval_size.value,
                    bucket_timestamps=bucket_timestamps,
                )
            elif self.input_prediction_timestamps is not None:
                validity_time_ranges = get_validity_time_ranges(
                    prediction_interval_size=str(self.data_interval.value),
                    bucket_timestamps=bucket_timestamps,
                )
            else:
                raise Exception(
                    "prediction_interval_size and input_prediction_timestamps are both None!!"
                )

            # Compute Confidence Score
            if self.config.COMPUTE_CONFIDENCE_SCORE:
                confidence_score = get_confidence_score(
                    self.config, model, self.data, str(self.data_interval.value)
                )
                if confidence_score is not None:
                    confidence_score_info = ConfidenceScoreInfo(
                        score=1 - confidence_score
                    )
                else:
                    confidence_score_info = None
            else:
                confidence_score_info = ConfidenceScoreInfo()

            # Create Buckets
            buckets = [
                Bucket(
                    bucket_number=i + 1,
                    bucket_size=(
                        self.prediction_interval_size.value
                        if self.prediction_interval_size is not None
                        else self.data_interval.value
                    ),
                    bucket_start_time=validity_time_ranges[i][0],
                    bucket_end_time=validity_time_ranges[i][1],
                    bucket_id=f"{i + 1}_{validity_time_ranges[i][0]}_{validity_time_ranges[i][1]}",
                    min_value=min_values[i],
                    max_value=max_values[i],
                    min_value_with_buffer=min_values_with_buffer[i],
                    max_value_with_buffer=max_values_with_buffer[i],
                )
                for i in range(len(min_values))
            ]
            # For Debugging
            if self.data is not None and train_data is not None:
                self.data.loc[:, "is_train"] = self.data.ds.apply(
                    lambda x: True if x in train_data["ds"] else False
                )
            metric_forecast_info = MetricForecastInfo(
                confidence_score_info=confidence_score_info,
                best_params=best_params,
                data_standard_deviation=self._data_standard_deviation,
            )
        except Exception as e:
            logger.exception(f"Failed to forecast due to {e}")
            return [], MetricForecastInfo(
                confidence_score_info=None,
                best_params=None,
                data_standard_deviation=None,
            )
        return buckets, metric_forecast_info
