import logging
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.metrics import mean_absolute_percentage_error

from datahub_executor.common.monitor.inference.metric_projection.config import (
    MetricProjectorConfig,
)

logger = logging.getLogger(__name__)

# Prophet uses cmdstanpy, which has somewhat spammy logging.
# See https://stackoverflow.com/questions/66667909/stop-printing-infocmdstanpystart-chain-1-infocmdstanpyfinish-chain-1
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# TODO: Digest and provide unit tests.
# This code was copied from datahub-inference-engine where it was original defined
# and tested.


def find_spikes(anomaly_labels: List[bool]) -> List[int]:
    """
    This function is used in the output post-processing phase to identify the indices of anomaly
    spikes in the predicted outputs.

    return: list of integers
    """
    anomaly_indices = []
    try:
        for i in range(len(anomaly_labels) - 1):
            if (i == 0 and anomaly_labels[i] and anomaly_labels[i + 1]) or (
                i != 0
                and not anomaly_labels[i - 1]
                and anomaly_labels[i]
                and anomaly_labels[i + 1]
            ):
                anomaly_indices.append(i)
            else:
                continue
    except Exception as e:
        logger.exception(
            f"{e}, Error encountered while identifying anomalous spikes in prediction"
        )
    return anomaly_indices


def get_iqr_ranges(values: np.ndarray, k: float = 2.5) -> Tuple[float, float]:
    """
    For any given array of data this function returns a lower and upper bound range of the data using iqr technique.
    """
    quantile75 = np.percentile(values[~np.isnan(values)], 75)
    quantile25 = np.percentile(values[~np.isnan(values)], 25)
    iqr = quantile75 - quantile25
    if iqr == 0:
        buffer = 0.1 * k * quantile75
    else:
        buffer = k * iqr
    upper_bound = quantile75 + buffer
    lower_bound = quantile25 - buffer
    return lower_bound, upper_bound  # type: ignore


def detect_anomaly_iqr(
    past_values: np.ndarray, outlier_coefficient: float = 2.5
) -> Optional[List[bool]]:
    """
    This function determines the anomalous values in a given series of data using iqr method.
    @param past_values:
                        Type: numpy ndarray
    @param outlier_coefficient:
                        Type: float
    :return: list of boolean values, returns a boolean list of length equal to the number of past_values.
    """
    is_anomaly = []
    try:
        lower_bound, upper_bound = get_iqr_ranges(past_values, k=outlier_coefficient)
        for value in past_values:
            if value < lower_bound or value > upper_bound:
                is_anomaly.append(True)
            else:
                is_anomaly.append(False)
    except Exception as e:
        logger.exception(f"{e} Error encountered while generating anomaly labels")
        return None
    return is_anomaly


def is_anomaly_pair(
    config: MetricProjectorConfig,
    v1: float,
    v2: float,
    lower_bound: float,
    upper_bound: float,
) -> Tuple[bool, bool]:
    """
    This function checks if the pair of values provided are of opposite sign and are of same magnitude approximately.
    label the two points anomalous if:
        i.    both are outside the normal range (between lower_bound and upper_bound)
        ii.   are of opposite sign.
        iii.  are of approximately same magnitude (difference not more than DELTA_DIFFERENCE_FOR_SPIKES fraction of v1).
    """
    v1_sign = "negative" if v1 < 0 else "positive"
    v2_sign = "negative" if v2 < 0 else "positive"
    v1_anomaly_label: bool = False
    v2_anomaly_label: bool = False
    if (v1_sign != v2_sign) and (
        (v1 < lower_bound or v1 > upper_bound)
        and (v2 < lower_bound or v2 > upper_bound)
    ):
        diff = abs(abs(v1) - abs(v2))
        if diff < config.DELTA_DIFFERENCE_FOR_SPIKES * abs(v1):
            v1_anomaly_label = True
            v2_anomaly_label = True
    return v1_anomaly_label, v2_anomaly_label


def detect_anomaly_spikes(
    config: MetricProjectorConfig, df: pd.DataFrame
) -> Optional[List[bool]]:
    """
    This function identifies the anomaly spikes in the given array of data,
    Steps:
    1. Compute the deltas from the given metric value series.
    2. Identify the normal value ranges for deltas with iqr method.
    3. Detect any two consecutive points which are
        i.      both outside the normal range (calculated with iqr method)
        ii.     are of opposite sign.
        iii.    are of approximately same magnitude.
    4. Label the pair as anomalous.
    5. Create a new array of anomaly labels which corresponds to the metric values instead of the deltas using the following logic:
        - For any observed consecutive anomalies in the deltas the first corresponding metric value will be labelled as anomaly.
        metric values:          100,    200,    1000,   300,    400
        deltas                  nan,    100,    800,    -700,   100
        delta anomalies:        False,  False,  True,   True,   False
        metric value anomalies  False,  False,  True,   False,  False
    """
    try:
        values = np.array(df["y"].diff())
        lower_bound, upper_bound = get_iqr_ranges(
            values, k=config.OUTLIER_COEFFICIENT_FOR_SPIKES_DETECTION
        )
        anomaly_labels: List[bool] = []
        anomaly_labels_corrected: List[bool] = []
        is_next_anomaly = False
        for i in range(len(values)):
            if i == (len(values) - 1):
                anomaly_labels.append(is_next_anomaly)
                break
            else:
                if is_next_anomaly:
                    anomaly_labels.append(True)
                    is_next_anomaly = False
                else:
                    v1, v2 = values[i], values[i + 1]
                    pair_anomaly_status = is_anomaly_pair(
                        config, float(v1), float(v2), lower_bound, upper_bound
                    )
                    anomaly_labels.append(pair_anomaly_status[0])
                    is_next_anomaly = pair_anomaly_status[1]
        for x in anomaly_labels:
            if x and anomaly_labels_corrected[-1]:
                anomaly_labels_corrected.append(False)
            else:
                anomaly_labels_corrected.append(x)
    except Exception as e:
        logger.exception(f"Encountered error in anomaly detection {e}")
        return None
    return anomaly_labels_corrected


def fit_predict_model(params: dict, data: pd.DataFrame) -> Tuple[pd.DataFrame, Prophet]:
    m = Prophet(**params)
    m = m.fit(data)
    forecast = m.predict(data)
    return forecast, m


def create_test_train_splits(
    config: MetricProjectorConfig,
    data: pd.DataFrame,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    try:
        num_train_samples = int(config.SPLIT_RATIO * len(data))
        train_data = data.head(num_train_samples)
        test_data = data.loc[data.ds > max(train_data.ds)]
    except Exception as e:
        logger.exception(f"Could not create splits due to {e}!!")
        return None, None
    return train_data, test_data


def perform_basic_checks(config: MetricProjectorConfig, data: pd.DataFrame) -> bool:
    # check minimum number of samples to proceed
    # threshold infer from config
    is_valid_data = False
    try:
        if data is not None:
            if len(data) < config.MIN_DATA_LENGTH:
                return False
            else:
                return True
        else:
            raise Exception("No Data Available")
    except Exception as e:
        logger.exception(f"Could not perform checks on the data due to {e}")
    return is_valid_data


def infer_frequency(config: MetricProjectorConfig, data: pd.DataFrame) -> Optional[str]:
    inferred_interval = None
    try:
        for interval in config.VALID_INTERVALS:
            resampled_df = (
                data.loc[:, ["ds", "y"]]
                .set_index("ds")
                .resample(interval)
                .sum(min_count=1)
            )
            na_count = resampled_df.isna().y.sum()
            missing_info_ratio = na_count / len(resampled_df)
            if missing_info_ratio < config.INTERVAL_THRESHOLDS[interval]:
                inferred_interval = interval
                break
    except Exception as e:
        logger.exception(f"Could not infer data interval due to {e}")
    return inferred_interval


def post_process_predictions(
    config: MetricProjectorConfig,
    data: pd.DataFrame,
    train_data_forecast: pd.DataFrame,
    floor_forecast: pd.DataFrame,
    ceil_forecast: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    In this function the predicted values are checked for any outlier spikes which are removed and regenerated
    by interpolated.
    The corrected predictions are returned from the function.
    """
    try:
        floor_forecast.sort_values("ds", inplace=True)
        floor_forecast.reset_index(drop=True, inplace=True)
        ceil_forecast.sort_values("ds", inplace=True)
        ceil_forecast.reset_index(drop=True, inplace=True)
        all_predictions = pd.concat([floor_forecast, ceil_forecast.tail(1)], axis=0)
        all_predictions = all_predictions.sort_values("ds")
        all_predictions.loc[:, "y"] = all_predictions["yhat"]
        data = pd.concat([data, all_predictions[["ds", "y"]]], axis=0)
        data = data.sort_values("ds")
        data.loc[:, "delta"] = data["y"].diff()
        anomaly_labels = detect_anomaly_iqr(
            np.array(data["delta"]), outlier_coefficient=config.OUTLIER_COEFFICIENT
        )
        prediction_anomaly_labels = (
            anomaly_labels[-len(all_predictions) :]
            if anomaly_labels is not None
            else []
        )
        if all(value for value in prediction_anomaly_labels):
            raise Exception(
                "Anomaly labels for predictions are either all True or Empty!!"
            )
        # all_predictions.loc[:, 'anomaly_label'] = prediction_anomaly_labels
        floor_anomaly_indices = find_spikes(prediction_anomaly_labels)
        ceil_anomaly_indices = [
            index - 1 for index in floor_anomaly_indices if index > 0
        ]

        floor_forecast.loc[floor_anomaly_indices, "yhat_lower"] = np.nan
        floor_forecast.loc[floor_anomaly_indices, "yhat_upper"] = np.nan
        floor_forecast.loc[floor_anomaly_indices, "yhat"] = np.nan
        ceil_forecast.loc[ceil_anomaly_indices, "yhat_lower"] = np.nan
        ceil_forecast.loc[ceil_anomaly_indices, "yhat_upper"] = np.nan
        ceil_forecast.loc[ceil_anomaly_indices, "yhat"] = np.nan
        combined_floor_forecast = pd.concat([train_data_forecast, floor_forecast])

        combined_ceil_forecast = pd.concat([train_data_forecast, ceil_forecast])
        combined_floor_forecast.loc[:, "yhat_lower"] = (
            combined_floor_forecast.yhat_lower.interpolate()
        )
        combined_floor_forecast.loc[:, "yhat_upper"] = (
            combined_floor_forecast.yhat_upper.interpolate()
        )
        combined_floor_forecast.loc[:, "yhat"] = (
            combined_floor_forecast.yhat.interpolate()
        )

        combined_ceil_forecast.loc[:, "yhat_lower"] = (
            combined_floor_forecast.yhat_lower.interpolate()
        )
        combined_ceil_forecast.loc[:, "yhat_upper"] = (
            combined_floor_forecast.yhat_upper.interpolate()
        )
        combined_ceil_forecast.loc[:, "yhat"] = (
            combined_floor_forecast.yhat_upper.interpolate()
        )
        floor_forecast = combined_floor_forecast[
            combined_floor_forecast.ds.isin(floor_forecast.ds)
        ]
        ceil_forecast = combined_ceil_forecast[
            combined_ceil_forecast.ds.isin(ceil_forecast.ds)
        ]
    except Exception as e:
        logger.exception(f"Not able to post process predictions because {e}")
        return floor_forecast, ceil_forecast
    return floor_forecast, ceil_forecast


def get_confidence_score(
    config: MetricProjectorConfig,
    model: Prophet,
    df: pd.DataFrame,
    data_interval: str,
) -> Optional[float]:
    """
    This function is used to generate a confidence score of the predictions using cross validation
    on the training data with the finalized model
    """
    try:
        initial = round(int(0.7 * len(df) / 7)) * 7
        if len(df) > 30:
            horizon = int(0.2 * len(df))
            # period = int(0.2 * len(df))
        else:
            horizon = int(0.1 * len(df))
            # period = int(0.1 * len(df))
        if "H" in data_interval:
            df_cv = cross_validation(
                model, initial=f"{initial} hours", horizon=f"{horizon} hours"
            )
        else:
            df_cv = cross_validation(
                model, initial=f"{initial} days", horizon=f"{horizon} days"
            )
        df_p = performance_metrics(df_cv)
        score = df_p[config.SCORING_METRIC].mean()  # type: ignore
    except Exception as e:
        logger.warning(f"Confidence Score is not calculated due to {e}")
        return None
    return score


def create_dataset(
    config: MetricProjectorConfig,
    timestamps: List[Union[int, float]],
    metric_values: List[Union[int, float]],
) -> pd.DataFrame:
    """
    This function returns a dataframe which has timestamps and metric values as columns.
    The duplicated rows are removed from the dataframe, and it is sorted by the timestamps in ascending order
    Depending upon the configuration spike anomalies are also removed from in this data preprocessing step.
    """
    data = pd.DataFrame({"ds": timestamps, "y": metric_values})
    data.loc[:, "ds"] = pd.to_datetime(data["ds"], unit="ms")
    data.drop_duplicates(inplace=True)
    data.sort_values(by="ds", inplace=True)
    data.reset_index(drop=True, inplace=True)
    if config.REMOVE_ANOMALIES:
        anomaly_labels = detect_anomaly_spikes(config, data)
        if anomaly_labels is not None:
            data.loc[:, "is_anomaly"] = anomaly_labels
            data = data.loc[~data.is_anomaly, :]
    return data


def fit_model(
    config: MetricProjectorConfig,
    all_params: List[dict],
    data: pd.DataFrame,
    train_data: Optional[pd.DataFrame],
    test_data: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, Prophet, dict]:
    # Train a Prophet model with hyperparameter tuning, get the projections for future
    try:
        if all_params:
            logger.debug(
                f"Tuning model hyperparameters with {len(all_params)} configurations: {all_params}"
            )
            if train_data is not None and test_data is not None:
                if not config.USE_PARALLELIZATION:
                    mapes = []
                    for params in all_params:
                        mapes.append(run_prophet(params, train_data, test_data))
                else:
                    mapes = Parallel(n_jobs=config.CPU_COUNT)(
                        delayed(run_prophet)(params, train_data, test_data)
                        for params in all_params
                    )

                best_params = all_params[mapes.index(min(mapes))]  # type: ignore
            else:
                raise Exception(
                    "No training and test data available to tune hyperparameters"
                )
        else:
            best_params = config.DEFAULT_PARAMS
        full_data_preds_df, m = fit_predict_model(
            best_params,
            data=data,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to fit model: {e}") from e
    return full_data_preds_df, m, best_params


def get_prediction_timestamps(
    start_timestamp: float, prediction_interval_size: str, num_intervals: int
) -> List[pd.Timestamp]:
    """
    This function is used to create the future bucket timestamps using the prediction interval size info.
    if the interval is weekly, the gap between each subsequent bucket timestamp would be a week.
    """
    start_time: pd.Timestamp = pd.to_datetime(start_timestamp, unit="ms")
    if prediction_interval_size == "H":
        bucket_timestamps = [
            start_time + pd.Timedelta(hours=i) for i in range(num_intervals)
        ]
    elif prediction_interval_size == "D":
        bucket_timestamps = [
            start_time + pd.Timedelta(days=i) for i in range(num_intervals)
        ]
    elif prediction_interval_size == "W":
        bucket_timestamps = [
            start_time + pd.Timedelta(days=i * 7) for i in range(num_intervals)
        ]
    elif prediction_interval_size == "M":
        bucket_timestamps = [
            start_time + pd.DateOffset(months=i) for i in range(num_intervals)
        ]
    else:
        raise Exception("Unable to generate prediction timestamps!!")
    return bucket_timestamps


def get_round_off_level(prediction_interval_size: str, data_interval: str) -> str:
    """
    This function is used to identify the interval which would be used to get the validity span of each predicted values.
    For example if the user request to get the buckets to be predicted at a "Daily" interval and data
    frequency is at "weekly" interval, the validity of a prediction will be for a week.
    """
    interval_pairs = ["DH", "WH", "MH", "WD", "MD", "MW"]
    interval_pair = f"{prediction_interval_size}{data_interval}"

    if interval_pair in interval_pairs:
        round_off_level = prediction_interval_size
    else:
        round_off_level = data_interval
    return round_off_level


def get_floor_and_ceil_dataframes(
    round_off_level: str, bucket_timestamps: List[pd.Timestamp]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    This function returns the dataframes containing bucket start times and bucket end times.
    For each bucket the min and max values are calculated by looking at the predictions for
    both the start time of the bucket and end time of the bucket.
    """
    ceil_prediction_timestamps = [
        pd.to_datetime(pd.Period(timestamp, freq=round_off_level).end_time)
        for timestamp in bucket_timestamps
    ]
    floor_prediction_timestamps = [
        pd.to_datetime(pd.Period(timestamp, freq=round_off_level).start_time)
        for timestamp in bucket_timestamps
    ]
    ceil_future_dataframe = pd.DataFrame({"ds": ceil_prediction_timestamps})
    floor_future_dataframe = pd.DataFrame({"ds": floor_prediction_timestamps})
    return floor_future_dataframe, ceil_future_dataframe


def get_validity_time_ranges(
    prediction_interval_size: str, bucket_timestamps: List[pd.Timestamp]
) -> List[Tuple[float, float]]:
    """
    This function is used to calculate the validity time spans for every bucket prediction.
    """
    if prediction_interval_size is not None:
        validity_upper_limit = [
            pd.to_datetime(pd.Period(timestamp, freq=prediction_interval_size).end_time)
            for timestamp in bucket_timestamps
        ]
        validity_lower_limit = [
            pd.to_datetime(
                pd.Period(timestamp, freq=prediction_interval_size).start_time
            )
            for timestamp in bucket_timestamps
        ]
    else:
        raise Exception("Prediction Interval/Bucket Size is not defined!!")
    validity_time_ranges = [
        (
            validity_lower_limit[i].timestamp() * 1000,
            validity_upper_limit[i].timestamp() * 1000 - 1,
        )
        for i in range(len(bucket_timestamps))
    ]
    return validity_time_ranges


def run_prophet(
    params: dict, train_data: pd.DataFrame, test_data: pd.DataFrame
) -> float:
    """
    This function is used in the parallelized operation during hyperparameter tuning
    """
    _, m = fit_predict_model(
        params,
        data=train_data,
    )
    forecast = m.predict(test_data[["ds"]])
    mape = mean_absolute_percentage_error(test_data.y.tolist(), forecast.yhat.tolist())
    return mape  # type: ignore
