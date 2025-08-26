import os
import pickle
from datetime import timedelta

import numpy as np
import pandas as pd
from tqdm import tqdm

from datahub_executor.common.monitor.inference.metric_projection.config import (
    MetricProjectorConfig,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_forecaster import (
    MetricForecaster,
)

CURRENT_WDR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(CURRENT_WDR, "test_input")
INPUT_DATASET_PATH = os.path.join(INPUT_DIR, "test_data.pkl")

with open(INPUT_DATASET_PATH, "rb") as f:
    test_datasets = pickle.load(f)
    f.close()

np.random.seed(100)

results = {}
num_predictions = 7
for data_key, timestamps, metric_values in tqdm(
    test_datasets, total=len(test_datasets)
):
    start_time = (
        pd.to_datetime(max(timestamps), unit="ms") + pd.Timedelta(days=1)
    ).timestamp() * 1000

    forecaster = MetricForecaster(
        timestamps=timestamps,
        metric_values=metric_values,
        start_time=start_time,
        prediction_interval_size=timedelta(hours=1),
        num_intervals=7,
        config=MetricProjectorConfig(
            TUNE_HYPERPARAMETERS=True,
            REMOVE_ANOMALIES=True,
            USE_PARALLELIZATION=False,
        ),
    )
    buckets, metric_forecast_info = forecaster.forecast_metric()
    best_params = metric_forecast_info.best_params
    min_values = [bucket.min_value for bucket in buckets]
    max_values = [bucket.max_value for bucket in buckets]
    min_values_with_buffer = [bucket.min_value_with_buffer for bucket in buckets]
    max_values_with_buffer = [bucket.max_value_with_buffer for bucket in buckets]
    buckets_start_times = [bucket.bucket_start_time for bucket in buckets]
    buckets_end_times = [bucket.bucket_end_time for bucket in buckets]

    results[data_key] = {
        "min_values": min_values,
        "max_values": max_values,
        "min_values_with_buffer": min_values_with_buffer,
        "max_values_with_buffer": max_values_with_buffer,
        "buckets_start_times": buckets_start_times,
        "buckets_end_times": buckets_end_times,
        "data_interval": forecaster.data_interval,
        "data_standard_deviation": metric_forecast_info.data_standard_deviation,
        "best_params": best_params,
    }
with open(
    "test_input/prophet_metric_value_forecasting_expected_outputs.pkl", "wb"
) as f:
    pickle.dump(results, f)
    f.close()
