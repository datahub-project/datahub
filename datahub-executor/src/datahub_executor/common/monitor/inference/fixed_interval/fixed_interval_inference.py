"""
This module consists of the "predict_max_normal_interval" API which infers the maximum normally possible
time difference between the two subsequent events from the given historical event information.
The steps involved in inferring the max_normal_interval are following:

STEP1 -> Preprocess the raw timestamps passed to the function:
    * create a dataframe from the timestamp series, with column name 'ds'.
    * remove duplicate timestamps from the data.
    * Sort the timestamps and calculate the diff between subsequent timestamps.

STEP2 -> Remove Anomalies and calculate penalties associated with anomalies :
    * Remove Anomaly:
        - keep last two weeks of data for anomaly calculation.
        - Determine anomalies from two separate methods:
            - z score
            - local outlier factor (lof)
        - Consider the values as anomalies which are predicted as anomalies by
         both the methods (intersection).
        - label the data.
    * Penalty calculation:
        There are two penalties that are calculated around the anomalies:
            - NUM ANOMALIES PENALTY:
                - 1 - ratio of log values of total number of anomalies predicted and
                log of total number of samples in the last two weeks of data.
                        1-log(anomaly_count)/log(len(data))
            - ANOMALY CONFIDENCE PENALTY:
                - ratio of total number of disagreements in anomaly prediction by
                 the two methods ('z-score' and 'lof') and total number of anomalies
                 predicted by both methods combined(UNION).
                        n((A UNION B) - (A INTERSECTION B)) / n(A UNION B)
                - this penalty is then scaled with a scaling factor:
                    np.log(n(A UNION B) + 1) / np.log(total_data_len + 1)

    * Filter data:
        - Remove the data labelled as anomaly by both the methods.

STEP3 -> Find MAX_NORMAL:
    * After filtering the data and removing all predicted anomalies from the data, the max
    normal is determined as the maximum value of the remaining data.

STEP4 -> # Find Peaks and Centroids in total data:
    * generate kernel density estimation:
        - In statistics, kernel density estimation (KDE) is the application of kernel smoothing
         for probability density estimation, i.e., a non-parametric method to estimate the
         probability density function of a random variable based on kernels as weights.
         KDE answers a fundamental data smoothing problem where inferences about the population
         are made, based on a finite data sample (source: WIKIPEDIA).

    * Finding peaks and centroids in the kde:
        - Identifying peaks in the kernel density estimation allows us to identify different
        modes in the multimodal distribution. These modes are expected to be corresponding to the
        most commonly occurring time differences in the total data and the mean of the
        distributions around the peaks is represented by 'centroids'.

STEP5 -> # Label data and create clusters:
        - after the centroids are determined, each data point is labelled based on their distance
        from the centroids:
            for example:
                - centroids: 10, 20 and 30
                - labels: 10 -> 0, 20 -> 1, 30 -> 2
                - data points: 9, 11, 18, 21, 37
                - assigned labels based on minimum distance from the centroids:
                    0,0,1,1,2

STEP6 -> # Identify the max normal cluster:
    - In this step the custer is identified in which the calculated max normal lies.

STEP7 -> # Calculate cluster index and spread penalties:
    * CLUSTER INDEX PENALTY:
        - this penalty is calculated as:
            1 - (max normal cluster index) / (total number of clusters)

        - Since the max normal is determined from last 2 weeks of data and anomalies are also
        identified from the same data, if there are greater time differences present in the rest of
        the dataset, they won't be identified in the anomaly detection process. It is quite possible
        that when clusters are created on the complete data, these greater values of time
        differences fall in a different cluster and max normal lies in a cluster with smaller centroid.

        - to account for this difference of pattern in the last 2 weeks of data and the rest of the
        data, this penalty is calculated.

    * INTER CLUSTER PENALTY:
        - This penalty accounts for the total spread of the cluster in which the max normal lies.
        - The idea is to penalize the consistency score of the predicted max_interval if the standard
        deviation of the distribution within the cluster is large.

STEP8 -> # Calculate CONSISTENCY_SCORE:
    - consistency score is calculated as:
        1 - total_penalty
    where 'total_penalty' is the sum of all the penalties calculated in the previous steps.

STEP9 -> # Return max normal with the consistency_info object.
"""

import datetime
import logging
from typing import List, Optional, Sequence, Union

import numpy as np
import pandas as pd

from datahub_executor.common.monitor.inference.fixed_interval import config
from datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_helper_classes import (
    ConsistencyScoreInfo,
    MaxNormalIntervalResult,
)
from datahub_executor.common.monitor.inference.fixed_interval.utils import (
    get_cluster_penalties,
    get_missing_days_penalty,
    get_time_span_penalty,
    predict_anomaly,
    preprocess_data,
)

logger = logging.getLogger(__name__)


def predict_max_normal_interval(
    timestamps: Sequence[Union[int, pd.Timestamp, datetime.datetime]],
    buffer_ratio: Optional[float] = None,
) -> MaxNormalIntervalResult:
    unit = "minutes"
    score = 1.0
    try:
        assert len(timestamps) > 2, "minimum 2 timestamps are required!!!"

        # Preprocess Data:
        ts = preprocess_data(timestamps)

        # Compute Time Span Penalty
        time_delta = ts.ds.iloc[-1] - ts.ds.iloc[0]
        time_delta_in_days = time_delta.round("D").days
        time_span_penalty = get_time_span_penalty(
            time_delta_in_days, max_weight=config.TIME_SPAN_PENALTY_WEIGHT
        )

        # Filter two weeks data
        date_two_weeks_ago = ts.ds.dt.date.max() - pd.Timedelta(days=15)
        recent_data = (
            ts[ts.ds.dt.date > date_two_weeks_ago]["time_delta"]
            .dropna()
            .values.astype("float")
        )

        # Calculate Missing Days Penalty:
        unique_dates_in_sorted_order = np.sort(
            ts[ts.ds.dt.date > date_two_weeks_ago]["date"].unique()
        )
        datewise_counts = np.array(
            [int(x.days) for x in np.ediff1d(unique_dates_in_sorted_order)]
        )
        missing_days_penalty = get_missing_days_penalty(
            datewise_counts, config.MISSING_DAYS_PENALTY_WEIGHT
        )

        # Remove Anomalies and calculate penalty:
        is_anomaly: Optional[List[bool]]
        (is_anomaly, anomaly_confidence_penalty) = predict_anomaly(recent_data)

        if is_anomaly is not None:
            recent_data = recent_data[~np.array(is_anomaly)]
            num_anomalies = sum(is_anomaly)
            num_anomalies_penalty = config.NUM_ANOMALY_PENALTY_WEIGHT * (
                np.log(num_anomalies + 1) / np.log(len(recent_data) + 1)
            )
        else:
            raise Exception("Error in anomaly detection!!!")

        # MAX NORMAL:
        max_normal_interval = max(recent_data)

        # Calculate Cluster Penalties:
        all_data = (
            ts[ts.ds.dt.date <= date_two_weeks_ago]["time_delta"]
            .dropna()
            .values.astype("float")
        )
        all_data = np.hstack([all_data, recent_data])
        (
            inter_cluster_penalty,
            cluster_index_penalty,
            max_normal_interval,
        ) = get_cluster_penalties(all_data, recent_data, max_normal_interval)

        # Calculate total penalty:
        total_penalty = sum(
            filter(  # type: ignore
                lambda x: x > 0,  # type: ignore
                [
                    # anomaly_confidence_penalty,
                    num_anomalies_penalty,
                    cluster_index_penalty,
                    inter_cluster_penalty,
                    missing_days_penalty,
                    time_span_penalty,
                ],
            )
        )
        # Calculate Consistency Score:
        consistency_score = score - total_penalty
        consistency_info = ConsistencyScoreInfo(
            score=consistency_score,
            anomaly_confidence_penalty=anomaly_confidence_penalty,
            num_anomalies_penalty=num_anomalies_penalty,
            cluster_index_penalty=cluster_index_penalty,
            inter_cluster_penalty=inter_cluster_penalty,
            missing_days_penalty=missing_days_penalty,
            time_span_penalty=time_span_penalty,
        )
        if buffer_ratio is None:
            buffer_ratio = config.BUFFER_RATIO
        max_normal_interval_with_buffer = int(max_normal_interval * (1 + buffer_ratio))
        max_normal_interval_result = MaxNormalIntervalResult(
            unit=unit,
            max_normal_interval=int(max_normal_interval),
            max_normal_interval_with_buffer=max_normal_interval_with_buffer,
            consistency_info=consistency_info,
        )
    except Exception as e:
        logger.exception(f"Could not infer the interval!! {e}")
        return MaxNormalIntervalResult()
    return max_normal_interval_result
