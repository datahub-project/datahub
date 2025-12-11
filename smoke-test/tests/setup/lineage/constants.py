# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import datetime


def get_timestamp_millis_num_days_ago(num_days: int) -> int:
    return int(
        (datetime.datetime.now() - datetime.timedelta(days=num_days)).timestamp() * 1000
    )


SNOWFLAKE_DATA_PLATFORM = "snowflake"
BQ_DATA_PLATFORM = "bigquery"
AIRFLOW_DATA_PLATFORM = "airflow"

DATASET_ENTITY_TYPE = "dataset"
DATA_JOB_ENTITY_TYPE = "dataJob"
DATA_FLOW_ENTITY_TYPE = "dataFlow"


DATA_FLOW_INFO_ASPECT_NAME = "dataFlowInfo"
DATA_JOB_INFO_ASPECT_NAME = "dataJobInfo"
DATA_JOB_INPUT_OUTPUT_ASPECT_NAME = "dataJobInputOutput"

TIMESTAMP_MILLIS_ONE_DAY_AGO = get_timestamp_millis_num_days_ago(1)
TIMESTAMP_MILLIS_EIGHT_DAYS_AGO = get_timestamp_millis_num_days_ago(8)
