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
