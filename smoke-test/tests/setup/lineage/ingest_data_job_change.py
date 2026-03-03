from typing import List

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DateTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from tests.setup.lineage.constants import (
    AIRFLOW_DATA_PLATFORM,
    SNOWFLAKE_DATA_PLATFORM,
    TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
    TIMESTAMP_MILLIS_ONE_DAY_AGO,
)
from tests.setup.lineage.helper_classes import Dataset, Field, Pipeline, Task
from tests.setup.lineage.utils import (
    create_edge,
    create_node,
    create_nodes_and_edges,
    emit_mcps,
)

# Constants for Case 2
DAILY_TEMPERATURE_DATASET_ID = "climate.daily_temperature"
DAILY_TEMPERATURE_DATASET_URN = make_dataset_urn(
    platform=SNOWFLAKE_DATA_PLATFORM, name=DAILY_TEMPERATURE_DATASET_ID
)
DAILY_TEMPERATURE_DATASET = Dataset(
    id=DAILY_TEMPERATURE_DATASET_ID,
    platform=SNOWFLAKE_DATA_PLATFORM,
    schema_metadata=[
        Field(name="date", type=SchemaFieldDataTypeClass(type=DateTypeClass())),
        Field(
            name="temperature", type=SchemaFieldDataTypeClass(type=NumberTypeClass())
        ),
    ],
)

MONTHLY_TEMPERATURE_DATASET_ID = "climate.monthly_temperature"
MONTHLY_TEMPERATURE_DATASET_URN = make_dataset_urn(
    platform=SNOWFLAKE_DATA_PLATFORM, name=MONTHLY_TEMPERATURE_DATASET_ID
)
MONTHLY_TEMPERATURE_DATASET = Dataset(
    id=MONTHLY_TEMPERATURE_DATASET_ID,
    platform=SNOWFLAKE_DATA_PLATFORM,
    schema_metadata=[
        Field(name="month", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(
            name="mean_temperature",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
    ],
)

SNOWFLAKE_ETL_DATA_FLOW_ID = "snowflake_etl"
SNOWFLAKE_ETL_DATA_FLOW_URN = make_data_flow_urn(
    orchestrator=AIRFLOW_DATA_PLATFORM, flow_id=SNOWFLAKE_ETL_DATA_FLOW_ID
)
TEMPERATURE_ETL_1_DATA_JOB_ID = "temperature_etl_1"
TEMPERATURE_ETL_1_DATA_JOB_URN = make_data_job_urn_with_flow(
    flow_urn=SNOWFLAKE_ETL_DATA_FLOW_URN, job_id=TEMPERATURE_ETL_1_DATA_JOB_ID
)
TEMPERATURE_ETL_1_DATA_JOB_TASK = Task(
    name=TEMPERATURE_ETL_1_DATA_JOB_ID,
    upstream_edges=[
        create_edge(
            source_urn=TEMPERATURE_ETL_1_DATA_JOB_URN,
            destination_urn=DAILY_TEMPERATURE_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
        ),
    ],
    downstream_edges=[
        create_edge(
            source_urn=TEMPERATURE_ETL_1_DATA_JOB_URN,
            destination_urn=MONTHLY_TEMPERATURE_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
        ),
    ],
)
TEMPERATURE_ETL_2_DATA_JOB_ID = "temperature_etl_2"
TEMPERATURE_ETL_2_DATA_JOB_URN = make_data_job_urn_with_flow(
    flow_urn=SNOWFLAKE_ETL_DATA_FLOW_URN, job_id=TEMPERATURE_ETL_2_DATA_JOB_ID
)
TEMPERATURE_ETL_2_DATA_JOB_TASK = Task(
    name=TEMPERATURE_ETL_2_DATA_JOB_ID,
    upstream_edges=[
        create_edge(
            source_urn=TEMPERATURE_ETL_2_DATA_JOB_URN,
            destination_urn=DAILY_TEMPERATURE_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
        ),
    ],
    downstream_edges=[
        create_edge(
            source_urn=TEMPERATURE_ETL_2_DATA_JOB_URN,
            destination_urn=MONTHLY_TEMPERATURE_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
        ),
    ],
)
AIRFLOW_SNOWFLAKE_ETL = Pipeline(
    platform=AIRFLOW_DATA_PLATFORM,
    name=SNOWFLAKE_ETL_DATA_FLOW_ID,
    tasks=[TEMPERATURE_ETL_1_DATA_JOB_TASK, TEMPERATURE_ETL_2_DATA_JOB_TASK],
)


def ingest_data_job_change(graph_client: DataHubGraph) -> None:
    # Case 2. Data job changes from temperature_etl_1 to temperature_etl_2
    emit_mcps(graph_client, create_node(DAILY_TEMPERATURE_DATASET))
    emit_mcps(graph_client, create_node(MONTHLY_TEMPERATURE_DATASET))
    emit_mcps(graph_client, create_nodes_and_edges(AIRFLOW_SNOWFLAKE_ETL))


def get_data_job_change_urns() -> List[str]:
    return [
        SNOWFLAKE_ETL_DATA_FLOW_URN,
        TEMPERATURE_ETL_1_DATA_JOB_URN,
        TEMPERATURE_ETL_2_DATA_JOB_URN,
        DAILY_TEMPERATURE_DATASET_URN,
        MONTHLY_TEMPERATURE_DATASET_URN,
    ]
