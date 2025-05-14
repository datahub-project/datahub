from typing import List

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from tests.setup.lineage.constants import (
    AIRFLOW_DATA_PLATFORM,
    BQ_DATA_PLATFORM,
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

# Constants for Case 1
TRANSACTIONS_DATASET_ID = "transactions.transactions"
TRANSACTIONS_DATASET_URN = make_dataset_urn(
    platform=BQ_DATA_PLATFORM, name=TRANSACTIONS_DATASET_ID
)
TRANSACTIONS_DATASET = Dataset(
    id=TRANSACTIONS_DATASET_ID,
    platform=BQ_DATA_PLATFORM,
    schema_metadata=[
        Field(name="user_id", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(
            name="transaction_id", type=SchemaFieldDataTypeClass(type=StringTypeClass())
        ),
        Field(
            name="transaction_date",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        ),
        Field(name="amount", type=SchemaFieldDataTypeClass(type=NumberTypeClass())),
    ],
)

USER_PROFILE_DATASET_ID = "transactions.user_profile"
USER_PROFILE_DATASET_URN = make_dataset_urn(
    platform=BQ_DATA_PLATFORM, name=USER_PROFILE_DATASET_ID
)
USER_PROFILE_DATASET = Dataset(
    id=USER_PROFILE_DATASET_ID,
    platform=BQ_DATA_PLATFORM,
    schema_metadata=[
        Field(name="user_id", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(name="zip_code", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
    ],
)

AGGREGATED_TRANSACTIONS_DATASET_ID = "transactions.aggregated_transactions"
AGGREGATED_TRANSACTIONS_DATASET_URN = make_dataset_urn(
    platform=BQ_DATA_PLATFORM, name=AGGREGATED_TRANSACTIONS_DATASET_ID
)
AGGREGATED_TRANSACTIONS_DATASET = Dataset(
    id=AGGREGATED_TRANSACTIONS_DATASET_ID,
    platform=BQ_DATA_PLATFORM,
    schema_metadata=[
        Field(name="user_id", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(name="zip_code", type=SchemaFieldDataTypeClass(type=StringTypeClass())),
        Field(
            name="total_amount", type=SchemaFieldDataTypeClass(type=StringTypeClass())
        ),
    ],
)

BQ_ETL_DATA_FLOW_ID = "bq_etl"
BQ_ETL_DATA_FLOW_URN = make_data_flow_urn(
    orchestrator=AIRFLOW_DATA_PLATFORM, flow_id=BQ_ETL_DATA_FLOW_ID
)
TRANSACTION_ETL_DATA_JOB_ID = "transaction_etl"
TRANSACTION_ETL_DATA_JOB_URN = make_data_job_urn_with_flow(
    flow_urn=BQ_ETL_DATA_FLOW_URN, job_id=TRANSACTION_ETL_DATA_JOB_ID
)
TRANSACTION_ETL_DATA_JOB_TASK = Task(
    name=TRANSACTION_ETL_DATA_JOB_ID,
    upstream_edges=[
        create_edge(
            source_urn=TRANSACTION_ETL_DATA_JOB_URN,
            destination_urn=TRANSACTIONS_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
        ),
        create_edge(
            source_urn=TRANSACTION_ETL_DATA_JOB_URN,
            destination_urn=USER_PROFILE_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
        ),
    ],
    downstream_edges=[
        create_edge(
            source_urn=TRANSACTION_ETL_DATA_JOB_URN,
            destination_urn=AGGREGATED_TRANSACTIONS_DATASET_URN,
            created_timestamp_millis=TIMESTAMP_MILLIS_EIGHT_DAYS_AGO,
            updated_timestamp_millis=TIMESTAMP_MILLIS_ONE_DAY_AGO,
        ),
    ],
)
AIRFLOW_BQ_ETL = Pipeline(
    platform=AIRFLOW_DATA_PLATFORM,
    name=BQ_ETL_DATA_FLOW_ID,
    tasks=[TRANSACTION_ETL_DATA_JOB_TASK],
)


def ingest_input_datasets_change(graph_client: DataHubGraph) -> None:
    # Case 2. transactions_etl has one upstream originally (transactions), but later has both transactions and
    # user_profile.
    emit_mcps(graph_client, create_node(TRANSACTIONS_DATASET))
    emit_mcps(graph_client, create_node(USER_PROFILE_DATASET))
    emit_mcps(graph_client, create_node(AGGREGATED_TRANSACTIONS_DATASET))
    emit_mcps(graph_client, create_nodes_and_edges(AIRFLOW_BQ_ETL))


def get_input_datasets_change_urns() -> List[str]:
    return [
        BQ_ETL_DATA_FLOW_URN,
        TRANSACTION_ETL_DATA_JOB_URN,
        TRANSACTIONS_DATASET_URN,
        USER_PROFILE_DATASET_URN,
        AGGREGATED_TRANSACTIONS_DATASET_URN,
    ]
