from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

operation_type = "INSERT"
source_type = "DATA_PROCESS"  # Source of the operation (data platform or DAG task)

# Report a change operation for the Dataset.
operation_client.report_operation(
    urn=dataset_urn, operation_type=operation_type, source_type=source_type
)
