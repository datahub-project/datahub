from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

# Query for changes to the Dataset.
operations = operation_client.query_operations(
    urn=dataset_urn,
    # limit=5,
    # start_time_millis=<timestamp>,
    # end_time_millis=<timestamo>
)
