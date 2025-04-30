from datahub.sdk import DataHubClient, DatasetUrn
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

datajob_urn = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,etl_pipeline,PROD),transform_data)"
)
input_dataset_urn = DatasetUrn(platform="postgres", name="raw_data")
output_dataset_urn = DatasetUrn(platform="snowflake", name="processed_data")

lineage_client.add_datajob_lineage(
    datajob=datajob_urn, upstreams=[input_dataset_urn], downstreams=[output_dataset_urn]
)
