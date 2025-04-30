from datahub.sdk import DataHubClient
from datahub.sdk.lineage_client import LineageClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

transform_job_urn = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,data_pipeline,PROD),transform_job)"
)
upstream_job_urn = (
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,data_pipeline,PROD),extract_job)"
)

lineage_client.add_datajob_lineage(
    datajob=transform_job_urn,
    upstreams=[upstream_job_urn],
)
