from datahub.metadata.urns import DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dataflow import DataFlow

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="data_pipeline",
    platform="airflow",
)

client.lineage.add_lineage(
    upstream=DataJobUrn(flow=dataflow.urn, job_id="data_job_1"),
    downstream=DatasetUrn(platform="postgres", name="raw_data"),
)

client.entities.upsert(dataflow)
