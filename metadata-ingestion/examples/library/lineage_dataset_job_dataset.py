from datahub.metadata.urns import DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="flow1",
    platform="airflow",
)

datajob = DataJob(
    name="job1",
    flow=dataflow,
    inlets=[
        DatasetUrn(platform="mysql", name="librarydb.member", env="PROD"),
        DatasetUrn(platform="mysql", name="librarydb.checkout", env="PROD"),
    ],
    outlets=[
        DatasetUrn(
            platform="kafka",
            name="debezium.topics.librarydb.member_checkout",
            env="PROD",
        )
    ],
)

client.entities.upsert(datajob)

client.lineage.add_lineage(
    upstream=DataJobUrn(flow=dataflow.urn, job_id="job0"),
    downstream=datajob.urn,
)
