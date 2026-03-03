from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

datajob_urn = DataJobUrn(
    flow=DataFlowUrn(orchestrator="airflow", flow_id="flow1", cluster="PROD"),
    job_id="job1",
)
input_dataset_urn = DatasetUrn(platform="mysql", name="librarydb.member", env="PROD")
input_datajob_urn = DataJobUrn(
    flow=DataFlowUrn(orchestrator="airflow", flow_id="data_pipeline", cluster="PROD"),
    job_id="job0",
)
output_dataset_urn = DatasetUrn(
    platform="kafka", name="debezium.topics.librarydb.member_checkout", env="PROD"
)


# add datajob -> datajob lineage
client.lineage.add_lineage(
    upstream=input_datajob_urn,
    downstream=datajob_urn,
)

# add dataset -> datajob lineage
client.lineage.add_lineage(
    upstream=input_dataset_urn,
    downstream=datajob_urn,
)

# add datajob -> dataset lineage
client.lineage.add_lineage(
    upstream=datajob_urn,
    downstream=output_dataset_urn,
)
