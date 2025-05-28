from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

dataset_urn = DatasetUrn(platform="snowflake", name="upstream_table")
datajob_urn = DataJobUrn(
    job_id="example_datajob",
    flow=DataFlowUrn(orchestrator="airflow", flow_id="example_dag", cluster="PROD"),
)

client.lineage.add_lineage(upstream=datajob_urn, downstream=dataset_urn)

# you can add datajob lineage with following combinations:
# 1. dataset -> datajob
# 2. datajob -> dataset
# 3. datajob -> datajob
