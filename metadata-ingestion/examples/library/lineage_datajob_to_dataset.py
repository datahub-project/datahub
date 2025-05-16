from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

flow_urn = DataFlowUrn(orchestrator="airflow", flow_id="data_pipeline", cluster="PROD")

client.lineage.add_datajob_lineage(
    datajob=DataJobUrn(flow=flow_urn, job_id="data_pipeline"),
    upstreams=[DatasetUrn(platform="postgres", name="raw_data")],
    downstreams=[DatasetUrn(platform="snowflake", name="processed_data")],
)
