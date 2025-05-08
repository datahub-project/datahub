from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sdk.lineage_client import LineageClient
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()
lineage_client = LineageClient(client=client)

flow_urn = DataFlowUrn(orchestrator="airflow", flow_id="data_pipeline", cluster="PROD")

lineage_client.add_datajob_lineage(
    datajob=DataJobUrn(flow=flow_urn, job_id="data_pipeline"),
    upstreams=[DataJobUrn(flow=flow_urn, job_id="extract_job")],
)
