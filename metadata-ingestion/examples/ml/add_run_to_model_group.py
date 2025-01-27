import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def add_run_to_model_group(
        model_group_urn,
        run_urn,
        token: str = None,
        server_url: str = "http://localhost:8080"
) -> None:

    # Create model properties
    model_group_properties = models.MLModelGroupPropertiesClass(
        trainingJobs=[run_urn]
    )

    # Generate metadata change proposals
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=model_group_urn,
            entityType="mlModelGroup",
            aspectName="mlModelGroupProperties",
            aspect=model_group_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
    ]

    # Connect to DataHub and emit the changes
    graph = DataHubGraph(DatahubClientConfig(
        server=server_url,
        token=token,
        extra_headers={"Authorization": f"Bearer {token}"},
    ))

    with graph:
        for mcp in mcps:
            graph.emit(mcp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    add_run_to_model_group(
        model_group_urn="urn:li:mlModelGroup:my_test_model",
        run_urn="urn:li:dataProcessInstance:(urn:li:container:(urn:li:dataPlatform:local,my_experiment,PROD),my_run)",
    )