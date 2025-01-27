import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def add_input_dataset_to_run(
        run_urn: str,
        dataset_urn: str,
        token: str,
        server_url: str = "http://localhost:8080"
) -> None:

    # Create model propegp rties
    model_properties = models.MLModelPropertiesClass(
        groups=[model_group_urn]
    )

    # Generate metadata change proposals
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            entityType="mlModel",
            aspectName="mlModelProperties",
            aspect=model_properties,
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

    add_input_dataset_to_run(
        run_urn="urn:li",
        dataset_urn="unr:li"
        token=args.token
    )