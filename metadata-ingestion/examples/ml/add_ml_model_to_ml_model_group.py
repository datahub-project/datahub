import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def add_model_version_to_model(
        model_urn,
        model_group_urn,
        token: str,
        server_url: str = "http://localhost:8080"
) -> None:

    # Create model properties
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

    add_model_version_to_model(
        model_urn="urn:li:mlModel:(urn:li:dataPlatform:local,my_model)",
        model_group_urn="urn:li:mlModelGroup:(urn:li:dataPlatform:local,my_model_group)",
        token=args.token
    )