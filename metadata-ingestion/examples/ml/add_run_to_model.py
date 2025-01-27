import argparse

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from typing import Optional


def add_run_to_model(
    model_urn: str, run_urn: str, token: Optional[str], server_url: str = "http://localhost:8080"
) -> None:
    # Create model properties
    model_properties = models.MLModelPropertiesClass(
        trainingJobs=[run_urn], downstreamJobs=[run_urn]
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
    graph = DataHubGraph(
        DatahubClientConfig(
            server=server_url,
            token=token,
            extra_headers={"Authorization": f"Bearer {token}"},
        )
    )

    with graph:
        for mcp in mcps:
            graph.emit(mcp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    add_run_to_model(
        model_urn="urn:li:mlModel:(urn:li:dataPlatform:mlflow,arima_model_2,PROD)",
        run_urn="urn:li:dataProcessInstance:c29762bd7cc66e35414d95350454e542",
        token=args.token,
    )
