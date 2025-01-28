import argparse
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


def add_run_to_model_group(
    model_group_urn: str,
    run_urn: str,
    token: Optional[str],
    server_url: str = "http://localhost:8080",
) -> None:
    # Create model properties
    model_group_properties = models.MLModelGroupPropertiesClass(
        trainingJobs=[run_urn], downstreamJobs=[run_urn]
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

    add_run_to_model_group(
        model_group_urn="urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,airline_forecast_models,PROD)",
        run_urn="urn:li:dataProcessInstance:c29762bd7cc66e35414d95350454e542",
        token=args.token,
    )
