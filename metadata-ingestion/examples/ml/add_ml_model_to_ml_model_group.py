import argparse
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


def add_model_version_to_model(
    model_urn: str,
    model_group_urn: str,
    token: Optional[str],
    server_url: str = "http://localhost:8080",
) -> None:
    # Create model properties
    model_properties = models.MLModelPropertiesClass(groups=[model_group_urn])

    # Generate metadata change proposals
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            entityType="mlModel",
            aspectName="mlModelProperties",
            aspect=model_properties,
            changeType=models.ChangeTypeClass.UPSERT,  # TODO: this overwrites the existing model properties.
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

    add_model_version_to_model(
        model_urn="urn:li:mlModel:(urn:li:dataPlatform:mlflow,arima_model_2,PROD)",
        model_group_urn="urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,airline_forecast_models,PROD)",
        token=args.token,
    )
