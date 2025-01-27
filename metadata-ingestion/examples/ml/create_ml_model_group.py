import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def create_model_group(
        group_id: str,
        name: str,
        description: str,
        platform: str,
        custom_properties: dict,
        token: str,
        server_url: str = "http://localhost:8080"
) -> None:
    # Create basic model group properties
    platform_urn = f"urn:li:dataPlatform:{platform}"
    model_group_urn = f"urn:li:mlModelGroup:({platform_urn},{group_id})"

    current_time = int(time.time() * 1000)
    model_group_info = models.MLModelGroupPropertiesClass(
        name=name,
        description=description,
        customProperties=custom_properties,
        created=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
    )

    # Generate metadata change proposal
    mcp = MetadataChangeProposalWrapper(
        entityUrn=model_group_urn,
        entityType="mlModelGroup",
        aspectName="mlModelGroupProperties",
        aspect=model_group_info,
        changeType=models.ChangeTypeClass.UPSERT,
    )

    # Connect to DataHub and emit the changes
    graph = DataHubGraph(DatahubClientConfig(
        server=server_url,
        token=token,
        extra_headers={"Authorization": f"Bearer {token}"},
    ))

    with graph:
        graph.emit(mcp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    create_model_group(
        group_id="airline_forecast_models",
        name="Airline Forecast Models",
        description="ML models for airline passenger forecasting",
        platform="mlflow",
        custom_properties={
            "stage": "production",
            "team": "data_science"
        },
        token=args.token
    )