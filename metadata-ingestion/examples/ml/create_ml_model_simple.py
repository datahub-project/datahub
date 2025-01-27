import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def create_model(
        model_id: str,
        name: str,
        description: str,
        platform: str,
        version: str,
        token: str,
        server_url: str = "http://localhost:8080"
) -> None:
    # Create URNs
    platform_urn = f"urn:li:dataPlatform:{platform}"
    model_urn = f"urn:li:mlModel:({platform_urn},{model_id})"
    version_set_urn = f"urn:li:versionSet:(mlmodel_{model_id}_versions,mlModel)"

    current_time = int(time.time() * 1000)

    # Create model properties
    model_properties = models.MLModelPropertiesClass(
        name=name,
        description=description,
    )

    # Create version properties
    version_properties = models.VersionPropertiesClass(
        version=models.VersionTagClass(versionTag=version),
        versionSet=version_set_urn,
        sortId=version,
    )

    # Create version set properties
    version_set_properties = models.VersionSetPropertiesClass(
        latest=model_urn,
        versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",
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
        MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            entityType="mlModel",
            aspectName="versionProperties",
            aspect=version_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=version_set_urn,
            entityType="versionSet",
            aspectName="versionSetProperties",
            aspect=version_set_properties,
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


    create_model(
        model_id="arima_model_1",
        name="ARIMA Model 1",
        description="ARIMA model for airline passenger forecasting",
        platform="mlflow",
        version="1.0",
        token=args.token
    )