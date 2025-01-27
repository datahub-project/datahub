import argparse

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import MlModelUrn, VersionSetUrn


def create_model(
    model_id: str,
    name: str,
    description: str,
    platform: str,
    version: str,
    token: str,
    server_url: str = "http://localhost:8080",
) -> None:
    # Create URNs
    model_urn = MlModelUrn(platform=platform, name=model_id)
    version_set_urn = VersionSetUrn(
        id=f"mlmodel_{model_id}_versions", entity_type="mlModel"
    )

    # Create model properties
    model_properties = models.MLModelPropertiesClass(
        name=name,
        description=description,
    )

    # Create version properties
    version_properties = models.VersionPropertiesClass(
        version=models.VersionTagClass(versionTag=version),
        versionSet=str(version_set_urn),
        sortId="AAAAAAAA",
    )

    # Create version set properties
    version_set_properties = models.VersionSetPropertiesClass(
        latest=str(model_urn),
        versioningScheme="ALPHANUMERIC_GENERATED_BY_DATAHUB",
    )

    # Generate metadata change proposals
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            entityType="mlModel",
            aspectName="mlModelProperties",
            aspect=model_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=str(version_set_urn),
            entityType="versionSet",
            aspectName="versionSetProperties",
            aspect=version_set_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=str(model_urn),
            entityType="mlModel",
            aspectName="versionProperties",
            aspect=version_properties,
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

    create_model(
        model_id="arima_model_1",
        name="ARIMA Model 1",
        description="ARIMA model for airline passenger forecasting",
        platform="mlflow",
        version="1.0",
        token=args.token,
    )
