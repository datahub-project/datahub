import argparse
import time

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import MlModelUrn, VersionSetUrn


def create_model(
    model_id: str,
    name: str,
    description: str,
    platform: str,
    custom_properties: dict,
    version: str,
    metrics: list,  # List of dicts with name, value, description
    hyperparameters: list,  # List of dicts with name, value, description
    token: str,
    server_url: str = "http://localhost:8080",
) -> None:
    # Create URNs
    model_urn = MlModelUrn(platform=platform, name=model_id)
    version_set_urn = VersionSetUrn(
        id=f"mlmodel_{model_id}_versions", entity_type="mlModel"
    )

    current_time = int(time.time() * 1000)

    # Create model properties
    model_properties = models.MLModelPropertiesClass(
        name=name,
        description=description,
        version=models.VersionTagClass(versionTag=version),
        customProperties=custom_properties,
        created=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=current_time, actor="urn:li:corpuser:datahub"
        ),
        trainingMetrics=[
            models.MLMetricClass(
                name=m["name"], value=str(m["value"]), description=m["description"]
            )
            for m in metrics
        ],
        hyperParams=[
            models.MLHyperParamClass(
                name=h["name"], value=str(h["value"]), description=h["description"]
            )
            for h in hyperparameters
        ],
    )

    # Create version properties
    version_properties = models.VersionPropertiesClass(
        version=models.VersionTagClass(versionTag=version),
        versionSet=str(version_set_urn),
        aliases=[models.VersionTagClass(versionTag="champion")],
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

    # Example metrics and hyperparameters
    example_metrics = [
        {"name": "accuracy", "value": 0.85, "description": "Test accuracy"},
        {"name": "f1_score", "value": 0.83, "description": "Test F1 score"},
    ]

    example_hyperparameters = [
        {"name": "n_estimators", "value": 100, "description": "Number of trees"},
        {"name": "max_depth", "value": 10, "description": "Maximum tree depth"},
    ]

    create_model(
        model_id="arima_model_2",
        name="ARIMA Model 2",
        description="ARIMA model for airline passenger forecasting",
        platform="mlflow",
        custom_properties={"framework": "statsmodels", "python_version": "3.8"},
        version="2.0",
        metrics=example_metrics,
        hyperparameters=example_hyperparameters,
        token=args.token,
    )
