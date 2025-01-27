import argparse

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import ContainerUrn, DataPlatformUrn
from typing import Optional

def create_experiment(
    experiment_id: str,
    name: str,
    description: str,
    platform: str,
    custom_properties: dict,
    token: Optional[str],
    server_url: str = "http://localhost:8080",
) -> None:
    # Create basic experiment properties
    container_urn = ContainerUrn(guid=experiment_id)
    platform_urn = DataPlatformUrn(platform_name=platform)
    container_subtype = models.SubTypesClass(typeNames=["ML Experiment"])
    container_info = models.ContainerPropertiesClass(
        name=name,
        description=description,
        customProperties=custom_properties,
    )
    browse_path = models.BrowsePathsV2Class(path=[])
    platform_instance = models.DataPlatformInstanceClass(
        platform=str(platform_urn),
    )

    # Generate metadata change proposal
    mcps = MetadataChangeProposalWrapper.construct_many(
        entityUrn=str(container_urn),
        aspects=[container_subtype, container_info, browse_path, platform_instance],
    )

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
    # Example usage
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    create_experiment(
        experiment_id="airline_forecast_experiment",
        name="Airline Forecast Experiment",
        description="Experiment for forecasting airline passengers",
        platform="mlflow",
        custom_properties={"experiment_type": "forecasting"},
        token=args.token,
    )
