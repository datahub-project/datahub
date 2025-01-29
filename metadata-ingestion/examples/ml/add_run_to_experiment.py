import argparse
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


def add_run_to_experiment(
    run_urn: str,
    experiment_urn: str,
    token: Optional[str],
    server_url: str = "http://localhost:8080",
) -> None:
    mcp = MetadataChangeProposalWrapper(
        entityUrn=run_urn,
        aspect=models.ContainerClass(container=experiment_urn),
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
        graph.emit(mcp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    add_run_to_experiment(
        run_urn="urn:li:dataProcessInstance:d5c707514d6e15a2c992204bdfdbf1a1",
        experiment_urn="urn:li:container:airline_forecast_experiment",
        token=args.token,
    )
