import argparse
from typing import Optional, List

from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceOutput,
)
from datahub.metadata.schema_classes import ChangeTypeClass


def create_dataset(
        platform: str,
        name: str,
        token: str,
        description: str = "",
        server_url: str = "http://localhost:8080",
) -> str:
    """Create a dataset in DataHub and return its URN.

    Args:
        platform: Platform identifier
        name: Dataset name
        token: DataHub access token
        description: Dataset description
        server_url: DataHub server URL

    Returns:
        str: Dataset URN
    """
    dataset = Dataset(id=name, platform=platform, name=name, description=description)

    graph = DataHubGraph(
        DatahubClientConfig(
            server=server_url,
            token=token,
            extra_headers={"Authorization": f"Bearer {token}"},
        )
    )

    with graph:
        for mcp in dataset.generate_mcp():
            graph.emit(mcp)

    if dataset.urn is None:
        raise ValueError(f"Failed to create dataset URN for {name}")

    return dataset.urn


def add_output_datasets_to_run(
        run_urn: str,
        dataset_urns: List[str],
        token: str,
        server_url: str = "http://localhost:8080",
) -> None:
    """Add output datasets to a data process instance run.

    Args:
        run_urn: Run URN
        dataset_urns: List of dataset URNs
        token: DataHub access token
        server_url: DataHub server URL
    """
    # Create the output aspect
    outputs_aspect = DataProcessInstanceOutput(outputs=dataset_urns)

    # Generate metadata change proposal
    mcp = MetadataChangeProposalWrapper(
        entityUrn=run_urn,
        entityType="dataProcessInstance",
        aspectName="dataProcessInstanceOutput",
        aspect=outputs_aspect,
        changeType=ChangeTypeClass.UPSERT,
    )

    # Connect to DataHub and emit the change
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

    # Example: Create datasets
    datasets = [
        {
            "platform": "s3",
            "name": "output_data",
            "description": "output_dataset for model",
        }
    ]

    # Create datasets and collect their URNs
    dataset_urns: List[str] = []
    for dataset_info in datasets:
        try:
            dataset_urn = create_dataset(
                platform=dataset_info["platform"],
                name=dataset_info["name"],
                description=dataset_info["description"],
                token=args.token,
            )
            dataset_urns.append(dataset_urn)
        except ValueError as e:
            print(f"Failed to create dataset: {e}")
            continue

    if dataset_urns:  # Only proceed if we have valid URNs
        add_output_datasets_to_run(
            run_urn="urn:li:dataProcessInstance:c29762bd7cc66e35414d95350454e542",
            dataset_urns=dataset_urns,
            token=args.token,
        )