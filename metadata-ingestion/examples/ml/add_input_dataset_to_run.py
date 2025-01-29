import argparse
from typing import Optional

from datahub.api.entities.dataset.dataset import Dataset
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceInput,
)
from datahub.metadata.schema_classes import ChangeTypeClass


def create_dataset(
    platform: str,
    name: str,
    token: str,
    description: Optional[str] = "",
    server_url: str = "http://localhost:8080",
) -> Optional[str]:
    """
    Create a dataset in DataHub and return its URN
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

    return dataset.urn


def add_input_datasets_to_run(
    run_urn: str,
    dataset_urns: list,
    token: str,
    server_url: str = "http://localhost:8080",
) -> None:
    """
    Add input datasets to a data process instance run
    """
    # Create the input aspect
    inputs_aspect = DataProcessInstanceInput(inputs=dataset_urns)

    # Generate metadata change proposal
    mcp = MetadataChangeProposalWrapper(
        entityUrn=run_urn,
        entityType="dataProcessInstance",
        aspectName="dataProcessInstanceInput",
        aspect=inputs_aspect,
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

    # Example: Create two datasets
    datasets = [
        {
            "platform": "hdfs",
            "name": "training_data",
            "description": "Training dataset for model",
        },
        {
            "platform": "s3",
            "name": "validation_data",
            "description": "Validation dataset for model",
        },
    ]

    # Create datasets and collect their URNs
    # if the dataset already exists, comment out the create_dataset function
    dataset_urns = []
    for dataset_info in datasets:
        dataset_urn = create_dataset(
            platform=dataset_info["platform"],
            name=dataset_info["name"],
            description=dataset_info["description"],
            token=args.token,
        )
        dataset_urns.append(dataset_urn)

    # Link datasets to the run
    add_input_datasets_to_run(
        run_urn="urn:li:dataProcessInstance:c29762bd7cc66e35414d95350454e542",
        dataset_urns=dataset_urns,
        token=args.token,
    )
