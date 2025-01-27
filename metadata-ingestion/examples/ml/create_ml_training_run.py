import argparse
import time

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstancePropertiesClass,
)
from typing import Optional

def create_minimal_training_run(
    run_id: str, name: str, token: Optional[str], server_url: str = "http://localhost:8080"
) -> None:
    # Create a container key (required for DataProcessInstance)

    dpi_urn = f"urn:li:dataProcessInstance:{run_id}"

    dpi_subtypes = models.SubTypesClass(typeNames=["ML Training Run"])

    # Create the properties aspect
    dpi_props = DataProcessInstancePropertiesClass(
        name=name,
        created=AuditStampClass(
            time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
        ),
    )
    # Add the properties MCP
    mcps = [
        MetadataChangeProposalWrapper(entityUrn=str(dpi_urn), aspect=dpi_props),
        MetadataChangeProposalWrapper(entityUrn=str(dpi_urn), aspect=dpi_subtypes),
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

    create_minimal_training_run(
        run_id="simple_training_run_3", name="Simple Training Run 3", token=args.token
    )
