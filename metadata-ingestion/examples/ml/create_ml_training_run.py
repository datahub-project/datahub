import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def create_training_run(
        run_id: str,
        name: str,
        platform: str,
        experiment_id: str,  # Need this to link run to experiment
        token: str,
        server_url: str = "http://localhost:8080"
) -> None:
    # Create URNs
    platform_urn = f"urn:li:dataPlatform:{platform}"
    experiment_urn = f"urn:li:container:({platform_urn},{experiment_id},PROD)"
    instance_urn = f"urn:li:dataProcessInstance:({experiment_urn},{run_id})"

    current_time = int(time.time() * 1000)

    # Create DPI properties
    dpi_properties = models.DataProcessInstancePropertiesClass(
        name=name,
        created=models.AuditStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        )
    )

    # Create subtype
    subtype = models.SubTypesClass(
        typeNames=["ML Training Run"]
    )


    # Generate metadata change proposals
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=instance_urn,
            entityType="dataProcessInstance",
            aspectName="dataProcessInstanceProperties",
            aspect=dpi_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=instance_urn,
            entityType="dataProcessInstance",
            aspectName="subTypes",
            aspect=subtype,
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


    create_training_run(
        run_id="run_1",
        name="Training Run 1",
        platform="mlflow",
        experiment_id="airline_forecast_experiment",  # Should match the experiment ID from create_experiment
        token=args.token
    )