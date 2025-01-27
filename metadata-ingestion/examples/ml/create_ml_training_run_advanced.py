import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import argparse
import time


def create_training_run(
        run_id: str,
        name: str,
        description: str,
        platform: str,
        experiment_id: str,  # Need this to link run to experiment
        custom_properties: dict,
        metrics: list,  # List of dicts with name, value, description
        hyperparameters: list,  # List of dicts with name, value, description
        start_time: int = None,  # Unix timestamp in milliseconds
        duration_minutes: int = None,  # Duration in minutes
        status: str = "SUCCESS",  # SUCCESS or FAILURE
        token: str = None,
        server_url: str = "http://localhost:8080"
) -> None:
    # Create URNs
    platform_urn = f"urn:li:dataPlatform:{platform}"
    experiment_urn = f"urn:li:container:({platform_urn},{experiment_id},PROD)"
    instance_urn = f"urn:li:dataProcessInstance:({experiment_urn},{run_id})"

    current_time = int(time.time() * 1000)
    start_time = start_time or current_time
    end_time = start_time + (duration_minutes * 60 * 1000) if duration_minutes else current_time

    # Create DPI properties
    dpi_properties = models.DataProcessInstancePropertiesClass(
        name=name,
        description=description,
        customProperties=custom_properties,
        created=models.AuditStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        )
    )

    # Create ML training run properties
    ml_training_properties = models.MLTrainingRunPropertiesClass(
        id=run_id,
        hyperParams=[
            models.MLHyperParamClass(
                name=h["name"],
                value=str(h["value"]),
                description=h["description"]
            ) for h in hyperparameters
        ],
        trainingMetrics=[
            models.MLMetricClass(
                name=m["name"],
                value=str(m["value"]),
                description=m["description"]
            ) for m in metrics
        ],
    )

    # Create subtype
    subtype = models.SubTypesClass(
        typeNames=["ML Training Run"]
    )

    # Create start event
    start_event = models.DataProcessInstanceStartEventClass(
        timestampMillis=start_time,
    )

    # Create end event
    end_event = models.DataProcessInstanceEndEventClass(
        timestampMillis=end_time,
        startTimeMillis=start_time,
        result=status,
        resultType=status,
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
        MetadataChangeProposalWrapper(
            entityUrn=instance_urn,
            entityType="dataProcessInstance",
            aspectName="mlTrainingRunProperties",
            aspect=ml_training_properties,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=instance_urn,
            entityType="dataProcessInstance",
            aspectName="dataProcessInstanceStartEvent",
            aspect=start_event,
            changeType=models.ChangeTypeClass.UPSERT,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=instance_urn,
            entityType="dataProcessInstance",
            aspectName="dataProcessInstanceEndEvent",
            aspect=end_event,
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

    # Example metrics and hyperparameters
    example_metrics = [
        {
            "name": "accuracy",
            "value": 0.85,
            "description": "Test accuracy"
        },
        {
            "name": "f1_score",
            "value": 0.83,
            "description": "Test F1 score"
        }
    ]

    example_hyperparameters = [
        {
            "name": "n_estimators",
            "value": 100,
            "description": "Number of trees"
        },
        {
            "name": "max_depth",
            "value": 10,
            "description": "Maximum tree depth"
        }
    ]

    create_training_run(
        run_id="run_1",
        name="Training Run 1",
        description="First training run for airline passenger forecasting",
        platform="mlflow",
        experiment_id="airline_forecast_experiment",  # Should match the experiment ID from create_experiment
        custom_properties={
            "framework": "statsmodels",
            "python_version": "3.8"
        },
        metrics=example_metrics,
        hyperparameters=example_hyperparameters,
        start_time=int(time.time() * 1000),
        duration_minutes=45,
        status="SUCCESS",
        token=args.token
    )