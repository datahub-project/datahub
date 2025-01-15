import time
from typing import Iterable
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.urns import MlModelGroupUrn, MlModelUrn
from datahub.ingestion.graph.client import get_default_graph

def create_model_group() -> tuple[MlModelGroupUrn, MetadataChangeProposalWrapper]:
    """Create a model group and return its URN and MCP"""
    model_group_urn = MlModelGroupUrn(platform="mlflow", name="simple_model_group")
    current_time = int(time.time() * 1000)
    
    model_group_info = models.MLModelGroupPropertiesClass(
        description="Simple ML model group example",
        customProperties={
            "stage": "production",
            "team": "data_science",
        },
        created=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
        lastModified=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
        trainingJobs=[],
    )

    model_group_mcp = MetadataChangeProposalWrapper(
        entityUrn=str(model_group_urn),
        aspect=model_group_info,
    )
    
    return model_group_urn, model_group_mcp

def create_single_model(
    model_name: str,
    model_group_urn: str,
) -> MetadataChangeProposalWrapper:
    """Create a single ML model and return its MCP"""
    model_urn = MlModelUrn(platform="mlflow", name=model_name)
    current_time = int(time.time() * 1000)
    
    # Define example metrics and hyperparameters
    training_metrics = [
        models.MLMetricClass(
            name="accuracy",
            value="0.95",
            description="Test accuracy"
        ),
        models.MLMetricClass(
            name="f1_score",
            value="0.93",
            description="Test F1 score"
        )
    ]
    
    hyper_params = [
        models.MLHyperParamClass(
            name="n_estimators",
            value="100",
            description="Number of trees"
        ),
        models.MLHyperParamClass(
            name="max_depth",
            value="10",
            description="Maximum tree depth"
        )
    ]

    model_info = models.MLModelPropertiesClass(
        name=model_name,
        description="Simple example ML model",
        version=models.VersionTagClass(versionTag="1"),
        groups=[str(model_group_urn)],
        date=current_time,
        lastModified=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
        created=models.TimeStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        ),
        tags=["stage:production", "team:data_science"],
        trainingMetrics=training_metrics,
        hyperParams=hyper_params,
        trainingJobs=[],
        downstreamJobs=[],
    )

    return MetadataChangeProposalWrapper(
        entityUrn=str(model_urn),
        aspect=model_info,
    )

def main():
    # Create the model group and model
    model_group_urn, model_group_mcp = create_model_group()
    model_mcp = create_single_model("simple_model", str(model_group_urn))
    
    # Emit the metadata to DataHub
    with get_default_graph() as graph:
        graph.emit(model_group_mcp)
        graph.emit(model_mcp)
        print("Successfully created model group and model in DataHub")

if __name__ == "__main__":
    main()