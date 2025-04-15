import argparse
from datetime import datetime

from dh_ai_client import DatahubAIClient

from datahub.emitter.mcp_builder import (
    ContainerKey,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import RunResultType
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataProcessInstancePropertiesClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
)
from datahub.metadata.urns import (
    CorpUserUrn,
    DataProcessInstanceUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

parser = argparse.ArgumentParser()
parser.add_argument("--token", required=False, help="DataHub access token")
parser.add_argument(
    "--server_url",
    required=False,
    default="http://localhost:8080",
    help="DataHub server URL (defaults to http://localhost:8080)",
)
args = parser.parse_args()

# Initialize client
client = DatahubAIClient(token=args.token, server_url=args.server_url)

# Use a unique prefix for all IDs to avoid conflicts
prefix = "0414"

# Define all entity IDs upfront
# Basic entity IDs
basic_model_group_id = f"{prefix}_basic_group"
basic_model_id = f"{prefix}_basic_model"
basic_experiment_id = f"{prefix}_basic_experiment"
basic_run_id = f"{prefix}_basic_run"
basic_dataset_id = f"{prefix}_basic_dataset"

# Advanced entity IDs
advanced_model_group_id = f"{prefix}_airline_forecast_models_group"
advanced_model_id = f"{prefix}_arima_model"
advanced_experiment_id = f"{prefix}_airline_forecast_experiment"
advanced_run_id = f"{prefix}_simple_training_run"
advanced_input_dataset_id = f"{prefix}_iris_input"
advanced_output_dataset_id = f"{prefix}_iris_output"

# Display names with prefix
basic_model_group_name = f"{prefix} Basic Group"
basic_model_name = f"{prefix} Basic Model"
basic_experiment_name = f"{prefix} Basic Experiment"
basic_run_name = f"{prefix} Basic Run"
basic_dataset_name = f"{prefix} Basic Dataset"

advanced_model_group_name = f"{prefix} Airline Forecast Models Group"
advanced_model_name = f"{prefix} ARIMA Model"
advanced_experiment_name = f"{prefix} Airline Forecast Experiment"
advanced_run_name = f"{prefix} Simple Training Run"
advanced_input_dataset_name = f"{prefix} Iris Training Input Data"
advanced_output_dataset_name = f"{prefix} Iris Model Output Data"


def create_basic_model_group():
    """Create a basic model group."""
    print("Creating basic model group...")
    basic_model_group = MLModelGroup(
        id=basic_model_group_id,
        platform="mlflow",
        name=basic_model_group_name,
    )
    client._emit_mcps(basic_model_group.as_mcps())
    return basic_model_group


def create_advanced_model_group():
    """Create an advanced model group."""
    print("Creating advanced model group...")
    advanced_model_group = MLModelGroup(
        id=advanced_model_group_id,
        platform="mlflow",
        name=advanced_model_group_name,
        description="Group of models for airline passenger forecasting",
        created=datetime.now(),
        last_modified=datetime.now(),
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        external_url="https://www.linkedin.com/in/datahub",
        tags=["urn:li:tag:forecasting", "urn:li:tag:arima"],
        terms=["urn:li:glossaryTerm:forecasting"],
        custom_properties={"team": "forecasting"},
    )
    client._emit_mcps(advanced_model_group.as_mcps())
    return advanced_model_group


def create_basic_model():
    """Create a basic model."""
    print("Creating basic model...")
    basic_model = MLModel(
        id=basic_model_id,
        platform="mlflow",
        name=basic_model_name,
    )
    client._emit_mcps(basic_model.as_mcps())
    return basic_model


def create_advanced_model():
    """Create an advanced model."""
    print("Creating advanced model...")
    advanced_model = MLModel(
        id=advanced_model_id,
        platform="mlflow",
        name=advanced_model_name,
        description="ARIMA model for airline passenger forecasting",
        created=datetime.now(),
        last_modified=datetime.now(),
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        external_url="https://www.linkedin.com/in/datahub",
        tags=["urn:li:tag:forecasting", "urn:li:tag:arima"],
        terms=["urn:li:glossaryTerm:forecasting"],
        custom_properties={"team": "forecasting"},
        version="1",
        aliases=["champion"],
        hyper_params={"learning_rate": "0.01"},
        training_metrics={"accuracy": "0.9"},
    )
    client._emit_mcps(advanced_model.as_mcps())
    return advanced_model


def create_basic_experiment():
    """Create a basic experiment."""
    print("Creating basic experiment...")
    basic_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=basic_experiment_id),
        display_name=basic_experiment_name,
    )
    client._emit_mcps(basic_experiment.as_mcps())
    return basic_experiment


def create_advanced_experiment():
    """Create an advanced experiment."""
    print("Creating advanced experiment...")
    advanced_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=advanced_experiment_id),
        display_name=advanced_experiment_name,
        description="Experiment to forecast airline passenger numbers",
        extra_properties={"team": "forecasting"},
        created=datetime(2025, 4, 9, 22, 30),
        last_modified=datetime(2025, 4, 9, 22, 30),
        subtype=MLAssetSubTypes.MLFLOW_EXPERIMENT,
    )
    client._emit_mcps(advanced_experiment.as_mcps())
    return advanced_experiment


def create_basic_training_run():
    """Create a basic training run."""
    print("Creating basic training run...")
    basic_run_urn = client.create_training_run(
        run_id=basic_run_id,
        run_name=basic_run_name,
    )
    return basic_run_urn


def create_advanced_training_run():
    """Create an advanced training run."""
    print("Creating advanced training run...")
    advanced_run_urn = client.create_training_run(
        run_id=advanced_run_id,
        properties=DataProcessInstancePropertiesClass(
            name=advanced_run_name,
            created=AuditStampClass(
                time=1628580000000, actor="urn:li:corpuser:datahub"
            ),
            customProperties={"team": "forecasting"},
        ),
        training_run_properties=MLTrainingRunPropertiesClass(
            id=advanced_run_id,
            outputUrls=["s3://my-bucket/output"],
            trainingMetrics=[MLMetricClass(name="accuracy", value="0.9")],
            hyperParams=[MLHyperParamClass(name="learning_rate", value="0.01")],
            externalUrl="https:localhost:5000",
        ),
        run_result=RunResultType.FAILURE,
        start_timestamp=1628580000000,
        end_timestamp=1628580001000,
    )
    return advanced_run_urn


def create_basic_dataset():
    """Create a basic dataset."""
    print("Creating basic dataset...")
    basic_input_dataset = Dataset(
        platform="snowflake",
        name=basic_dataset_id,
        display_name=basic_dataset_name,
    )
    client._emit_mcps(basic_input_dataset.as_mcps())
    return basic_input_dataset


def create_advanced_datasets():
    """Create advanced datasets."""
    print("Creating advanced datasets...")
    advanced_input_dataset = Dataset(
        platform="snowflake",
        name=advanced_input_dataset_id,
        description="Raw Iris dataset used for training ML models",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name=advanced_input_dataset_name,
        tags=["urn:li:tag:ml_data", "urn:li:tag:iris"],
        terms=["urn:li:glossaryTerm:raw_data"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "data_source": "UCI Repository",
            "records": "150",
            "features": "4",
        },
    )
    client._emit_mcps(advanced_input_dataset.as_mcps())

    advanced_output_dataset = Dataset(
        platform="snowflake",
        name=advanced_output_dataset_id,
        description="Processed Iris dataset with model predictions",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name=advanced_output_dataset_name,
        tags=["urn:li:tag:ml_data", "urn:li:tag:predictions"],
        terms=["urn:li:glossaryTerm:model_output"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "model_version": "1.0",
            "records": "150",
            "accuracy": "0.95",
        },
    )
    client._emit_mcps(advanced_output_dataset.as_mcps())
    return advanced_input_dataset, advanced_output_dataset


# Split relationship functions into individual top-level functions
def add_model_to_model_group(model, model_group):
    """Add model to model group relationship."""
    print("Adding model to model group...")
    model.add_group(model_group.urn)
    client._emit_mcps(model.as_mcps())


def add_run_to_experiment(run_urn, experiment):
    """Add run to experiment relationship."""
    print("Adding run to experiment...")
    client.add_run_to_experiment(run_urn=run_urn, experiment_urn=str(experiment.urn))


def add_run_to_model(model, run_id):
    """Add run to model relationship."""
    print("Adding run to model...")
    model.add_training_job(DataProcessInstanceUrn(run_id))
    client._emit_mcps(model.as_mcps())


def add_run_to_model_group(model_group, run_id):
    """Add run to model group relationship."""
    print("Adding run to model group...")
    model_group.add_training_job(DataProcessInstanceUrn(run_id))
    client._emit_mcps(model_group.as_mcps())


def add_input_dataset_to_run(run_urn, input_dataset):
    """Add input dataset to run relationship."""
    print("Adding input dataset to run...")
    client.add_input_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(input_dataset.urn)]
    )


def add_output_dataset_to_run(run_urn, output_dataset):
    """Add output dataset to run relationship."""
    print("Adding output dataset to run...")
    client.add_output_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(output_dataset.urn)]
    )


def update_model_properties(model):
    """Update model properties."""
    print("Updating model properties...")

    # Update model version
    model.set_version("2")

    # Add tags and terms
    model.add_tag(TagUrn("marketing"))
    model.add_term(GlossaryTermUrn("marketing"))

    # Add version alias
    model.add_version_alias("challenger")

    # Save the changes
    client._emit_mcps(model.as_mcps())


def update_model_group_properties(model_group):
    """Update model group properties."""
    print("Updating model group properties...")

    # Update description
    model_group.set_description("Updated description for airline forecast models")

    # Add tags and terms
    model_group.add_tag(TagUrn("production"))
    model_group.add_term(GlossaryTermUrn("time-series"))

    # Update custom properties
    model_group.set_custom_properties(
        {"team": "forecasting", "business_unit": "operations", "status": "active"}
    )

    # Save the changes
    client._emit_mcps(model_group.as_mcps())


def update_experiment_properties():
    """Update experiment properties."""
    print("Updating experiment properties...")

    # Create a container object for the existing experiment
    existing_experiment = Container(
        container_key=ContainerKey(platform="mlflow", name=advanced_experiment_id),
        display_name=advanced_experiment_name,
    )

    # Update properties
    existing_experiment.set_description(
        "Updated experiment for forecasting passenger numbers"
    )
    existing_experiment.add_tag(TagUrn("time-series"))
    existing_experiment.add_term(GlossaryTermUrn("forecasting"))
    existing_experiment.set_custom_properties(
        {"team": "forecasting", "priority": "high", "status": "active"}
    )

    # Save the changes
    client._emit_mcps(existing_experiment.as_mcps())


def main():
    # Parse arguments
    print("Creating AI assets...")

    # Comment in/out the functions you want to run
    # Create basic entities
    create_basic_model_group()
    create_basic_model()
    create_basic_experiment()
    create_basic_training_run()
    create_basic_dataset()

    # Create advanced entities
    advanced_model_group = create_advanced_model_group()
    advanced_model = create_advanced_model()
    advanced_experiment = create_advanced_experiment()
    advanced_run_urn = create_advanced_training_run()
    advanced_input_dataset, advanced_output_dataset = create_advanced_datasets()

    # # Create relationships - each can be commented out independently
    add_model_to_model_group(advanced_model, advanced_model_group)
    add_run_to_experiment(advanced_run_urn, advanced_experiment)
    add_run_to_model(advanced_model, advanced_run_id)
    add_run_to_model_group(advanced_model_group, advanced_run_id)
    add_input_dataset_to_run(advanced_run_urn, advanced_input_dataset)
    add_output_dataset_to_run(advanced_run_urn, advanced_output_dataset)

    # # Update properties - each can be commented out independently
    update_model_properties(advanced_model)
    update_model_group_properties(advanced_model_group)
    update_experiment_properties()

    print("All done! AI entities created successfully.")


if __name__ == "__main__":
    main()
