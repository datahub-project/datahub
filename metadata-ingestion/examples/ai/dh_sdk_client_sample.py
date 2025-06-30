from datetime import datetime

from datahub.emitter.mcp_builder import (
    ContainerKey,
)
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.metadata.urns import (
    CorpUserUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

run_id = "simple_training_run"
run_name = "Simple Training Run"
experiment_id = "airline_forecast_experiment"
experiment_name = "Airline Forecast Experiment"
model_id = "arima_model"
model_name = "ARIMA Model"
model_group_id = "airline_forecast_models_group"
model_group_name = "Airline Forecast Models Group"

if __name__ == "__main__":
    client = DataHubClient.from_env()

    # Create model group
    model_group = MLModelGroup(
        id=model_group_id,
        platform="mlflow",
        name=model_group_name,
        description="Group of models for airline passenger forecasting",
        created=datetime.now(),
        last_modified=datetime.now(),
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        external_url="https://www.linkedin.com/in/datahub",
        tags=["urn:li:tag:forecasting", "urn:li:tag:arima"],
        terms=["urn:li:glossaryTerm:forecasting"],
        custom_properties={"team": "forecasting"},
    )

    # Create model
    model = MLModel(
        id=model_id,
        platform="mlflow",
        name=model_name,
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
        # group=str(model_group.urn),
        hyper_params={"learning_rate": "0.01"},
        training_metrics={"accuracy": "0.9"},
    )

    # create experiment
    experiment = Container(
        container_key=ContainerKey(
            platform="mlflow",
            name=experiment_id,
        ),
        display_name=experiment_name,
        description="Experiment to forecast airline passenger numbers",
        extra_properties={"team": "forecasting"},
        created=datetime(2025, 4, 9, 22, 30),
        last_modified=datetime(2025, 4, 9, 22, 30),
        subtype=MLAssetSubTypes.MLFLOW_EXPERIMENT,
    )

    client.entities.upsert(experiment)
    # Create datasets
    input_dataset = Dataset(
        platform="snowflake",
        name="iris_input",
    )
    client.entities.upsert(input_dataset)

    output_dataset = Dataset(
        platform="snowflake",
        name="iris_output",
    )
    client.entities.upsert(output_dataset)

    model.set_model_group(model_group.urn)

    model.add_version_alias("challenger")

    model.add_term(GlossaryTermUrn("marketing"))

    model.add_tag(TagUrn("marketing"))

    model.set_version("2")

    client.entities.upsert(model)

    client.entities.upsert(model_group)
