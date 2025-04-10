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
from datahub.metadata.urns import CorpUserUrn, DataProcessInstanceUrn
from datahub.sdk.container import Container
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

prefix = "mid_test2"
run_id = f"{prefix}_simple_training_run"
run_name = f"{prefix}_Simple Training Run"
experiment_id = f"{prefix}_airline_forecast_experiment"
experiment_name = f"{prefix}_Airline Forecast Experiment"
model_id = f"{prefix}_arima_model"
model_name = f"{prefix}_ARIMA Model"
model_group_id = f"{prefix}_airline_forecast_models_group"
model_group_name = f"{prefix}_Airline Forecast Models Group"

if __name__ == "__main__":
    # Example usage
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=False, help="DataHub access token")
    parser.add_argument(
        "--server_url",
        required=False,
        default="http://localhost:8080",
        help="DataHub server URL (defaults to http://localhost:8080)",
    )
    args = parser.parse_args()

    client = DatahubAIClient(token=args.token, server_url=args.server_url)

    # Creating an experiment with property class
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

    client._emit_mcps(experiment.as_mcps())

    run_urn = client.create_training_run(
        run_id=run_id,
        properties=DataProcessInstancePropertiesClass(
            name=run_name,
            created=AuditStampClass(
                time=1628580000000, actor="urn:li:corpuser:datahub"
            ),
            customProperties={"team": "forecasting"},
        ),
        training_run_properties=MLTrainingRunPropertiesClass(
            id=run_id,
            outputUrls=["s3://my-bucket/output"],
            trainingMetrics=[MLMetricClass(name="accuracy", value="0.9")],
            hyperParams=[MLHyperParamClass(name="learning_rate", value="0.01")],
            externalUrl="https:localhost:5000",
        ),
        run_result=RunResultType.FAILURE,
        start_timestamp=1628580000000,
        end_timestamp=1628580001000,
    )

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
        training_jobs=[DataProcessInstanceUrn(run_id)],
        custom_properties={"team": "forecasting"},
    )

    client._emit_mcps(model_group.as_mcps())

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
        training_jobs=[DataProcessInstanceUrn(run_id)],
        custom_properties={"team": "forecasting"},
        version="1",
        aliases=["champion"],
        group=str(model_group.urn),
        hyper_params={"learning_rate": "0.01"},
        training_metrics={"accuracy": "0.9"},
    )

    client._emit_mcps(model.as_mcps())

    # Create datasets
    input_dataset_urns = [
        client.create_dataset(
            platform="snowflake",
            name="iris_input",
        )
    ]

    output_dataset_urns = [
        client.create_dataset(
            platform="snowflake",
            name="iris_ouptut",
        )
    ]

    # Add run to experiment
    client.add_run_to_experiment(run_urn=run_urn, experiment_urn=str(experiment.urn))

    # Add input and output datasets to run
    client.add_input_datasets_to_run(run_urn=run_urn, dataset_urns=input_dataset_urns)

    client.add_output_datasets_to_run(run_urn=run_urn, dataset_urns=output_dataset_urns)
