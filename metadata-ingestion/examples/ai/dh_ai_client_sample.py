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

run_id = "simple_training_run"
run_name = "Simple Training Run"
experiment_id = "airline_forecast_experiment"
experiment_name = "Airline Forecast Experiment"
model_id = "arima_model"
model_name = "ARIMA Model"
model_group_id = "airline_forecast_models_group"
model_group_name = "Airline Forecast Models Group"


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
        # training_jobs=[DataProcessInstanceUrn(run_id)],
        custom_properties={"team": "forecasting"},
    )

    client._emit_mcps(model_group.as_mcps())

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
        # training_jobs=[DataProcessInstanceUrn(run_id)],
        custom_properties={"team": "forecasting"},
        version="1",
        aliases=["champion"],
        model_group=str(model_group.urn),
        hyper_params={"learning_rate": "0.01"},
        training_metrics={"accuracy": "0.9"},
    )

    client._emit_mcps(model.as_mcps())

    # Creating an experiment
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

    # Create datasets
    input_dataset = Dataset(
        platform="snowflake",
        name="iris_input",
        description="Raw Iris dataset used for training ML models",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name="Iris Training Input Data",
        tags=["urn:li:tag:ml_data", "urn:li:tag:iris"],
        terms=["urn:li:glossaryTerm:raw_data"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "data_source": "UCI Repository",
            "records": "150",
            "features": "4",
        },
    )
    client._emit_mcps(input_dataset.as_mcps())

    output_dataset = Dataset(
        platform="snowflake",
        name="iris_output",
        description="Processed Iris dataset with model predictions",
        schema=[("id", "number"), ("name", "string"), ("species", "string")],
        display_name="Iris Model Output Data",
        tags=["urn:li:tag:ml_data", "urn:li:tag:predictions"],
        terms=["urn:li:glossaryTerm:model_output"],
        owners=[CorpUserUrn("urn:li:corpuser:datahub")],
        custom_properties={
            "model_version": "1.0",
            "records": "150",
            "accuracy": "0.95",
        },
    )
    client._emit_mcps(output_dataset.as_mcps())

    # Add run to experiment
    print(experiment.urn)
    client.add_run_to_experiment(run_urn=run_urn, experiment_urn=str(experiment.urn))

    # Add input and output datasets to run
    client.add_input_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(input_dataset.urn)]
    )

    client.add_output_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(output_dataset.urn)]
    )

    model.add_training_job(DataProcessInstanceUrn(run_id))

    model_group.add_training_job(DataProcessInstanceUrn(run_id))

    model.set_model_group(model_group.urn)

    model.add_version_alias("challenger")

    model.add_term(GlossaryTermUrn("marketing"))

    model.add_tag(TagUrn("marketing"))

    model.set_version("2")

    client._emit_mcps(model.as_mcps())

    client._emit_mcps(model_group.as_mcps())
