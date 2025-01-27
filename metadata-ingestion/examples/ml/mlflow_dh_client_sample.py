import argparse

from mlflow_dh_client import MLflowDatahubClient

import datahub.metadata.schema_classes as models
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import RunResultType

if __name__ == "__main__":
    # Example usage
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="DataHub access token")
    args = parser.parse_args()

    client = MLflowDatahubClient(token=args.token)

    # Create model group
    # Using property classes directly
    model_group_urn = client.create_model_group(
        group_id="airline_forecast_models_group_4",
        properties=models.MLModelGroupPropertiesClass(
            name="Airline Forecast Models Group 4",
            description="Group of models for airline passenger forecasting",
            created=models.TimeStampClass(
                time=1628580000000, actor="urn:li:corpuser:datahub"
            ),
        ),
    )

    # Creating a model with property classes
    model_urn = client.create_model(
        model_id="arima_model_5",
        properties=models.MLModelPropertiesClass(
            name="ARIMA Model 6",
            description="ARIMA model for airline passenger forecasting",
            customProperties={"team": "forecasting"},
        ),
        version="6.0",
        alias="arima_model_6_alias",
    )

    # Creating an experiment with property class
    experiment_urn = client.create_experiment(
        experiment_id="airline_forecast_experiment",
        properties=models.ContainerPropertiesClass(
            name="Airline Forecast Experiment",
            description="Experiment to forecast airline passenger numbers",
            customProperties={"team": "forecasting"},
        ),
    )

    run_urn = client.create_training_run(
        run_id="simple_training_run_4",
        properties=models.DataProcessInstancePropertiesClass(
            name="Simple Training Run 4",
            created=models.AuditStampClass(
                time=1628580000000, actor="urn:li:corpuser:datahub"
            ),
        ),
        training_run_properties=models.MLTrainingRunPropertiesClass(
            id="simple_training_run_4",
            outputUrls=["s3://my-bucket/output"],
            trainingMetrics=[models.MLMetricClass(name="accuracy", value="0.9")],
        ),
        run_result=RunResultType.FAILURE,
        start_timestamp=1628580000000,
        end_timestamp=1628580001000,
    )
    # Create datasets
    input_dataset_urn = client.create_dataset(
        platform="snowflake",
        name="iris_input",
    )

    output_dataset_urn = client.create_dataset(
        platform="snowflake",
        name="iris_ouptut",
    )

    # Add run to experiment
    client.add_run_to_experiment(run_urn=run_urn, experiment_urn=experiment_urn)

    # Add model to model group
    client.add_model_to_model_group(model_urn=model_urn, group_urn=model_group_urn)

    # Add run to model
    client.add_run_to_model(
        model_urn=model_urn,
        run_urn=run_urn,
    )

    # add run to model group
    client.add_run_to_model_group(
        model_group_urn=model_group_urn,
        run_urn=run_urn,
    )

    # Add input and output datasets to run
    client.add_input_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(input_dataset_urn)]
    )

    client.add_output_datasets_to_run(
        run_urn=run_urn, dataset_urns=[str(output_dataset_urn)]
    )
