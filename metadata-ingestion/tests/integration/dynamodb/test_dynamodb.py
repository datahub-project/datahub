import pathlib

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_dynamodb

from datahub.ingestion.glossary.classification_mixin import ClassificationConfig
from datahub.ingestion.glossary.classifier import DynamicTypedClassifierConfig
from datahub.ingestion.glossary.datahub_classifier import (
    DataHubClassifierConfig,
    InfoTypeConfig,
    PredictionFactorsAndWeights,
)
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

test_resources_dir = pathlib.Path(__file__).parent
FROZEN_TIME = "2023-08-30 12:00:00"


@freeze_time(FROZEN_TIME)
@mock_dynamodb
@pytest.mark.integration
def test_dynamodb(pytestconfig, tmp_path):
    boto3.setup_default_session()
    client = boto3.client("dynamodb", region_name="us-west-2")
    client.create_table(
        TableName="Location",
        KeySchema=[
            {"AttributeName": "partitionKey", "KeyType": "HASH"},
            {"AttributeName": "city", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "partitionKey", "AttributeType": "S"},
            {"AttributeName": "city", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    client.put_item(
        TableName="Location",
        Item={
            "partitionKey": {"S": "1"},
            "city": {"S": "San Francisco"},
            "address": {"S": "1st Market st"},
            "zip": {"N": "94000"},
            "contactNumbers": {  # List type
                "L": [
                    {"S": "+14150000000"},
                    {"S": "+14151111111"},
                ]
            },
            "services": {  # Map type
                "M": {
                    "parking": {"BOOL": True},
                    "wifi": {"S": "Free"},
                    "hours": {  # Map type inside Map for nested structure
                        "M": {"open": {"S": "08:00"}, "close": {"S": "22:00"}}
                    },
                }
            },
        },
    )

    pipeline_default_platform_instance = Pipeline.create(
        {
            "run_id": "dynamodb-test",
            "source": {
                "type": "dynamodb",
                "config": {
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                    "aws_session_token": "test",
                    "aws_region": "us-west-2",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/dynamodb_default_platform_instance_mces.json",
                },
            },
        }
    )
    pipeline_default_platform_instance.run()
    pipeline_default_platform_instance.raise_from_status()
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/dynamodb_default_platform_instance_mces.json",
        golden_path=test_resources_dir
        / "dynamodb_default_platform_instance_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )

    pipeline_with_platform_instance = Pipeline.create(
        {
            "run_id": "dynamodb-test",
            "source": {
                "type": "dynamodb",
                "config": {
                    "platform_instance": "dynamodb_test",
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                    "aws_session_token": "test",
                    "aws_region": "us-west-2",
                    "classification": ClassificationConfig(
                        enabled=True,
                        classifiers=[
                            DynamicTypedClassifierConfig(
                                type="datahub",
                                config=DataHubClassifierConfig(
                                    minimum_values_threshold=1,
                                    info_types_config={
                                        "Phone_Number": InfoTypeConfig(
                                            prediction_factors_and_weights=PredictionFactorsAndWeights(
                                                name=0.7,
                                                description=0,
                                                datatype=0,
                                                values=0.3,
                                            )
                                        )
                                    },
                                ),
                            )
                        ],
                    ),
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/dynamodb_platform_instance_mces.json",
                },
            },
        }
    )
    pipeline_with_platform_instance.run()
    pipeline_with_platform_instance.raise_from_status()
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/dynamodb_platform_instance_mces.json",
        golden_path=test_resources_dir / "dynamodb_platform_instance_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )
