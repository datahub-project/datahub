import pathlib

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_dynamodb

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

test_resources_dir = pathlib.Path(__file__).parent
FROZEN_TIME = "2023-08-30 12:00:00"


@freeze_time(FROZEN_TIME)
@mock_dynamodb
@pytest.mark.integration
def test_dynamodb(pytestconfig, tmp_path, mock_time):
    boto3.setup_default_session()
    client = boto3.client("dynamodb", region_name="us-west-2")
    client.create_table(
        TableName="Location",
        KeySchema=[
            {"AttributeName": "partitionKey", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "partitionKey", "AttributeType": "S"},
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
