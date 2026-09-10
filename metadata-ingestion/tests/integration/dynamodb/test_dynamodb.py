"""Integration test for the DynamoDB source.

Runs against the Floci AWS emulator (``floci/floci``) via the standard
``docker_compose_runner`` fixture instead of the previous in-process ``moto``
mock. Test data is seeded with boto3 pointed at the emulator endpoint; ingestion
runs a full ``Pipeline`` and the output is asserted against golden files
(regenerate with ``--update-golden-files``).

Unlike moto, the emulator implements ``list_tags_of_resource``, so table-tag
extraction is exercised for real (no ``_get_dynamodb_table_tags`` patch).
"""

import pathlib
from typing import Any, Dict

import boto3
import pytest
import time_machine
from botocore.exceptions import ClientError

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

pytestmark = pytest.mark.integration

test_resources_dir = pathlib.Path(__file__).parent
FROZEN_TIME = "2023-08-30 12:00:00"

ENDPOINT_URL = "http://localhost:14570"
WEST = "us-west-2"
EAST = "us-east-1"

# The rich table exercised by the parity goldens: composite key + nested
# List/Map attributes so schema inference of nested structures is covered.
LOCATION_ITEM: Dict[str, Any] = {
    "partitionKey": {"S": "1"},
    "city": {"S": "San Francisco"},
    "address": {"S": "1st Market st"},
    "zip": {"N": "94000"},
    "contactNumbers": {"L": [{"S": "+14150000000"}, {"S": "+14151111111"}]},
    "services": {
        "M": {
            "parking": {"BOOL": True},
            "wifi": {"S": "Free"},
            "hours": {"M": {"open": {"S": "08:00"}, "close": {"S": "22:00"}}},
        }
    },
}


def _client(region: str) -> Any:
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _create_table(
    client: Any, name: str, tags: Any = None, composite: bool = False
) -> None:
    key_schema = [{"AttributeName": "partitionKey", "KeyType": "HASH"}]
    attr_defs = [{"AttributeName": "partitionKey", "AttributeType": "S"}]
    if composite:
        key_schema.append({"AttributeName": "city", "KeyType": "RANGE"})
        attr_defs.append({"AttributeName": "city", "AttributeType": "S"})
    try:
        client.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            **({"Tags": tags} if tags else {}),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise


def _seed_test_data() -> None:
    """Idempotently create the tables the golden files expect.

    ``us-west-2`` holds only ``Location`` so the parity recipes need no table
    filter; ``us-east-1`` holds two tables to exercise ``table_pattern`` and the
    (single-region-per-run) region handling."""
    west = _client(WEST)
    _create_table(
        west,
        "Location",
        tags=[
            {"Key": "env", "Value": "testing"},
            {"Key": "team", "Value": "datahub"},
        ],
        composite=True,
    )
    west.put_item(TableName="Location", Item=LOCATION_ITEM)

    east = _client(EAST)
    _create_table(east, "Products")
    east.put_item(
        TableName="Products",
        Item={
            "partitionKey": {"S": "p1"},
            "name": {"S": "Widget"},
            "price": {"N": "9"},
        },
    )
    _create_table(east, "Orders")
    east.put_item(
        TableName="Orders",
        Item={"partitionKey": {"S": "o1"}, "total": {"N": "42"}},
    )


def _run(pipeline_config: Dict[str, Any], tmp_path: pathlib.Path, out: str) -> str:
    output = f"{tmp_path}/{out}"
    pipeline = Pipeline.create(
        {
            "run_id": "dynamodb-test",
            "source": {"type": "dynamodb", "config": pipeline_config},
            "sink": {"type": "file", "config": {"filename": output}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return output


def _base_config(region: str = WEST) -> Dict[str, Any]:
    return {
        "aws_endpoint_url": ENDPOINT_URL,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "aws_region": region,
    }


@pytest.fixture(scope="module")
def dynamodb(docker_compose_runner, pytestconfig):
    # `up -d --wait` blocks on the compose healthcheck, so the emulator is ready
    # once the context manager returns — no test-layer port polling needed.
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "dynamodb",
        setup_command=["up -d --wait"],
    ) as docker_services:
        _seed_test_data()
        yield docker_services


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dynamodb_default(dynamodb, pytestconfig, tmp_path):
    """Ingest the us-west-2 table with defaults; schema inference incl. nested types."""
    output = _run(
        _base_config(), tmp_path, "dynamodb_default_platform_instance_mces.json"
    )
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir
        / "dynamodb_default_platform_instance_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dynamodb_platform_instance(dynamodb, pytestconfig, tmp_path):
    """Ingest with an explicit platform_instance stamped onto the dataset URN."""
    config = {**_base_config(), "platform_instance": "dynamodb_test"}
    output = _run(config, tmp_path, "dynamodb_platform_instance_mces.json")
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "dynamodb_platform_instance_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dynamodb_with_tags(dynamodb, pytestconfig, tmp_path):
    """Extract real table tags via list_tags_of_resource (no moto patch needed)."""
    config = {**_base_config(), "extract_table_tags": True}
    output = _run(config, tmp_path, "dynamodb_with_tags_mces.json")
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "dynamodb_with_tags_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dynamodb_table_pattern(dynamodb, pytestconfig, tmp_path):
    """Filter tables by region.table pattern with multiple candidates present.

    us-east-1 holds Products + Orders; the pattern keeps only Products.
    """
    config = {
        **_base_config(EAST),
        "table_pattern": {"allow": ["us-east-1.Products"]},
    }
    output = _run(config, tmp_path, "dynamodb_table_pattern_mces.json")
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "dynamodb_table_pattern_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_dynamodb_us_east_region(dynamodb, pytestconfig, tmp_path):
    """Ingest a non-default region end-to-end (region is single-per-run).

    Both us-east-1 tables surface with the region embedded in their dataset names.
    """
    output = _run(_base_config(EAST), tmp_path, "dynamodb_us_east_region_mces.json")
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "dynamodb_us_east_region_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )
