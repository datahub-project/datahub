import json
from typing import Any, Dict

import aerospike
import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.integration
def test_aerospike_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/aerospike"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "aerospike"
    ) as docker_services:
        wait_for_port(docker_services, "testaerospike", 3000, pause=10)

        populate_aerospike(test_resources_dir)
        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "aerospike-test",
                "source": {
                    "type": "aerospike",
                    "config": {
                        "inferSchemaDepth": -1,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/aerospike_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "aerospike_mces.json",
            golden_path=test_resources_dir / "aerospike_mces_golden.json",
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "aerospike-test-small-schema-size",
                "source": {
                    "type": "aerospike",
                    "config": {
                        "inferSchemaDepth": -1,
                        "maxSchemaSize": 10,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/aerospike_mces_small_schema_size.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "aerospike_mces_small_schema_size.json",
            golden_path=test_resources_dir
            / "aerospike_mces_small_schema_size_golden.json",
        )


def populate_aerospike(test_resources_dir):
    write_policy = {"key": aerospike.POLICY_KEY_SEND}
    policies = {"write": write_policy}

    client_config: Dict[str, Any] = {
        "hosts": [("localhost", 3000, None)],
        "auth_mode": aerospike.AUTH_INTERNAL,
        "tls": {"enable": False},
        "policies": policies,
    }
    aerospike_client = aerospike.client(client_config).connect()

    first_set_file = test_resources_dir / "first_set.json"
    second_set_file = test_resources_dir / "second_set.json"
    large_set_file = test_resources_dir / "large_set.json"

    with first_set_file.open() as first_set_json:
        first_set = json.loads(first_set_json.read())

    with second_set_file.open() as second_set_json:
        second_set = json.loads(second_set_json.read())

    with large_set_file.open() as large_set_json:
        large_set = json.loads(large_set_json.read())

    for record in first_set:
        aerospike_client.put(("test", "first_set", record["name"]), record)

    for record in second_set:
        aerospike_client.put(("test", "second_set", record["name"]), record)

    for record in large_set:
        aerospike_client.put(("test", "large_set", next(iter(record.values()))), record)

    aerospike_client.put(("test", "empty_set", "empty"), {"test": "test"})
    aerospike_client.remove(("test", "empty_set", "empty"))
