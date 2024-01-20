import logging

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)

# TODO impl freezegun
# FROZEN_TIME = "2024-01-01 10:00:00"


@pytest.mark.integration
def test_cassandra_ingest(docker_compose_runner, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/cassandra"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "cassandra"
    ) as docker_services:
        wait_for_port(docker_services, "test-cassandra", 9042, timeout=150)

        # Run the metadata ingestion pipeline.
        logger.info("Starting the ingestion test...")
        pipeline = Pipeline.create(
            {
                "run_id": "cassandra-test",
                "source": {
                    "type": "cassandra",
                    "config": {
                        "contact_point": "localhost",
                        "port": 9042,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/cassandra_mcps.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        logger.info("Verifying output.")
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "cassandra_mcps.json",
            golden_path=test_resources_dir / "cassandra_mcps_golden.json",
        )
