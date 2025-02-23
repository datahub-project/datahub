import logging
import pathlib
import shutil
import time

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)

_resources_dir = pathlib.Path(__file__).parent


@pytest.mark.integration
def test_cassandra_ingest(docker_compose_runner, pytestconfig, tmp_path, monkeypatch):
    # Tricky: The cassandra container makes modifications directly to the cassandra.yaml
    # config file.
    # See https://github.com/docker-library/cassandra/issues/165
    # To avoid spurious diffs, we copy the config file to a temporary location
    # and depend on that instead. The docker-compose file has the corresponding
    # env variable usage to pick up the config file.
    cassandra_config_file = _resources_dir / "setup/cassandra.yaml"
    shutil.copy(cassandra_config_file, tmp_path / "cassandra.yaml")
    monkeypatch.setenv("CASSANDRA_CONFIG_DIR", str(tmp_path))

    with docker_compose_runner(
        _resources_dir / "docker-compose.yml", "cassandra"
    ) as docker_services:
        wait_for_port(docker_services, "test-cassandra", 9042)

        time.sleep(5)

        # Run the metadata ingestion pipeline.
        logger.info("Starting the ingestion test...")
        pipeline = Pipeline.create(
            {
                "run_id": "cassandra-test",
                "source": {
                    "type": "cassandra",
                    "config": {
                        "platform_instance": "dev_instance",
                        "contact_point": "localhost",
                        "port": 9042,
                        "profiling": {"enabled": True},
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
            output_path=f"{tmp_path}/cassandra_mcps.json",
            golden_path=_resources_dir / "cassandra_mcps_golden.json",
        )
