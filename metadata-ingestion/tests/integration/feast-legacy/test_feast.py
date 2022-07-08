import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

# from datahub.ingestion.run.pipeline import Pipeline
# from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


# make sure that mock_time is excluded here because it messes with feast
@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_feast_ingest(docker_compose_runner, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/feast-legacy"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "feast"
    ) as docker_services:
        wait_for_port(docker_services, "testfeast", 6565, timeout=120)

        # container listens to this port once test cases have been setup
        wait_for_port(
            docker_services, "testfeast_setup", 6789, timeout=120, hostname="localhost"
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "feast-test",
                "source": {
                    "type": "feast-legacy",
                    "config": {
                        "core_url": "localhost:6565",
                        "use_local_build": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/feast_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "feast_mces.json",
            golden_path=test_resources_dir / "feast_mces_golden.json",
        )
