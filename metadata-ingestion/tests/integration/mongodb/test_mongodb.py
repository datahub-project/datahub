import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.integration
def test_mongodb_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mongodb"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "mongo"
    ) as docker_services:
        wait_for_port(docker_services, "testmongodb", 27017)

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test",
                "source": {
                    "type": "mongodb",
                    "config": {
                        "connect_uri": "mongodb://localhost:57017",
                        "username": "mongoadmin",
                        "password": "examplepass",
                        "maxDocumentSize": 25000,
                        "platform_instance": "instance",
                        "schemaSamplingSize": None,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces.json",
            golden_path=test_resources_dir / "mongodb_mces_golden.json",
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test-small-schema-size",
                "source": {
                    "type": "mongodb",
                    "config": {
                        "connect_uri": "mongodb://localhost:57017",
                        "username": "mongoadmin",
                        "password": "examplepass",
                        "maxSchemaSize": 10,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces_small_schema_size.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces_small_schema_size.json",
            golden_path=test_resources_dir
            / "mongodb_mces_small_schema_size_golden.json",
        )

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "mongodb-test-no-random-sampling",
                "source": {
                    "type": "mongodb",
                    "config": {
                        "connect_uri": "mongodb://localhost:57017",
                        "username": "mongoadmin",
                        "password": "examplepass",
                        "useRandomSampling": False,
                        "platform_instance": "instance",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/mongodb_mces_no_random_sampling.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mongodb_mces_no_random_sampling.json",
            golden_path=test_resources_dir
            / "mongodb_mces_no_random_sampling_golden.json",
        )
