import pytest
import pytest_docker.plugin
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_db

from datahub.ingestion.run.pipeline import Pipeline


@pytest.fixture
def mongodb(pytestconfig, docker_compose_project_name, docker_cleanup):
    test_docker_compose = (
        pytestconfig.rootpath / "tests/integration/mongodb/docker-compose.yml"
    )
    with pytest_docker.plugin.get_docker_services(
        test_docker_compose, f"{docker_compose_project_name}-mongo", docker_cleanup
    ) as docker_services:
        breakpoint()
        yield wait_for_db(docker_services, "testmongodb", 27017)


@pytest.mark.slow
def test_mongodb_ingest(mongodb, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mongodb"

    pipeline = Pipeline.create(
        {
            "run_id": "mongodb-test",
            "source": {
                "type": "mongodb",
                "config": {
                    "connect_uri": "mongodb://localhost:57017",
                    "username": "mongoadmin",
                    "password": "examplepass",
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

    output = mce_helpers.load_json_file(str(tmp_path / "mongodb_mces.json"))
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "mongodb_mce_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)
