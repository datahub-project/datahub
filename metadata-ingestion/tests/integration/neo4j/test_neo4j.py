import pathlib

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

_resources_dir = pathlib.Path(__file__).parent

pytestmark = pytest.mark.integration_batch_2


@pytest.fixture(scope="module")
def neo4j_runner(docker_compose_runner):
    with docker_compose_runner(
        _resources_dir / "docker-compose.yml", "neo4j"
    ) as docker_services:
        wait_for_port(docker_services, "neo4j", 7687)

        # Wait for test data to be loaded by checking if we can query some test nodes
        docker_services.wait_until_responsive(
            timeout=120,
            pause=2,
            check=lambda: check_neo4j_setup_completed(),
        )

        yield docker_services


def check_neo4j_setup_completed() -> bool:
    """Check if test data has been loaded into Neo4j"""
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            "neo4j://localhost:7687", auth=("neo4j", "testpassword")
        )
        with driver.session() as session:
            # Check if test data is loaded
            result = session.run("MATCH (n) RETURN count(n) as count")
            record = result.single()
            if record is None:
                driver.close()
                return False
            count = record["count"]

            driver.close()
            # Waiting for count > 0 is not enough, the test is very flaky with that
            # check. count > 9 typically take one extra pause (2s) to satisfy and makes
            # the test a lot less flaky.
            return count > 9
    except Exception:
        return False


@pytest.mark.integration
def test_neo4j_ingest(neo4j_runner, pytestconfig, tmp_path):
    pipeline = Pipeline.create(
        {
            "run_id": "neo4j-test",
            "source": {
                "type": "neo4j",
                "config": {
                    "uri": "neo4j://localhost:7687",
                    "username": "neo4j",
                    "password": "testpassword",
                    "env": "TEST",
                    "platform_instance": "test_instance",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/neo4j_mcps.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/neo4j_mcps.json",
        golden_path=_resources_dir / "neo4j_mcps_golden.json",
    )
