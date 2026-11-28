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
        wait_for_port(docker_services, "test-neo4j", 7687)
        bolt_port = docker_services.port_for("neo4j", 7687)

        # Wait for test data to be loaded by checking if we can query some test nodes
        docker_services.wait_until_responsive(
            timeout=120,
            pause=2,
            check=lambda: check_neo4j_setup_completed(bolt_port),
        )

        yield bolt_port


def check_neo4j_setup_completed(bolt_port: int) -> bool:
    """Check if test data has been loaded into Neo4j"""
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            f"bolt://localhost:{bolt_port}", auth=("neo4j", "testpassword")
        )
        with driver.session() as session:
            # init.cypher creates nodes first and the FOLLOWS / SIMILAR_TO
            # relationships last, so a node-count check can pass while
            # neo4j-init is still running and the golden file then misses
            # those two relationship types. Wait for the final statements.
            result = session.run(
                "OPTIONAL MATCH ()-[f:FOLLOWS]->() WITH count(f) AS follows "
                "OPTIONAL MATCH ()-[s:SIMILAR_TO]->() "
                "RETURN follows, count(s) AS similar_to"
            )
            record = result.single()
            driver.close()
            if record is None:
                return False
            return record["follows"] > 0 and record["similar_to"] > 0
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
                    "uri": f"bolt://localhost:{neo4j_runner}",
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
