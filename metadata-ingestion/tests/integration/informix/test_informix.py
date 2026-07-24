import subprocess

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_4
INFORMIX_PORT = 9088
INFORMIX_PASSWORD = "in4mix"


def _is_healthy(container: str) -> bool:
    # The image's own HEALTHCHECK only reports "healthy" once the Informix
    # server has finished its multi-minute first-boot initialization, which
    # is a more reliable readiness signal than any single log line.
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Health.Status}}", container],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "healthy"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/informix"


@pytest.fixture(scope="module")
def informix_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "informix"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testinformix",
            INFORMIX_PORT,
            timeout=600,
            checker=lambda: _is_healthy("testinformix"),
        )

        subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "root",
                "testinformix",
                "bash",
                "-c",
                f"echo 'informix:{INFORMIX_PASSWORD}' | chpasswd",
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "cp",
                str(test_resources_dir / "setup" / "setup.sql"),
                "testinformix:/tmp/setup.sql",
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "informix",
                "testinformix",
                "bash",
                "-lc",
                "dbaccess - /tmp/setup.sql",
            ],
            check=True,
        )

        yield docker_services


@pytest.mark.integration
def test_informix_ingest(informix_runner, pytestconfig, test_resources_dir, tmp_path):
    output_path = tmp_path / "informix_mces.json"
    golden_path = test_resources_dir / "informix_mces_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "informix",
                "config": {
                    "host_port": f"localhost:{INFORMIX_PORT}",
                    "server": "informix",
                    "database": "testdb",
                    "username": "informix",
                    "password": INFORMIX_PASSWORD,
                    "accept_ibm_jdbc_license": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
            r"root\[\d+\]\['aspect'\]\['json'\].+\[\d+\]\['auditStamp'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
        ],
    )
