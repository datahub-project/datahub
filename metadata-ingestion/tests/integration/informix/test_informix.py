import subprocess

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_4
INFORMIX_PORT = 9088
INFORMIX_PASSWORD = "in4mix"


def _informix_ready(container: str) -> bool:
    # Probe the server directly with onstat rather than the image's Docker
    # HEALTHCHECK: on slow CI runners the healthcheck flaps to "unhealthy"
    # before the multi-minute first-boot init finishes, aborting the wait.
    # `onstat -` prints "On-Line" once the server accepts connections. Mirrors
    # the db2/mysql "run a readiness command in the container" pattern.
    result = subprocess.run(
        ["docker", "exec", "-u", "informix", container, "bash", "-lc", "onstat -"],
        capture_output=True,
        text=True,
    )
    return "On-Line" in result.stdout


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/informix"


@pytest.fixture(scope="module")
def informix_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    # Bring the container up detached rather than with pytest-docker's default
    # `up --build --wait`: `--wait` blocks on the container's healthcheck and
    # aborts ("container testinformix is unhealthy") before our readiness probe
    # runs. We disable the healthcheck (see docker-compose.yml) and gate on
    # `_informix_ready` (onstat) below, which tolerates the slow first boot.
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "informix",
        setup_command=["up -d"],
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testinformix",
            INFORMIX_PORT,
            timeout=600,
            checker=lambda: _informix_ready("testinformix"),
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
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\].+\[\d+\]\['auditStamp'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
        ],
    )
