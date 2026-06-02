"""Integration test for the AWS Kinesis source.

Runs against LocalStack 3.0 (Community). The full ingestion is asserted via
a golden file (`kinesis_mces_golden.json`). Initial golden generation uses
`--update-golden-files`.

Conventions:
  - time_machine (NOT freezegun) — per testing.md BLOCKER.
  - Test data is seeded by setup_test_data.py (run via fixture).
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import time_machine

from datahub.testing.mce_helpers import check_golden_file

FROZEN_TIME = "2026-05-20 12:00:00"
TEST_RESOURCES_DIR = Path(__file__).resolve().parent
COMPOSE_FILE = TEST_RESOURCES_DIR / "docker-compose.yml"
RECIPE = TEST_RESOURCES_DIR / "recipe.yml"
RECIPE_FILE_SINK = TEST_RESOURCES_DIR / "recipe_file_sink.yml"
SETUP_SCRIPT = TEST_RESOURCES_DIR / "setup_test_data.py"
GOLDEN_FILE = TEST_RESOURCES_DIR / "kinesis_mces_golden.json"


def _localstack_kinesis_ready() -> bool:
    """Return True if a LocalStack endpoint with kinesis is already reachable."""
    try:
        import urllib.request

        with urllib.request.urlopen(
            "http://localhost:4566/_localstack/health", timeout=2
        ) as resp:
            body = resp.read().decode("utf-8")
            return (
                '"kinesis": "available"' in body
                or '"kinesis":"available"' in body
                or '"kinesis": "running"' in body
                or '"kinesis":"running"' in body
            )
    except Exception:
        return False


@pytest.fixture(scope="module")
def kinesis_runner():
    """Bring up LocalStack (if not already running), seed data."""
    started_by_us = False
    if not _localstack_kinesis_ready():
        subprocess.check_call(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"]
        )
        started_by_us = True
        # Wait for healthy
        for _ in range(20):
            if _localstack_kinesis_ready():
                break
            time.sleep(3)
    # Seed data
    env = {
        **os.environ,
        "AWS_ENDPOINT_URL": "http://localhost:4566",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
    }
    subprocess.check_call([sys.executable, str(SETUP_SCRIPT)], env=env)
    yield
    # Only tear down if we started LocalStack ourselves; otherwise leave the
    # shared instance alone for the rest of the harness.
    if started_by_us:
        subprocess.check_call(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"]
        )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_kinesis_ingestion_golden_file(kinesis_runner, pytestconfig, tmp_path):
    """Run the recipe + compare emitted MCEs against the golden file."""
    output = tmp_path / "kinesis_mces.json"

    # Use a separate recipe with a file sink so the test doesn't require a
    # running DataHub. `OUTPUT_FILE` is interpolated by the recipe loader.
    env = {
        **os.environ,
        "AWS_ENDPOINT_URL": "http://localhost:4566",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "OUTPUT_FILE": str(output),
    }
    # Prefer DATAHUB_BIN if set (avoids picking up a stale global `datahub`).
    datahub_bin = os.environ.get("DATAHUB_BIN", "datahub")
    subprocess.check_call(
        [
            datahub_bin,
            "ingest",
            "-c",
            str(RECIPE_FILE_SINK),
            "--no-default-report",
            "--report-to",
            str(tmp_path / "ingest-report.json"),
            "--strict-warnings",
        ],
        env=env,
    )

    check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output,
        golden_path=GOLDEN_FILE,
    )
