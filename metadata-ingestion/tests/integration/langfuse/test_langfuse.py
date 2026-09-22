import logging

import pytest
import requests
import tenacity

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.integration.langfuse.setup_test_data import (
    PROMPT_GREETING,
    PROMPT_SYSTEM,
    seed,
)
from tests.test_helpers import fs_helpers

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:3211"
PUBLIC_KEY = "pk-lf-integrationtest00000000000000"
SECRET_KEY = "sk-lf-integrationtest00000000000000"

# Dynamic, seed-time-relative fields that cannot be pinned even though every
# entity URN and identifier in this fixture is deterministic:
# - `created.time` / `timestampMillis`: derived from OTel span timestamps,
#   which are offsets from setup_test_data.py's run time, not a fixed date.
# - `trainingMetrics[*].createdAt`: the Score's server-assigned creation
#   timestamp (no explicit timestamp is sent when creating test scores).
GOLDEN_IGNORE_PATHS = [
    r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]\['time'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
    r"root\[\d+\]\['aspect'\]\['json'\]\['trainingMetrics'\]\[\d+\]\['createdAt'\]",
]


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/langfuse"


def _wait_for_health(timeout: int = 180) -> None:
    for attempt in tenacity.Retrying(
        stop=tenacity.stop_after_delay(timeout),
        wait=tenacity.wait_fixed(3),
        reraise=True,
    ):
        with attempt:
            resp = requests.get(f"{BASE_URL}/api/public/health", timeout=10)
            resp.raise_for_status()
            assert resp.json().get("status") == "OK"


@pytest.fixture(scope="module")
def seeded_langfuse(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(test_resources_dir / "docker-compose.yml", "langfuse"):
        # Langfuse's web image doesn't ship a shell suitable for the default
        # docker-exec-based wait_for_port check, and this compose file already
        # binds a fixed host port, so poll the real health endpoint directly
        # instead - this is also exactly how the connector itself connects.
        _wait_for_health(timeout=180)

        logger.info("Seeding deterministic test data into Langfuse")
        summary = seed(BASE_URL, PUBLIC_KEY, SECRET_KEY)

        yield summary


def test_langfuse_ingest(seeded_langfuse, pytestconfig, tmp_path, test_resources_dir):
    summary = seeded_langfuse

    with fs_helpers.isolated_filesystem(tmp_path):
        pipeline = Pipeline.create(
            {
                "run_id": "langfuse-integration-test",
                "source": {
                    "type": "langfuse",
                    "config": {
                        "connection": {
                            "host": BASE_URL,
                            "public_key": PUBLIC_KEY,
                            "secret_key": SECRET_KEY,
                        },
                        # The fixture is seeded "now" on every run, not at a
                        # fixed calendar date, so the window must be wide
                        # enough to always contain it regardless of when the
                        # test executes.
                        "window": {"start_time": "-1d"},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": "./langfuse_mces.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        report = pipeline.source.get_report()
        assert report.failures == []
        assert report.warnings == []

        # Business-logic assertions against the ground truth the fixture
        # itself declares, independent of the golden-file byte comparison
        # below - these fail loudly and specifically if counting logic
        # regresses, rather than just "some JSON diff line changed".
        assert report.traces_scanned == summary.trace_count
        assert report.generations_scanned == summary.generation_count
        assert (
            report.non_generation_observations_skipped
            == summary.non_generation_observation_count
        )
        assert report.scores_attached == summary.attachable_score_count
        assert report.scores_dropped_unattachable_subject == summary.dropped_score_count
        assert report.prompts_scanned == len(summary.prompt_names)
        assert report.prompt_versions_scanned == summary.prompt_version_count

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="langfuse_mces.json",
            golden_path=test_resources_dir / "langfuse_mces_golden.json",
            ignore_paths=GOLDEN_IGNORE_PATHS,
        )


def test_langfuse_test_connection(seeded_langfuse):
    from datahub.ingestion.source.langfuse.langfuse import LangfuseSource

    report = LangfuseSource.test_connection(
        {
            "connection": {
                "host": BASE_URL,
                "public_key": PUBLIC_KEY,
                "secret_key": SECRET_KEY,
            }
        }
    )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None


def test_langfuse_prompt_versions_and_labels(seeded_langfuse):
    """Exercises the Prompt -> versioned Dataset + VersionSet path against a
    real multi-version prompt, independent of the golden file, to pin down
    exactly which behavior would break if this regressed."""
    session = requests.Session()
    session.auth = (PUBLIC_KEY, SECRET_KEY)

    resp = session.get(
        f"{BASE_URL}/api/public/v2/prompts", params={"name": PROMPT_GREETING}
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    assert len(data) == 1
    assert sorted(data[0]["versions"]) == [1, 2]

    resp = session.get(
        f"{BASE_URL}/api/public/v2/prompts", params={"name": PROMPT_SYSTEM}
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    assert len(data) == 1
    assert data[0]["versions"] == [1]
    assert data[0]["type"] == "chat"
