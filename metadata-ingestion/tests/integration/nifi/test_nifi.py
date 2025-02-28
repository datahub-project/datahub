import logging
import time

import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port

pytestmark = pytest.mark.integration_batch_2

FROZEN_TIME = "2021-12-03 12:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/nifi"


@pytest.fixture(scope="module")
def loaded_nifi(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "nifi"
    ) as docker_services:
        wait_for_port(
            docker_services,
            container_name="nifi1",
            container_port=9443,
            timeout=300,
        )
        wait_for_port(
            docker_services,
            container_name="nifi01",
            container_port=9080,
            timeout=60,
        )
        wait_for_port(
            docker_services,
            container_name="nifi02",
            container_port=9081,
            timeout=60,
        )
        wait_for_port(
            docker_services,
            container_name="nifi03",
            container_port=9082,
            timeout=60,
        )
        yield docker_services

    # The nifi image is pretty large, so we remove it after the test.
    cleanup_image("apache/nifi")


@freeze_time(FROZEN_TIME)
def test_nifi_ingest_standalone(
    loaded_nifi, pytestconfig, tmp_path, test_resources_dir
):
    # Wait for nifi standalone to execute all lineage processors, max wait time 120 seconds
    url = "http://localhost:9443/nifi-api/flow/process-groups/80404c81-017d-1000-e8e8-af7420af06c1"
    for i in range(23):
        logging.info("waiting...")
        time.sleep(5)
        resp = requests.get(url)
        if resp.status_code != 200:
            continue
        else:
            pgs = resp.json()["processGroupFlow"]["flow"]["processors"]
            statuses = [pg["status"] for pg in pgs]
            status = next(s for s in statuses if s["name"] == "FetchS3Object")

            if status["aggregateSnapshot"]["flowFilesOut"] >= 1:
                logging.info(f"Waited for time {i * 5} seconds")
                break

    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):
        # Run nifi ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "nifi-test-standalone",
                "source": {
                    "type": "nifi",
                    "config": {
                        "site_url": "http://localhost:9443/nifi/",
                        "process_group_pattern": {"deny": ["^WIP"]},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": "./nifi_mces.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output. ignore values for aspects having last_event_time values
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="nifi_mces.json",
            golden_path=test_resources_dir / "nifi_mces_golden_standalone.json",
            ignore_paths=[
                *mce_helpers.IGNORE_PATH_TIMESTAMPS,
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_event_time'\]",
            ],
        )


@freeze_time(FROZEN_TIME)
def test_nifi_ingest_cluster(loaded_nifi, pytestconfig, tmp_path, test_resources_dir):
    # Wait for nifi cluster to execute all lineage processors, max wait time 120 seconds
    url = "http://localhost:9080/nifi-api/flow/process-groups/root"
    for i in range(23):
        logging.info("waiting...")
        time.sleep(5)
        resp = requests.get(url)
        if resp.status_code != 200:
            continue
        else:
            pgs = resp.json()["processGroupFlow"]["flow"]["processGroups"]
            statuses = [pg["status"] for pg in pgs]
            status = next(s for s in statuses if s["name"] == "Cluster_Site_S3_to_S3")
            if status["aggregateSnapshot"]["flowFilesSent"] >= 1:
                logging.info(f"Waited for time {i * 5} seconds")
                break
    test_resources_dir = pytestconfig.rootpath / "tests/integration/nifi"
    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):
        # Run nifi ingestion run.
        pipeline = Pipeline.create(
            {
                "run_id": "nifi-test-cluster",
                "source": {
                    "type": "nifi",
                    "config": {
                        "site_url": "http://localhost:9080/nifi/",
                        "auth": "NO_AUTH",
                        "site_url_to_site_name": {
                            "http://nifi01:9080/nifi/": "default",
                            "http://nifi02:9081/nifi/": "default",
                            "http://nifi03:9082/nifi/": "default",
                        },
                        "incremental_lineage": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": "./nifi_mces_cluster.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="nifi_mces_cluster.json",
            golden_path=test_resources_dir / "nifi_mces_golden_cluster.json",
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['last_event_time'\]",
            ],
        )
