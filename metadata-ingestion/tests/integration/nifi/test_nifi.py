import time

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-12-03 12:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_integration
def test_nifi_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/nifi"
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

        # Wait for nifi to execute all processors
        time.sleep(120)

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
                            #                        "auth": "CLIENT_CERT",
                            #                        "client_cert_file": f"{test_resources_dir}/setup/ssl_files/client-cert.pem",
                            #                        "client_key_file": f"{test_resources_dir}/setup/ssl_files/client-private-key.pem",
                            #                        "client_key_password": "datahub",
                            #                        "ca_file": f"{test_resources_dir}/setup/ssl_files/server_certfile.pem",
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
            # TODO: ignore paths with respect to aspect value in case of MCPs
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path="nifi_mces.json",
                golden_path=test_resources_dir / "nifi_mces_golden_standalone.json",
                ignore_paths=[
                    r"root\[5\]\['aspect'\]\['value'\]",
                    r"root\[9\]\['aspect'\]\['value'\]",
                ],
            )

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
                            },
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
            # TODO: ignore paths with respect to aspect value in case of MCPs
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path="nifi_mces_cluster.json",
                golden_path=test_resources_dir / "nifi_mces_golden_cluster.json",
                ignore_paths=[
                    r"root\[5\]\['aspect'\]\['value'\]",
                    r"root\[9\]\['aspect'\]\['value'\]",
                    r"root\[17\]\['aspect'\]\['value'\]",
                    r"root\[25\]\['aspect'\]\['value'\]",
                ],
            )
