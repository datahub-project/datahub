import base64
import json
import logging
import os
import shutil
import time
from pathlib import Path

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from datahub.ingestion.glossary.classification_mixin import ClassificationConfig
from datahub.ingestion.glossary.classifier import DynamicTypedClassifierConfig
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.couchbase.retry import retry
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

retries = Retry(total=5, backoff_factor=2, status_forcelist=[404, 405, 500, 503])
adapter = HTTPAdapter(max_retries=retries)
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)


class BasicAuth(AuthBase):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode("ascii")
        auth_encoded = base64.b64encode(auth_bytes)
        request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }
        r.headers.update(request_headers)
        return r


def http_test_get(url: str, auth: BasicAuth) -> int:
    response = session.get(
        url,
        verify=False,
        timeout=15,
        auth=auth,
    )
    return response.status_code


def http_test_post(url: str, auth: BasicAuth, data: dict) -> dict:
    response = session.post(
        url,
        verify=False,
        timeout=15,
        auth=auth,
        data=data,
    )
    assert response.status_code == 200
    return json.loads(response.text)


@pytest.mark.integration_batch_2
def test_couchbase_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/couchbase"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "couchbase"
    ) as docker_services:
        if "PYTEST_ENABLE_FILE_LOGGING" in os.environ:
            log_file = os.path.join(Path.home(), "pytest.log")
            file_handler = logging.FileHandler(log_file)
            logging.getLogger().addHandler(file_handler)

        wait_for_port(docker_services, "testdb", 8093)

        result = http_test_get(
            "http://127.0.0.1:8091/pools/default/buckets/data",
            auth=BasicAuth("Administrator", "password"),
        )
        assert result == 200

        @retry(factor=1)
        def collection_wait():
            response = http_test_post(
                "http://127.0.0.1:8093/query/service",
                auth=BasicAuth("Administrator", "password"),
                data={"statement": "SELECT count(*) as count FROM data.data.customers"},
            )
            results = response.get("results", [{}])
            assert results[0].get("count", 0) == 1000

        collection_wait()

        time.sleep(2)

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(
            {
                "run_id": "couchbase-test",
                "source": {
                    "type": "couchbase",
                    "config": {
                        "connect_string": "couchbases://127.0.0.1",
                        "username": "Administrator",
                        "password": "password",
                        "cluster_name": "testdb",
                        "profiling": {
                            "enabled": True,
                            "profile_nested_fields": True,
                        },
                        "classification": ClassificationConfig(
                            enabled=True,
                            classifiers=[
                                DynamicTypedClassifierConfig(
                                    type="datahub",
                                    config=DataHubClassifierConfig(
                                        minimum_values_threshold=1,
                                    ),
                                )
                            ],
                        ),
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/couchbase_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        if "PYTEST_SAVE_OUTPUT_FILE" in os.environ:
            shutil.copyfile(
                f"{tmp_path}/couchbase_mces.json",
                os.path.join(Path.home(), "couchbase_mces.json"),
            )
        else:
            # Verify the output.
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=tmp_path / "couchbase_mces.json",
                golden_path=test_resources_dir / "couchbase_mces_golden.json",
                ignore_paths=[
                    r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
                ],
            )
