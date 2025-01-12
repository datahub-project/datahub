import pytest
import base64
import requests
import logging
import os
import shutil

from requests.auth import AuthBase
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pathlib import Path

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.glossary.classification_mixin import ClassificationConfig
from datahub.ingestion.glossary.classifier import DynamicTypedClassifierConfig
from datahub.ingestion.glossary.datahub_classifier import (
    DataHubClassifierConfig,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port


class BasicAuth(AuthBase):

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode('ascii')
        auth_encoded = base64.b64encode(auth_bytes)
        request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }
        r.headers.update(request_headers)
        return r


@pytest.mark.integration
def test_couchbase_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/couchbase"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "couchbase"
    ) as docker_services:
        if 'PYTEST_ENABLE_FILE_LOGGING' in os.environ:
            log_file = os.path.join(Path.home(), 'pytest.log')
            file_handler = logging.FileHandler(log_file)
            logging.getLogger().addHandler(file_handler)

        wait_for_port(docker_services, "testdb", 8091)

        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=[404])
        adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.get(f"http://127.0.0.1:8091/pools/default/buckets/data", verify=False, timeout=15, auth=BasicAuth("Administrator", "password"))

        assert response.status_code == 200

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
                            "enabled": True
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

        if 'PYTEST_SAVE_OUTPUT_FILE' in os.environ:
            shutil.copyfile(f"{tmp_path}/couchbase_mces.json", os.path.join(Path.home(), "couchbase_mces.json"))

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "couchbase_mces.json",
            golden_path=test_resources_dir / "couchbase_mces_golden.json",
            ignore_paths=[
                r"root\[\d+\]\['aspect'\]\['json'\]\['fieldProfiles'\]\[\d+\]\['sampleValues'\]",
            ],
        )
