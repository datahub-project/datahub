# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pathlib
import tempfile

import requests

from datahub.configuration.env_vars import get_docker_compose_base

DOCKER_COMPOSE_BASE = (
    get_docker_compose_base()
    or "https://raw.githubusercontent.com/datahub-project/datahub/master"
)
BOOTSTRAP_MCES_FILE = "metadata-ingestion/examples/mce_files/bootstrap_mce.json"
BOOTSTRAP_MCES_URL = f"{DOCKER_COMPOSE_BASE}/{BOOTSTRAP_MCES_FILE}"


def download_sample_data() -> pathlib.Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_file:
        path = pathlib.Path(tmp_file.name)

        # Download the bootstrap MCE file from GitHub.
        mce_json_download_response = requests.get(BOOTSTRAP_MCES_URL)
        mce_json_download_response.raise_for_status()
        tmp_file.write(mce_json_download_response.content)
    return path
