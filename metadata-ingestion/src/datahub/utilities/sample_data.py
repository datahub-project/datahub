import os
import pathlib
import tempfile
from typing import Optional

import requests

from datahub import git_tag

BOOTSTRAP_MCES_FILE = "metadata-ingestion/examples/mce_files/bootstrap_mce.json"


def bootstrap_mces_url(version: Optional[str] = None) -> str:
    return f"{docker_compose_base(version)}/{BOOTSTRAP_MCES_FILE}"


def docker_compose_base(version: Optional[str] = None) -> str:
    return os.getenv(
        "DOCKER_COMPOSE_BASE",
        f"https://raw.githubusercontent.com/datahub-project/datahub/{git_tag(version)}",
    )


def download_sample_data() -> pathlib.Path:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_file:
        path = pathlib.Path(tmp_file.name)

        # Download the bootstrap MCE file from GitHub.
        mce_json_download_response = requests.get(bootstrap_mces_url())
        mce_json_download_response.raise_for_status()
        tmp_file.write(mce_json_download_response.content)
    return path
