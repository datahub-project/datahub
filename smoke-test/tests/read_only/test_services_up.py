import os
import re

import pytest
import requests

from tests.utils import get_gms_url, wait_for_healthcheck_util

# Kept separate so that it does not cause failures in PRs
DATAHUB_VERSION = os.getenv("TEST_DATAHUB_VERSION")


@pytest.mark.read_only
def test_services_up():
    wait_for_healthcheck_util()


def looks_like_a_short_sha(sha: str) -> bool:
    return len(sha) == 7 and re.match(r"[0-9a-f]{7}", sha) is not None


@pytest.mark.read_only
def test_gms_config_accessible() -> None:
    gms_config = requests.get(f"{get_gms_url()}/config").json()
    assert gms_config is not None

    if DATAHUB_VERSION is not None:
        assert gms_config["versions"]["acryldata/datahub"]["version"] == DATAHUB_VERSION
    else:
        print("[WARN] TEST_DATAHUB_VERSION is not set")

    # Make sure that the default CLI version gets generated properly.
    # While we don't want to hardcode the actual value, we can make
    # sure it mostly looks like a version string.
    default_cli_version: str = gms_config["managedIngestion"]["defaultCliVersion"]
    print(f"Default CLI version: {default_cli_version}")
    assert not default_cli_version.startswith("@")
    assert "." in default_cli_version or looks_like_a_short_sha(
        default_cli_version
    ), "Default CLI version does not look like a version string"
