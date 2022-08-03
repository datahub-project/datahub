import os

import pytest
import requests

from tests.utils import get_gms_url, wait_for_healthcheck_util

DATAHUB_VERSION = os.getenv("DATAHUB_VERSION")


@pytest.mark.read_only
def test_services_up():
    wait_for_healthcheck_util()


@pytest.mark.read_only
def test_gms_config_accessible():
    gms_config = requests.get(f"{get_gms_url()}/config").json()
    assert gms_config is not None

    if DATAHUB_VERSION is not None:
        assert gms_config["versions"]["linkedin/datahub"]["version"] == DATAHUB_VERSION
    else:
        print("[WARN] DATAHUB_VERSION is not set")
