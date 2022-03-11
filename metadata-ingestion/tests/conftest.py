import logging
import os
import time

import pytest

from tests.test_helpers.docker_helpers import docker_compose_runner  # noqa: F401
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401

try:
    # See https://github.com/spulec/freezegun/issues/98#issuecomment-590553475.
    import pandas  # noqa: F401
except ImportError:
    pass

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.putenv("DATAHUB_DEBUG", "1")

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)

    yield


def pytest_addoption(parser):
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )
