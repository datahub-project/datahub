import logging
import os
import sys
import time

import pytest

from tests.test_helpers.docker_helpers import docker_compose_runner  # noqa: F401

# See https://stackoverflow.com/a/33515264.
sys.path.append(os.path.join(os.path.dirname(__file__), "test_helpers"))

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.putenv("DATAHUB_DEBUG", "1")


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)
    yield
