import logging
import os
import time

import pytest

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.environ["DATAHUB_DEBUG"] = "1"

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

# Reduce retries on GMS, because this causes tests to hang while sleeping
# between retries.
os.environ["DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES"] = "1"

# We need our imports to go below the os.environ updates, since mere act
# of importing some datahub modules will load env variables.
from tests.test_helpers.docker_helpers import docker_compose_runner  # noqa: F401,E402
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401,E402

try:
    # See https://github.com/spulec/freezegun/issues/98#issuecomment-590553475.
    import pandas  # noqa: F401
except ImportError:
    pass


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
