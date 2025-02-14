import logging
import os
import pathlib
import time
from typing import List

import pytest

os.environ["DATAHUB_SUPPRESS_LOGGING_MANAGER"] = "1"
os.environ["DATAHUB_TEST_MODE"] = "1"

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
from datahub.testing.pytest_hooks import (  # noqa: F401,E402
    load_golden_flags,
    pytest_addoption,
)
from tests.test_helpers.docker_helpers import (  # noqa: F401,E402
    docker_compose_command,
    docker_compose_runner,
)
from tests.test_helpers.state_helpers import (  # noqa: F401,E402
    mock_datahub_graph,
    mock_datahub_graph_instance,
)

try:
    # See https://github.com/spulec/freezegun/issues/98#issuecomment-590553475.
    import pandas  # noqa: F401
except ImportError:
    pass

import freezegun  # noqa: E402

# The freezegun library has incomplete type annotations.
# See https://github.com/spulec/freezegun/issues/469
freezegun.configure(extend_ignore_list=["datahub.utilities.cooperative_timeout"])  # type: ignore[attr-defined]


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)

    yield


def pytest_collection_modifyitems(
    config: pytest.Config, items: List[pytest.Item]
) -> None:
    # https://docs.pytest.org/en/latest/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    # Adapted from https://stackoverflow.com/a/57046943/5004662.

    root = pathlib.Path(config.rootpath)
    integration_path = root / "tests/integration"

    for item in items:
        test_path = item.path

        if (
            "docker_compose_runner" in item.fixturenames  # type: ignore[attr-defined]
            or any(
                marker.name == "integration_batch_2" for marker in item.iter_markers()
            )
        ):
            item.add_marker(pytest.mark.slow)

        is_already_integration = any(
            marker.name == "integration" for marker in item.iter_markers()
        )

        if integration_path in test_path.parents or is_already_integration:
            # If it doesn't have a marker yet, put it in integration_batch_0.
            if not any(
                marker.name.startswith("integration_batch_")
                for marker in item.iter_markers()
            ):
                item.add_marker(pytest.mark.integration_batch_0)

            # Mark everything as an integration test.
            if not is_already_integration:
                item.add_marker(pytest.mark.integration)
