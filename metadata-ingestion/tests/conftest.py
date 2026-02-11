import logging
import os
import pathlib
import time
from typing import List, Optional

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
        return 1615443388.0975091  # 2021-03-11 06:16:28.097509

    monkeypatch.setattr(time, "time", fake_time)

    yield


def pytest_ignore_collect(
    collection_path: pathlib.Path, config: pytest.Config
) -> Optional[bool]:
    """Prevent collecting non-recording tests when running recording batch.

    Pytest's collection phase imports all test files to check their markers.
    Some test files (e.g., test_postgres_source.py) use @patch decorators
    that patch SQLAlchemy's create_engine at import time. These patches
    interfere with the recording system's own patching mechanism.

    When running with integration_batch_recording marker, we skip collection
    (and thus import) of all test files outside the recording directory to
    prevent this interference.
    """
    # Check if we're running with integration_batch_recording marker
    marker_expr = config.getoption("-m", default="")
    if marker_expr and "integration_batch_recording" in marker_expr:
        # collection_path is already a pathlib.Path in pytest 7.0+
        path_obj = collection_path

        # Only collect files from the recording directory
        root = pathlib.Path(config.rootpath)
        recording_dir = root / "tests" / "integration" / "recording"
        # Allow collection if:
        # 1. File is in the recording directory, OR
        # 2. It's a conftest.py (needed for fixtures), OR
        # 3. It's not a test file (e.g., __init__.py, helpers)
        is_recording_test = (
            recording_dir in path_obj.parents or path_obj.parent == recording_dir
        )
        is_test_file = path_obj.name.startswith("test_") and path_obj.suffix == ".py"
        is_conftest = path_obj.name == "conftest.py"

        # Skip collection if it's a test file outside recording directory
        # (conftest.py files are allowed to be collected for fixtures)
        if is_test_file and not is_recording_test and not is_conftest:
            return True  # Ignore this file

    return None  # Let pytest decide for other cases


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
                marker.name.startswith("integration_batch_")
                for marker in item.iter_markers()
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
