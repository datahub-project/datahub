import pytest

from tests.test_helpers.masking_state_helpers import reset_masking_process_state


@pytest.fixture(autouse=True)
def isolated_masking_state():
    reset_masking_process_state()
    yield
    reset_masking_process_state()


@pytest.fixture(autouse=True)
def _no_ambient_credentials(monkeypatch):
    # Unit tests must not depend on the developer's environment. The REST emitter
    # (and thus DataHubGraph and any client built on it) resolves these from the
    # process env, so a machine with any of them exported would change what auth
    # tests assert. A test that wants one set uses monkeypatch.setenv, which runs
    # after this fixture and wins.
    for var in (
        "DATAHUB_AUTH_TYPE",
        "DATAHUB_GMS_TOKEN",
        "DATAHUB_SYSTEM_CLIENT_ID",
        "DATAHUB_SYSTEM_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)
