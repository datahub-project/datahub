import pytest

from tests.utilities.messaging_transport import is_pgqueue_transport


@pytest.fixture
def require_pgqueue(auth_session):
    """Skip unless GMS is running with pgQueue as the active messaging transport."""
    if not is_pgqueue_transport(auth_session):
        pytest.skip("pgQueue is not the active messaging transport")
    return auth_session
