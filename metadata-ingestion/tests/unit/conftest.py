import pytest

from tests.test_helpers.masking_state_helpers import reset_masking_process_state


@pytest.fixture(autouse=True)
def isolated_masking_state():
    reset_masking_process_state()
    yield
    reset_masking_process_state()
