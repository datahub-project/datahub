import sys

import pytest

import datahub.masking.bootstrap as masking_bootstrap
from datahub.masking.masking_filter import uninstall_masking_filter
from datahub.masking.secret_registry import SecretRegistry


def _reset_masking_process_state() -> None:
    uninstall_masking_filter()
    if masking_bootstrap._original_excepthook is not None:
        sys.excepthook = masking_bootstrap._original_excepthook
        masking_bootstrap._original_excepthook = None
    masking_bootstrap._bootstrap_completed = False
    masking_bootstrap._bootstrap_error = None
    SecretRegistry.reset_instance()


@pytest.fixture(autouse=True)
def isolated_masking_state():
    _reset_masking_process_state()
    yield
    _reset_masking_process_state()
