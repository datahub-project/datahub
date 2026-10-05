"""Test-only reset of the process-global masking state.

Masking has no production teardown; tests reset the state directly to stay
isolated. Shared between the unit and integration suites so the cleanup
cannot drift.
"""

import sys

import datahub.masking.bootstrap as masking_bootstrap
from datahub.masking.masking_filter import uninstall_masking_filter
from datahub.masking.secret_registry import SecretRegistry


def reset_masking_process_state() -> None:
    uninstall_masking_filter()
    if isinstance(sys.excepthook, masking_bootstrap._MaskingExceptHook):
        sys.excepthook = sys.excepthook.original_excepthook
    masking_bootstrap._bootstrap_completed = False
    masking_bootstrap._bootstrap_error = None
    SecretRegistry.reset_instance()
