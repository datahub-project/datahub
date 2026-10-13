"""Test-only reset of the process-global masking state.

Masking has no production teardown; tests reset the state directly to stay
isolated. Shared between the unit and integration suites so the cleanup
cannot drift.
"""

import sys
from typing import Iterator

import pytest

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


@pytest.fixture
def _isolate_secret_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The registry is a process-global singleton and masking is opt-out, so a
    secret registered by one test stays registered for the rest of the session
    and would silently mask it out of a later test's output. Loading a stdin
    envelope now registers, so contain it here rather than leave an
    order-dependent flake for someone to find.

    Not autouse: a test driving `datahub recipe` opts in with
    `pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")`."""
    # Imported here, not at the top: the root conftest loads this module for
    # every test, and only the ones opting in need the CLI module.
    import datahub.cli.recipe_cli as rc

    # Cleared IN PLACE rather than replaced. SecretMaskingFilter caches the
    # registry it was built with (masking_filter.py: `self._registry =
    # secret_registry or SecretRegistry.get_instance()`), and the `recipe`
    # group installs those filters on process-global handlers that outlive
    # this fixture. reset_instance() swaps the singleton underneath them, so
    # every handler installed by an earlier test goes on masking against a
    # dead registry and a later test's secrets reach the output unmasked --
    # the very leak this fixture exists to prevent, arriving by a different
    # door. Verified: with reset_instance(), an already-built filter masks
    # nothing registered afterwards; with clear(), it picks up the new
    # secrets and forgets the old ones.
    SecretRegistry.get_instance().clear()

    # The stdin envelope globals, for the same reason and in the same place,
    # rather than in each test's body. Tests that dispatch the CLI get this
    # from the `recipe` group callback too; the ones calling
    # rc._load_recipe("-") directly bypass the group, so the fixture is what
    # covers them.
    monkeypatch.setattr(rc, "_stdin_secrets", {})

    yield
    SecretRegistry.get_instance().clear()
