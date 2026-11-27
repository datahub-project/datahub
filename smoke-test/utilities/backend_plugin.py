"""Pytest plugin selecting smoke tests by the capabilities of the backend they run on.

Loaded from the root conftest, like domain_plugin, so it applies to every test
path. See utilities/backends.py for the ``requires`` marker and the
SMOKE_BACKEND / SMOKE_BACKEND_MODE variables.
"""

import logging
from typing import List, Tuple

import pytest
import requests
from _pytest.nodes import Item

from utilities import env_vars
from utilities.backends import (
    Backend,
    Mode,
    backend_filtered_items,
    parse_backend,
    parse_mode,
    requirements_of,
)
from utilities.messaging_transport import pgqueue_configured

logger = logging.getLogger(__name__)

_BACKEND = pytest.StashKey[Tuple[Backend, Mode]]()


def configured_backend(config: pytest.Config) -> Tuple[Backend, Mode]:
    """The backend and mode validated at configure time."""
    return config.stash[_BACKEND]


def pytest_configure(config: pytest.Config) -> None:
    try:
        backend = parse_backend(env_vars.get_smoke_backend())
        mode = parse_mode(env_vars.get_smoke_backend_mode())
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc
    # The canary runs the tests the local stack cannot, so it only makes sense
    # against Docker.
    if mode is Mode.CANARY and backend is not Backend.DOCKER:
        raise pytest.UsageError(
            f"SMOKE_BACKEND_MODE=canary needs SMOKE_BACKEND=docker, got {backend.value!r}"
        )
    config.stash[_BACKEND] = (backend, mode)


def pytest_sessionstart(session: pytest.Session) -> None:
    """Stop a local-lane run that is pointed at a Kafka-backed GMS.

    The local stack always runs pgQueue. A Kafka transport means the lane is
    wired to the wrong stack, and the run would test something other than
    what its selection assumes. An unreachable GMS is left to the session
    fixtures, which wait for it to come up and fail with their own error.
    """
    config = session.config
    backend, _ = configured_backend(config)
    gms_url = env_vars.get_gms_url()
    if backend is not Backend.LOCAL or config.option.collectonly or not gms_url:
        return
    try:
        response = requests.get(f"{gms_url}/config", timeout=10)
        response.raise_for_status()
        gms_config = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning(
            "SMOKE_BACKEND=local: could not read %s/config: %s", gms_url, exc
        )
        return
    if not pgqueue_configured(gms_config):
        pytest.exit(
            f"SMOKE_BACKEND=local, but GMS at {gms_url} is not running pgQueue "
            "(its /config has no pgQueue section): this looks like a Docker "
            "stack. Unset SMOKE_BACKEND or point DATAHUB_GMS_URL at the local stack.",
            returncode=pytest.ExitCode.USAGE_ERROR,
        )


def pytest_collection_modifyitems(config: pytest.Config, items: List[Item]) -> None:
    """Deselect tests the configured backend cannot run (or, in canary mode, can).

    Not trylast, so this runs before the trylast hook in tests/e2e/conftest.py
    that applies the batch plan and retry filter to the selected items.
    """
    backend, mode = configured_backend(config)
    try:
        selected = backend_filtered_items(backend, mode, items)
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc
    if len(selected) == len(items):
        return

    kept = set(map(id, selected))
    config.hook.pytest_deselected(items=[i for i in items if id(i) not in kept])
    logger.info(
        "SMOKE_BACKEND=%s SMOKE_BACKEND_MODE=%s: selected %s of %s test(s)",
        backend.value,
        mode.value,
        len(selected),
        len(items),
    )
    items[:] = selected


def pytest_runtest_setup(item: Item) -> None:
    """Record what a test requires in JUnit, so a lane's report shows why it ran there."""
    required = requirements_of(item)
    if required:
        item.user_properties.append(("requires", ",".join(sorted(required))))
