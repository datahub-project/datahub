"""
Backend capabilities for the smoke-test suite.

Smoke tests run against one of two stacks:

- ``docker``: the Docker quickstart. It has every capability below.
- ``local``: the Docker-free stack (``datahub-dev.sh start --local``): GMS over
  H2, embedded OpenSearch, an in-memory pgQueue, and the integrations service
  and executor as local processes.

A test that needs something the local stack lacks declares it:

    pytestmark = pytest.mark.requires(Capability.KAFKA, Capability.SQL_DB)

    @pytest.mark.requires("kafka")
    def test_needs_a_broker(): ...

``SMOKE_BACKEND`` picks the stack and ``SMOKE_BACKEND_MODE`` picks what to
run on it:

- ``select`` (default): every test the backend can run. On ``docker`` that
  is every test, so the default changes nothing.
- ``canary``: only the tests the local stack cannot run, plus tests marked
  ``docker_canary``. Used for the Docker lane that runs next to the local one.

Selection deselects rather than skips: a skipped test reads as a pass in
reports and in the retry action.
"""

from enum import Enum
from typing import FrozenSet, List, Sequence, Set, TypeVar

from _pytest.nodes import Item

from utilities import env_vars

REQUIRES_MARKER = "requires"
DOCKER_CANARY_MARKER = "docker_canary"


class Capability(str, Enum):
    """Something a test needs from the stack it runs against."""

    KAFKA = "kafka"
    SQL_DB = "sql_db"
    DOCKER = "docker"
    PLAY_FRONTEND = "play_frontend"
    EXECUTOR_IMAGE = "executor_image"
    OBJECT_STORAGE = "object_storage"
    INTEGRATIONS = "integrations"
    # Runs on Docker, fails on the Docker-free stack for a reason still under
    # investigation. Remove the mark once the cause is fixed or given a real
    # capability.
    DOCKER_FREE_INVESTIGATION = "docker_free_investigation"


ALL_CAPABILITIES: FrozenSet[str] = frozenset(c.value for c in Capability)

# What the Docker-free local stack provides. Grow this as the launcher grows;
# it is the only place a capability moves onto the local lane.
LOCAL_CAPABILITIES: FrozenSet[str] = frozenset({Capability.INTEGRATIONS.value})


class Backend(str, Enum):
    DOCKER = "docker"
    LOCAL = "local"

    @property
    def weights_stem(self) -> str:
        """Weights file (``<stem>.json`` + ``<stem>_meta.json``) that shard_pack packs
        this stack's batches on. Tests run at different speeds on the Docker-free
        stack, so it has its own durations, refreshed by update-test-weights.yml."""
        if self is Backend.LOCAL:
            return "pytest_test_weights_docker_free"
        return "pytest_test_weights"


class Mode(str, Enum):
    SELECT = "select"
    CANARY = "canary"


def parse_backend(value: str) -> Backend:
    try:
        return Backend(value.strip().lower())
    except ValueError:
        raise ValueError(
            f"Unknown SMOKE_BACKEND {value!r}. "
            f"Valid values: {', '.join(b.value for b in Backend)}"
        ) from None


def parse_mode(value: str) -> Mode:
    try:
        return Mode(value.strip().lower())
    except ValueError:
        raise ValueError(
            f"Unknown SMOKE_BACKEND_MODE {value!r}. "
            f"Valid values: {', '.join(m.value for m in Mode)}"
        ) from None


def requirements_of(item: Item) -> Set[str]:
    """Capabilities a test needs: the union of its ``requires`` markers at any level."""
    required = {
        arg.value if isinstance(arg, Capability) else str(arg).strip().lower()
        for marker in item.iter_markers(REQUIRES_MARKER)
        for arg in marker.args
    }
    unknown = sorted(required - ALL_CAPABILITIES)
    if unknown:
        raise ValueError(
            f"{item.nodeid}: unknown requires(...) capability {', '.join(unknown)}. "
            f"Valid capabilities: {', '.join(sorted(ALL_CAPABILITIES))}"
        )
    return required


def capabilities_of(backend: Backend) -> FrozenSet[str]:
    return ALL_CAPABILITIES if backend is Backend.DOCKER else LOCAL_CAPABILITIES


def is_local_blocked(item: Item) -> bool:
    return not requirements_of(item) <= LOCAL_CAPABILITIES


_T = TypeVar("_T", bound=Item)


def backend_filtered_items(
    backend: Backend, mode: Mode, items: Sequence[_T]
) -> List[_T]:
    """The items to run for ``backend`` and ``mode``; ``items`` is not mutated.

    Raises ValueError on an unknown capability name even when nothing is
    filtered, so a typo fails every lane rather than only the local one.
    """
    if mode is Mode.CANARY:
        return [
            item
            for item in items
            if is_local_blocked(item) or item.get_closest_marker(DOCKER_CANARY_MARKER)
        ]
    available = capabilities_of(backend)
    return [item for item in items if requirements_of(item) <= available]


def require_capability(capability: Capability) -> None:
    """Fail fast when a test reaches a capability the configured backend lacks.

    Called from the helpers that hand out Kafka and SQL connection details. On
    the local stack a test missing its ``requires`` marker would otherwise fail
    on a connection timeout; this names the marker to add instead.
    """
    backend = parse_backend(env_vars.get_smoke_backend())
    if capability.value not in capabilities_of(backend):
        raise RuntimeError(
            f"This test needs {capability.value!r}, which SMOKE_BACKEND={backend.value} "
            f"does not provide. Add pytest.mark.requires(Capability."
            f"{capability.name}) to the test or its module (utilities/backends.py)."
        )
