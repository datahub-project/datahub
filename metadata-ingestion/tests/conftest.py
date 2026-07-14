import json
import logging
import os
import pathlib
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Sequence

import pytest
import time_machine

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

_MOCK_TIME = 1615443388.0975091  # 2021-03-11 06:16:28.097509+00:00


@pytest.fixture
def mock_time():
    with time_machine.travel(_MOCK_TIME, tick=False):
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
        root = pathlib.Path(config.rootpath)
        recording_dir = root / "tests" / "integration" / "recording"
        # Allow collection if:
        # 1. File is in the recording directory, OR
        # 2. It's a conftest.py (needed for fixtures), OR
        # 3. It's not a test file (e.g., __init__.py, helpers)
        is_recording_test = recording_dir in collection_path.parents
        is_test_file = (
            collection_path.name.startswith("test_") and collection_path.suffix == ".py"
        )
        is_conftest = collection_path.name == "conftest.py"

        # Skip collection if it's a test file outside recording directory
        # (conftest.py files are allowed to be collected for fixtures)
        if is_test_file and not is_recording_test and not is_conftest:
            return True  # Ignore this file

    return None  # Let pytest decide for other cases


_INTEGRATION_BATCH_COUNT = 6
# Fallback weight (seconds) when a test is absent from the weights file and no
# class-level median is available.
_DEFAULT_TEST_WEIGHT = 1.0
_INTEGRATION_WEIGHTS_FILE = (
    pathlib.Path(__file__).parent / "integration_test_weights.json"
)
_TARGET_BATCH_RE = re.compile(r"integration_batch_([0-5])(?![0-9])")
_RECORDING_MARKER = "integration_batch_recording"
# Pin marker: a connector whose existing integration_batch_N marker must be
# honored instead of being rebalanced. Rare; see setup.cfg for the contract.
_NO_BALANCE_MARKER = "integration_no_balance"

logger = logging.getLogger(__name__)


class _WeightedItem(Protocol):
    """Structural contract for items read by the balancing helpers.

    Only ``nodeid`` and ``path`` are ever read by ``_item_weight`` /
    ``_connector_key`` / bin-packing, so a full ``pytest.Item`` is not required.
    Real ``pytest.Item`` instances satisfy this; unit tests can pass a minimal
    stub with just these two attributes. Both attributes are declared as
    read-only properties to match ``pytest.Item``, where they have no setter.
    """

    @property
    def nodeid(self) -> str: ...

    @property
    def path(self) -> pathlib.Path: ...


@dataclass
class _ConnectorGroup:
    """All integration items belonging to one connector directory."""

    key: str
    items: Sequence[_WeightedItem]
    weight: float


def _nodeid_to_test_id(nodeid: str) -> str:
    """Convert a pytest nodeid to the junit ``classname::name`` test id.

    JUnit ``classname`` is the dotted module path plus the test class (if any),
    and ``name`` is the test function. This must match the format produced by
    ``.github/scripts/generate_test_weights.py`` so weight lookups succeed.

    Caveat: pytest splits on ``::``, so a parametrize id that itself contains
    ``::`` would mis-split. No current connector produces such ids, so this is
    accepted as a known limitation rather than defended against.
    """
    parts = nodeid.split("::")
    module = parts[0].replace("/", ".").removesuffix(".py")
    if len(parts) >= 3:
        return f"{module}.{parts[1]}::{parts[2]}"
    return f"{module}::{parts[1]}"


def _load_integration_weights() -> Dict[str, float]:
    """Load {test_id: seconds} from the weights file, or {} if unavailable."""
    if not _INTEGRATION_WEIGHTS_FILE.exists():
        return {}
    try:
        with open(_INTEGRATION_WEIGHTS_FILE) as f:
            data = json.load(f)
        return {entry["testId"]: float(entry["duration"].rstrip("s")) for entry in data}
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        logger.warning("Failed to load integration test weights: %s", exc)
        return {}


def _build_class_weight_index(weights: Dict[str, float]) -> Dict[str, float]:
    """Median weight per class id (``module.Class``) for class-level fallback."""
    grouped: Dict[str, List[float]] = defaultdict(list)
    for test_id, duration in weights.items():
        if "::" in test_id:
            grouped[test_id.split("::", 1)[0]].append(duration)
    return {
        class_id: statistics.median(durations)
        for class_id, durations in grouped.items()
    }


def _item_weight(
    item: _WeightedItem,
    weights: Dict[str, float],
    class_medians: Dict[str, float],
) -> float:
    """Resolve a per-test weight: exact → class median → constant."""
    test_id = _nodeid_to_test_id(item.nodeid)
    weight = weights.get(test_id)
    if weight is not None:
        return weight
    return class_medians.get(test_id.split("::", 1)[0], _DEFAULT_TEST_WEIGHT)


def _connector_key(item: _WeightedItem, integration_path: pathlib.Path) -> str:
    """Group key: the connector directory under tests/integration.

    Files directly under tests/integration (no connector subdir) collapse into
    a single ``__root__`` group so they are never split. Callers guarantee
    ``integration_path`` is a parent of ``item.path``.
    """
    parts = item.path.relative_to(integration_path).parts
    if len(parts) >= 2:
        return parts[0]
    return "__root__"


def _bin_pack_groups(
    groups: List[_ConnectorGroup], n_bins: int
) -> List[List[_ConnectorGroup]]:
    """Greedy largest-first bin-packing minimizing the maximum bin weight.

    Deterministic: ties on weight are broken by connector key so every batch
    job (each a separate pytest invocation) computes identical bins.
    """
    ordered = sorted(groups, key=lambda g: (-g.weight, g.key))
    bins: List[List[_ConnectorGroup]] = [[] for _ in range(n_bins)]
    bin_weights = [0.0] * n_bins
    for group in ordered:
        target = min(range(n_bins), key=lambda i: (bin_weights[i], i))
        bins[target].append(group)
        bin_weights[target] += group.weight
    return bins


def _explicit_batch(item: pytest.Item) -> Optional[int]:
    """Return the explicit integration_batch_N marker on an item, if any."""
    for marker in item.iter_markers():
        match = _TARGET_BATCH_RE.match(marker.name)
        if match:
            return int(match.group(1))
    return None


def _target_batch(config: pytest.Config) -> Optional[int]:
    """The batch this pytest invocation is selecting via ``-m``, if any."""
    expr = config.getoption("-m", default="") or ""
    matches = _TARGET_BATCH_RE.findall(expr)
    if len(matches) == 1:
        return int(matches[0])
    return None


def _is_recording_run(config: pytest.Config) -> bool:
    expr = config.getoption("-m", default="") or ""
    return _RECORDING_MARKER in expr


def pytest_collection_modifyitems(
    config: pytest.Config, items: List[pytest.Item]
) -> None:
    # https://docs.pytest.org/en/latest/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    # Adapted from https://stackoverflow.com/a/57046943/5004662.

    root = pathlib.Path(config.rootpath)
    integration_path = root / "tests/integration"

    # 1. Preserve existing slow/integration marking behaviour.
    for item in items:
        names = [marker.name for marker in item.iter_markers()]
        is_already_integration = "integration" in names

        if (
            "docker_compose_runner" in item.fixturenames  # type: ignore[attr-defined]
            or any(name.startswith("integration_batch_") for name in names)
        ):
            item.add_marker(pytest.mark.slow)

        if integration_path in item.path.parents and not is_already_integration:
            item.add_marker(pytest.mark.integration)

    # 2. Weight-based bin-packing of integration tests into the 6 batches.
    # Recording runs keep their own collection path (see pytest_ignore_collect)
    # and must not be rebalanced.
    if _is_recording_run(config):
        return

    # Carry the connector key alongside each item so it is computed once.
    integration_items: List[pytest.Item] = []
    item_keys: Dict[pytest.Item, str] = {}
    for item in items:
        if integration_path not in item.path.parents:
            continue
        if any(marker.name == _RECORDING_MARKER for marker in item.iter_markers()):
            continue
        key = _connector_key(item, integration_path)
        if key == "recording":
            continue
        integration_items.append(item)
        item_keys[item] = key
    if not integration_items:
        return

    weights = _load_integration_weights()
    class_medians = _build_class_weight_index(weights)
    target_batch = _target_batch(config)

    # Split out pinned items (integration_no_balance): these keep their explicit
    # integration_batch_N marker and are excluded from rebalancing.
    pinned: Dict[int, List[pytest.Item]] = defaultdict(list)
    grouped: Dict[str, List[pytest.Item]] = defaultdict(list)
    for item in integration_items:
        if any(marker.name == _NO_BALANCE_MARKER for marker in item.iter_markers()):
            explicit = _explicit_batch(item)
            pinned[explicit if explicit is not None else 0].append(item)
        else:
            grouped[item_keys[item]].append(item)

    groups = [
        _ConnectorGroup(
            key=key,
            items=group_items,
            weight=sum(
                _item_weight(item, weights, class_medians) for item in group_items
            ),
        )
        for key, group_items in grouped.items()
    ]
    bins = _bin_pack_groups(groups, _INTEGRATION_BATCH_COUNT)

    item_batch: Dict[_WeightedItem, int] = {}
    for batch_index, bin_groups in enumerate(bins):
        for group in bin_groups:
            for grouped_item in group.items:
                item_batch[grouped_item] = batch_index
    for batch_index, pinned_items in pinned.items():
        for item in pinned_items:
            item_batch[item] = batch_index

    # Assign the batch marker and (for batch-specific runs) keep only the
    # target batch's items. Setting items[:] here is what prevents a connector
    # from running in more than one batch: stale module-level pytestmark markers
    # cannot re-select an item that has been removed from the collection list.
    kept: List[pytest.Item] = []
    for item in items:
        if item not in item_batch:
            kept.append(item)  # non-integration / recording, leave as-is
            continue
        batch_index = item_batch[item]
        item.add_marker(getattr(pytest.mark, f"integration_batch_{batch_index}"))
        if target_batch is None or batch_index == target_batch:
            kept.append(item)
    items[:] = kept
