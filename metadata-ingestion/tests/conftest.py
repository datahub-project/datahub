import atexit
import fnmatch
import json
import logging
import os
import pathlib
import re
import statistics
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

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

# Give column-level lineage a large budget rather than the 10s default. The deadline
# is wall-clock (time.perf_counter_ns), so a CI runner that stalls under load exhausts
# it without the parser doing any work; column lineage is then dropped and the affected
# node is emitted with an empty schema and no fineGrainedLineages, which reads as a
# golden-file mismatch rather than the graceful degradation it is in production. A 30x
# budget makes that vanishingly unlikely while still bounding a pathological parse.
os.environ["SQL_LINEAGE_TIMEOUT_SECONDS"] = "300"

# Never look for cloud credentials on the instance metadata server. A test that
# builds a boto3 client or calls google.auth.default() without credentials makes
# botocore query the EC2 metadata endpoint and google-auth probe the GCE one
# (3 attempts x 3s). Where 169.254.169.254 refuses at once, as on a laptop, that
# costs nothing; where it drops packets, as on some CI runners, each of those
# tests waits out the timeouts, and the unit suite grows from ~20 to ~45 min.
# The lookups find no credentials either way, so the outcome is unchanged.
# google-auth reads NO_GCE_CHECK at import, so this has to stay above the imports.
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["NO_GCE_CHECK"] = "true"

# Installs the sys.meta_path finder that provides a pkg_resources shim when
# setuptools>=82 has removed it, before any test imports the redshift/cockroachdb
# dialects (which import pkg_resources at load).
import datahub._pkg_resources_finder  # noqa: E402,F401


@atexit.register
def _report_threads_alive_at_exit() -> None:
    # A non-daemon thread from a native client library that is still running when the
    # interpreter finalizes can segfault the process long after the test session
    # reported success (see the intermittent exit-139 CI failures). faulthandler shows
    # the faulting thread; this names the threads that outlived the session.
    lingering = [t for t in threading.enumerate() if t is not threading.main_thread()]
    if lingering:
        print(
            f"[atexit] {len(lingering)} non-main thread(s) alive at interpreter shutdown:",
            flush=True,
        )
        for thread in lingering:
            print(f"[atexit]   {thread.name} daemon={thread.daemon}", flush=True)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # The Power BI M-Query tests leave the process-wide MiniRacer open. Finalized
    # during interpreter shutdown, it joins its event-loop thread, which can no
    # longer run, and the process hangs after every test passed. Close it while
    # threads still run.
    try:
        from datahub.ingestion.source.powerbi.m_query._bridge import _clear_bridge

        _clear_bridge()
    except ImportError:
        pass


# We need our imports to go below the os.environ updates, since mere act
# of importing some datahub modules will load env variables.
from datahub.sql_parsing.sqlglot_lineage import (  # noqa: E402
    SQL_LINEAGE_TIMEOUT_SECONDS,
)
from datahub.testing.pytest_hooks import (  # noqa: F401,E402
    load_golden_flags,
    local_timezone,
    pytest_addoption,
    pytest_configure,
)
from tests.test_helpers.docker_helpers import (  # noqa: F401,E402
    docker_compose_command,
    docker_compose_runner,
)
from tests.test_helpers.masking_state_helpers import (  # noqa: F401,E402
    _isolate_secret_registry,
)
from tests.test_helpers.state_helpers import (  # noqa: F401,E402
    mock_datahub_graph,
    mock_datahub_graph_instance,
)

# sqlglot_lineage reads SQL_LINEAGE_TIMEOUT_SECONDS into a module constant at import
# time, so the env var above only has an effect while it is set before that import.
# Moving these imports above the env block would silently restore the 10s deadline and
# with it the flake, so assert the outcome rather than trusting the ordering to survive
# a tidy-up.
assert SQL_LINEAGE_TIMEOUT_SECONDS >= 300, (
    f"SQL lineage timeout is {SQL_LINEAGE_TIMEOUT_SECONDS}s, not the 300s set above:"
    " the os.environ block in this file has to stay above the datahub imports for it"
    " to take effect."
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

    shard = _unit_shard(config)
    if shard is not None and shard.owns(collection_path) is False:
        return True

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


class _MarkerProto(Protocol):
    """Read-only marker contract: only ``name`` is read by the helpers below."""

    @property
    def name(self) -> str: ...


class _MarkedItemProto(Protocol):
    """Item contract for helpers that only read marker names via ``iter_markers``."""

    def iter_markers(self) -> Iterable[_MarkerProto]: ...


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


def _read_integration_weights() -> Dict[str, float]:
    """Read {test_id: seconds} from the weights file, raising if it is unusable."""
    with open(_INTEGRATION_WEIGHTS_FILE) as f:
        data = json.load(f)
    return {entry["testId"]: float(entry["duration"].rstrip("s")) for entry in data}


_WEIGHTS_ERRORS = (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError)


def _load_integration_weights() -> Dict[str, float]:
    """Load {test_id: seconds} from the weights file, or {} if unavailable."""
    if not _INTEGRATION_WEIGHTS_FILE.exists():
        return {}
    try:
        return _read_integration_weights()
    except _WEIGHTS_ERRORS as exc:
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


# Unit test sharding. CI splits tests/unit across parallel jobs by setting
# DATAHUB_UNIT_TEST_SHARD="<index>/<count>" (0-based). Whole files are assigned,
# so module-level fixtures and imports stay together, and a shard never imports
# another shard's files. Weights come from the same file as the integration
# batches, which the update-test-weights workflow refreshes from CI runs.
_UNIT_SHARD_ENV = "DATAHUB_UNIT_TEST_SHARD"
_UNIT_SHARD_RE = re.compile(r"^(\d+)/(\d+)$")
# Per-file cost the JUnit test times do not include: importing the module and
# its module-scoped fixtures. Measured on the first sharded CI run as (session
# wall time - sum of test times) / files, which came to 0.22-0.27s on each of
# the three multi-file shards. Re-measure if collection gets much slower.
_UNIT_FILE_OVERHEAD_SECONDS = 0.25


def _parse_unit_shard(value: str) -> Optional[Tuple[int, int]]:
    """Parse ``"<index>/<count>"``; empty means no sharding."""
    if not value.strip():
        return None
    match = _UNIT_SHARD_RE.match(value.strip())
    if not match:
        raise pytest.UsageError(
            f"{_UNIT_SHARD_ENV}={value!r}: expected <index>/<count>, e.g. 0/4"
        )
    index, count = int(match.group(1)), int(match.group(2))
    if count < 1 or index >= count:
        raise pytest.UsageError(
            f"{_UNIT_SHARD_ENV}={value!r}: index must be in [0, count)"
        )
    return index, count


def _module_id(path: pathlib.Path, root: pathlib.Path) -> str:
    """``tests/unit/foo/test_x.py`` -> ``tests.unit.foo.test_x`` (JUnit classname)."""
    return ".".join(path.relative_to(root).with_suffix("").parts)


def _unit_file_weights(
    modules: Sequence[str], weights: Dict[str, float]
) -> Dict[str, float]:
    """Estimated seconds per test module.

    A test id's classname is the module, optionally followed by a class name, so
    each id is attributed to the longest module prefix. Modules with no recorded
    tests (new files) get the median module weight.
    """
    module_set = set(modules)
    totals: Dict[str, float] = defaultdict(float)
    for test_id, duration in weights.items():
        candidate = test_id.split("::", 1)[0]
        while candidate and candidate not in module_set:
            candidate = candidate.rpartition(".")[0]
        if candidate:
            totals[candidate] += duration
    fallback = statistics.median(totals.values()) if totals else _DEFAULT_TEST_WEIGHT
    return {
        module: totals.get(module, fallback) + _UNIT_FILE_OVERHEAD_SECONDS
        for module in modules
    }


def _assign_unit_shards(module_weights: Dict[str, float], count: int) -> Dict[str, int]:
    """Map every module to exactly one shard, balancing estimated time."""
    groups = [
        _ConnectorGroup(key=module, items=[], weight=weight)
        for module, weight in module_weights.items()
    ]
    return {
        group.key: index
        for index, bin_groups in enumerate(_bin_pack_groups(groups, count))
        for group in bin_groups
    }


@dataclass
class _UnitShard:
    index: int
    count: int
    root: pathlib.Path
    unit_dir: pathlib.Path
    python_files: List[str]
    assignment: Dict[str, int]
    estimated_seconds: float

    def is_test_file(self, path: pathlib.Path) -> bool:
        return path.suffix == ".py" and any(
            fnmatch.fnmatch(path.name, pattern) for pattern in self.python_files
        )

    def owns(self, path: pathlib.Path) -> Optional[bool]:
        """Whether this shard runs ``path``; None for anything but a unit test file."""
        if self.unit_dir not in path.parents or not self.is_test_file(path):
            return None
        module = _module_id(path, self.root)
        # A file created after the assignment was computed cannot occur within a
        # session, but if it does, run it rather than drop it.
        return self.assignment.get(module, self.index) == self.index


_UNIT_SHARD_KEY = pytest.StashKey[Optional[_UnitShard]]()


def _build_unit_shard(
    index: int,
    count: int,
    root: pathlib.Path,
    python_files: List[str],
    weights: Dict[str, float],
) -> _UnitShard:
    """Assign every test file under tests/unit to a shard and return shard ``index``."""
    unit_dir = root / "tests" / "unit"
    shard = _UnitShard(
        index=index,
        count=count,
        root=root,
        unit_dir=unit_dir,
        python_files=python_files,
        assignment={},
        estimated_seconds=0.0,
    )
    modules = sorted(
        _module_id(path, root)
        for path in unit_dir.rglob("*.py")
        if shard.is_test_file(path)
    )
    module_weights = _unit_file_weights(modules, weights)
    shard.assignment = _assign_unit_shards(module_weights, count)
    shard.estimated_seconds = sum(
        weight
        for module, weight in module_weights.items()
        if shard.assignment[module] == index
    )
    return shard


def _unit_shard(config: pytest.Config) -> Optional[_UnitShard]:
    if _UNIT_SHARD_KEY in config.stash:
        return config.stash[_UNIT_SHARD_KEY]
    parsed = _parse_unit_shard(os.environ.get(_UNIT_SHARD_ENV, ""))
    shard: Optional[_UnitShard] = None
    if parsed is not None:
        # Each shard computes the assignment on its own, so all of them must
        # read the same weights. Falling back to {} on one runner would give it
        # a different partition, and some files would then run in no shard while
        # every job still passed. Fail the job instead.
        try:
            weights = _read_integration_weights()
        except _WEIGHTS_ERRORS as exc:
            raise pytest.UsageError(
                f"{_UNIT_SHARD_ENV} is set but {_INTEGRATION_WEIGHTS_FILE} "
                f"could not be read: {exc}"
            ) from exc
        shard = _build_unit_shard(
            parsed[0],
            parsed[1],
            pathlib.Path(config.rootpath),
            list(config.getini("python_files")),
            weights,
        )
    config.stash[_UNIT_SHARD_KEY] = shard
    return shard


def pytest_report_header(config: pytest.Config) -> Optional[str]:
    shard = _unit_shard(config)
    if shard is None:
        return None
    files = sum(1 for index in shard.assignment.values() if index == shard.index)
    return (
        f"unit test shard {shard.index}/{shard.count}: {files} of "
        f"{len(shard.assignment)} files, ~{shard.estimated_seconds / 60:.1f} min "
        f"estimated"
    )


def _explicit_batch(item: _MarkedItemProto) -> Optional[int]:
    """Return the explicit integration_batch_N marker on an item, if any."""
    for marker in item.iter_markers():
        match = _TARGET_BATCH_RE.match(marker.name)
        if match:
            return int(match.group(1))
    return None


def _target_batch(config: pytest.Config) -> Optional[int]:
    """The batch this pytest invocation is selecting via ``-m``, if any.

    Returns the batch number when exactly one ``integration_batch_N`` marker is
    in the ``-m`` expression. Zero matches means a full-suite run (e.g.
    ``testFull``) — keep every batch. More than one match is ambiguous: we fall
    back to keeping every batch and warn, since CI never selects multiple
    batches in one invocation.
    """
    expr = config.getoption("-m", default="") or ""
    matches = _TARGET_BATCH_RE.findall(expr)
    if len(matches) == 1:
        return int(matches[0])
    if len(matches) > 1:
        logger.warning(
            "Multiple integration_batch_N markers in -m expression %r; "
            "keeping all batches. Use a single batch marker for batch runs.",
            expr,
        )
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
        # Any item routed to an integration batch is a slow integration test.
        # Step 1 only marks items slow if they already had a batch marker or a
        # docker fixture; dynamically-assigned non-docker integration tests
        # would otherwise miss `slow`, so add it here too (idempotent).
        item.add_marker(pytest.mark.slow)
        if target_batch is None or batch_index == target_batch:
            kept.append(item)
    items[:] = kept
