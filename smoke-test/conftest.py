pytest_plugins = ["tests.utilities.agent_reporter"]

import json
import logging
import os
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest
import requests
from _pytest.nodes import Item

from datahub.ingestion.graph.client import (
    DatahubClientConfig,
    DataHubGraph,
    get_default_graph,
)
from tests.test_result_msg import send_message
from tests.utilities import env_vars
from tests.utilities.domains import (
    ALL_DOMAINS,
    domains_of,
    is_selected,
    parse_requested_domains,
)
from tests.utils import (
    TestSessionWrapper,
    assert_admin_corpuser_info_preserved,
    delete_urns,
    delete_urns_from_file,
    fetch_admin_corpuser_info,
    get_frontend_session,
    ingest_file_via_rest,
    materialize_unique_dataset,
    wait_for_admin_corpuser_system_bootstrap,
    wait_for_healthcheck_util,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"
# Suppress logging manager to prevent I/O errors during pytest teardown
os.environ["DATAHUB_SUPPRESS_LOGGING_MANAGER"] = "1"


def build_auth_session():
    """Build an auth session.

    Token-based (preferred for remote instances — no login round-trip):
        Set DATAHUB_GMS_TOKEN=<pat> and DATAHUB_GMS_URL=<gms-url>.
        Frontend URL is not required; GraphQL routes through the GMS directly.

    Login-based (default for local dev):
        Set ADMIN_USERNAME / ADMIN_PASSWORD.
    """
    prebuilt_token = os.environ.get("DATAHUB_GMS_TOKEN")
    if prebuilt_token:
        logger.info("Token-based auth: using DATAHUB_GMS_TOKEN (skipping login)")
        return TestSessionWrapper(requests.Session(), prebuilt_token=prebuilt_token)

    wait_for_healthcheck_util(requests)
    auth_session = TestSessionWrapper(get_frontend_session())
    # Lag polls always use DATAHUB_GMS_TOKEN (VIEW_SYSTEM_STATUS or
    # MANAGE_SYSTEM_OPERATIONS). Publish the bootstrap admin PAT here, before
    # any wait_for_writes_to_sync() call. Restricted-user TestSessionWrappers
    # must not overwrite this.
    os.environ["DATAHUB_GMS_TOKEN"] = auth_session.gms_token()
    wait_for_admin_corpuser_system_bootstrap(auth_session)
    return auth_session


@pytest.fixture(scope="session", autouse=True)
def auth_session():
    auth_session = build_auth_session()
    os.environ["DATAHUB_GMS_TOKEN"] = auth_session.gms_token()
    yield auth_session
    auth_session.destroy()


def build_graph_client(auth_session, openapi_ingestion=False):
    graph: DataHubGraph = DataHubGraph(
        config=DatahubClientConfig(
            server=auth_session.gms_url(),
            token=auth_session.gms_token(),
            openapi_ingestion=openapi_ingestion,
        )
    )
    return graph


@pytest.fixture(scope="session")
def graph_client(auth_session) -> DataHubGraph:
    return build_graph_client(auth_session)


@pytest.fixture(scope="session")
def openapi_graph_client(auth_session) -> DataHubGraph:
    return build_graph_client(auth_session, openapi_ingestion=True)


@pytest.fixture(scope="function", autouse=True)
def clear_graph_cache():
    """Clear the get_default_graph LRU cache before each test.

    This ensures that tests using run_datahub_cmd() with custom environment
    variables get a fresh DataHubGraph instance instead of a cached one with
    stale credentials.
    """
    get_default_graph.cache_clear()
    yield


@pytest.fixture(scope="session")
def admin_corpuser_info_baseline(auth_session):
    """Snapshot privileged admin corpUserInfo flags after session bootstrap."""
    if os.environ.get("DATAHUB_GMS_TOKEN"):
        return None
    return fetch_admin_corpuser_info(auth_session)


@pytest.fixture(scope="function", autouse=True)
def verify_admin_corpuser_info_unchanged(
    auth_session, admin_corpuser_info_baseline, request
):
    """Detect tests that overwrite admin corpUserInfo and clear system/support flags."""
    yield
    if admin_corpuser_info_baseline is None:
        return
    assert_admin_corpuser_info_preserved(
        auth_session,
        admin_corpuser_info_baseline,
        context=request.node.nodeid,
    )


def _ingest_cleanup_data_impl(
    auth_session,
    graph_client,
    data_file: str,
    test_name: str,
    to_delete_urns: Optional[List[str]] = None,
):
    """Helper for ingesting test data with automatic cleanup.

    Args:
        auth_session: The authenticated session
        graph_client: The DataHub graph client
        data_file: Path to the data file to ingest
        test_name: Name of the test (for logging)
        to_delete_urns: URNs to delete after cleanup

    Usage in test files:
        @pytest.fixture(scope="module", autouse=True)
        def ingest_cleanup_data(auth_session, graph_client):
            yield from _ingest_cleanup_data_impl(
                auth_session, graph_client,
                "tests/tags_and_terms/data.json",
                "tags_and_terms"
            )
    """
    logger.info(f"deleting {test_name} test data for idempotency")
    delete_urns_from_file(graph_client, data_file)
    logger.info(f"ingesting {test_name} test data")
    ingest_file_via_rest(auth_session, data_file)
    yield
    logger.info(f"removing {test_name} test data")
    delete_urns_from_file(graph_client, data_file)
    if to_delete_urns:
        delete_urns(graph_client, to_delete_urns)
        wait_for_writes_to_sync()


def _ingest_cleanup_unique_dataset_impl(
    auth_session,
    graph_client,
    data_file: str,
    test_name: str,
    dataset_name: str,
    tmp_dir,
    platform: str = "kafka",
    env: str = "PROD",
):
    """Like :func:`_ingest_cleanup_data_impl`, but rewrites ``dataset_name`` in
    ``data_file`` to a run-unique name before ingesting and yields the unique
    dataset URN. Isolates a file-driven test's dataset so concurrent modules
    never collide on a shared URN under xdist ``--dist=loadscope``.

    Usage in test files:
        @pytest.fixture(scope="module", autouse=True)
        def dataset_urn(auth_session, graph_client, tmp_path_factory):
            yield from _ingest_cleanup_unique_dataset_impl(
                auth_session, graph_client,
                "tests/tags_and_terms/data.json", "tags_and_terms",
                "test-tags-terms-sample-kafka", tmp_path_factory.mktemp("data"),
            )
    """
    unique_file, dataset_urn = materialize_unique_dataset(
        data_file, dataset_name, tmp_dir, platform=platform, env=env
    )
    # No pre-ingest idempotency delete (unlike _ingest_cleanup_data_impl): the
    # URN is freshly unique per run, so nothing pre-exists to clean up.
    logger.info(f"ingesting {test_name} test data (dataset={dataset_urn})")
    ingest_file_via_rest(auth_session, unique_file)
    yield dataset_urn
    logger.info(f"removing {test_name} test data")
    delete_urns_from_file(graph_client, unique_file)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--domain",
        action="append",
        default=[],
        metavar="DOMAIN",
        help=(
            "Only run tests owned by this product domain. Repeatable, e.g. "
            "--domain catalog --domain ingestion. Valid values: "
            f"{', '.join(sorted(ALL_DOMAINS))}."
        ),
    )
    parser.addoption(
        "--tier",
        action="store",
        default="full",
        choices=["p0", "full"],
        help=(
            "Criticality tier to run. 'p0' runs only tests marked p0 -- the set "
            "that gates pull requests; 'full' (the default) runs everything. "
            "CI passes this through from SMOKE_TIER in smoke.sh."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    # Validate here rather than during collection: a bad value raised from
    # pytest_collection_modifyitems surfaces as an INTERNALERROR instead of a
    # readable usage error.
    try:
        parse_requested_domains(config.getoption("--domain"))
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc


def _module_is_changed(item: Item, changed: List[str]) -> bool:
    """True when the item's module is one this PR touched.

    ``changed`` holds repo-relative paths while ``item.fspath`` is absolute, so
    match by suffix -- the same approach the FILTERED_TESTS retry path uses.
    """
    if not changed:
        return False
    module_path = str(item.fspath)
    return any(module_path.endswith(path) for path in changed)


def _apply_tier_filter(config: pytest.Config, items: List[Item]) -> None:
    """Deselect tests outside the criticality tier requested with --tier.

    Deliberately not expressed as ``-m p0``: a command-line ``-m`` *replaces*
    the expression in addopts, and pytest applies ``-m`` deselection only after
    this hook runs -- so the weight-based batching below would still pack
    batches from the whole suite and leave most of them nearly empty.
    """
    if config.getoption("--tier") != "p0":
        return

    # A PR's own new or edited tests are not p0, so a tier-only selection would
    # merge them without ever running them: they would first execute post-merge,
    # where a failure lands on the default branch instead of on the author's PR.
    # CI passes the touched modules in SMOKE_CHANGED_TESTS. Known gap -- a changed
    # non-test helper, fixture or conftest pulls in no test module of its own, so
    # a PR that needs broader coverage than its own touched modules asks for the
    # whole suite with the full-suite PR label.
    changed = env_vars.get_smoke_changed_tests()

    selected: List[Item] = []
    deselected: List[Item] = []
    changed_only = 0
    for item in items:
        is_p0 = item.get_closest_marker("p0") is not None
        if is_p0 or _module_is_changed(item, changed):
            selected.append(item)
            if not is_p0:
                changed_only += 1
        else:
            deselected.append(item)

    if items and not selected:
        # Otherwise every batch collects nothing, pytest exits 5, and smoke.sh
        # treats that as success: a p0 run that tested nothing would go green.
        pytest.exit(
            f"--tier p0 selected 0 of {len(items)} collected test(s): "
            "no test carries the p0 marker.",
            returncode=pytest.ExitCode.USAGE_ERROR,
        )

    if changed:
        collected = {str(item.fspath) for item in items}
        unmatched = [
            path
            for path in changed
            if not any(module.endswith(path) for module in collected)
        ]
        if unmatched:
            # Deleted test files land here harmlessly, but so would a change in
            # the path format CI emits -- which would silently stop unioning a
            # PR's own tests, the exact failure this union exists to prevent.
            logger.warning(
                "SMOKE_CHANGED_TESTS: %s of %s path(s) matched no collected module: %s",
                len(unmatched),
                len(changed),
                ", ".join(sorted(unmatched)[:5]),
            )

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    logger.info(
        "--tier p0: selected %s of %s test(s); %s of them from the %s module(s) "
        "this PR touched",
        len(selected),
        len(items),
        changed_only,
        len(changed),
    )
    items[:] = selected


def _apply_domain_filter(config: pytest.Config, items: List[Item]) -> None:
    """Deselect tests outside the domains requested with --domain."""
    requested = parse_requested_domains(config.getoption("--domain"))
    if not requested:
        return

    selected: List[Item] = []
    deselected: List[Item] = []
    for item in items:
        declared = domains_of(item.get_closest_marker("domain"))
        target = selected if is_selected(declared, requested) else deselected
        target.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    logger.info(
        "--domain %s: selected %s of %s test(s)",
        ",".join(sorted(requested)),
        len(selected),
        len(items),
    )
    items[:] = selected


def pytest_sessionfinish(session, exitstatus):
    """whole test run finishes."""
    send_message(exitstatus)


def bin_pack_tasks(tasks, n_buckets):
    """
    Bin-pack tasks into n_buckets with roughly equal weights.

    Parameters:
    tasks (list): List of (task, weight) tuples. If only task is provided, weight defaults to 1.
    n_buckets (int): Number of buckets to distribute tasks into.

    Returns:
    list: List of buckets, where each bucket is a list of tasks.
    """
    # Normalize the tasks to ensure they're all (task, weight) tuples
    normalized_tasks = []
    for task in tasks:
        if isinstance(task, tuple) and len(task) == 2:
            normalized_tasks.append(task)
        else:
            normalized_tasks.append((task, 1))

    # Sort tasks by weight in descending order
    sorted_tasks = sorted(normalized_tasks, key=lambda x: x[1], reverse=True)

    # Initialize the buckets with zero weight
    buckets: List = [[] for _ in range(n_buckets)]
    bucket_weights: List[int] = [0] * n_buckets

    # Assign each task to the bucket with the lowest current weight
    for task, weight in sorted_tasks:
        # Find the bucket with the minimum weight
        min_bucket_idx = bucket_weights.index(min(bucket_weights))

        # Add the task to this bucket
        buckets[min_bucket_idx].append(task)
        bucket_weights[min_bucket_idx] += weight

    return buckets


def load_pytest_test_weights() -> Dict[str, float]:
    """
    Load pytest test weights from JSON file.

    Returns:
        Dictionary mapping test IDs (classname::test_name) to durations in seconds.
        Returns empty dict if weights file doesn't exist.
    """
    weights_file = Path(__file__).parent / "pytest_test_weights.json"

    if not weights_file.exists():
        return {}

    try:
        with open(weights_file) as f:
            weights_data = json.load(f)

        # Convert to dict: {"test_e2e::test_gms_get_dataset": 262.807, ...}
        return {
            item["testId"]: float(item["duration"][:-1])  # Strip 's' suffix
            for item in weights_data
        }
    except Exception as e:
        logger.warning(f"Warning: Failed to load pytest test weights: {e}")
        return {}


def find_pytest_test_weight(
    item: Item, test_weights: Dict[str, float]
) -> Optional[float]:
    """Recorded weight for a test, or None when the weights file has no entry.

    Includes the OSS class-refactor fallback: when tests move into classes junit
    nodeids gain a class segment, while weights files may still key by
    module::test_name.

    Returning None rather than a default is what lets callers both count
    uncovered tests and derive a tier-appropriate fallback weight.
    """
    nodeid = item.nodeid
    test_id = nodeid.replace("/", ".").replace(".py::", "::")
    weight = test_weights.get(test_id)
    if weight is not None:
        return weight

    nodeid_parts = nodeid.split("::")
    if len(nodeid_parts) > 2:
        module_id = nodeid_parts[0].replace("/", ".").removesuffix(".py")
        return test_weights.get(f"{module_id}::{nodeid_parts[-1]}")

    return None


def get_pytest_test_weight(
    item: Item, test_weights: Dict[str, float], default_weight: float = 1.0
) -> float:
    """Recorded weight for a test, or ``default_weight`` when it has no entry."""
    found = find_pytest_test_weight(item, test_weights)
    return default_weight if found is None else found


def compute_default_test_weight(
    test_weights: Dict[str, float], tier_items: Optional[List[Item]] = None
) -> float:
    """Weight for a test with no entry in pytest_test_weights.json.

    The flat 1.0s fallback under-weights an uncovered module by ~an order of
    magnitude, so a batch that draws several of them overruns what the packer
    predicted and becomes the critical path. That bias is diluted across a full
    suite but dominates a tier-narrowed run, where the surviving tests are the
    heavy end-to-end ones. When ``tier_items`` is given, derive the default from
    the weights actually known for those items instead.
    """
    if tier_items:
        known = [
            weight
            for weight in (
                find_pytest_test_weight(item, test_weights) for item in tier_items
            )
            if weight is not None
        ]
        if known:
            # Mean rather than median: the packer *sums* weights per module, so
            # the unbiased estimator for a sum is the mean. These distributions
            # are heavily right-skewed -- a handful of multi-minute end-to-end
            # tests among many fast ones -- so the median can sit below even the
            # suite-wide default and would under-weight uncovered modules, the
            # one direction that makes a batch overrun its prediction.
            # Floored at 1.0s so narrowing a run can never lower the fallback.
            return max(statistics.fmean(known), 1.0)
    return 1.0


def aggregate_module_weights(
    items: List[Item],
    test_weights: Dict[str, float],
    tier_narrowed: bool = False,
) -> List[Tuple[str, List[Item], float, float]]:
    """
    Group test items by module, splitting each module's weight by execution phase.

    smoke.sh runs each batch as two pytest invocations: non-mutator tests under
    xdist, then policy mutators serially. Those two buckets cost different
    amounts of wall clock per second of test time, so they are accumulated
    separately here and combined by the caller, which knows the worker count.

    Args:
        items: List of pytest test items
        test_weights: Dictionary mapping test IDs to durations
        tier_narrowed: True when a tier filter (``--tier``) already reduced
            ``items``, so the fallback weight is derived from those items
            rather than from the whole suite.

    Returns:
        List of (module_path, items_in_module, parallel_seconds, serial_seconds)
    """

    # Group items by module (file path)
    modules: Dict[str, List[Item]] = defaultdict(list)
    for item in items:
        # Get the module path from the item's fspath
        module_path = str(item.fspath)
        modules[module_path].append(item)

    default_weight = compute_default_test_weight(
        test_weights, items if tier_narrowed else None
    )

    # Each item's weight is looked up exactly once, here.
    module_data = []
    for module_path, module_items in modules.items():
        parallel_seconds = 0.0
        serial_seconds = 0.0
        for item in module_items:
            weight = get_pytest_test_weight(item, test_weights, default_weight)
            if _is_global_policy_mutator(item):
                serial_seconds += weight
            else:
                parallel_seconds += weight

        module_data.append(
            (module_path, module_items, parallel_seconds, serial_seconds)
        )

    return module_data


def _is_global_policy_mutator(item: Item) -> bool:
    return item.get_closest_marker("global_policy_mutator") is not None


def phase_aware_module_weight(
    parallel_seconds: float, serial_seconds: float, xdist_workers: int
) -> float:
    """Estimate a module's contribution to a batch's *wall clock*, not its total
    test time.

    smoke.sh runs each batch in two pytest invocations: non-mutator tests under
    xdist (``-n N --dist=loadscope``), then policy mutators serially. A serial
    minute therefore costs about N times what a parallel minute does.

    Packing batches by raw summed duration ignores that and systematically
    overloads whichever batch happens to draw the mutator-heavy modules --
    ``tests/authorization/test_aspect_write_auth.py`` alone is ~7.6 min of
    strictly serial work. Measured across master runs, the resulting spread was
    ~1.7x between the slowest and fastest batch even though every batch had an
    identical summed weight.

    With xdist_workers == 1 this reduces to the plain sum, i.e. the previous
    behaviour, which is correct because both phases are then serial.
    """
    return parallel_seconds / max(1, xdist_workers) + serial_seconds


def _apply_smoke_policy_phase_filter(items: List[Item]) -> None:
    """Keep batch assignment stable across smoke.sh's two pytest invocations.

    Batching runs on the full module set first; this filter then selects
    non-mutators (phase 1) or mutators (phase 2). Unset means run everything
    (ad-hoc local pytest without smoke.sh).
    """
    phase = env_vars.get_smoke_policy_phase()
    if phase is None:
        return
    if phase == "1":
        items[:] = [item for item in items if not _is_global_policy_mutator(item)]
        logger.info("SMOKE_POLICY_PHASE=1: running %s non-mutator test(s)", len(items))
        return
    if phase == "2":
        items[:] = [item for item in items if _is_global_policy_mutator(item)]
        logger.info("SMOKE_POLICY_PHASE=2: running %s mutator test(s)", len(items))
        return
    logger.warning("Unknown SMOKE_POLICY_PHASE=%r; running all collected tests", phase)


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[Item]
) -> None:
    # Runs before every early return below, and before the weight-based batching,
    # so batches are packed from the selected tests only.
    _apply_tier_filter(config, items)
    _apply_domain_filter(config, items)

    # Check if FILTERED_TESTS is set (for retry logic)
    filtered_tests_file = env_vars.get_filtered_tests_file()
    if filtered_tests_file:
        logger.info(f"Reading filtered test modules from {filtered_tests_file}")
        try:
            with open(filtered_tests_file) as f:
                # Read non-empty lines, strip whitespace, ignore comments
                filtered_modules = set(
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                )

            logger.info(f"Found {len(filtered_modules)} filtered module(s) to run")

            # Filter items to only those from the specified modules
            filtered_items = []
            for item in items:
                # Get the module path from the item's fspath
                module_path = str(item.fspath)

                # Check if this item's module is in the filtered list
                # Need to handle both absolute and relative paths
                if any(
                    module_path.endswith(filtered_mod)
                    for filtered_mod in filtered_modules
                ):
                    filtered_items.append(item)

            logger.info(
                f"RETRY MODE: Running {len(filtered_items)} tests from {len(filtered_modules)} failed module(s)"
            )
            items[:] = filtered_items
            _apply_smoke_policy_phase_filter(items)
            return
        except Exception as e:
            logger.warning(
                f"Failed to read filtered tests file: {e}. Running all tests."
            )
            # Fall through to normal batching logic

    # Get batch configuration
    batch_count_env = env_vars.get_batch_count()
    batch_count = int(batch_count_env)
    batch_number_env = env_vars.get_batch_number()
    batch_number = int(batch_number_env)

    if batch_count <= 1:
        _apply_smoke_policy_phase_filter(items)
        return

    # Load test weights
    test_weights = load_pytest_test_weights()

    # Group items by module and aggregate weights
    module_data = aggregate_module_weights(
        items, test_weights, tier_narrowed=config.getoption("--tier") == "p0"
    )

    # Sort modules by path for stability
    module_data.sort(key=lambda x: x[0])

    # Create weighted tuples for bin-packing: (module_path, weight)
    # We'll also keep track of the items for each module
    module_map = {
        module_path: module_items for module_path, module_items, _, _ in module_data
    }
    # Weight by estimated wall clock rather than summed duration -- serial
    # policy-mutator tests cost xdist_workers times more than parallel ones.
    xdist_workers = env_vars.get_pytest_xdist_workers()
    weighted_modules = [
        (
            module_path,
            phase_aware_module_weight(parallel_seconds, serial_seconds, xdist_workers),
        )
        for module_path, _, parallel_seconds, serial_seconds in module_data
    ]

    logger.info(
        f"Batching {len(items)} tests from {len(weighted_modules)} modules across "
        f"{batch_count} batches (xdist_workers={xdist_workers})"
    )

    # Apply bin-packing to modules
    module_batches = bin_pack_tasks(weighted_modules, batch_count)

    # Get the modules for this batch
    selected_modules = module_batches[batch_number]

    # Flatten back to individual test items
    # Tests within each module maintain their original collection order
    selected_items = []
    for module_path in selected_modules:
        selected_items.extend(module_map[module_path])

    logger.info(
        f"Batch {batch_number}: Running {len(selected_items)} tests from {len(selected_modules)} modules"
    )

    # Replace items with the filtered list, then apply smoke.sh phase filter
    items[:] = selected_items
    _apply_smoke_policy_phase_filter(items)
