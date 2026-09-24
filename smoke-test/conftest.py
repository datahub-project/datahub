pytest_plugins = ["tests.utilities.agent_reporter"]

import logging
import os
from typing import List, Optional, Set

import pytest
import requests
from _pytest.nodes import Item

from datahub.ingestion.graph.client import (
    DatahubClientConfig,
    DataHubGraph,
    get_default_graph,
)
from shard_pack import is_global_policy_mutator, items_for_batch, plan_collected_items
from tests.test_result_msg import send_message
from tests.utilities import env_vars
from tests.utilities.domains import (
    ALL_DOMAINS,
    domains_of,
    is_selected,
    junit_user_properties,
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


def pytest_configure(config: pytest.Config) -> None:
    # Validate here rather than during collection: a bad value raised from
    # pytest_collection_modifyitems surfaces as an INTERNALERROR instead of a
    # readable usage error.
    try:
        parse_requested_domains(config.getoption("--domain"))
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc


def pytest_runtest_setup(item: Item) -> None:
    """Copy domain markers into JUnit user_properties for CI / PostHog."""
    item.user_properties.extend(
        junit_user_properties(item.get_closest_marker("domain"))
    )


# Test modules this PR touches, from CI. Read once: the environment is fixed for
# the life of the process.
_CHANGED_TESTS: List[str] = env_vars.get_smoke_changed_tests()
_CHANGED_MATCHED: Set[str] = set()


def pytest_itemcollected(item: Item) -> None:
    """Mark tests from modules this PR touches as p0.

    Runs per item during collection, before any ``pytest_collection_modifyitems``
    hook, so pytest's own ``-m`` deselection then keeps them. This is the
    marker-injection pattern from pytest's docs, and it is what lets a PR's own
    new or edited tests run under ``-m p0`` without a second selection mechanism.

    ``_CHANGED_TESTS`` holds repo-relative paths while ``item.fspath`` is
    absolute, so match by suffix -- the same approach the FILTERED_TESTS retry
    path uses.
    """
    if not _CHANGED_TESTS:
        return
    module_path = str(item.fspath)
    for path in _CHANGED_TESTS:
        if module_path.endswith(path):
            _CHANGED_MATCHED.add(path)
            item.add_marker(pytest.mark.p0)
            break


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
        items[:] = [item for item in items if not is_global_policy_mutator(item)]
        logger.info("SMOKE_POLICY_PHASE=1: running %s non-mutator test(s)", len(items))
        return
    if phase == "2":
        items[:] = [item for item in items if is_global_policy_mutator(item)]
        logger.info("SMOKE_POLICY_PHASE=2: running %s mutator test(s)", len(items))
        return
    logger.warning("Unknown SMOKE_POLICY_PHASE=%r; running all collected tests", phase)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[Item]
) -> None:
    # Runs before every early return below, and before the weight-based batching,
    # so batches are packed from the selected tests only.
    if _CHANGED_TESTS:
        unmatched = [p for p in _CHANGED_TESTS if p not in _CHANGED_MATCHED]
        if unmatched:
            # Deleted test files land here harmlessly, but so would a change in
            # the path format CI emits -- which would silently stop a PR's own
            # tests being marked p0, the exact failure this injection prevents.
            logger.warning(
                "SMOKE_CHANGED_TESTS: %s of %s path(s) matched no collected module: %s",
                len(unmatched),
                len(_CHANGED_TESTS),
                ", ".join(sorted(unmatched)[:5]),
            )

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

    xdist_workers = env_vars.get_pytest_xdist_workers()
    packed = plan_collected_items(items, batch_count, xdist_workers)

    logger.info(
        "Batching %s tests from %s scopes across %s batches (xdist_workers=%s)",
        len(items),
        len(packed.shards),
        batch_count,
        xdist_workers,
    )

    for i, plan in enumerate(packed.plans):
        test_count = sum(
            len(packed.items_by_scope[scope_key]) for scope_key in plan.module_paths
        )
        logger.info(
            "Batch %s: predicted_wall=%.1fs phase1_makespan=%.1fs serial=%.1fs "
            "scopes=%s tests=%s",
            i,
            plan.predicted_wall,
            plan.phase1_makespan,
            plan.serial_seconds,
            len(plan.module_paths),
            test_count,
        )

    selected_items = items_for_batch(packed, batch_number)
    selected_scopes = packed.plans[batch_number].module_paths

    logger.info(
        "Batch %s: Running %s tests from %s scopes",
        batch_number,
        len(selected_items),
        len(selected_scopes),
    )

    # Replace items with the filtered list, then apply smoke.sh phase filter
    items[:] = selected_items
    _apply_smoke_policy_phase_filter(items)
