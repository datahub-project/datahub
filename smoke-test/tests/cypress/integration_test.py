import datetime
import json
import os
import subprocess
import threading
from typing import List

import pytest

from conftest import bin_pack_tasks
from tests.setup.lineage.ingest_time_lineage import (
    get_time_lineage_urns,
    ingest_time_lineage,
)
from tests.utils import (
    create_datahub_step_state_aspects,
    delete_urns,
    delete_urns_from_file,
    get_admin_username,
    ingest_file_via_rest,
)

CYPRESS_TEST_DATA_DIR = "tests/cypress"

TEST_DATA_FILENAME = "data.json"
TEST_INCIDENT_DATA_FILENAME = "incidents_test.json"
TEST_DBT_DATA_FILENAME = "cypress_dbt_data.json"
TEST_PATCH_DATA_FILENAME = "patch-data.json"
TEST_ONBOARDING_DATA_FILENAME: str = "onboarding.json"

HOME_PAGE_ONBOARDING_IDS: List[str] = [
    "global-welcome-to-datahub",
    "home-page-ingestion",
    "home-page-domains",
    "home-page-platforms",
    "home-page-most-popular",
    "home-page-search-bar",
]

SEARCH_ONBOARDING_IDS: List[str] = [
    "search-results-filters",
    "search-results-advanced-search",
    "search-results-filters-v2-intro",
    "search-results-browse-sidebar",
]

ENTITY_PROFILE_ONBOARDING_IDS: List[str] = [
    "entity-profile-entities",
    "entity-profile-properties",
    "entity-profile-documentation",
    "entity-profile-lineage",
    "entity-profile-schema",
    "entity-profile-owners",
    "entity-profile-tags",
    "entity-profile-glossary-terms",
    "entity-profile-domains",
]

INGESTION_ONBOARDING_IDS: List[str] = [
    "ingestion-create-source",
    "ingestion-refresh-sources",
]

BUSINESS_GLOSSARY_ONBOARDING_IDS: List[str] = [
    "business-glossary-intro",
    "business-glossary-create-term",
    "business-glossary-create-term-group",
]

DOMAINS_ONBOARDING_IDS: List[str] = [
    "domains-intro",
    "domains-create-domain",
]

USERS_ONBOARDING_IDS: List[str] = [
    "users-intro",
    "users-sso",
    "users-invite-link",
    "users-assign-role",
]

GROUPS_ONBOARDING_IDS: List[str] = [
    "groups-intro",
    "groups-create-group",
]

ROLES_ONBOARDING_IDS: List[str] = [
    "roles-intro",
]

POLICIES_ONBOARDING_IDS: List[str] = [
    "policies-intro",
    "policies-create-policy",
]

LINEAGE_GRAPH_ONBOARDING_IDS: List[str] = [
    "lineage-graph-intro",
    "lineage-graph-time-filter",
]

ONBOARDING_ID_LISTS: List[List[str]] = [
    HOME_PAGE_ONBOARDING_IDS,
    SEARCH_ONBOARDING_IDS,
    ENTITY_PROFILE_ONBOARDING_IDS,
    INGESTION_ONBOARDING_IDS,
    BUSINESS_GLOSSARY_ONBOARDING_IDS,
    DOMAINS_ONBOARDING_IDS,
    USERS_ONBOARDING_IDS,
    GROUPS_ONBOARDING_IDS,
    ROLES_ONBOARDING_IDS,
    POLICIES_ONBOARDING_IDS,
    LINEAGE_GRAPH_ONBOARDING_IDS,
]

ONBOARDING_IDS: List[str] = []
for id_list in ONBOARDING_ID_LISTS:
    ONBOARDING_IDS.extend(id_list)


def print_now():
    print(f"current time is {datetime.datetime.now(datetime.timezone.utc)}")


def ingest_data(auth_session, graph_client):
    print_now()
    print("creating onboarding data file")
    create_datahub_step_state_aspects(
        get_admin_username(),
        ONBOARDING_IDS,
        f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}",
    )

    print_now()
    print("ingesting test data")
    ingest_file_via_rest(auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    ingest_file_via_rest(
        auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}"
    )
    ingest_file_via_rest(
        auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_PATCH_DATA_FILENAME}"
    )
    ingest_file_via_rest(
        auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}"
    )
    ingest_file_via_rest(
        auth_session, f"{CYPRESS_TEST_DATA_DIR}/{TEST_INCIDENT_DATA_FILENAME}"
    )
    ingest_time_lineage(graph_client)
    print_now()
    print("completed ingesting test data")


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    ingest_data(auth_session, graph_client)
    yield
    print_now()
    print("removing test data")
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    delete_urns_from_file(
        graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}"
    )
    delete_urns_from_file(
        graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_PATCH_DATA_FILENAME}"
    )
    delete_urns_from_file(
        graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}"
    )
    delete_urns(graph_client, get_time_lineage_urns())
    delete_urns_from_file(
        graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_INCIDENT_DATA_FILENAME}"
    )

    print_now()
    print("deleting onboarding data file")
    if os.path.exists(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}"):
        os.remove(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")
    print_now()
    print("deleted onboarding data")


def _get_js_files(base_path: str):
    file_paths = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".js"):
                file_paths.append(os.path.relpath(os.path.join(root, file), base_path))
    return sorted(file_paths)  # sort to make the order stable across batch runs


def _get_cypress_tests_batch():
    """
    Batching is configured via env vars BATCH_COUNT and BATCH_NUMBER.  All cypress tests are split into exactly
    BATCH_COUNT batches. When BATCH_NUMBER env var is set (zero based index), that batch alone is run.
    Github workflow via test_matrix, runs all batches in parallel to speed up the test elapsed time.
    If either of these vars are not set, all tests are run sequentially.
    :return:
    """
    all_tests = _get_js_files("tests/cypress/cypress/e2e")

    tests_with_weights = []

    with open("tests/cypress/test_weights.json") as f:
        weights_data = json.load(f)

    # File has file path relative to cypress/e2e folder and duration in seconds (with s suffix), pulled from codecov report.
    # Use some other method to automate finding the weights - may be use junits directly
    test_weights = {
        item["filePath"]: float(item["duration"][:-1]) for item in weights_data
    }

    for test in all_tests:
        if test in test_weights:
            tests_with_weights.append((test, test_weights[test]))
        else:
            tests_with_weights.append(test)

    test_batches = bin_pack_tasks(tests_with_weights, int(os.getenv("BATCH_COUNT", 1)))
    return test_batches[int(os.getenv("BATCH_NUMBER", 0))]


def test_run_cypress(auth_session):
    # Run with --record option only if CYPRESS_RECORD_KEY is non-empty
    record_key = os.getenv("CYPRESS_RECORD_KEY")
    tag_arg = ""
    test_strategy = os.getenv("TEST_STRATEGY", None)
    if record_key:
        record_arg = " --record "
        batch_number = os.getenv("BATCH_NUMBER")
        batch_count = os.getenv("BATCH_COUNT")
        if batch_number and batch_count:
            batch_suffix = f"-{batch_number}{batch_count}"
        else:
            batch_suffix = ""
        tag_arg = f" --tag {test_strategy}{batch_suffix}"
    else:
        record_arg = " "

    print(f"test strategy is {test_strategy}")
    test_spec_arg = ""
    specs_str = ",".join([f"**/{f}" for f in _get_cypress_tests_batch()])
    test_spec_arg = f" --spec '{specs_str}' "

    print("Running Cypress tests with command")
    node_options = "--max-old-space-size=6000"
    command = f'NO_COLOR=1 NODE_OPTIONS="{node_options}" npx cypress run {record_arg} {test_spec_arg} {tag_arg} --config numTestsKeptInMemory=2'
    print(command)
    # Add --headed --spec '**/mutations/mutations.js' (change spec name)
    # in case you want to see the browser for debugging
    print_now()
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=f"{CYPRESS_TEST_DATA_DIR}",
        text=True,  # Use text mode for string output
        bufsize=1,  # Line buffered
    )
    assert proc.stdout is not None
    assert proc.stderr is not None

    # Function to read and print output from a pipe
    def read_and_print(pipe, prefix=""):
        for line in pipe:
            print(f"{prefix}{line}", end="")

    # Read and print output in real-time

    stdout_thread = threading.Thread(target=read_and_print, args=(proc.stdout,))
    stderr_thread = threading.Thread(
        target=read_and_print, args=(proc.stderr, "stderr: ")
    )

    # Set threads as daemon so they exit when the main thread exits
    stdout_thread.daemon = True
    stderr_thread.daemon = True

    # Start the threads
    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    return_code = proc.wait()

    # Wait for the threads to finish
    stdout_thread.join()
    stderr_thread.join()

    print("return code", return_code)
    print_now()
    assert return_code == 0
