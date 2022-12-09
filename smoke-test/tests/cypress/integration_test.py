from typing import List

import pytest
import subprocess
import os

from tests.utils import (
    create_datahub_step_state_aspects,
    get_admin_username,
    ingest_file_via_rest,
    delete_urns_from_file,
)

CYPRESS_TEST_DATA_DIR = "tests/cypress"

TEST_DATA_FILENAME = "data.json"
TEST_DBT_DATA_FILENAME = "cypress_dbt_data.json"
TEST_SCHEMA_BLAME_DATA_FILENAME = "schema-blame-data.json"
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
]

ONBOARDING_IDS: List[str] = []
for id_list in ONBOARDING_ID_LISTS:
    ONBOARDING_IDS.extend(id_list)


def ingest_data():
    print("creating onboarding data file")
    create_datahub_step_state_aspects(
        get_admin_username(),
        ONBOARDING_IDS,
        f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}",
    )

    print("ingesting test data")
    ingest_file_via_rest(f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    ingest_file_via_rest(f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}")
    ingest_file_via_rest(f"{CYPRESS_TEST_DATA_DIR}/{TEST_SCHEMA_BLAME_DATA_FILENAME}")
    ingest_file_via_rest(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data():
    ingest_data()
    yield
    print("removing test data")
    delete_urns_from_file(f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    delete_urns_from_file(f"{CYPRESS_TEST_DATA_DIR}/{TEST_DBT_DATA_FILENAME}")
    delete_urns_from_file(f"{CYPRESS_TEST_DATA_DIR}/{TEST_SCHEMA_BLAME_DATA_FILENAME}")
    delete_urns_from_file(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")

    print("deleting onboarding data file")
    if os.path.exists(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}"):
        os.remove(f"{CYPRESS_TEST_DATA_DIR}/{TEST_ONBOARDING_DATA_FILENAME}")


def test_run_cypress(frontend_session, wait_for_healthchecks):
    # Run with --record option only if CYPRESS_RECORD_KEY is non-empty
    record_key = os.getenv("CYPRESS_RECORD_KEY")
    if record_key:
        print("Running Cypress tests with recording")
        command = "NO_COLOR=1 npx cypress run --record"
    else:
        print("Running Cypress tests without recording")
        # command = "NO_COLOR=1 npx cypress --version"
        command = "NO_COLOR=1 npx cypress run"
        # Add --headed --spec '**/mutations/mutations.js' (change spec name)
        # in case you want to see the browser for debugging
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=f"{CYPRESS_TEST_DATA_DIR}",
    )
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    return_code = proc.wait()
    print(stdout.decode("utf-8"))
    print("stderr output:")
    print(stderr.decode("utf-8"))
    print("return code", return_code)
    assert return_code == 0
