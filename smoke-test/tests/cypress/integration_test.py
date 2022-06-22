import pytest
import subprocess
import os

from tests.utils import ingest_file_via_rest
from tests.utils import delete_urns_from_file


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data():
    print("ingesting test data")
    ingest_file_via_rest("tests/cypress/data.json")
    ingest_file_via_rest("tests/cypress/cypress_dbt_data.json")
    ingest_file_via_rest("tests/cypress/schema-blame-data.json")
    yield
    print("removing test data")
    delete_urns_from_file("tests/cypress/data.json")
    delete_urns_from_file("tests/cypress/cypress_dbt_data.json")
    delete_urns_from_file("tests/cypress/schema-blame-data.json")


def test_run_cypress(frontend_session, wait_for_healthchecks):
    # Run with --record option only if CYPRESS_RECORD_KEY is non-empty
    record_key = os.getenv("CYPRESS_RECORD_KEY")
    if record_key:
        print('Running Cypress tests with recording')
        command = f"npx cypress run --record"
    else:
        print('Running Cypress tests without recording')
        command = f"npx cypress run"
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd="tests/cypress")
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    return_code = proc.wait()
    print(stdout.decode("utf-8"))
    print('stderr output:')
    print(stderr.decode("utf-8"))
    print('return code', return_code)
    assert(return_code == 0)
