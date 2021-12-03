from pprint import pprint

import pytest
import subprocess

from tests.utils import FRONTEND_ENDPOINT
from tests.utils import ingest_file_via_rest
from tests.utils import delete_urns_from_file


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(request):
    print('ingest?')
    pass
    yield
    pass


def test_login_to_datahub(frontend_session, wait_for_healthchecks):
    command = f"npx cypress run --spec cypress/integration/login.js"
    print('starting?')
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd="tests/cypress")
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    return_code = proc.wait()
    print('out:')
    print(stdout.decode("utf-8"))
    print('error:')
    print(stderr.decode("utf-8"))
    print('return code', return_code)
    assert(return_code == 0)
