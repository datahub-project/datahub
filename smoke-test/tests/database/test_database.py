import logging

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from tests.utilities.concurrent_openapi import run_tests
from tests.utils import delete_urns, wait_for_writes_to_sync

logger = logging.getLogger(__name__)


generated_urns = [make_dataset_urn("test", f"database_test_{i}") for i in range(0, 100)]


@pytest.fixture(scope="module")
def ingest_cleanup_data(graph_client, request):
    print("removing test data before")
    delete_urns(graph_client, generated_urns)
    wait_for_writes_to_sync()
    yield
    print("removing test data after")
    delete_urns(graph_client, generated_urns)
    wait_for_writes_to_sync()


def test_mysql_deadlock_gap_locking(auth_session, ingest_cleanup_data):
    # This generates concurrent batches with interleaved urn ids
    run_tests(
        auth_session,
        fixture_globs=["tests/database/v3/mysql_gap_deadlock/*.json"],
        num_workers=8,
    )
