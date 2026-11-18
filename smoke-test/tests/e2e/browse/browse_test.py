import logging
from typing import Any, Dict, List

import pytest

from tests.e2e.conftest import _ingest_cleanup_data_impl
from tests.e2e.utils import (
    execute_graphql,
    reads_search_v3,
    wait_for_browse_path_entities,
    wait_for_browse_path_entity,
)
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

TEST_DATASET_1_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-1,PROD)"
TEST_DATASET_2_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-2,PROD)"
TEST_DATASET_3_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-browse-3,PROD)"

_BROWSE_QUERY = """query browse($input: BrowseInput!) {
  browse(input: $input) {
    total
    entities { urn }
    groups { name count }
    metadata { path totalNumEntities }
  }
}"""


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/e2e/browse/data.json", "browse"
    )


def _browse(auth_session, path: List[str], count: int, sync: bool) -> Dict[str, Any]:
    res_data = execute_graphql(
        auth_session,
        _BROWSE_QUERY,
        {"input": {"type": "DATASET", "path": path, "start": 0, "count": count}},
        no_sync_wait=not sync,
    )
    return res_data["data"]["browse"]


def _assert_legacy_browse_paths(auth_session) -> None:
    # /prod -- There should be one entity
    # These are all read-only browse queries with no writes in between, so the
    # first two skip the sync wait -- only the final query's response needs one.
    wait_for_browse_path_entity(
        auth_session,
        path=["prod"],
        expected_urn=TEST_DATASET_3_URN,
        entity_type="DATASET",
    )
    wait_for_browse_path_entities(
        auth_session,
        path=["prod", "kafka1"],
        expected_urns=[TEST_DATASET_1_URN, TEST_DATASET_2_URN, TEST_DATASET_3_URN],
        entity_type="DATASET",
    )

    browse = _browse(auth_session, ["prod"], count=100, sync=False)
    logger.info(browse)
    assert browse["entities"] == [{"urn": TEST_DATASET_3_URN}]

    browse = _browse(auth_session, ["prod", "kafka1"], count=10, sync=False)
    assert browse == {
        "total": 3,
        "entities": [
            {"urn": TEST_DATASET_1_URN},
            {"urn": TEST_DATASET_2_URN},
            {"urn": TEST_DATASET_3_URN},
        ],
        "groups": [],
        "metadata": {"path": ["prod", "kafka1"], "totalNumEntities": 0},
    }

    browse = _browse(auth_session, ["prod", "kafka2"], count=10, sync=True)
    assert browse == {
        "total": 2,
        "entities": [{"urn": TEST_DATASET_1_URN}, {"urn": TEST_DATASET_2_URN}],
        "groups": [],
        "metadata": {"path": ["prod", "kafka2"], "totalNumEntities": 0},
    }


def test_get_browse_paths(auth_session, ingest_cleanup_data):
    if reads_search_v3(auth_session):
        pytest.skip(
            "Legacy browse returns no entities once reads use Search V3; use browseV2"
        )
    _assert_legacy_browse_paths(auth_session)
