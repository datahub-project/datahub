import logging
from typing import Any, Dict, List, Optional, Tuple

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import execute_graphql, wait_for_writes_to_sync, with_test_retry

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session,
        graph_client,
        "tests/incidents/health_batch_data.json",
        "health_batch",
    )


def _ds(n: int) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:kafka,health_batch_ds_{n},PROD)"


def _inc(slug: str) -> str:
    return f"urn:li:incident:health-batch-{slug}"


# alias -> (dataset urn, expected active-incident count, expected latest active incident urn).
# count == 0 means the incidents dimension must resolve to PASS (no active incidents).
EXPECTATIONS: Dict[str, Tuple[str, int, Optional[str]]] = {
    # two active incidents -> FAIL, latest is the newer one (time 2000).
    "d1": (_ds(1), 2, _inc("1b")),
    # one active incident -> FAIL.
    "d2": (_ds(2), 1, _inc("2a")),
    # only a RESOLVED incident -> PASS.
    "d3": (_ds(3), 0, None),
    # no incidents at all -> PASS.
    "d4": (_ds(4), 0, None),
    # one ACTIVE + one newer RESOLVED incident -> the state=ACTIVE filter must exclude the
    # resolved one from both the count and the "latest" selection, so count 1 with the active urn.
    "d5": (_ds(5), 1, _inc("5a")),
}

# Multi-alias query: resolving every dataset's `health` field in a single GraphQL request forces the
# EntityHealthBatchLoader DataLoader to accumulate all keys into one batch, which exercises the
# batched getActiveIncidentStats aggregation (a single terms(entities.keyword) + top_hits query)
# rather than the per-entity path. This is the code path enabled by entityHealthBatchLoadEnabled.
_FRAGMENT = """fragment health on Dataset {
  urn
  health {
    type
    status
    message
    activeIncidentHealthDetails {
      latestIncidentUrn
    }
  }
}"""


def _build_query() -> Tuple[str, Dict[str, Any]]:
    var_decls = ", ".join(f"${alias}: String!" for alias in EXPECTATIONS)
    selections = "\n".join(
        f"  {alias}: dataset(urn: ${alias}) {{ ...health }}" for alias in EXPECTATIONS
    )
    query = f"query healthBatch({var_decls}) {{\n{selections}\n}}\n\n{_FRAGMENT}"
    variables = {alias: urn for alias, (urn, _, _) in EXPECTATIONS.items()}
    return query, variables


def _incident_health(dataset: Dict[str, Any]) -> Dict[str, Any]:
    healths: List[Dict[str, Any]] = dataset["health"] or []
    incident_entries = [h for h in healths if h["type"] == "INCIDENTS"]
    # The incidents dimension is always enabled for datasets, so there is exactly one entry.
    assert len(incident_entries) == 1, (
        f"expected exactly one INCIDENTS health entry, got {healths}"
    )
    return incident_entries[0]


def test_batched_incident_health(auth_session):
    wait_for_writes_to_sync()
    query, variables = _build_query()

    @with_test_retry()
    def check():
        data = execute_graphql(auth_session, query, variables)["data"]

        for alias, (_, expected_count, expected_latest) in EXPECTATIONS.items():
            health = _incident_health(data[alias])
            if expected_count == 0:
                assert health["status"] == "PASS", f"{alias}: {health}"
                assert health["activeIncidentHealthDetails"] is None, (
                    f"{alias}: {health}"
                )
            else:
                plural = "s" if expected_count > 1 else ""
                assert health["status"] == "FAIL", f"{alias}: {health}"
                assert (
                    health["message"] == f"{expected_count} active incident{plural}"
                ), f"{alias}: {health}"
                assert (
                    health["activeIncidentHealthDetails"]["latestIncidentUrn"]
                    == expected_latest
                ), f"{alias}: {health}"

    check()
