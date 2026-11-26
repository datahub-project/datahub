"""Unit tests for ElasticsearchClient — uses mocked Sessions; never hits a real ES."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.es_client import ElasticsearchClient
from utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.PLATFORM)


def _resp(json_data: object = None, status: int = 200, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_data
    r.text = text
    r.raise_for_status = MagicMock()
    if status >= 400:
        r.raise_for_status.side_effect = Exception(f"HTTP {status}")
    return r


@pytest.fixture
def client() -> ElasticsearchClient:
    return ElasticsearchClient(gms_url="http://gms:8080", es_url="http://es:9200")


class TestListIndices:
    def test_filters_by_prefix(self, client: ElasticsearchClient) -> None:
        cat_payload = [
            {"index": "datasetindex_v2"},
            {"index": "datasetindex_v2_1714000000"},
            {"index": "dashboardindex_v2"},
        ]
        with patch.object(client._es_session, "get", return_value=_resp(cat_payload)):
            assert sorted(client.list_indices(prefix="datasetindex_v2")) == [
                "datasetindex_v2",
                "datasetindex_v2_1714000000",
            ]

    def test_empty_when_no_match(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp([])):
            assert client.list_indices(prefix="nope") == []


class TestGetAliasTargets:
    def test_returns_index_keys(self, client: ElasticsearchClient) -> None:
        payload: dict[str, object] = {
            "datasetindex_v2_1714000000": {"aliases": {"datasetindex_v2": {}}}
        }
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            assert client.get_alias_targets("datasetindex_v2") == [
                "datasetindex_v2_1714000000"
            ]

    def test_404_returns_empty(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "get", return_value=_resp({}, status=404)
        ):
            assert client.get_alias_targets("missing") == []


class TestGetDocCount:
    def test_returns_count(self, client: ElasticsearchClient) -> None:
        with patch.object(client._es_session, "get", return_value=_resp({"count": 42})):
            assert client.get_doc_count("datasetindex_v2") == 42

    def test_404_returns_zero(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "get", return_value=_resp({}, status=404)
        ):
            assert client.get_doc_count("missing") == 0


class TestGetDoc:
    _URN = "urn:li:dashboard:(test,zdu-tc-304)"

    def test_searches_by_urn_field_and_returns_source(
        self, client: ElasticsearchClient
    ) -> None:
        # V3 docs carry a hashed _id, so the lookup must go through the urn field.
        payload = {"hits": {"hits": [{"_id": "370f2d24", "_source": {"urn": "x"}}]}}
        with patch.object(
            client._es_session, "post", return_value=_resp(payload)
        ) as mock_post:
            assert client.get_doc("dashboardindex_v3", self._URN) == {"urn": "x"}
            assert mock_post.call_args[0][0] == (
                "http://es:9200/dashboardindex_v3/_search"
            )
            assert mock_post.call_args.kwargs["json"]["query"] == {
                "term": {"urn": self._URN}
            }

    def test_no_hits_returns_none(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "post", return_value=_resp({"hits": {"hits": []}})
        ):
            assert client.get_doc("dashboardindex_v3", self._URN) is None

    def test_missing_index_returns_none(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "post", return_value=_resp({}, status=404)
        ):
            assert client.get_doc("dashboardindex_v3", self._URN) is None


class TestGetRefreshInterval:
    def test_reads_value_from_resolved_index(self, client: ElasticsearchClient) -> None:
        payload = {
            "datasetindex_v3_v1_9_0rc1-2_1795280496462": {
                "settings": {"index": {"refresh_interval": "7s"}}
            }
        }
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            assert client.get_refresh_interval("datasetindex_v3") == "7s"

    def test_missing_index_returns_none(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "get", return_value=_resp({}, status=404)
        ):
            assert client.get_refresh_interval("datasetindex_v3") is None


class TestSearchForEntity:
    """GMS-mediated path retained for Suite N backwards compatibility."""

    def test_finds_urn_in_results(self, client: ElasticsearchClient) -> None:
        payload: dict[str, object] = {
            "value": {"entities": [{"entity": {"urn": "urn:li:dataset:x"}}]}
        }
        with patch.object(client._gms_session, "get", return_value=_resp(payload)):
            assert client.search_for_entity("urn:li:dataset:x") is True

    def test_missing_urn_returns_false(self, client: ElasticsearchClient) -> None:
        payload: dict[str, object] = {"value": {"entities": []}}
        with patch.object(client._gms_session, "get", return_value=_resp(payload)):
            assert client.search_for_entity("urn:li:dataset:x") is False


class TestGetMappings:
    def test_returns_mappings_inner_dict(self, client: ElasticsearchClient) -> None:
        # ES returns: {<index_name>: {"mappings": {"properties": {...}}}}
        payload = {
            "datasetindex_v2": {
                "mappings": {"properties": {"urn": {"type": "keyword"}}}
            }
        }
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            mappings = client.get_mappings("datasetindex_v2")
            assert mappings == {"properties": {"urn": {"type": "keyword"}}}

    def test_404_returns_empty(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "get", return_value=_resp({}, status=404)
        ):
            assert client.get_mappings("missing") == {}


class TestCatEndpoints:
    def test_cat_indices_returns_text_body(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session,
            "get",
            return_value=_resp(
                text="health status index\nyellow open dashboardindex_v2\n"
            ),
        ) as mock_get:
            out = client.cat_indices()
        assert "dashboardindex_v2" in out
        called = mock_get.call_args.args[0]
        assert called.endswith("/_cat/indices?v")

    def test_cat_aliases_returns_text_body(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session,
            "get",
            return_value=_resp(
                text="alias index\ndashboardindex_v2 dashboardindex_v2_old\n"
            ),
        ) as mock_get:
            out = client.cat_aliases()
        assert "dashboardindex_v2_old" in out
        called = mock_get.call_args.args[0]
        assert called.endswith("/_cat/aliases?v")

    def test_cat_returns_empty_string_on_error(
        self, client: ElasticsearchClient
    ) -> None:
        with patch.object(
            client._es_session, "get", side_effect=RuntimeError("ES down")
        ):
            assert client.cat_indices() == ""
            assert client.cat_aliases() == ""


class TestListTasks:
    def test_flattens_nodes_and_attaches_task_id(
        self, client: ElasticsearchClient
    ) -> None:
        # ES /_tasks shape: {nodes: {<node_id>: {tasks: {<task_id>: {<task_fields>}}}}}
        original_task = {
            "action": "indices:data/write/reindex",
            "running_time_in_nanos": 100,
        }
        payload = {
            "nodes": {
                "node-1": {
                    "tasks": {
                        "abc:42": original_task,
                    }
                }
            }
        }
        with patch.object(client._es_session, "get", return_value=_resp(payload)):
            tasks = client.list_tasks()

        assert len(tasks) == 1
        assert tasks[0]["task_id"] == "abc:42"
        assert tasks[0]["action"] == "indices:data/write/reindex"
        # Verify the input dict was NOT mutated
        assert "task_id" not in original_task
