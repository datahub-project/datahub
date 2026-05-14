"""Unit tests for ElasticsearchClient — uses mocked Sessions; never hits a real ES."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.es_client import ElasticsearchClient


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
    def test_returns_source(self, client: ElasticsearchClient) -> None:
        payload = {"_source": {"urn": "x"}, "_id": "abc"}
        with patch.object(
            client._es_session, "get", return_value=_resp(payload)
        ) as mock_get:
            assert client.get_doc("datasetindex_v2", "abc") == {"urn": "x"}
            # Verify the URL was constructed correctly (no double-encoding for plain ids)
            called_url = mock_get.call_args[0][0]
            assert called_url == "http://es:9200/datasetindex_v2/_doc/abc"

    def test_404_returns_none(self, client: ElasticsearchClient) -> None:
        with patch.object(
            client._es_session, "get", return_value=_resp({}, status=404)
        ):
            assert client.get_doc("datasetindex_v2", "abc") is None

    def test_doc_id_with_special_chars_is_double_encoded(
        self, client: ElasticsearchClient
    ) -> None:
        """DataHub stores the URL-encoded URN literally as the ES ``_id``.
        ES path-decodes URL-encoded path components once during routing, so
        ``get_doc`` must double-encode the raw URN — once to produce the
        stored ``_id`` form, once more so ES's decode leaves us with the
        encoded ``_id`` for lookup."""
        payload = {"_source": {"urn": "urn:li:corpuser:datahub"}}
        with patch.object(
            client._es_session, "get", return_value=_resp(payload)
        ) as mock_get:
            # Caller passes the RAW URN, not pre-encoded.
            doc_id = "urn:li:corpuser:datahub"
            client.get_doc("corpuserindex_v2", doc_id)
            called_url = mock_get.call_args[0][0]
            # ``:`` (%3A) is encoded once → ``%3A``, then again → ``%253A``;
            # ES decodes once on routing and looks up ``urn%3Ali%3Acorpuser%3Adatahub``.
            assert called_url == (
                "http://es:9200/corpuserindex_v2/_doc/"
                "urn%253Ali%253Acorpuser%253Adatahub"
            )


class TestSearchForEntity:
    """GMS-mediated path retained for Suite A backwards compatibility."""

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
