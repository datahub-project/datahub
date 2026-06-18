"""Unit tests for DataHubGraph: offset pagination and entity aspect specs fetch."""

import pathlib
from typing import List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.graph.client import DataHubGraph


def _page(elements: List[dict], total: int, count: int) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"elements": elements, "total": total, "count": count}
    resp.raise_for_status.return_value = None
    return resp


def _graph(session: MagicMock) -> DataHubGraph:
    """A DataHubGraph wired with only the attrs these methods touch."""
    graph = DataHubGraph.__new__(DataHubGraph)
    graph._session = session
    graph._gms_server = "http://localhost:8080"
    graph._entity_aspect_specs = None
    return graph


class TestPaginateOffset:
    def test_walks_all_pages(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _page([{"i": 0}, {"i": 1}], total=3, count=2),
            _page([{"i": 2}], total=3, count=1),
        ]
        elements = list(_graph(session)._paginate_offset("http://x/list", page_size=2))
        assert elements == [{"i": 0}, {"i": 1}, {"i": 2}]
        assert session.request.call_count == 2

    def test_empty_page_terminates(self) -> None:
        session = MagicMock()
        session.request.return_value = _page([], total=0, count=0)
        assert list(_graph(session)._paginate_offset("http://x/list")) == []
        assert session.request.call_count == 1


class TestGetEntityAspectSpecs:
    def test_fetches_and_memoizes(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _page(
                [
                    {
                        "name": "dataset",
                        "aspectSpecs": [{"aspectAnnotation": {"name": "status"}}],
                    }
                ],
                total=1,
                count=1,
            ),
        ]
        graph = _graph(session)

        specs = graph.get_entity_aspect_specs()
        assert specs is not None
        assert specs.supports("dataset", "status")

        # Second call is served from the in-memory memo, not re-fetched.
        assert graph.get_entity_aspect_specs() is specs
        assert session.request.call_count == 1

    def test_fetch_failure_returns_none_and_allows_retry(self) -> None:
        session = MagicMock()
        session.request.side_effect = Exception("boom")
        graph = _graph(session)

        assert graph.get_entity_aspect_specs() is None
        # Memo stays unset so a later call can retry.
        assert graph._entity_aspect_specs is None

    def test_disk_cache_shared_across_clients(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import datahub.ingestion.graph.client as client_module

        monkeypatch.setattr(client_module._ENTITY_SPECS_CACHE, "_dir", tmp_path)
        # server_config is a read-only property; stub it to advertise a commit hash.
        monkeypatch.setattr(
            DataHubGraph,
            "server_config",
            property(lambda self: MagicMock(commit_hash="commit-abc")),
        )
        page = _page(
            [
                {
                    "name": "dataset",
                    "aspectSpecs": [{"aspectAnnotation": {"name": "status"}}],
                }
            ],
            total=1,
            count=1,
        )

        # First client fetches over the wire and writes to disk.
        session1 = MagicMock()
        session1.request.return_value = page
        specs1 = _graph(session1).get_entity_aspect_specs()
        assert specs1 is not None and specs1.supports("dataset", "status")
        assert session1.request.call_count == 1

        # A second client with an empty memo reads from disk — no HTTP call.
        session2 = MagicMock()
        specs2 = _graph(session2).get_entity_aspect_specs()
        assert specs2 is not None and specs2.supports("dataset", "status")
        assert session2.request.call_count == 0
