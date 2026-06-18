"""Unit tests for DataHubGraph: offset pagination and entity aspect specs fetch."""

from typing import List
from unittest.mock import MagicMock

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
