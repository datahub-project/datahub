"""Unit tests for DataHubGraph: pagination, aspect specs, and lowercased-URN lookup."""

import pathlib
from typing import List
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.entity_aspect_specs import EntityAspectSpecs


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

    def test_memo_invalidated_on_commit_hash_change(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A long-running client must re-fetch specs when the server's commit
        # hash changes (i.e. the server was upgraded), not keep serving the
        # in-memory memo built against the old version.
        import datahub.ingestion.graph.client as client_module

        monkeypatch.setattr(client_module._ENTITY_SPECS_CACHE, "_dir", tmp_path)
        hashes = iter(["commit-old", "commit-new"])
        monkeypatch.setattr(
            DataHubGraph,
            "server_config",
            property(lambda self: MagicMock(commit_hash=next(hashes))),
        )
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
            _page(
                [
                    {
                        "name": "dataset",
                        "aspectSpecs": [{"aspectAnnotation": {"name": "domains"}}],
                    }
                ],
                total=1,
                count=1,
            ),
        ]
        graph = _graph(session)

        first = graph.get_entity_aspect_specs()
        assert first is not None and first.supports("dataset", "status")

        second = graph.get_entity_aspect_specs()
        assert second is not None and second.supports("dataset", "domains")
        assert second is not first
        assert session.request.call_count == 2

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


class TestSupportsDatasetAliases:
    """The check gating resolve_lowercased_urns.

    It must answer False rather than raise on every failure mode: a filter on a field the
    server does not index returns zero hits rather than an error, so a caller has to be able
    to tell an unsupported server from a lookup that matched nothing.
    """

    @staticmethod
    def _graph_with_specs(entity_aspects: dict) -> DataHubGraph:
        graph = DataHubGraph.__new__(DataHubGraph)
        graph.get_entity_aspect_specs = MagicMock(  # type: ignore[method-assign]
            return_value=EntityAspectSpecs(
                entity_aspects={k: set(v) for k, v in entity_aspects.items()}
            )
        )
        return graph

    def test_supported_when_aliases_registered(self) -> None:
        graph = self._graph_with_specs({"dataset": ["datasetKey", "aliases"]})
        assert graph.supports_dataset_aliases() is True

    def test_unsupported_when_aliases_absent(self) -> None:
        graph = self._graph_with_specs({"dataset": ["datasetKey"]})
        assert graph.supports_dataset_aliases() is False

    def test_unsupported_when_dataset_not_registered(self) -> None:
        # EntityAspectSpecs.supports raises ValueError for an unregistered entity type.
        graph = self._graph_with_specs({"chart": ["chartKey"]})
        assert graph.supports_dataset_aliases() is False

    def test_unsupported_when_registry_unavailable(self) -> None:
        # get_entity_aspect_specs returns None when the registry API can't be reached, e.g.
        # a token without MANAGE_SYSTEM_OPERATIONS.
        graph = DataHubGraph.__new__(DataHubGraph)
        graph.get_entity_aspect_specs = MagicMock(return_value=None)  # type: ignore[method-assign]
        assert graph.supports_dataset_aliases() is False


class TestResolveLowercasedUrns:
    STORED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
    UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.ORDERS,PROD)"

    @staticmethod
    def _graph_returning(urns: List[str]) -> DataHubGraph:
        graph = DataHubGraph.__new__(DataHubGraph)
        graph.get_urns_by_filter = MagicMock(return_value=iter(urns))  # type: ignore[method-assign]
        return graph

    def test_groups_hits_by_key(self) -> None:
        graph = self._graph_returning([self.STORED])
        assert graph.resolve_lowercased_urns([self.STORED]) == {
            self.STORED: [self.STORED]
        }

        kwargs = graph.get_urns_by_filter.call_args.kwargs  # type: ignore[attr-defined]
        assert kwargs["entity_types"] == ["dataset"]
        rule = kwargs["extra_or_filters"][0]["and"][0]
        assert rule["field"] == "lowercasedUrn"
        assert rule["values"] == [self.STORED]

    def test_collision_returns_every_match(self) -> None:
        # Two entities sharing one key on a case-sensitive platform. Both must come back so
        # the caller can decline to guess between them.
        graph = self._graph_returning([self.STORED, self.UPPER])
        assert sorted(
            graph.resolve_lowercased_urns([self.STORED])[self.STORED]
        ) == sorted([self.STORED, self.UPPER])

    def test_missing_key_is_absent(self) -> None:
        graph = self._graph_returning([])
        assert graph.resolve_lowercased_urns([self.STORED]) == {}

    def test_no_keys_makes_no_request(self) -> None:
        graph = self._graph_returning([])
        assert graph.resolve_lowercased_urns([]) == {}
        graph.get_urns_by_filter.assert_not_called()  # type: ignore[attr-defined]

    def test_keys_are_deduplicated_and_chunked(self) -> None:
        # Callers pass any number of keys and never think about request limits.
        keys = [
            f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t{i},PROD)"
            for i in range(1200)
        ]
        graph = DataHubGraph.__new__(DataHubGraph)
        graph.get_urns_by_filter = MagicMock(side_effect=lambda **_: iter([]))  # type: ignore[method-assign]

        graph.resolve_lowercased_urns(keys + keys)  # duplicated on purpose

        sent = [
            len(call.kwargs["extra_or_filters"][0]["and"][0]["values"])
            for call in graph.get_urns_by_filter.call_args_list  # type: ignore[attr-defined]
        ]
        assert sent == [500, 500, 200]
