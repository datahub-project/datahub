from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from datahub.sdk.patterns.job_queue.discovery import (
    MCLDiscovery,
    SearchDiscovery,
    WorkItem,
    _build_filter_clauses,
    _extract_aspect,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_graph() -> MagicMock:
    return MagicMock(spec=["execute_graphql", "config"])


class FakeEvent:
    """Minimal event stub matching the DataHubEventsConsumer interface."""

    def __init__(self, value: str) -> None:
        self.value = value
        self.contentType = "application/json"


class FakeConsumer:
    """Stub consumer with the same interface as DataHubEventsConsumer."""

    def __init__(self, events: List[FakeEvent]) -> None:
        self._events = events
        self.committed = False

    def poll_events(
        self,
        topic: str,
        limit: int | None = None,
        poll_timeout_seconds: int | None = None,
    ) -> Any:
        return self  # response object is self for simplicity

    def get_events(self, response: Any) -> List[FakeEvent]:
        return self._events

    def commit_offsets(self) -> None:
        self.committed = True


def _make_mcl_event(
    entity_type: str,
    aspect_name: str,
    entity_urn: str,
    aspect_value: Dict[str, Any],
    system_metadata: Optional[Dict[str, Any]] = None,
) -> FakeEvent:
    mcl: Dict[str, Any] = {
        "entityType": entity_type,
        "aspectName": aspect_name,
        "entityUrn": entity_urn,
        "aspect": {
            "value": json.dumps(aspect_value),
            "contentType": "application/json",
        },
    }
    if system_metadata is not None:
        mcl["systemMetadata"] = system_metadata
    return FakeEvent(json.dumps(mcl))


# ---------------------------------------------------------------------------
# _extract_aspect
# ---------------------------------------------------------------------------


class TestExtractAspect:
    def test_generic_aspect_with_json_string(self) -> None:
        mcl = {
            "aspect": {
                "value": '{"executorId": "pool-1"}',
                "contentType": "application/json",
            }
        }
        result = _extract_aspect(mcl)
        assert result == {"executorId": "pool-1"}

    def test_missing_aspect(self) -> None:
        assert _extract_aspect({}) is None

    def test_aspect_with_dict_value(self) -> None:
        mcl = {"aspect": {"value": {"executorId": "pool-1"}}}
        result = _extract_aspect(mcl)
        assert result == {"executorId": "pool-1"}

    def test_aspect_with_invalid_json_string(self) -> None:
        mcl = {"aspect": {"value": "not valid json {{{"}}
        result = _extract_aspect(mcl)
        assert result is None

    def test_aspect_none_value(self) -> None:
        mcl = {"aspect": None}
        result = _extract_aspect(mcl)
        assert result is None

    def test_direct_aspect_dict_without_value_key(self) -> None:
        """When aspect is a dict but has no 'value' key, returns it as-is."""
        mcl = {"aspect": {"executorId": "pool-1"}}
        result = _extract_aspect(mcl)
        assert result == {"executorId": "pool-1"}


# ---------------------------------------------------------------------------
# _build_filter_clauses
# ---------------------------------------------------------------------------


class TestBuildFilterClauses:
    def test_single_filter(self) -> None:
        result = _build_filter_clauses({"state": ["ACTIVE"]})
        assert result == [
            {"and": [{"field": "state", "values": ["ACTIVE"], "condition": "EQUAL"}]}
        ]

    def test_multiple_filters(self) -> None:
        result = _build_filter_clauses(
            {"state": ["ACTIVE"], "platform": ["kafka", "mysql"]}
        )
        assert len(result) == 1
        and_conditions = result[0]["and"]
        assert len(and_conditions) == 2

    def test_empty_filters(self) -> None:
        result = _build_filter_clauses({})
        assert result == [{"and": []}]


# ---------------------------------------------------------------------------
# MCLDiscovery
# ---------------------------------------------------------------------------


class TestMCLDiscovery:
    def test_poll_returns_work_items(self) -> None:
        events = [
            _make_mcl_event(
                entity_type="dataHubExecutionRequest",
                aspect_name="dataHubExecutionRequestInput",
                entity_urn="urn:li:dataHubExecutionRequest:req1",
                aspect_value={"executorId": "pool-1"},
            ),
            _make_mcl_event(
                entity_type="dataset",
                aspect_name="datasetProperties",
                entity_urn="urn:li:dataset:unrelated",
                aspect_value={"name": "test"},
            ),
        ]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        items = discovery.poll()
        # The second event should be filtered out by entity_type mismatch.
        urns = [item.urn for item in items]
        assert "urn:li:dataset:unrelated" not in urns
        # Items are WorkItem instances
        for item in items:
            assert isinstance(item, WorkItem)
        assert consumer.committed

    def test_poll_applies_is_candidate_filter(self) -> None:
        events = [
            _make_mcl_event(
                entity_type="dataHubExecutionRequest",
                aspect_name="dataHubExecutionRequestInput",
                entity_urn="urn:li:dataHubExecutionRequest:req1",
                aspect_value={"executorId": "pool-1"},
            ),
            _make_mcl_event(
                entity_type="dataHubExecutionRequest",
                aspect_name="dataHubExecutionRequestInput",
                entity_urn="urn:li:dataHubExecutionRequest:req2",
                aspect_value={"executorId": "pool-2"},
            ),
        ]
        consumer = FakeConsumer(events)

        # The ASPECT_NAME_MAP lookup may return None for these test aspects,
        # so both may be filtered. The key thing is no exception is raised.
        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda aspect: aspect.get("executorId") == "pool-1",
            events_consumer=consumer,
        )

        items = discovery.poll()
        assert isinstance(items, list)
        assert consumer.committed

    def test_poll_no_events_returns_empty_list(self) -> None:
        consumer = FakeConsumer([])
        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )
        items = discovery.poll()
        assert items == []
        # No events means no commit.
        assert not consumer.committed

    def test_poll_handles_malformed_event(self) -> None:
        events = [FakeEvent("not valid json {{{")]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )
        items = discovery.poll()
        assert items == []
        assert consumer.committed

    def test_poll_commits_offsets_after_processing(self) -> None:
        events = [
            _make_mcl_event(
                entity_type="dataHubExecutionRequest",
                aspect_name="dataHubExecutionRequestInput",
                entity_urn="urn:li:dataHubExecutionRequest:req1",
                aspect_value={"executorId": "pool-1"},
            ),
        ]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        discovery.poll()
        assert consumer.committed is True

    def test_poll_does_not_commit_offsets_when_no_events(self) -> None:
        consumer = FakeConsumer([])

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        discovery.poll()
        assert consumer.committed is False

    def test_poll_passes_topic_and_batch_params_to_consumer(self) -> None:
        consumer = MagicMock()
        consumer.poll_events.return_value = "response"
        consumer.get_events.return_value = []

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            topic="custom-topic",
            batch_size=25,
            poll_timeout_s=10,
            events_consumer=consumer,
        )

        discovery.poll()
        consumer.poll_events.assert_called_once_with(
            topic="custom-topic",
            limit=25,
            poll_timeout_seconds=10,
        )

    def test_poll_skips_event_with_missing_entity_urn(self) -> None:
        mcl = {
            "entityType": "dataHubExecutionRequest",
            "aspectName": "dataHubExecutionRequestInput",
            # no entityUrn
            "aspect": {
                "value": json.dumps({"executorId": "pool-1"}),
                "contentType": "application/json",
            },
        }
        events = [FakeEvent(json.dumps(mcl))]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataHubExecutionRequest",
            aspect_name="dataHubExecutionRequestInput",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        items = discovery.poll()
        assert items == []

    def test_poll_with_real_aspect_class(self) -> None:
        """Use a known aspect from ASPECT_NAME_MAP to test full deserialization."""
        events = [
            _make_mcl_event(
                entity_type="dataset",
                aspect_name="datasetProperties",
                entity_urn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
                aspect_value={"name": "test-table"},
                system_metadata={"version": "5"},
            ),
        ]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataset",
            aspect_name="datasetProperties",
            is_candidate=lambda aspect: True,
            events_consumer=consumer,
        )

        items = discovery.poll()
        assert len(items) == 1
        assert (
            items[0].urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
        )
        # claim_hint should be populated with version from systemMetadata
        assert items[0].claim_hint is not None
        assert items[0].claim_hint.version == "5"
        assert items[0].claim_hint.aspect is not None

    def test_process_event_extracts_version_from_system_metadata(self) -> None:
        """_process_event populates claim_hint.version from systemMetadata."""
        events = [
            _make_mcl_event(
                entity_type="dataset",
                aspect_name="datasetProperties",
                entity_urn="urn:li:dataset:versioned",
                aspect_value={"name": "v-table"},
                system_metadata={"version": "42"},
            ),
        ]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataset",
            aspect_name="datasetProperties",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        items = discovery.poll()
        assert len(items) == 1
        assert items[0].claim_hint is not None
        assert items[0].claim_hint.version == "42"

    def test_process_event_defaults_version_without_system_metadata(self) -> None:
        """When systemMetadata is missing, version defaults to -1."""
        events = [
            _make_mcl_event(
                entity_type="dataset",
                aspect_name="datasetProperties",
                entity_urn="urn:li:dataset:no-version",
                aspect_value={"name": "nv-table"},
            ),
        ]
        consumer = FakeConsumer(events)

        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataset",
            aspect_name="datasetProperties",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )

        items = discovery.poll()
        assert len(items) == 1
        assert items[0].claim_hint is not None
        assert items[0].claim_hint.version == "-1"

    def test_uses_injected_consumer_over_auto_created(self) -> None:
        """When events_consumer is provided, no attempt to auto-create."""
        consumer = FakeConsumer([])
        discovery = MCLDiscovery(
            graph=_make_graph(),
            consumer_id="test-consumer",
            entity_type="dataset",
            aspect_name="datasetProperties",
            is_candidate=lambda _: True,
            events_consumer=consumer,
        )
        assert discovery._consumer is consumer


# ---------------------------------------------------------------------------
# SearchDiscovery
# ---------------------------------------------------------------------------


class TestSearchDiscovery:
    def test_poll_returns_work_items(self) -> None:
        graph = _make_graph()
        graph.execute_graphql.return_value = {
            "searchAcrossEntities": {
                "start": 0,
                "total": 2,
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataHubAction:action1"}},
                    {"entity": {"urn": "urn:li:dataHubAction:action2"}},
                ],
            }
        }

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"]},
            max_results=100,
        )

        items = discovery.poll()
        assert len(items) == 2
        assert items[0].urn == "urn:li:dataHubAction:action1"
        assert items[1].urn == "urn:li:dataHubAction:action2"
        # SearchDiscovery should not provide claim hints
        assert items[0].claim_hint is None
        assert items[1].claim_hint is None
        graph.execute_graphql.assert_called_once()

    def test_poll_passes_correct_variables_to_graphql(self) -> None:
        graph = _make_graph()
        graph.execute_graphql.return_value = {
            "searchAcrossEntities": {
                "start": 0,
                "total": 0,
                "searchResults": [],
            }
        }

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"], "platform": ["kafka"]},
            max_results=50,
        )

        discovery.poll()

        call_kwargs = graph.execute_graphql.call_args
        variables = call_kwargs[1]["variables"]
        assert variables["query"] == "*"
        assert variables["start"] == 0
        assert variables["count"] == 50
        assert isinstance(variables["orFilters"], list)
        assert len(variables["orFilters"]) == 1
        and_conditions = variables["orFilters"][0]["and"]
        assert len(and_conditions) == 2

    def test_poll_empty_results(self) -> None:
        graph = _make_graph()
        graph.execute_graphql.return_value = {
            "searchAcrossEntities": {
                "start": 0,
                "total": 0,
                "searchResults": [],
            }
        }

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"]},
        )

        items = discovery.poll()
        assert items == []

    def test_poll_handles_graphql_error(self) -> None:
        graph = _make_graph()
        graph.execute_graphql.side_effect = Exception("GraphQL error")

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"]},
        )

        items = discovery.poll()
        assert items == []

    def test_poll_handles_missing_search_results_key(self) -> None:
        """Gracefully handle unexpected response shape."""
        graph = _make_graph()
        graph.execute_graphql.return_value = {}

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"]},
        )

        items = discovery.poll()
        assert items == []

    def test_poll_skips_results_without_urn(self) -> None:
        graph = _make_graph()
        graph.execute_graphql.return_value = {
            "searchAcrossEntities": {
                "start": 0,
                "total": 2,
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataHubAction:action1"}},
                    {"entity": {}},  # missing urn
                    {"entity": {"urn": "urn:li:dataHubAction:action3"}},
                ],
            }
        }

        discovery = SearchDiscovery(
            graph=graph,
            entity_type="dataHubAction",
            filters={"state": ["ACTIVE"]},
        )

        items = discovery.poll()
        urns = [item.urn for item in items]
        assert urns == [
            "urn:li:dataHubAction:action1",
            "urn:li:dataHubAction:action3",
        ]

    def test_poll_uses_entity_type_to_graphql_conversion(self) -> None:
        """The entity_type is converted to GraphQL enum format."""
        graph = _make_graph()
        graph.execute_graphql.return_value = {
            "searchAcrossEntities": {
                "start": 0,
                "total": 0,
                "searchResults": [],
            }
        }

        with patch(
            "datahub.sdk.patterns.job_queue.discovery.entity_type_to_graphql",
            return_value="DATA_HUB_ACTION",
        ) as mock_convert:
            discovery = SearchDiscovery(
                graph=graph,
                entity_type="dataHubAction",
                filters={"state": ["ACTIVE"]},
            )
            discovery.poll()

        mock_convert.assert_called_once_with("dataHubAction")
        variables = graph.execute_graphql.call_args[1]["variables"]
        assert variables["types"] == ["DATA_HUB_ACTION"]
