"""Layer 2b: Pluggable discovery strategies for finding work candidates.

Each strategy implements the :class:`Discovery` protocol by returning
a list of entity URNs.  Two concrete implementations are provided:

* :class:`MCLDiscovery` -- polls the DataHub MCL event stream.
* :class:`SearchDiscovery` -- queries DataHub search.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

from datahub._codegen.aspect import _Aspect
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.graph.client import DataHubGraph, entity_type_to_graphql
from datahub.metadata.schema_classes import ASPECT_NAME_MAP
from datahub.sdk.patterns._shared.conditional_writer import VersionedAspect

logger = logging.getLogger(__name__)


@dataclass
class WorkItem:
    """A discovered work candidate with an optional pre-fetched aspect hint.

    When :class:`MCLDiscovery` produces a ``WorkItem``, it can populate
    *claim_hint* with the version and aspect already deserialized from the
    MCL event.  :class:`Claim` can then skip the initial read round trip
    when the hint's aspect type matches the claim aspect.
    """

    urn: str
    claim_hint: Optional[VersionedAspect] = dataclass_field(default=None)


@runtime_checkable
class Discovery(Protocol):
    """Protocol for work-discovery strategies."""

    def poll(self) -> List[WorkItem]:
        """Return work items that are candidates for claiming."""
        ...


class MCLDiscovery:
    """Discover work by polling the DataHub MCL event stream.

    Filters events by entity type, aspect name, and a caller-provided
    predicate.  Requires a ``DataHubEventsConsumer``-compatible consumer;
    this class uses the same Events API used by DataHub Actions and the
    Remote Executor.

    The consumer must expose:

    * ``poll_events(topic, limit, poll_timeout_seconds) -> response``
    * ``get_events(response) -> list[event]``  (each event has ``.value``)
    * ``commit_offsets()``

    The ``datahub-actions`` package and the ``datahub-integrations-service``
    both provide a ``DataHubEventsConsumer`` class with this interface.

    Args:
        graph: DataHub graph client (used for constructing the consumer
            if not provided directly via *events_consumer*).
        consumer_id: Unique identifier for this consumer instance.
        entity_type: Entity type to filter on (e.g. ``"dataHubExecutionRequest"``).
        aspect_name: Aspect name to filter on (e.g. ``"dataHubExecutionRequestInput"``).
        is_candidate: Predicate that receives a deserialized aspect and returns
            ``True`` if the entity is a work candidate.
        topic: Kafka topic name for MCL events.
        batch_size: Maximum number of events to fetch per poll.
        poll_timeout_s: Long-poll timeout in seconds.
        lookback_days: How many days of history to look back on first poll.
        events_consumer: Optional pre-built events consumer.  If not supplied,
            one will be created from *graph* and *consumer_id*.  This allows
            callers who already have a consumer (e.g. from ``datahub-actions``
            or ``datahub-integrations-service``) to inject it directly.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        consumer_id: str,
        entity_type: str,
        aspect_name: str,
        is_candidate: Callable[[_Aspect], bool],
        topic: str = "MetadataChangeLog_Versioned_v1",
        batch_size: int = 10,
        poll_timeout_s: int = 5,
        lookback_days: int = 1,
        events_consumer: Optional[Any] = None,
    ) -> None:
        self._graph = graph
        self._entity_type = entity_type
        self._aspect_name = aspect_name
        self._is_candidate = is_candidate
        self._topic = topic
        self._batch_size = batch_size
        self._poll_timeout_s = poll_timeout_s
        self._aspect_class = ASPECT_NAME_MAP.get(aspect_name)

        if events_consumer is not None:
            self._consumer = events_consumer
        else:
            self._consumer = _create_events_consumer(
                graph=graph,
                consumer_id=consumer_id,
                lookback_days=lookback_days,
            )

    def poll(self) -> List[WorkItem]:
        """Poll MCL events, parse aspects, filter with ``is_candidate``.

        Commits offsets after processing.  Returns :class:`WorkItem` instances
        with *claim_hint* populated from the MCL event data.
        """
        response = self._consumer.poll_events(
            topic=self._topic,
            limit=self._batch_size,
            poll_timeout_seconds=self._poll_timeout_s,
        )
        events = self._consumer.get_events(response)

        items: List[WorkItem] = []
        for event in events:
            try:
                item = self._process_event(event)
                if item is not None:
                    items.append(item)
            except Exception:
                logger.warning("Failed to process MCL event", exc_info=True)

        if events:
            self._consumer.commit_offsets()

        return items

    def _process_event(self, event: Any) -> Optional[WorkItem]:
        """Parse a single event and return a :class:`WorkItem` if it matches."""
        mcl = json.loads(event.value)

        # Filter by entity type.
        if mcl.get("entityType") != self._entity_type:
            return None

        # Filter by aspect name.
        if mcl.get("aspectName") != self._aspect_name:
            return None

        entity_urn = mcl.get("entityUrn")
        if not entity_urn:
            return None

        # Deserialize the aspect and apply the candidate predicate.
        aspect_obj = _extract_aspect(mcl)
        if aspect_obj is None or self._aspect_class is None:
            return None

        try:
            aspect = self._aspect_class.from_obj(post_json_transform(aspect_obj))
        except Exception:
            logger.debug("Failed to deserialize aspect %s from MCL", self._aspect_name)
            return None

        if not self._is_candidate(aspect):
            return None

        # Extract version from systemMetadata if available.
        version = "-1"
        sys_meta = mcl.get("systemMetadata")
        if isinstance(sys_meta, dict):
            raw_version = sys_meta.get("version")
            if raw_version is not None:
                version = str(raw_version)

        return WorkItem(
            urn=entity_urn,
            claim_hint=VersionedAspect(version=version, aspect=aspect),
        )


class SearchDiscovery:
    """Discover work by querying DataHub search.

    Uses ``graph.execute_graphql()`` with a ``searchAcrossEntities`` query
    to find entities matching the given filters.

    Args:
        graph: DataHub graph client.
        entity_type: Entity type to search (e.g. ``"dataHubAction"``).
        filters: Mapping of field name to list of acceptable values.
        sort_field: Optional field to sort results by.
        max_results: Maximum number of results to return per poll.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        entity_type: str,
        filters: Dict[str, List[str]],
        sort_field: Optional[str] = None,
        max_results: int = 100,
    ) -> None:
        self._graph = graph
        self._entity_type = entity_type
        self._filters = filters
        self._sort_field = sort_field
        self._max_results = max_results

    def poll(self) -> List[WorkItem]:
        """Search for entities matching filters. Returns :class:`WorkItem` instances."""
        graphql_type = entity_type_to_graphql(self._entity_type)
        filter_clauses = _build_filter_clauses(self._filters)

        query = _SEARCH_QUERY
        variables: Dict[str, Any] = {
            "types": [graphql_type],
            "query": "*",
            "start": 0,
            "count": self._max_results,
            "orFilters": filter_clauses,
        }

        try:
            response = self._graph.execute_graphql(query, variables=variables)
        except Exception:
            logger.warning("SearchDiscovery query failed", exc_info=True)
            return []

        search_data = response.get("searchAcrossEntities", {})
        results = search_data.get("searchResults", [])

        items: List[WorkItem] = []
        for result in results:
            entity = result.get("entity", {})
            urn = entity.get("urn")
            if urn:
                items.append(WorkItem(urn=urn))

        logger.debug(
            "SearchDiscovery found %d candidates for %s",
            len(items),
            self._entity_type,
        )
        return items


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_SEARCH_QUERY = """\
query searchWork($types: [EntityType!], $query: String!, $start: Int!, $count: Int!, $orFilters: [AndFilterInput!]) {
  searchAcrossEntities(
    input: {types: $types, query: $query, start: $start, count: $count, orFilters: $orFilters}
  ) {
    start
    total
    searchResults {
      entity {
        urn
      }
    }
  }
}
"""


def _build_filter_clauses(
    filters: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    """Convert ``{field: [values]}`` into GraphQL ``AndFilterInput`` format."""
    and_conditions: List[Dict[str, Any]] = []
    for field, values in filters.items():
        and_conditions.append(
            {
                "field": field,
                "values": values,
                "condition": "EQUAL",
            }
        )
    return [{"and": and_conditions}]


def _extract_aspect(mcl: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract the aspect value from a raw MCL JSON dict.

    The MCL may carry the aspect in either ``aspect.value`` (GenericAspect
    with JSON-serialized content) or directly under ``aspect``.
    """
    aspect_wrapper = mcl.get("aspect")
    if aspect_wrapper is None:
        return None

    # GenericAspect: value is a JSON-encoded string inside {"value": "...", "contentType": "..."}
    if isinstance(aspect_wrapper, dict) and "value" in aspect_wrapper:
        raw_value = aspect_wrapper["value"]
        if isinstance(raw_value, str):
            try:
                return json.loads(raw_value)
            except (json.JSONDecodeError, TypeError):
                return None
        return raw_value

    return aspect_wrapper


def _create_events_consumer(
    graph: DataHubGraph,
    consumer_id: str,
    lookback_days: int,
) -> Any:
    """Create a DataHubEventsConsumer, trying available implementations.

    Attempts to import from ``datahub_integrations`` first (Integration
    Service), then falls back to ``datahub_actions`` (Actions Framework).
    Raises ``ImportError`` if neither is available.
    """
    # Try the integration-service consumer first.
    try:
        from datahub_integrations.sources.remote_actions.events.datahub_events_consumer import (  # type: ignore[import-untyped]
            DataHubEventsConsumer,
        )

        return DataHubEventsConsumer(
            graph=graph,
            consumer_id=consumer_id,
            lookback_days=lookback_days,
        )
    except ImportError:
        pass

    # Fall back to the actions-framework consumer.
    try:
        from datahub_actions.plugin.source.acryl.datahub_cloud_events_consumer import (  # type: ignore[import-untyped]
            DataHubEventsConsumer,
        )

        return DataHubEventsConsumer(
            graph=graph,
            consumer_id=consumer_id,
            lookback_days=lookback_days,
        )
    except ImportError:
        pass

    raise ImportError(
        "MCLDiscovery requires a DataHubEventsConsumer implementation. "
        "Install either 'datahub-integrations-service' or 'datahub-actions' "
        "to provide one, or pass an events_consumer directly."
    )
