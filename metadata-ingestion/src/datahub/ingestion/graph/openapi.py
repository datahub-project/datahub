import json
import logging
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
)

import requests
from typing_extensions import deprecated

from datahub._codegen.aspect import _Aspect
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.filters import RawSearchFilter
from datahub.metadata.schema_classes import (
    ASPECT_NAME_MAP,
    SystemMetadataClass,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

_JSON_HEADERS: Dict[str, str] = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

EntityAspects = Dict[str, Tuple[_Aspect, Optional[SystemMetadataClass]]]


class SortCriterionDict(TypedDict, total=False):
    field: str
    order: Literal["ASCENDING", "DESCENDING"]


@dataclass
class ScrollResult:
    scroll_id: Optional[str]
    entities: Dict[str, EntityAspects]
    total_count: int


@dataclass
class RelatedEntity:
    urn: str
    relationship_type: str
    via: Optional[str] = None


@dataclass
class Relationship:
    relationship_type: str
    source_urn: str
    source_entity_type: str
    destination_urn: str
    destination_entity_type: str


@dataclass
class RelationshipScrollResult:
    scroll_id: Optional[str]
    relationships: List[Relationship]


class RelationshipDirection(StrEnum):
    INCOMING = "INCOMING"
    OUTGOING = "OUTGOING"


class OpenAPIGraphProtocol(Protocol):
    _gms_server: str
    config: DatahubClientConfig
    _session: requests.Session

    def _get_generic(self, url: str, params: Optional[Dict] = None) -> Dict: ...


class OpenApiAPI(OpenAPIGraphProtocol):
    # Redefine for backwards compatibility
    RelationshipDirection = RelationshipDirection

    def _deserialize_entities(
        self,
        entities: List[Dict[str, Any]],
        with_system_metadata: bool,
        context: str = "response",
    ) -> Dict[str, EntityAspects]:
        result: Dict[str, EntityAspects] = {}
        for entity in entities:
            entity_urn = entity.get("urn")
            if entity_urn is None:
                logger.warning(f"Missing URN in {context}: {entity}, skipping")
                continue

            entity_aspects: EntityAspects = {}
            for aspect_name, aspect_obj in entity.items():
                if aspect_name == "urn":
                    continue

                aspect_class = ASPECT_NAME_MAP.get(aspect_name)
                if aspect_class is None:
                    logger.warning(
                        f"Unknown aspect type {aspect_name}, skipping deserialization"
                    )
                    continue

                aspect_value = aspect_obj.get("value")
                if aspect_value is None:
                    logger.warning(
                        f"Unknown aspect value for aspect {aspect_name}, skipping deserialization"
                    )
                    continue

                try:
                    post_json_obj = post_json_transform(aspect_value)
                    typed_aspect = aspect_class.from_obj(post_json_obj)
                    assert isinstance(typed_aspect, aspect_class) and isinstance(
                        typed_aspect, _Aspect
                    )

                    system_metadata = None
                    if with_system_metadata:
                        system_metadata_obj = aspect_obj.get("systemMetadata")
                        if system_metadata_obj:
                            system_metadata = SystemMetadataClass.from_obj(
                                system_metadata_obj
                            )

                    entity_aspects[aspect_name] = (typed_aspect, system_metadata)
                except Exception as e:
                    logger.error(f"Error deserializing aspect {aspect_name}: {e}")
                    raise

            if entity_aspects:
                result[entity_urn] = entity_aspects

        return result

    def get_related_entities(
        self,
        entity_urn: str,
        relationship_types: List[str],
        direction: RelationshipDirection,
    ) -> Iterable[RelatedEntity]:
        url = f"{self._gms_server}/openapi/relationships/v1/"
        done = False
        start = 0
        while not done:
            response = self._get_generic(
                url=url,
                params={
                    "urn": entity_urn,
                    "direction": direction.value,
                    "relationshipTypes": relationship_types,
                    "start": start,
                },
            )
            for related_entity in response.get("entities", []):
                yield RelatedEntity(
                    urn=related_entity["urn"],
                    relationship_type=related_entity["relationshipType"],
                    via=related_entity.get("via"),
                )
            done = response.get("count", 0) == 0 or response.get("count", 0) < len(
                response.get("entities", [])
            )
            start = start + response.get("count", 0)

    def scroll_relationships(
        self,
        *,
        relationship_types: Optional[List[str]] = None,
        source_types: Optional[List[str]] = None,
        destination_types: Optional[List[str]] = None,
        source_urns: Optional[List[str]] = None,
        destination_urns: Optional[List[str]] = None,
        count: Optional[int] = None,
        scroll_id: Optional[str] = None,
        include_soft_delete: Optional[bool] = None,
        slice_id: Optional[int] = None,
        slice_max: Optional[int] = None,
        pit_keep_alive: Optional[str] = None,
    ) -> RelationshipScrollResult:
        """Scroll through relationships using the OpenAPI v3 relationship scroll endpoint.

        Supports filtering by relationship types, source/destination entity types, and
        source/destination URNs. Parameters left as None use the server's defaults
        (count=10, all relationship types, no entity type or URN filters,
        includeSoftDelete=false, pitKeepAlive="5m").

        Args:
            relationship_types: Relationship types to include (e.g. ["DownstreamOf"]).
                If None or empty, all relationship types are returned.
            source_types: Entity types to filter source nodes (e.g. ["dataset"]).
            destination_types: Entity types to filter destination nodes.
            source_urns: URNs to filter source entities (OR logic across values).
            destination_urns: URNs to filter destination entities (OR logic across values).
            count: Number of results per page.
            scroll_id: Pagination cursor from a previous scroll response.
            include_soft_delete: If True, include soft-deleted entities.
            slice_id: Slice index for parallel scrolling.
            slice_max: Total number of slices for parallel scrolling.
            pit_keep_alive: Point-in-time keep-alive duration (e.g. "5m").

        Returns:
            A RelationshipScrollResult with:
            - scroll_id: cursor to pass in the next call (None when exhausted)
            - relationships: list of Relationship objects with source/destination info
        """
        url = f"{self._gms_server}/openapi/v3/relationship/scroll"

        params: Dict[str, Any] = {}
        if relationship_types is not None:
            params["relationshipTypes"] = relationship_types
        if source_types is not None:
            params["sourceTypes"] = source_types
        if destination_types is not None:
            params["destinationTypes"] = destination_types
        if source_urns is not None:
            params["sourceUrns"] = source_urns
        if destination_urns is not None:
            params["destinationUrns"] = destination_urns
        if count is not None:
            params["count"] = count
        if scroll_id is not None:
            params["scrollId"] = scroll_id
        if include_soft_delete is not None:
            params["includeSoftDelete"] = str(include_soft_delete).lower()
        if slice_id is not None:
            params["sliceId"] = slice_id
        if slice_max is not None:
            params["sliceMax"] = slice_max
        if pit_keep_alive is not None:
            params["pitKeepAlive"] = pit_keep_alive

        response = self._get_generic(url=url, params=params)

        relationships = [
            Relationship(
                relationship_type=r["relationshipType"],
                source_urn=r["source"]["urn"],
                source_entity_type=r["source"]["entityType"],
                destination_urn=r["destination"]["urn"],
                destination_entity_type=r["destination"]["entityType"],
            )
            for r in response.get("results", [])
        ]

        return RelationshipScrollResult(
            scroll_id=response.get("scrollId"),
            relationships=relationships,
        )

    def get_kafka_consumer_offsets(
        self,
    ) -> dict:
        """
        Get Kafka consumer offsets from the DataHub API.

        Args:
            graph (DataHubGraph): The DataHub graph client

        """
        urls = {
            "mcp": f"{self.config.server}/openapi/operations/kafka/mcp/consumer/offsets",
            "mcl": f"{self.config.server}/openapi/operations/kafka/mcl/consumer/offsets",
            "mcl-timeseries": f"{self.config.server}/openapi/operations/kafka/mcl-timeseries/consumer/offsets",
        }

        params = {"skipCache": "true", "detailed": "true"}
        results = {}
        for key, url in urls.items():
            response = self._get_generic(url=url, params=params)
            results[key] = response
            if "errors" in response:
                logger.error(f"Error: {response['errors']}")
        return results

    @deprecated("Use get_entities instead which returns typed aspects")
    def get_entities_v2(
        self,
        entity_name: str,
        urns: List[str],
        aspects: Optional[List[str]] = None,
        with_system_metadata: bool = False,
    ) -> Dict[str, Any]:
        aspects = aspects or []
        payload = {
            "urns": urns,
            "aspectNames": aspects,
            "withSystemMetadata": with_system_metadata,
        }
        url = f"{self._gms_server}/openapi/v2/entity/batch/{entity_name}"
        response = self._session.post(
            url, data=json.dumps(payload), headers=_JSON_HEADERS
        )
        response.raise_for_status()

        json_resp = response.json()
        entities = json_resp.get("entities", [])
        aspects_set = set(aspects)
        retval: Dict[str, Any] = {}

        for entity in entities:
            entity_aspects = entity.get("aspects", {})
            entity_urn = entity.get("urn", None)

            if entity_urn is None:
                continue
            for aspect_key, aspect_value in entity_aspects.items():
                # Include all aspects if aspect filter is empty
                if len(aspects) == 0 or aspect_key in aspects_set:
                    retval.setdefault(entity_urn, {})
                    retval[entity_urn][aspect_key] = aspect_value
        return retval

    def get_entities(
        self,
        entity_name: str,
        urns: List[str],
        aspects: Optional[List[str]] = None,
        with_system_metadata: bool = False,
    ) -> Dict[str, EntityAspects]:
        """
        Get entities using the OpenAPI v3 endpoint, deserializing aspects into typed objects.

        Args:
            entity_name: The entity type name
            urns: List of entity URNs to fetch
            aspects: Optional list of aspect names to fetch. If None, all aspects will be fetched.
            with_system_metadata: If True, return system metadata along with each aspect.

        Returns:
            A dictionary mapping URNs to a dictionary of aspect name to tuples of
            (typed aspect object, system metadata). If with_system_metadata is False,
            the system metadata in the tuple will be None.
        """
        aspects = aspects or []

        request_payload = []
        for urn in urns:
            entity_request: Dict[str, Any] = {"urn": urn}
            for aspect_name in aspects:
                entity_request[aspect_name] = {}
            request_payload.append(entity_request)

        url = f"{self._gms_server}/openapi/v3/entity/{entity_name}/batchGet"
        if with_system_metadata:
            url += "?systemMetadata=true"

        response = self._session.post(
            url, data=json.dumps(request_payload), headers=_JSON_HEADERS
        )
        response.raise_for_status()

        return self._deserialize_entities(
            response.json(), with_system_metadata=with_system_metadata
        )

    def scroll_entities(
        self,
        *,
        entity_name: Optional[str] = None,
        aspects: Optional[List[str]] = None,
        count: Optional[int] = None,
        query: Optional[str] = None,
        scroll_id: Optional[str] = None,
        sort_field: Optional[str] = None,
        with_system_metadata: Optional[bool] = None,
        skip_cache: Optional[bool] = None,
        skip_aggregation: Optional[bool] = None,
        include_soft_delete: Optional[bool] = None,
        scroll_id_per_entity: Optional[bool] = None,
        slice_id: Optional[int] = None,
        slice_max: Optional[int] = None,
        pit_keep_alive: Optional[str] = None,
        filter: Optional[RawSearchFilter] = None,
        sort_criteria: Optional[List[SortCriterionDict]] = None,
    ) -> ScrollResult:
        """Scroll through entities using the OpenAPI v3 scroll endpoint.

        Parameters left as None use the server's defaults (count=10, query="*", sort="urn",
        systemMetadata=false, skipCache=false, skipAggregation=true, includeSoftDelete=false,
        scrollIdPerEntity=false, pitKeepAlive="5m").

        Args:
            entity_name: The entity type name (e.g. "dataset", "dashboard"). If None, the
                generic cross-entity scroll endpoint is used.
            aspects: Aspect names to include in the response. If None, all aspects are returned.
            count: Number of results per page.
            query: Search query string.
            scroll_id: Pagination cursor from a previous scroll response.
            sort_field: Field to sort by.
            with_system_metadata: If True, return system metadata alongside each aspect.
            skip_cache: If True, bypass the server-side cache.
            skip_aggregation: If True, skip computing facet aggregations.
            include_soft_delete: If True, include soft-deleted entities.
            scroll_id_per_entity: If True, use per-entity scroll IDs.
            slice_id: Slice index for parallel scrolling.
            slice_max: Total number of slices for parallel scrolling.
            pit_keep_alive: Point-in-time keep-alive duration (e.g. "5m").
            filter: Optional filter as a RawSearchFilter (same type returned by
                ``generate_filter()``). Each inner ``and`` list is ANDed; the outer list
                is ORed:
                ``[{"and": [{"field": "platform", "values": ["snowflake"]}]}]``.
            sort_criteria: Optional sort criteria list, each entry matching the SortCriterion
                schema: ``[{"field": "urn", "order": "ASCENDING"}]``.

        Returns:
            A ScrollResult with:
            - scroll_id: cursor to pass in the next call (None when exhausted)
            - entities: mapping of URN → aspect name → (typed aspect, system metadata)
            - total_count: total number of matching entities
        """
        if entity_name is not None:
            url = f"{self._gms_server}/openapi/v3/entity/{entity_name}/scroll"
        else:
            url = f"{self._gms_server}/openapi/v3/entity/scroll"

        def _bool(v: Optional[bool]) -> Optional[str]:
            return None if v is None else str(v).lower()

        params: Dict[str, Any] = {
            k: v
            for k, v in {
                "count": count,
                "query": query,
                "scrollId": scroll_id,
                "sort": sort_field,
                "systemMetadata": _bool(with_system_metadata),
                "skipCache": _bool(skip_cache),
                "skipAggregation": _bool(skip_aggregation),
                "includeSoftDelete": _bool(include_soft_delete),
                "scrollIdPerEntity": _bool(scroll_id_per_entity),
                "sliceId": slice_id,
                "sliceMax": slice_max,
                "pitKeepAlive": pit_keep_alive,
            }.items()
            if v is not None
        }

        body: Dict[str, Any] = {}
        if aspects is not None:
            body["aspects"] = aspects
        if filter is not None:
            body["filter"] = {"or": filter}
        if sort_criteria is not None:
            body["sortCriteria"] = sort_criteria

        response = self._session.post(
            url, params=params, data=json.dumps(body), headers=_JSON_HEADERS
        )
        response.raise_for_status()
        resp_json = response.json()

        return ScrollResult(
            scroll_id=resp_json.get("scrollId"),
            entities=self._deserialize_entities(
                resp_json.get("entities", []),
                with_system_metadata=bool(with_system_metadata),
                context="scroll response",
            ),
            total_count=resp_json.get("totalCount", 0),
        )
