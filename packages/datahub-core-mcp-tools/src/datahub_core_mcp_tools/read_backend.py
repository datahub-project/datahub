"""Replaceable read operations beneath shared tool validation and formatting.

Tools read entities through ``get_read_backend()`` instead of calling GraphQL
directly. A host can bind its own backend in the request's ToolContext: the
context curator, for example, serves a workspace's edited entities before the
GraphQL ones. Without a binding, reads go to the caller's DataHub.

The protocol grows with the tools: each tool port adds the read it needs, with
its GraphQL implementation here.
"""

from dataclasses import dataclass
from typing import Protocol, cast

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import VersionSetKeyClass
from datahub.metadata.urns import SchemaFieldUrn
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import JsonValue

from . import graphql_helpers
from .openapi_client import (
    RawVersionedBatch,
    VersionedAspectPair,
    VersionedOpenApiClient,
)
from .sibling_merge import SIBLINGS_KEY, SIBLINGS_SEARCH_KEY, sibling_merge_enabled

JsonObject = dict[str, JsonValue]


class ReadBackend(Protocol):
    def exists(self, urn: str) -> bool: ...

    def entity(self, urn: str, *, include_contract: bool = False) -> JsonObject | None:
        """The entity as ``get_entities`` reads it, raw (before cleaning); None when
        GMS returns nothing for it.

        A dataset with siblings carries their documentation-bearing parts under
        ``siblingsSearch`` for sibling_merge. ``include_contract`` also reads a
        dataset's data contract.
        """
        ...

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        """Each document's urn, subType, title and body (``document_content.gql``);
        None for one that is deleted or not viewable. URNs GMS does not know may be
        left out."""
        ...

    def related_documents(self, urn: str, start: int, count: int) -> JsonObject:
        """``getRelatedDocuments`` for the variables of ``related_documents.gql``."""
        ...

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        """Raw aspects keyed by aspect name (e.g. ``schemaMetadata``), each in
        ``to_obj()`` form; None when the entity does not exist.

        Unlike a merged GraphQL projection, this keeps aspect boundaries, so a
        caller can report which aspect a match came from.
        """
        ...

    def search_entities(self, variables: JsonObject) -> JsonObject:
        """``searchAcrossEntities`` for the variables of ``search.gql``."""
        ...

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        """``searchAcrossEntities`` over documents for the variables of
        ``document_search.gql``; with ``semantic``, ``semanticSearchAcrossEntities``
        for those of ``document_semantic_search.gql``."""
        ...

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        """``searchAcrossLineage`` for the ``input`` of ``GetEntityLineage``
        (``entity_details.gql``): one direction, each node with its entity preview
        and, for column lineage, the column paths."""
        ...

    def lineage_paths(self, variables: JsonObject) -> JsonObject:
        """``searchAcrossLineage`` for the ``input`` of ``lineage_paths.gql``: each
        node's urn and type, its paths, and whether GMS cut the traversal short
        (``isPartial``)."""
        ...

    def list_queries(self, variables: JsonObject) -> JsonObject:
        """``listQueries`` for the ``input`` of ``queries.gql``."""
        ...

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        """The dataset's ``usageStats`` over ``time_range`` (a GraphQL TimeRange)
        as ``dataset_usage.gql`` selects them, under ``usageStats``; None when GMS
        returns no dataset."""
        ...

    def aspect_versions(self, pairs: list[VersionedAspectPair]) -> RawVersionedBatch:
        """One stored version of each (urn, aspect) pair as its raw envelope
        (``value``, ``systemMetadata``, ``auditStamp``); version 0 is the current
        value. A failed read is reported per pair in ``errors``, not raised."""
        ...

    def glossary_term_version(self, urn: str) -> JsonObject | None:
        """The term's ``versionProperties`` (``getTermVersionSetUrn`` in
        ``glossary_versioning.gql``); None when GMS returns no entity."""
        ...

    def version_set_history(
        self, urn: str, start: int, count: int
    ) -> JsonObject | None:
        """The version set's ``versionsSearch`` page (``getVersionSetHistory`` in
        ``glossary_versioning.gql``); None when GMS returns no version set."""
        ...

    def glossary_terms(self, urns: list[str]) -> list[JsonObject | None]:
        """Each term's definition, owners, tags, deprecation and version
        (``getGlossaryTermsForCompare`` in ``glossary_versioning.gql``)."""
        ...

    def tags_named(self, name: str, count: int) -> list[JsonObject]:
        """Up to ``count`` tags a search for ``name`` finds, each with its urn and
        ``properties.name`` (``findTagsByName`` in ``tags.gql``). Search matches
        loosely and lags writes: the caller compares names, and a tag created
        moments ago may be missing."""
        ...

    def domains_named(
        self, name: str, parent_domain: str | None, count: int
    ) -> list[JsonObject]:
        """Up to ``count`` domains directly under ``parent_domain`` (the top level
        when None) that a search for ``name`` finds, each with its urn and
        ``properties.name`` (``findDomainsByName`` in ``domains.gql``). Search
        matches loosely: the caller compares names."""
        ...

    def ownership_types_named(self, name: str, count: int) -> list[JsonObject]:
        """Up to ``count`` ownership types a search for ``name`` finds, each with
        its urn and ``info.name`` (``findOwnershipTypesByName`` in
        ``owners.gql``). Search matches loosely: the caller compares names."""
        ...

    def description_sources(self, urn: str) -> JsonObject | None:
        """The entity's ``type`` and every description it has, edited in DataHub
        and from the source system, for it and its columns
        (``getDescriptionSources`` in ``descriptions.gql``); None when GMS
        returns no entity.

        A dataset with siblings also carries their documentation-bearing parts
        under ``siblingsSearch`` for sibling_merge, as ``entity`` does. Unlike
        there, a failed sibling read raises: the caller builds a write on the
        merged text.
        """
        ...

    def structured_property(self, urn: str) -> JsonObject | None:
        """The entity at ``urn`` with its ``type`` and, for a structured
        property, ``exists`` and its ``definition`` (qualified name, cardinality,
        value type;
        ``getStructuredPropertyDefinition`` in ``structured_properties.gql``);
        None when GMS returns no entity."""
        ...


@dataclass(frozen=True)
class ReadBackendBinding:
    backend: ReadBackend


class GraphQLReadBackend:
    """Reads from the caller's DataHub."""

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def exists(self, urn: str) -> bool:
        if urn.startswith("urn:li:versionSet:"):
            # VersionSetUrn.entity_type is the type of the entities the set
            # versions, not "versionSet", so DataHubGraph.exists would look for
            # the wrong key aspect and report every version set missing.
            return self.graph.get_aspect(urn, VersionSetKeyClass) is not None
        return self.graph.exists(urn)

    def entity(self, urn: str, *, include_contract: bool = False) -> JsonObject | None:
        variables: JsonObject = {"urn": urn}
        if urn.startswith("urn:li:query:"):
            # QueryEntity is not part of the Entity union GetEntity selects from.
            document, operation = "query_entity.gql", "GetQueryEntity"
        elif urn.startswith("urn:li:assertion:"):
            document, operation = "assertion_entity.gql", "GetAssertionEntity"
        else:
            document, operation = "entity_details.gql", "GetEntity"
            variables["includeContract"] = include_contract
        entity = cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document(document),
                variables=variables,
                operation_name=operation,
            )["entity"],
        )
        # Read here rather than in the tools, so every backend that wraps this one
        # returns siblings too.
        if entity is not None and self._has_siblings(entity):
            try:
                siblings = self._sibling_search(urn)
            except Exception as e:
                # The entity's own metadata still answers the call.
                logger.warning(
                    f"Sibling read failed for {urn}; returning its own metadata: {e}"
                )
            else:
                entity[SIBLINGS_SEARCH_KEY] = siblings
        return entity

    @staticmethod
    def _has_siblings(entity: JsonObject) -> bool:
        # GetEntity and getDescriptionSources select `siblings` only to learn
        # whether there are any, so only datasets with siblings pay for the
        # sibling query.
        return entity.get(SIBLINGS_KEY) is not None and sibling_merge_enabled()

    def _sibling_search(self, urn: str) -> JsonValue:
        siblings = graphql_helpers.execute_graphql(
            self.graph,
            query=graphql_helpers.load_gql_document("entity_details.gql"),
            variables={"urn": urn},
            operation_name="GetEntitySiblings",
        )["entity"]
        return cast(JsonValue, (siblings or {}).get(SIBLINGS_SEARCH_KEY))

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        entities = cast(
            list[JsonObject | None],
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("document_content.gql"),
                variables={"urns": urns},
                operation_name="documentContent",
            ).get("entities")
            or [],
        )
        # entities(urns:) returns a hard-deleted or unviewable document as a stub
        # with info null: the same as a URN the server did not return.
        return [
            None if entity and "info" in entity and entity["info"] is None else entity
            for entity in entities
        ]

    def related_documents(self, urn: str, start: int, count: int) -> JsonObject:
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("related_documents.gql"),
                variables={"urn": urn, "input": {"start": start, "count": count}},
                operation_name="getRelatedDocuments",
            ),
        )

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        # get_entity_semityped returns at least the key aspect even for an entity
        # that does not exist, so it cannot answer existence on its own.
        if not self.exists(urn):
            return None
        raw = self.graph.get_entity_semityped(urn)
        return cast(
            dict[str, JsonObject],
            {k: (v.to_obj() if hasattr(v, "to_obj") else v) for k, v in raw.items()},
        )

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("search.gql"),
                variables=variables,
                operation_name="search",
            )["searchAcrossEntities"],
        )

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        if semantic:
            document, operation = (
                "document_semantic_search.gql",
                "documentSemanticSearch",
            )
            key = "semanticSearchAcrossEntities"
        else:
            document, operation = "document_search.gql", "documentSearch"
            key = "searchAcrossEntities"
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document(document),
                variables=variables,
                operation_name=operation,
            )[key],
        )

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        # In entity_details.gql: its nodes are read with entityPreview.
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("entity_details.gql"),
                variables={"input": variables},
                operation_name="GetEntityLineage",
            )["searchAcrossLineage"],
        )

    def lineage_paths(self, variables: JsonObject) -> JsonObject:
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("lineage_paths.gql"),
                variables={"input": variables},
                operation_name="GetEntityLineagePaths",
            )["searchAcrossLineage"],
        )

    def list_queries(self, variables: JsonObject) -> JsonObject:
        return cast(
            JsonObject,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("queries.gql"),
                variables={"input": variables},
                operation_name="listQueries",
            )["listQueries"],
        )

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        return cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("dataset_usage.gql"),
                variables={"urn": urn, "timeRange": time_range},
                operation_name="getDatasetUsageHistory",
            )["dataset"],
        )

    def aspect_versions(self, pairs: list[VersionedAspectPair]) -> RawVersionedBatch:
        # GraphQL serves only current values; OpenAPI v3 reads stored versions.
        return VersionedOpenApiClient(self.graph).get_entities(pairs)

    def glossary_term_version(self, urn: str) -> JsonObject | None:
        return cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("glossary_versioning.gql"),
                variables={"urn": urn},
                operation_name="getTermVersionSetUrn",
            )["entity"],
        )

    def version_set_history(
        self, urn: str, start: int, count: int
    ) -> JsonObject | None:
        return cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("glossary_versioning.gql"),
                variables={"urn": urn, "start": start, "count": count},
                operation_name="getVersionSetHistory",
            )["versionSet"],
        )

    def glossary_terms(self, urns: list[str]) -> list[JsonObject | None]:
        return cast(
            list[JsonObject | None],
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("glossary_versioning.gql"),
                variables={"urns": urns},
                operation_name="getGlossaryTermsForCompare",
            ).get("entities")
            or [],
        )

    def tags_named(self, name: str, count: int) -> list[JsonObject]:
        results = cast(
            list[JsonObject],
            (
                graphql_helpers.execute_graphql(
                    self.graph,
                    query=graphql_helpers.load_gql_document("tags.gql"),
                    variables={"query": name, "count": count},
                    operation_name="findTagsByName",
                )["search"]
                or {}
            ).get("searchResults")
            or [],
        )
        return [
            cast(JsonObject, r["entity"])
            for r in results
            if isinstance(r, dict) and isinstance(r.get("entity"), dict)
        ]

    def domains_named(
        self, name: str, parent_domain: str | None, count: int
    ) -> list[JsonObject]:
        result = graphql_helpers.execute_graphql(
            self.graph,
            query=graphql_helpers.load_gql_document("domains.gql"),
            variables={"query": name, "parentDomain": parent_domain, "count": count},
            operation_name="findDomainsByName",
        )
        domains = (result.get("listDomains") or {}).get("domains") or []
        return [cast(JsonObject, d) for d in domains if isinstance(d, dict)]

    def ownership_types_named(self, name: str, count: int) -> list[JsonObject]:
        result = graphql_helpers.execute_graphql(
            self.graph,
            query=graphql_helpers.load_gql_document("owners.gql"),
            variables={"query": name, "count": count},
            operation_name="findOwnershipTypesByName",
        )
        found = (result.get("listOwnershipTypes") or {}).get("ownershipTypes") or []
        return [cast(JsonObject, t) for t in found if isinstance(t, dict)]

    def description_sources(self, urn: str) -> JsonObject | None:
        entity = cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("descriptions.gql"),
                variables={"urn": urn},
                operation_name="getDescriptionSources",
            ).get("entity"),
        )
        if entity is not None and self._has_siblings(entity):
            entity[SIBLINGS_SEARCH_KEY] = self._sibling_search(urn)
        return entity

    def structured_property(self, urn: str) -> JsonObject | None:
        return cast(
            JsonObject | None,
            graphql_helpers.execute_graphql(
                self.graph,
                query=graphql_helpers.load_gql_document("structured_properties.gql"),
                variables={"urn": urn},
                operation_name="getStructuredPropertyDefinition",
            ).get("entity"),
        )


def get_read_backend() -> ReadBackend:
    """The backend bound for this request, or the caller's DataHub."""
    context = graphql_helpers.get_mcp_context()
    binding = context.tool_context.get(ReadBackendBinding)
    if isinstance(binding, ReadBackendBinding):
        return binding.backend
    return GraphQLReadBackend(context.client._graph)


def require_existing(backend: ReadBackend, urn: str) -> None:
    """Raise a ToolError unless ``urn`` exists.

    For tools whose search returns nothing for a URN that does not exist, which
    would read as "no lineage" or "no queries". A schemaField has no key aspect of
    its own, so exists() is false for real columns: its dataset is checked instead.
    """
    existence_urn = urn
    if urn.startswith("urn:li:schemaField:"):
        existence_urn = str(SchemaFieldUrn.from_string(urn).parent)
    if not backend.exists(existence_urn):
        raise ToolError(f"Entity {existence_urn} not found")
