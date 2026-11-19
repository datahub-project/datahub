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
from datahub.metadata.urns import SchemaFieldUrn
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import JsonValue

from . import graphql_helpers
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


@dataclass(frozen=True)
class ReadBackendBinding:
    backend: ReadBackend


class GraphQLReadBackend:
    """Reads from the caller's DataHub."""

    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def exists(self, urn: str) -> bool:
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
        # returns siblings too. GetEntity selects `siblings` only to learn whether
        # there are any, so only datasets with siblings pay for this second query.
        if (
            entity is not None
            and entity.get(SIBLINGS_KEY) is not None
            and sibling_merge_enabled()
        ):
            try:
                siblings = graphql_helpers.execute_graphql(
                    self.graph,
                    query=graphql_helpers.load_gql_document("entity_details.gql"),
                    variables={"urn": urn},
                    operation_name="GetEntitySiblings",
                )["entity"]
            except Exception as e:
                # The entity's own metadata still answers the call.
                logger.warning(
                    f"Sibling read failed for {urn}; returning its own metadata: {e}"
                )
            else:
                entity[SIBLINGS_SEARCH_KEY] = (siblings or {}).get(SIBLINGS_SEARCH_KEY)
        return entity

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
