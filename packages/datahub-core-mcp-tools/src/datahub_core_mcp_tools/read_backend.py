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
from pydantic import JsonValue

from . import graphql_helpers

JsonObject = dict[str, JsonValue]


class ReadBackend(Protocol):
    def exists(self, urn: str) -> bool: ...

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        """Raw aspects keyed by aspect name (e.g. ``schemaMetadata``), each in
        ``to_obj()`` form; None when the entity does not exist.

        Unlike a merged GraphQL projection, this keeps aspect boundaries, so a
        caller can report which aspect a match came from.
        """
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


def get_read_backend() -> ReadBackend:
    """The backend bound for this request, or the caller's DataHub."""
    context = graphql_helpers.get_mcp_context()
    binding = context.tool_context.get(ReadBackendBinding)
    if isinstance(binding, ReadBackendBinding):
        return binding.backend
    return GraphQLReadBackend(context.client._graph)
