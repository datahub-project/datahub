"""Context management for DataHub tools.

This module provides a context manager pattern for managing DataHubGraph instances
across tool calls without explicit parameter passing.
"""

import contextvars
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

# Context variable to store the current DataHubGraph instance
_graph_context: contextvars.ContextVar[Optional["DataHubGraph"]] = (
    contextvars.ContextVar("datahub_graph", default=None)
)


def get_graph() -> "DataHubGraph":
    """Get the current DataHubGraph from context.

    Returns:
        DataHubGraph instance from context

    Raises:
        RuntimeError: If no graph is set in context
    """
    graph = _graph_context.get()
    if graph is None:
        raise RuntimeError(
            "No DataHubGraph in context. "
            "Make sure to use DataHubContext context manager or set_graph() before calling tools."
        )
    return graph


def set_graph(graph: "DataHubGraph") -> contextvars.Token:
    """Set the DataHubGraph in context.

    Args:
        graph: DataHubGraph instance to set

    Returns:
        Token that can be used to reset the context
    """
    return _graph_context.set(graph)


def reset_graph(token: contextvars.Token) -> None:
    """Reset the DataHubGraph context to its previous value.

    Args:
        token: Token returned by set_graph()
    """
    _graph_context.reset(token)


class DataHubContext:
    """Context manager for DataHub tool execution.

    This context manager sets the DataHubGraph in context for the duration
    of the with block, allowing tools to access it without explicit parameter passing.

    Example:
        from datahub.sdk.main_client import DataHubClient
        from datahub_agent_context.context import DataHubContext
        from datahub_agent_context.mcp_tools import search

        client = DataHubClient(...)

        with DataHubContext(client.graph):
            results = search(query="users")  # No graph parameter needed!
    """

    def __init__(self, graph: "DataHubGraph"):
        """Initialize the context manager.

        Args:
            graph: DataHubGraph instance to use in this context
        """
        self.graph = graph
        self._token: Optional[contextvars.Token] = None

    def __enter__(self) -> "DataHubGraph":
        """Enter the context and set the graph.

        Returns:
            The DataHubGraph instance
        """
        self._token = set_graph(self.graph)
        return self.graph

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context and reset the graph."""
        if self._token is not None:
            reset_graph(self._token)
            self._token = None
