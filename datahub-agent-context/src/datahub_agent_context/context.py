"""Context management for DataHub tools.

This module provides a context manager pattern for managing DataHubClient instances
across tool calls without explicit parameter passing.
"""

import contextvars
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.sdk.main_client import DataHubClient

# Context variable to store the current DataHubClient instance
_client_context: contextvars.ContextVar[Optional["DataHubClient"]] = (
    contextvars.ContextVar("datahub_client", default=None)
)


def get_datahub_client() -> "DataHubClient":
    """Get the current DataHubClient from context.

    Returns:
        DataHubClient instance from context

    Raises:
        RuntimeError: If no client is set in context
    """
    client = _client_context.get()
    if client is None:
        raise RuntimeError(
            "No DataHubClient in context. "
            "Make sure to use DataHubContext context manager or set_client() before calling tools."
        )
    return client


def get_graph() -> "DataHubGraph":
    """Get the current DataHubGraph from context (convenience method).

    Returns:
        DataHubGraph instance from the client in context

    Raises:
        RuntimeError: If no client is set in context
    """
    return get_datahub_client()._graph


def set_client(client: "DataHubClient") -> contextvars.Token:
    """Set the DataHubClient in context.

    Args:
        client: DataHubClient instance to set

    Returns:
        Token that can be used to reset the context
    """
    return _client_context.set(client)


def reset_client(token: contextvars.Token) -> None:
    """Reset the DataHubClient context to its previous value.

    Args:
        token: Token returned by set_client()
    """
    _client_context.reset(token)


class DataHubContext:
    """Context manager for DataHub tool execution.

    This context manager sets the DataHubClient in context for the duration
    of the with block, allowing tools to access it without explicit parameter passing.

    Example:
        from datahub.sdk.main_client import DataHubClient
        from datahub_agent_context.context import DataHubContext
        from datahub_agent_context.mcp_tools import search

        client = DataHubClient(...)

        with DataHubContext(client):
            results = search(query="users")  # No client parameter needed!
    """

    def __init__(self, client: "DataHubClient"):
        """Initialize the context manager.

        Args:
            client: DataHubClient instance to use in this context
        """
        self.client = client
        self._token: Optional[contextvars.Token] = None

    def __enter__(self) -> "DataHubClient":
        """Enter the context and set the client.

        Returns:
            The DataHubClient instance
        """
        self._token = set_client(self.client)
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context and reset the client."""
        if self._token is not None:
            reset_client(self._token)
            self._token = None
