"""Utility functions for DataHub Agent Context."""

import contextvars
import functools
from typing import TYPE_CHECKING, Callable

from datahub.errors import ItemNotFoundError
from datahub_agent_context.context import reset_client, set_client

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


def create_context_wrapper(func: Callable, client: "DataHubClient") -> Callable:
    """Create a wrapper that sets DataHubClient context before calling the function.

    This wrapper uses contextvars to set the client in context for the duration
    of the function call, allowing the tool to retrieve it using get_datahub_client().

    Any ItemNotFoundError raised by the tool is caught and converted to a
    structured dict response so that agent/LLM callers receive a usable message
    rather than an unhandled exception.

    Args:
        func: The tool function that retrieves client from context
        client: DataHubClient instance to set in context

    Returns:
        Wrapped function that sets context before execution
    """

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        token: contextvars.Token = set_client(client)
        try:
            return func(*args, **kwargs)
        except ItemNotFoundError as e:
            return {"message": str(e)}
        finally:
            reset_client(token)

    return wrapper
