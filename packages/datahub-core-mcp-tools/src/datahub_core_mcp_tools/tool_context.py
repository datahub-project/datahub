from dataclasses import dataclass
from typing import Any, Iterable, Type, TypeVar

import anyio

T = TypeVar("T")


@dataclass(frozen=True)
class BackgroundExecution:
    """A host-owned worker budget; the host also owns exception logging."""

    limiter: anyio.CapacityLimiter


class ToolContext:
    """Type-indexed bag of context values for MCP tool execution.

    Lookup is by isinstance (first match). This keeps AgentRunner generic --
    only producers and consumers need to know about specific item types.

    Example:
        ctx = ToolContext([NoView()])
        view = ctx.get(ViewPreference, UseDefaultView())
    """

    def __init__(self, items: Iterable[Any] = ()) -> None:
        self._items: tuple[Any, ...] = tuple(items)

    @property
    def is_empty(self) -> bool:
        return not self._items

    def __repr__(self) -> str:
        items = ", ".join(repr(item) for item in self._items)
        return f"ToolContext([{items}])"

    def with_items(self, *items: object) -> "ToolContext":
        return ToolContext((*items, *self._items))

    def get(self, cls: Type[T], default: T | None = None) -> T | None:
        """Return the first item that is an instance of cls, or default."""
        for item in self._items:
            if isinstance(item, cls):
                return item
        return default

    def extended(self, item: Any) -> "ToolContext":
        """A new context with ``item`` appended. Items of different types coexist."""
        return ToolContext([*self._items, item])
