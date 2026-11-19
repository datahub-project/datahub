from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph

from . import graphql_helpers
from .view_helpers import fetch_global_default_view, fetch_user_default_view


class ViewPreference(ABC):
    """Determines which DataHub view to apply when searching.

    Implementations encapsulate the three possible states:
    - UseDefaultView: resolve the user's personal default view, falling back
      to the organization's global default view
    - NoView: no view filtering at all
    - CustomView: use a specific view by URN
    """

    @abstractmethod
    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        """Resolve to a concrete view URN (or None for no view)."""
        ...


@dataclass(frozen=True)
class UseDefaultView(ViewPreference):
    """Resolve the default view: user personal default first, then org global default."""

    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        return fetch_user_default_view(graph) or fetch_global_default_view(graph)


@dataclass(frozen=True)
class NoView(ViewPreference):
    """No view filtering at all."""

    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        return None


@dataclass(frozen=True)
class CustomView(ViewPreference):
    """Use a specific view by URN."""

    urn: str

    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        return self.urn


def resolve_view_urn(graph: DataHubGraph) -> Optional[str]:
    """The view the current tool call searches within: the caller's
    ViewPreference from the tool context, else the default view."""
    view = graphql_helpers.get_mcp_context().tool_context.get(
        ViewPreference,  # type: ignore[type-abstract]
        UseDefaultView(),
    )
    assert view is not None  # default guarantees non-None
    return view.get_view(graph)
