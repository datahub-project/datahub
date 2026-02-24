from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph


class ViewPreference(ABC):
    """Determines which DataHub view to apply when searching.

    Implementations encapsulate the three possible states:
    - UseDefaultView: use the organization's configured default global view
    - NoView: no view filtering at all
    - CustomView: use a specific view by URN
    """

    @abstractmethod
    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        """Resolve to a concrete view URN (or None for no view)."""
        ...


@dataclass(frozen=True)
class UseDefaultView(ViewPreference):
    """Use the organization's default global view (current behavior)."""

    def get_view(self, graph: DataHubGraph) -> Optional[str]:
        from datahub_integrations.mcp.mcp_server import fetch_global_default_view

        return fetch_global_default_view(graph)


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
