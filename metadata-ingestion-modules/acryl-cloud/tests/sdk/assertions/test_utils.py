"""Shared test utilities for assertion tests."""

from typing import Optional


class MockGraph:
    """Mock graph client that simulates the DataHubGraph.exists() method."""

    def __init__(self, existing_urns: Optional[set[str]] = None):
        self.existing_urns = existing_urns or set()

    def exists(self, urn: str) -> bool:
        """Return True if the URN exists in the configured set."""
        return urn in self.existing_urns
