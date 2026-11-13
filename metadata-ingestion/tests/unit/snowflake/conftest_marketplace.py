"""Shared test fixtures for Snowflake marketplace tests."""

from typing import Any, Dict, List, Optional


class FakeCursor:
    """Mock cursor for test database queries."""

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows
        self.rowcount = len(rows)

    def execute(self, _query: str) -> "FakeCursor":
        return self

    def __iter__(self):
        return iter(self._rows)


class FakeNativeConn:
    """Mock Snowflake connection for testing."""

    def __init__(self) -> None:
        self._last_query: Optional[str] = None
        self._mock_responses: Dict[str, List[Dict[str, Any]]] = {}

    def set_mock_response(self, query_pattern: str, rows: List[Dict[str, Any]]) -> None:
        """Set mock response for a query pattern."""
        self._mock_responses[query_pattern] = rows

    def cursor(self, _cursor_type):  # type: ignore[no-untyped-def]
        return self

    def execute(self, query: str) -> FakeCursor:
        self._last_query = query

        # Check mock responses
        for pattern, rows in self._mock_responses.items():
            if pattern in query:
                return FakeCursor(rows)

        # Default empty
        return FakeCursor([])

    def is_closed(self) -> bool:
        return False

    def close(self) -> None:
        return None
