"""Test helper for mocking the Fivetran log reader's SQLAlchemy engine on SA 2.0."""

from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock

QueryHandler = Callable[[str], List[Dict[str, Any]]]


def wire_sa2_engine_mock(engine_mock: MagicMock, query_handler: QueryHandler) -> None:
    """Adapt a mocked SQLAlchemy Engine to the SQLAlchemy 2.0 Fivetran reader.

    ``FivetranLogDbReader._query`` runs ``with engine.connect() as conn:
    conn.execute(text(q))`` and reads rows via ``row._mapping`` (SA 2.0 removed
    ``Engine.execute()``). Route the query string to ``query_handler`` (which
    returns a list of dicts) and return rows that expose ``_mapping`` so the
    reader sees the same data it did under the old ``engine.execute(q)`` path.
    """

    def _execute(clause: Any, *args: Any, **kwargs: Any) -> MagicMock:
        query = clause.text if hasattr(clause, "text") else str(clause)
        rows = query_handler(query) or []
        wrapped = [MagicMock(_mapping=row) for row in rows]
        result = MagicMock()
        result.__iter__.return_value = iter(wrapped)
        result.fetchone.return_value = wrapped[0] if wrapped else None
        result.fetchall.return_value = wrapped
        return result

    conn = engine_mock.connect.return_value.__enter__.return_value
    conn.execute.side_effect = _execute
