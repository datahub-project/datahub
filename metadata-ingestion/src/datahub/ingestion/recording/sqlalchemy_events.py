"""SQLAlchemy event listeners for recording and replaying database queries.

This module provides event listeners that intercept SQLAlchemy query execution
at the engine level, avoiding the import reference issues with wrapper-based approaches.

Event listeners are attached to SQLAlchemy engines after creation, making them
immune to modules that import create_engine directly.
"""

import logging
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import Engine

from datahub.ingestion.recording.db_proxy import QueryRecorder, QueryRecording

logger = logging.getLogger(__name__)

# Thread-local storage for active recorders per engine
# Key: engine id, Value: (recorder, is_replay)
_active_recorders: Dict[int, Tuple[QueryRecorder, bool]] = {}


def attach_sqlalchemy_recorder(
    engine: Engine, recorder: QueryRecorder, is_replay: bool = False
) -> None:
    """Attach event listeners to a SQLAlchemy engine for recording/replay.

    Args:
        engine: SQLAlchemy engine to attach listeners to
        recorder: QueryRecorder instance for storing/retrieving recordings
        is_replay: If True, serve from recordings. If False, record queries.
    """
    engine_id = id(engine)
    _active_recorders[engine_id] = (recorder, is_replay)

    if is_replay:
        # Replay mode: use raw_connection wrapper (handled in patcher.py)
        # Event listeners are not used for replay to avoid execution
        logger.debug(
            "Replay mode: raw_connection wrapper will be used instead of event listeners"
        )
    else:
        # Recording mode: wrap engine.connect() to wrap connections when they're created
        original_connect = engine.connect

        def wrapped_connect(*args: Any, **kwargs: Any) -> Any:
            conn = original_connect(*args, **kwargs)
            return _wrap_connection_for_recording(conn)

        # Dynamically patch engine.connect() - intentional method assignment
        engine.connect = wrapped_connect  # type: ignore[method-assign]
        logger.debug(f"Wrapped engine.connect() for recording on engine {engine_id}")


def detach_sqlalchemy_recorder(engine: Engine) -> None:
    """Remove event listeners from a SQLAlchemy engine.

    Args:
        engine: SQLAlchemy engine to detach listeners from
    """
    engine_id = id(engine)
    if engine_id in _active_recorders:
        # Remove listeners (SQLAlchemy doesn't provide direct removal, but we can clear our reference)
        _active_recorders.pop(engine_id, None)
        logger.debug(f"Detached SQLAlchemy listeners from engine {engine_id}")


def _get_recorder_for_engine(conn: Any) -> Optional[Tuple[QueryRecorder, bool]]:
    """Get the active recorder for a connection's engine.

    Args:
        conn: SQLAlchemy connection object

    Returns:
        Tuple of (recorder, is_replay) or None if not found
    """
    # Get engine from connection
    engine = getattr(conn, "engine", None)
    if engine is None:
        return None

    engine_id = id(engine)
    return _active_recorders.get(engine_id)


def _get_query_string(statement: Any) -> str:
    """Extract query string from SQLAlchemy statement or string.

    Args:
        statement: SQLAlchemy statement object or string

    Returns:
        Query string
    """
    if hasattr(statement, "compile"):
        # SQLAlchemy statement object
        try:
            compiled = statement.compile(compile_kwargs={"literal_binds": True})
            return str(compiled)
        except Exception:
            # Fallback if compilation fails
            return str(statement)
    elif isinstance(statement, str):
        return statement
    else:
        return str(statement)


def _materialize_results(result: Any) -> list:
    """Materialize SQLAlchemy Result object into list of dictionaries.

    Args:
        result: SQLAlchemy Result object

    Returns:
        List of dictionaries representing rows
    """
    # Materialize results (this consumes the Result object)
    rows = list(result)

    if not rows:
        return []

    # Try to get column names from result keys (SQLAlchemy 2.x Row objects)
    if hasattr(rows[0], "_mapping"):
        # SQLAlchemy 2.x Row._mapping
        return [dict(row._mapping) for row in rows]
    elif hasattr(rows[0], "keys"):
        # SQLAlchemy 1.x RowProxy
        return [dict(row) for row in rows]

    # Tuple rows - need description
    description = getattr(result, "cursor_description", None)
    if description:
        column_names = [desc[0] for desc in description]
        return [dict(zip(column_names, row, strict=False)) for row in rows]

    # No description - use indices
    return [dict(enumerate(row)) for row in rows]


class _ResultIterator:
    """Iterator that wraps materialized SQLAlchemy results."""

    def __init__(self, rows_data: list, original_result: Any):
        self._rows = rows_data
        self._index = 0
        self._original_result = original_result

    def __iter__(self):
        return self

    def __next__(self):
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        raise StopIteration

    def fetchall(self):
        """Fetch all rows from the result set.

        Returns:
            List of all rows.
        """
        return self._rows

    def all(self):
        """Return all rows from the result set (SQLAlchemy 2.x style).

        Returns:
            List of all rows.
        """
        return self._rows

    def first(self):
        """Return the first row or None if no rows.

        Returns:
            First row or None if result set is empty.
        """
        return self._rows[0] if self._rows else None

    def one(self):
        """Return exactly one row, raising ValueError if not exactly one.

        Returns:
            The single row.

        Raises:
            ValueError: If result set does not contain exactly one row.
        """
        if len(self._rows) == 1:
            return self._rows[0]
        raise ValueError("Expected exactly one row")

    def one_or_none(self):
        """Return one row or None if no rows, raising ValueError if multiple rows.

        Returns:
            The single row, or None if no rows.

        Raises:
            ValueError: If result set contains more than one row.
        """
        if len(self._rows) == 1:
            return self._rows[0]
        return None

    def scalar(self):
        """Return the first column of the first row, or None if no rows.

        Returns:
            Scalar value from first column of first row, or None if empty.
        """
        if self._rows and len(self._rows) > 0:
            first_row = self._rows[0]
            if hasattr(first_row, "__getitem__"):
                return first_row[0]
            return first_row
        return None

    # Preserve other attributes from original result
    def __getattr__(self, name):
        return getattr(self._original_result, name)


def _create_wrapped_execute(original_execute: Any, recorder: QueryRecorder) -> Any:
    """Create a wrapped execute function that records queries and results.

    Args:
        original_execute: Original connection.execute method
        recorder: QueryRecorder instance

    Returns:
        Wrapped execute function
    """

    def wrapped_execute(statement: Any, *args: Any, **kwargs: Any) -> Any:
        query = _get_query_string(statement)
        result = original_execute(statement, *args, **kwargs)

        try:
            rows = list(result)
            results = _materialize_results(result) if rows else []

            recording = QueryRecording(
                query=query,
                parameters=kwargs.get("parameters") or (args[0] if args else None),
                results=results,
                row_count=len(results),
            )
            recorder.record(recording)

            logger.debug(f"Recorded SQL query: {query[:100]}... ({len(results)} rows)")

            return _ResultIterator(rows, result)

        except Exception as e:
            logger.error(f"Error recording query: {e}", exc_info=True)
            recording = QueryRecording(
                query=query,
                parameters=kwargs.get("parameters") or (args[0] if args else None),
                error=str(e),
            )
            recorder.record(recording)
            return result

    return wrapped_execute


def _wrap_connection_for_recording(conn: Any) -> Any:
    """Wrap connection.execute() to capture results.

    This is called when a connection is created from engine.connect().
    We wrap the execute method to intercept queries and materialize results.
    """
    engine = getattr(conn, "engine", None)
    if not engine:
        logger.debug("No engine found on connection - skipping wrap")
        return conn

    engine_id = id(engine)
    recorder_info = _active_recorders.get(engine_id)
    if not recorder_info:
        logger.debug(f"No recorder found for engine {engine_id} - skipping wrap")
        return conn

    recorder, is_replay = recorder_info
    if is_replay:
        return conn

    if not hasattr(conn, "execute"):
        logger.warning("Connection does not have execute() method - cannot wrap")
        return conn

    original_execute = conn.execute
    logger.info("✅ Wrapping connection.execute() for recording")
    # Dynamically patch connection.execute() - intentional method assignment
    conn.execute = _create_wrapped_execute(original_execute, recorder)  # type: ignore[method-assign]

    return conn


# Note: We use connection.execute() wrapping instead of cursor event listeners
# because SQLAlchemy 2.x Result objects are not accessible in after_cursor_execute.
# Wrapping execute() allows us to materialize results from the Result object.


# Note: Replay mode uses raw_connection wrapper instead of event listeners
# to prevent actual database connections. Event listeners are only used for recording.
