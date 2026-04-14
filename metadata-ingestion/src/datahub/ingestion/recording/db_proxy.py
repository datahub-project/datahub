"""Generic database connection and cursor proxies for recording/replay.

This module provides proxy classes that wrap database connections and cursors
to intercept and record all database operations. During replay, the proxies
serve recorded results without making real database connections.

The design is intentionally generic to work with ANY database connector
(Snowflake, Redshift, BigQuery, SQLAlchemy, etc.) without requiring
connector-specific implementations.
"""

import functools
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Regex patterns for normalizing dynamic values in SQL queries
# These patterns match timestamp literals and functions that generate dynamic values
_TIMESTAMP_PATTERNS = [
    # Snowflake: to_timestamp_ltz(1764720000000, 3) -> to_timestamp_ltz(?, ?)
    (
        r"to_timestamp(?:_ltz|_ntz|_tz)?\s*\(\s*[\d]+\s*(?:,\s*[\d]+\s*)?\)",
        "to_timestamp(?)",
    ),
    # Snowflake: DATEADD(day, -30, CURRENT_TIMESTAMP()) or similar
    (r"DATEADD\s*\(\s*\w+\s*,\s*-?\d+\s*,\s*[^)]+\)", "DATEADD(?)"),
    # Generic timestamp literals: '2024-12-03 10:30:00' or '2024-12-03T10:30:00Z'
    (
        r"'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?'",
        "'?TIMESTAMP?'",
    ),
    # Date literals: '2024-12-03'
    (r"'\d{4}-\d{2}-\d{2}'", "'?DATE?'"),
    # Unix timestamps (13-digit milliseconds or 10-digit seconds)
    (r"\b\d{13}\b", "?EPOCH_MS?"),
    (r"\b\d{10}\b(?!\d)", "?EPOCH_S?"),
    # CURRENT_TIMESTAMP(), NOW(), GETDATE() with offsets
    (
        r"(?:CURRENT_TIMESTAMP|NOW|GETDATE|SYSDATE)\s*\(\s*\)\s*(?:-\s*INTERVAL\s+'[^']+'\s*)?",
        "?CURRENT_TIME?",
    ),
]

# Compile patterns for efficiency
_COMPILED_TIMESTAMP_PATTERNS = [
    (re.compile(p, re.IGNORECASE), r) for p, r in _TIMESTAMP_PATTERNS
]


def normalize_query_for_matching(query: str) -> str:
    """Normalize a SQL query by replacing dynamic values with placeholders.

    This enables matching queries that differ only in timestamps, date ranges,
    or other dynamic values that change between recording and replay.

    Args:
        query: The SQL query string to normalize.

    Returns:
        Normalized query string with dynamic values replaced by placeholders.
    """
    normalized = query

    # Apply all timestamp/dynamic value patterns
    for pattern, replacement in _COMPILED_TIMESTAMP_PATTERNS:
        normalized = pattern.sub(replacement, normalized)

    # Normalize whitespace (collapse multiple spaces, trim)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized


def compute_query_similarity(query1: str, query2: str) -> float:
    """Compute similarity ratio between two queries.

    Uses normalized forms for comparison to handle dynamic value differences.

    Args:
        query1: First query string.
        query2: Second query string.

    Returns:
        Similarity ratio between 0.0 and 1.0.
    """
    norm1 = normalize_query_for_matching(query1)
    norm2 = normalize_query_for_matching(query2)
    return SequenceMatcher(None, norm1, norm2).ratio()


def _serialize_value(value: Any) -> Any:
    """Serialize a value to be JSON-safe.

    Handles common database types that aren't JSON serializable:
    - datetime/date objects -> ISO format strings with type marker
    - Decimal -> float
    - bytes -> base64 string
    - Other objects -> string representation
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        # Store as dict with type marker so we can deserialize correctly
        return {"__type__": "datetime", "__value__": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "__value__": value.isoformat()}
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        import base64

        return base64.b64encode(value).decode("utf-8")
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    # For other types, convert to string
    return str(value)


def _deserialize_value(value: Any) -> Any:
    """Deserialize a value from JSON storage.

    Reverses the serialization done by _serialize_value, converting
    type-marked dictionaries back to their original types.

    Also handles ISO datetime strings from database results, converting
    them back to datetime objects for compatibility with source code.
    """
    if value is None:
        return value
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        # Try to parse as datetime if it looks like an ISO timestamp
        # This handles Snowflake DictCursor results which return datetimes as ISO strings
        if len(value) > 18 and ("T" in value or " " in value) and (":" in value):
            try:
                # Try parsing as datetime with timezone
                if "+" in value or value.endswith("Z") or "-" in value[-6:]:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                # Try parsing as datetime without timezone
                return datetime.fromisoformat(value)
            except (ValueError, AttributeError):
                # Not a datetime string, return as-is
                pass
        return value
    if isinstance(value, dict):
        # Check for type markers (serialized datetime, date, etc.)
        if "__type__" in value and "__value__" in value and len(value) == 2:
            # This is a type-marked value, convert it
            type_name = value["__type__"]
            val_str = value["__value__"]
            if type_name == "datetime":
                return datetime.fromisoformat(val_str)
            if type_name == "date":
                return date.fromisoformat(val_str)
            # Unknown type marker, return as-is
            return value
        # Regular dict, deserialize values recursively
        return {k: _deserialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize_value(item) for item in value]
    return value


@dataclass
class QueryRecording:
    """Represents a recorded database query and its results."""

    query: str
    parameters: Optional[Union[Dict[str, Any], Tuple[Any, ...]]] = None
    results: List[Dict[str, Any]] = field(default_factory=list)
    row_count: Optional[int] = None
    description: Optional[List[Tuple[Any, ...]]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Serializes all values to be JSON-safe, handling datetime objects,
        Decimals, and other database types.
        """
        return {
            "query": self.query,
            "parameters": self._serialize_params(self.parameters),
            "results": [_serialize_value(row) for row in self.results],
            "row_count": self.row_count,
            "description": _serialize_value(self.description),
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryRecording":
        """Create from dictionary.

        Deserializes values back to their original types (datetime, etc.)
        """
        raw_results = data.get("results", [])
        deserialized_results = [_deserialize_value(row) for row in raw_results]

        # Debug logging for first result if available
        if raw_results and deserialized_results:
            logger.debug(
                f"Deserialized {len(deserialized_results)} rows. "
                f"First row sample: {str(deserialized_results[0])[:100]}"
            )

        return cls(
            query=data["query"],
            parameters=data.get("parameters"),
            results=deserialized_results,
            row_count=data.get("row_count"),
            description=_deserialize_value(data.get("description")),
            error=data.get("error"),
        )

    @staticmethod
    def _serialize_params(
        params: Optional[Union[Dict[str, Any], Tuple[Any, ...]]],
    ) -> Optional[Union[Dict[str, Any], List[Any]]]:
        """Serialize parameters for JSON storage.

        Handles datetime objects and other non-JSON-serializable types
        by converting them using _serialize_value.
        """
        if params is None:
            return None
        if isinstance(params, tuple):
            return [_serialize_value(item) for item in params]
        if isinstance(params, dict):
            return {k: _serialize_value(v) for k, v in params.items()}
        if isinstance(params, list):
            return [_serialize_value(item) for item in params]
        # For single values, serialize directly
        return _serialize_value(params)

    def get_key(self) -> str:
        """Generate a unique key for this query based on query text and parameters.

        Uses exact query text for precise matching.
        """
        key_parts = [self.query]
        if self.parameters:
            key_parts.append(
                json.dumps(self._serialize_params(self.parameters), sort_keys=True)
            )
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def get_normalized_key(self) -> str:
        """Generate a key based on normalized query text.

        This key is used for fuzzy matching when exact match fails.
        Dynamic values like timestamps are replaced with placeholders.
        """
        normalized = normalize_query_for_matching(self.query)
        key_parts = [normalized]
        if self.parameters:
            key_parts.append(
                json.dumps(self._serialize_params(self.parameters), sort_keys=True)
            )
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]


class QueryRecorder:
    """Records database queries and their results.

    This class stores query recordings and provides lookup during replay.
    It supports JSONL format for streaming writes.

    Query matching strategy (in order):
    1. Exact match - hash of exact query text
    2. Normalized match - hash of query with timestamps/dynamic values normalized
    3. Fuzzy match - similarity-based matching for queries above threshold
    """

    # Minimum similarity ratio for fuzzy matching (0.0 to 1.0)
    FUZZY_MATCH_THRESHOLD = 0.85

    def __init__(self, recording_path: Path) -> None:
        """Initialize query recorder.

        Args:
            recording_path: Path to the JSONL file for storing recordings.
        """
        self.recording_path = recording_path
        # Primary index: exact key -> recording
        self._recordings: Dict[str, QueryRecording] = {}
        # Secondary index: normalized key -> list of recordings
        self._normalized_recordings: Dict[str, List[QueryRecording]] = {}
        # All recordings for fuzzy matching
        self._all_recordings: List[QueryRecording] = []
        self._file_handle: Optional[Any] = None

    def start_recording(self) -> None:
        """Start recording queries to file."""
        self.recording_path.parent.mkdir(parents=True, exist_ok=True)
        self._file_handle = open(self.recording_path, "w")
        logger.info(f"Started DB query recording to {self.recording_path}")

    def stop_recording(self) -> None:
        """Stop recording and close file."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
        logger.info(
            f"DB query recording complete. "
            f"Recorded {len(self._recordings)} unique query(ies)"
        )

    def __enter__(self) -> "QueryRecorder":
        """Context manager entry - start recording."""
        self.start_recording()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - stop recording."""
        self.stop_recording()

    def __del__(self) -> None:
        """Ensure file handle is closed on garbage collection."""
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.close()

    def record(self, recording: QueryRecording) -> None:
        """Record a query and its results."""
        key = recording.get_key()
        self._recordings[key] = recording

        # Write to file immediately for streaming
        if self._file_handle:
            self._file_handle.write(json.dumps(recording.to_dict()) + "\n")
            self._file_handle.flush()

    def load_recordings(self) -> None:
        """Load recordings from file for replay.

        Builds both exact and normalized indexes for efficient lookup.
        """
        if not self.recording_path.exists():
            raise FileNotFoundError(
                f"DB recordings not found: {self.recording_path}. "
                "Cannot replay without recorded queries."
            )

        self._recordings.clear()
        self._normalized_recordings.clear()
        self._all_recordings.clear()

        with open(self.recording_path, "r") as f:
            for line in f:
                if line.strip():
                    recording = QueryRecording.from_dict(json.loads(line))

                    # Store in exact key index
                    exact_key = recording.get_key()
                    self._recordings[exact_key] = recording

                    # Store in normalized key index
                    normalized_key = recording.get_normalized_key()
                    if normalized_key not in self._normalized_recordings:
                        self._normalized_recordings[normalized_key] = []
                    self._normalized_recordings[normalized_key].append(recording)

                    # Store for fuzzy matching
                    self._all_recordings.append(recording)

        logger.info(
            f"Loaded {len(self._recordings)} DB query recording(s) "
            f"({len(self._normalized_recordings)} normalized patterns)"
        )

    def get_recording(
        self, query: str, parameters: Optional[Any] = None
    ) -> Optional[QueryRecording]:
        """Look up a recorded query result using multi-level matching.

        Matching strategy (in order of preference):
        1. Exact match - fastest, most reliable
        2. Normalized match - handles dynamic timestamps
        3. Fuzzy match - handles minor query variations

        Args:
            query: The SQL query to look up.
            parameters: Optional query parameters.

        Returns:
            The matching QueryRecording, or None if not found.
        """
        temp_recording = QueryRecording(query=query, parameters=parameters)

        # Strategy 1: Exact match
        exact_key = temp_recording.get_key()
        if exact_key in self._recordings:
            logger.debug(f"Exact match found for query: {query[:80]}...")
            return self._recordings[exact_key]

        # Strategy 2: Normalized match
        normalized_key = temp_recording.get_normalized_key()
        if normalized_key in self._normalized_recordings:
            matches = self._normalized_recordings[normalized_key]
            if matches:
                logger.debug(
                    f"Normalized match found for query: {query[:80]}... "
                    f"({len(matches)} candidate(s))"
                )
                # Return first match (could be enhanced to pick best match)
                return matches[0]

        # Strategy 3: Fuzzy match (more expensive, use as fallback)
        best_match = self._fuzzy_match(query, parameters)
        if best_match:
            return best_match

        return None

    def _fuzzy_match(
        self, query: str, parameters: Optional[Any] = None
    ) -> Optional[QueryRecording]:
        """Find a recording using fuzzy string matching.

        This is a fallback for when exact and normalized matching fail.
        Uses normalized query forms for comparison.

        Args:
            query: The SQL query to match.
            parameters: Optional query parameters.

        Returns:
            Best matching recording above threshold, or None.
        """
        if not self._all_recordings:
            return None

        best_match: Optional[QueryRecording] = None
        best_similarity = 0.0

        # Normalize the query we're looking for
        normalized_query = normalize_query_for_matching(query)

        for recording in self._all_recordings:
            # Skip if parameters don't match (when both have parameters)
            if parameters is not None and recording.parameters is not None:
                if parameters != recording.parameters:
                    continue

            # Compute similarity using normalized forms
            normalized_recorded = normalize_query_for_matching(recording.query)
            similarity = SequenceMatcher(
                None, normalized_query, normalized_recorded
            ).ratio()

            if similarity > best_similarity:
                best_similarity = similarity
                best_match = recording

        if best_similarity >= self.FUZZY_MATCH_THRESHOLD:
            logger.info(
                f"Fuzzy match found (similarity: {best_similarity:.2%}) "
                f"for query: {query[:80]}..."
            )
            return best_match

        if best_match and best_similarity > 0.5:
            logger.debug(
                f"Best fuzzy match below threshold (similarity: {best_similarity:.2%}) "
                f"for query: {query[:80]}..."
            )

        return None


class CursorProxy:
    """Generic proxy that wraps ANY cursor-like object.

    This proxy intercepts execute/fetch methods to record queries and results.
    It works with any database cursor that follows the DB-API 2.0 interface.

    During RECORDING mode:
    - Forwards all calls to the real cursor
    - Records queries and their results

    During REPLAY mode:
    - Returns recorded results without calling the real cursor
    - Raises error if query not found in recordings
    """

    __slots__ = (
        "_cursor",
        "_recorder",
        "_is_replay",
        "_current_recording",
        "_result_iter",
    )

    def __init__(
        self,
        cursor: Any,
        recorder: QueryRecorder,
        is_replay: bool = False,
    ) -> None:
        """Initialize cursor proxy.

        Args:
            cursor: The real cursor to wrap (can be None in replay mode).
            recorder: QueryRecorder instance for storing/retrieving recordings.
            is_replay: If True, serve from recordings. If False, record.
        """
        object.__setattr__(self, "_cursor", cursor)
        object.__setattr__(self, "_recorder", recorder)
        object.__setattr__(self, "_is_replay", is_replay)
        object.__setattr__(self, "_current_recording", None)
        object.__setattr__(self, "_result_iter", None)

    def __getattr__(self, name: str) -> Any:
        """Forward attribute access to the real cursor."""
        # Intercept specific methods
        if name in ("execute", "executemany"):
            return self._wrap_execute(name)
        if name in ("fetchone", "fetchall", "fetchmany"):
            return self._wrap_fetch(name)

        # For replay mode without a real cursor, handle common attributes
        if self._is_replay and self._cursor is None:
            if name == "description":
                if self._current_recording:
                    return self._current_recording.description
                return None
            if name == "rowcount":
                if self._current_recording:
                    return self._current_recording.row_count
                return -1
            raise AttributeError(
                f"Cursor attribute '{name}' not available in replay mode"
            )

        return getattr(self._cursor, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Forward attribute setting to the real cursor."""
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        elif self._cursor is not None:
            setattr(self._cursor, name, value)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over results.

        In recording mode, we've already fetched all results to record them,
        so we return an iterator over the recorded results.
        In replay mode, we return the recorded results.
        """
        if self._is_replay:
            if self._current_recording:
                return iter(self._current_recording.results)
            return iter([])

        # In recording mode, return the recorded results
        # (the real cursor was already consumed by fetchall() in _record_execute)
        if self._current_recording:
            return iter(self._current_recording.results)

        # Fallback to real cursor if no recording available
        return iter(self._cursor)

    def __enter__(self) -> "CursorProxy":
        """Context manager entry."""
        if self._cursor is not None and hasattr(self._cursor, "__enter__"):
            self._cursor.__enter__()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        if self._cursor is not None and hasattr(self._cursor, "__exit__"):
            self._cursor.__exit__(*args)

    def _wrap_execute(self, method_name: str) -> Callable[..., Any]:
        """Wrap execute/executemany methods."""

        @functools.wraps(
            getattr(self._cursor, method_name) if self._cursor else lambda *a, **k: None
        )
        def wrapper(
            query: str, parameters: Any = None, *args: Any, **kwargs: Any
        ) -> Any:
            query_preview = query[:100] + "..." if len(str(query)) > 100 else str(query)
            logger.info(
                f"📊 CursorProxy.{method_name}() called (replay={self._is_replay}): "
                f"{query_preview}"
            )

            if self._is_replay:
                return self._replay_execute(query, parameters)

            # Recording mode
            if self._cursor is None:
                logger.error(
                    f"❌ CursorProxy.{method_name}() called but _cursor is None!"
                )
                raise RuntimeError("Cannot execute query: cursor is None")

            try:
                logger.debug("▶️ Executing query on real cursor...")
                result = self._record_execute(
                    method_name, query, parameters, *args, **kwargs
                )
                logger.debug("✅ Query executed and recorded successfully")
                return result
            except Exception as e:
                logger.error(
                    f"❌ Query execution failed: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise

        return wrapper

    def _record_execute(
        self,
        method_name: str,
        query: str,
        parameters: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute query on real cursor and record results.

        IMPORTANT: We fetch all results to record them, but then need to make
        them available again for the caller to iterate over. We store the results
        and make the cursor proxy iterable.
        """
        method = getattr(self._cursor, method_name)

        try:
            # Execute the query on the real cursor
            if parameters is not None:
                method(query, parameters, *args, **kwargs)
            else:
                method(query, *args, **kwargs)

            # Fetch all results for recording
            results = []
            description = None
            row_count = None

            if hasattr(self._cursor, "description"):
                description = self._cursor.description

            if hasattr(self._cursor, "rowcount"):
                row_count = self._cursor.rowcount

            # Try to fetch results if available
            try:
                if hasattr(self._cursor, "fetchall"):
                    fetched = self._cursor.fetchall()
                    if fetched:
                        # Check if results are already dicts (e.g., from DictCursor)
                        if fetched and isinstance(fetched[0], dict):
                            # Already in dict format (Snowflake DictCursor, etc.)
                            results = fetched
                        elif description:
                            # Convert tuples to dicts using column names
                            col_names = [d[0] for d in description]
                            results = [
                                dict(zip(col_names, row, strict=False))
                                for row in fetched
                            ]
                        else:
                            # No description, wrap in generic format
                            results = [{"row": list(row)} for row in fetched]
            except (AttributeError, TypeError, RuntimeError) as e:
                # Some cursors don't support fetchall after certain operations
                logger.debug(f"Could not fetch results: {e}")

            recording = QueryRecording(
                query=query,
                parameters=parameters,
                results=results,
                row_count=row_count,
                description=description,
            )
            self._recorder.record(recording)
            self._current_recording = recording
            logger.info(
                f"💾 Query recorded: {len(results)} rows, "
                f"description={'present' if description else 'none'}, "
                f"row_count={row_count}"
            )

            # Reset the result iterator so the caller can iterate over results
            # Since we consumed the cursor with fetchall(), we need to provide
            # the results again
            self._result_iter = iter(results)

            # Return self (the proxy) instead of the real cursor
            # This allows iteration to work via our __iter__ method
            return self

        except Exception as e:
            # Record the error too
            recording = QueryRecording(
                query=query,
                parameters=parameters,
                error=str(e),
            )
            self._recorder.record(recording)
            raise

    def _replay_execute(self, query: str, parameters: Any) -> Any:
        """Replay a recorded query execution."""
        recording = self._recorder.get_recording(query, parameters)

        if recording is None:
            raise RuntimeError(
                f"Query not found in recordings (air-gapped replay failed):\n"
                f"Query: {query[:200]}...\n"
                f"Parameters: {parameters}"
            )

        if recording.error:
            raise RuntimeError(f"Recorded query error: {recording.error}")

        self._current_recording = recording
        self._result_iter = iter(recording.results)

        logger.debug(f"Replayed query: {query[:100]}...")
        return self

    def _wrap_fetch(self, method_name: str) -> Callable[..., Any]:
        """Wrap fetch methods."""

        @functools.wraps(
            getattr(self._cursor, method_name) if self._cursor else lambda *a, **k: None
        )
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if self._is_replay:
                return self._replay_fetch(method_name, *args, **kwargs)

            # In recording mode, forward to real cursor
            return getattr(self._cursor, method_name)(*args, **kwargs)

        return wrapper

    def _replay_fetch(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Replay fetch operations from recordings."""
        if not self._current_recording:
            return None if method_name == "fetchone" else []

        results = self._current_recording.results

        if method_name == "fetchone":
            try:
                if self._result_iter is None:
                    self._result_iter = iter(results)
                return next(self._result_iter)
            except StopIteration:
                return None

        if method_name == "fetchall":
            return results

        if method_name == "fetchmany":
            size = args[0] if args else kwargs.get("size", 1)
            if self._result_iter is None:
                self._result_iter = iter(results)
            batch = []
            for _ in range(size):
                try:
                    batch.append(next(self._result_iter))
                except StopIteration:
                    break
            return batch

        return results


class ConnectionProxy:
    """Generic proxy that wraps ANY connection-like object.

    This proxy intercepts cursor() method to return CursorProxy instances.
    It works with any database connection that provides a cursor() method.

    During REPLAY mode:
    - The real connection is None (no actual database connection)
    - cursor() returns a replay-only CursorProxy
    """

    __slots__ = ("_connection", "_recorder", "_is_replay")

    def __init__(
        self,
        connection: Any,
        recorder: QueryRecorder,
        is_replay: bool = False,
    ) -> None:
        """Initialize connection proxy.

        Args:
            connection: The real connection to wrap (None in replay mode).
            recorder: QueryRecorder instance for storing/retrieving recordings.
            is_replay: If True, don't make real connections.
        """
        object.__setattr__(self, "_connection", connection)
        object.__setattr__(self, "_recorder", recorder)
        object.__setattr__(self, "_is_replay", is_replay)

        logger.info(
            f"🔗 ConnectionProxy created (replay={is_replay}, "
            f"connection_type={type(connection).__name__ if connection else 'None'})"
        )

    def __getattr__(self, name: str) -> Any:
        """Forward attribute access to the real connection."""
        if name == "cursor":
            return self._wrapped_cursor

        # In replay mode without real connection, handle common attributes
        if self._is_replay and self._connection is None:
            if name in ("closed", "is_closed"):
                return False
            if name == "autocommit":
                return True
            raise AttributeError(
                f"Connection attribute '{name}' not available in replay mode"
            )

        return getattr(self._connection, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Forward attribute setting to the real connection."""
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        elif self._connection is not None:
            setattr(self._connection, name, value)

    def __enter__(self) -> "ConnectionProxy":
        """Context manager entry."""
        if self._connection is not None and hasattr(self._connection, "__enter__"):
            self._connection.__enter__()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        if self._connection is not None and hasattr(self._connection, "__exit__"):
            self._connection.__exit__(*args)

    def _wrapped_cursor(self, *args: Any, **kwargs: Any) -> CursorProxy:
        """Return a CursorProxy wrapping the real cursor."""
        logger.debug(
            f"📝 ConnectionProxy.cursor() called (replay={self._is_replay}, "
            f"args={len(args)}, kwargs={list(kwargs.keys())})"
        )
        if self._is_replay:
            # In replay mode, return cursor proxy without real cursor
            logger.debug("🔄 Returning CursorProxy for replay mode")
            return CursorProxy(
                cursor=None,
                recorder=self._recorder,
                is_replay=True,
            )

        # In recording mode, wrap the real cursor
        if self._connection is None:
            logger.error("❌ ConnectionProxy.cursor() called but _connection is None!")
            raise RuntimeError("Cannot create cursor: connection is None")

        try:
            real_cursor = self._connection.cursor(*args, **kwargs)
            logger.debug(
                f"✅ Got real cursor: {type(real_cursor).__name__}, "
                f"wrapping with CursorProxy"
            )
            wrapped = CursorProxy(
                cursor=real_cursor,
                recorder=self._recorder,
                is_replay=False,
            )
            logger.debug("✅ CursorProxy created and ready for query recording")
            return wrapped
        except Exception as e:
            logger.error(
                f"❌ Failed to create cursor: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise

    def close(self) -> None:
        """Close the connection."""
        if self._connection is not None and hasattr(self._connection, "close"):
            self._connection.close()

    def commit(self) -> None:
        """Commit transaction."""
        if self._connection is not None and hasattr(self._connection, "commit"):
            self._connection.commit()

    def rollback(self) -> None:
        """Rollback transaction."""
        if self._connection is not None and hasattr(self._connection, "rollback"):
            self._connection.rollback()


class ReplayConnection:
    """A mock connection for replay mode that doesn't connect to anything.

    This is used during replay when we don't want to make any real
    database connections. All operations are served from recordings.
    """

    def __init__(self, recorder: QueryRecorder) -> None:
        """Initialize replay connection.

        Args:
            recorder: QueryRecorder with loaded recordings.
        """
        self._recorder = recorder

    def cursor(self, *args: Any, **kwargs: Any) -> CursorProxy:
        """Return a replay-only cursor."""
        return CursorProxy(
            cursor=None,
            recorder=self._recorder,
            is_replay=True,
        )

    def close(self) -> None:
        """No-op close."""
        pass

    def commit(self) -> None:
        """No-op commit."""
        pass

    def rollback(self) -> None:
        """No-op rollback."""
        pass

    def __enter__(self) -> "ReplayConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        pass
