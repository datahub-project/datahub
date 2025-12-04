"""Dynamic module patching for database connector interception.

This module provides a context manager that patches known database connector
modules to intercept connection creation. During recording, it wraps real
connections with recording proxies. During replay, it returns mock connections
that serve recorded data.

The patching is completely transparent - no modifications to source code needed.
All patches are restored on exit, even if an exception occurs.
"""

import importlib
import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Tuple

from datahub.ingestion.recording.db_proxy import (
    ConnectionProxy,
    QueryRecorder,
    ReplayConnection,
)

logger = logging.getLogger(__name__)


# Known database connector modules and their connect functions
# Format: module_path -> list of (function_name, wrapper_type)
PATCHABLE_CONNECTORS: Dict[str, List[Tuple[str, str]]] = {
    # Snowflake native connector
    "snowflake.connector": [("connect", "connection")],
    # Redshift native connector
    "redshift_connector": [("connect", "connection")],
    # Databricks SQL connector
    "databricks.sql": [("connect", "connection")],
    # SQLAlchemy engine (covers most SQL sources)
    "sqlalchemy": [("create_engine", "engine")],
}

# BigQuery is special - it uses a Client class, not a connect function
PATCHABLE_CLIENTS: Dict[str, List[Tuple[str, str]]] = {
    "google.cloud.bigquery": [("Client", "client")],
}


def _is_vcr_interference_error(exc: Exception) -> bool:
    """Detect if an error is likely from VCR interference with connection.

    VCR may interfere with database connections that use vendored or
    non-standard HTTP libraries (Snowflake, Databricks). This function
    uses two checks to minimize false positives:

    1. Verify VCR is actually active (has active cassettes)
    2. Check if error message matches known VCR interference patterns

    Trade-offs:
    - False positives: Acceptable - retry will still fail with real error,
      only adds ~1-5 seconds to failure time
    - False negatives: More problematic - VCR interference won't be detected,
      recording will fail without retry

    Therefore, we prefer being cautious (Option A: check VCR active) while
    still being liberal with error pattern matching.

    Args:
        exc: The exception that occurred during connection attempt

    Returns:
        True if the error is likely from VCR interference
    """
    # First, check if VCR is actually active
    # If VCR isn't running, it can't be causing interference
    try:
        import vcr as vcr_module

        # Check if VCR has the cassette module and current_cassettes attribute
        if not hasattr(vcr_module, "cassette"):
            logger.debug("VCR cassette module not found, not VCR interference")
            return False

        if not hasattr(vcr_module.cassette, "_current_cassettes"):
            logger.debug("VCR not tracking cassettes, not VCR interference")
            return False

        # Check if there are active cassettes
        current_cassettes = vcr_module.cassette._current_cassettes
        if not current_cassettes:
            logger.debug("No active VCR cassettes, not VCR interference")
            return False

        # VCR is active, now check error patterns
        logger.debug(
            f"VCR is active with {len(current_cassettes)} cassette(s), checking error pattern"
        )

    except (ImportError, AttributeError) as e:
        # VCR not available or structure changed
        logger.debug(f"VCR not available ({e}), not VCR interference")
        return False

    # Now check if the error matches known VCR interference patterns
    error_msg = str(exc).lower()

    # Common indicators of VCR interference with SSL/HTTP connections
    indicators = [
        "connection refused",
        "name resolution",
        "ssl error",
        "certificate verify failed",
        "certificate validation",
        "proxy",
        "connection reset",
        "connection aborted",
        "handshake failure",
        "ssl handshake",
    ]

    matches = any(indicator in error_msg for indicator in indicators)

    if matches:
        logger.debug(f"Error pattern matches VCR interference: {error_msg[:100]}")
    else:
        logger.debug(
            f"Error pattern does not match VCR interference: {error_msg[:100]}"
        )

    return matches


class ModulePatcher:
    """Context manager that patches database connector modules.

    During RECORDING mode:
    - Patches connect() functions to wrap returned connections
    - Real connections are made, queries are recorded

    During REPLAY mode:
    - Patches connect() functions to return mock connections
    - No real connections are made, queries served from recordings
    """

    def __init__(
        self,
        recorder: QueryRecorder,
        is_replay: bool = False,
    ) -> None:
        """Initialize module patcher.

        Args:
            recorder: QueryRecorder for recording/replaying queries.
            is_replay: If True, return mock connections instead of real ones.
        """
        self.recorder = recorder
        self.is_replay = is_replay
        self._originals: Dict[Tuple[str, str], Any] = {}
        self._patched_modules: List[str] = []

    def __enter__(self) -> "ModulePatcher":
        """Apply patches to available modules."""
        self._patch_connectors()
        self._patch_clients()

        if self._patched_modules:
            logger.info(
                f"Patched database modules for "
                f"{'replay' if self.is_replay else 'recording'}: "
                f"{', '.join(self._patched_modules)}"
            )
        else:
            logger.debug("No database connector modules found to patch")

        return self

    def __exit__(self, *args: Any) -> None:
        """Restore all original functions."""
        for (module_path, func_name), original in self._originals.items():
            try:
                module = importlib.import_module(module_path)
                setattr(module, func_name, original)
                logger.debug(f"Restored {module_path}.{func_name}")
            except Exception as e:
                logger.warning(f"Failed to restore {module_path}.{func_name}: {e}")

        self._originals.clear()
        self._patched_modules.clear()

    def _patch_connectors(self) -> None:
        """Patch connector modules (connect functions)."""
        for module_path, patches in PATCHABLE_CONNECTORS.items():
            try:
                module = importlib.import_module(module_path)

                for func_name, wrapper_type in patches:
                    if not hasattr(module, func_name):
                        continue

                    original = getattr(module, func_name)
                    self._originals[(module_path, func_name)] = original

                    if wrapper_type == "connection":
                        wrapped = self._create_connection_wrapper(original)
                    elif wrapper_type == "engine":
                        wrapped = self._create_engine_wrapper(original)
                    else:
                        continue

                    setattr(module, func_name, wrapped)
                    self._patched_modules.append(f"{module_path}.{func_name}")

            except ImportError:
                # Module not installed, skip
                logger.debug(f"Module {module_path} not installed, skipping")

    def _patch_clients(self) -> None:
        """Patch client modules (Client classes)."""
        for module_path, patches in PATCHABLE_CLIENTS.items():
            try:
                module = importlib.import_module(module_path)

                for class_name, _wrapper_type in patches:
                    if not hasattr(module, class_name):
                        continue

                    original_class = getattr(module, class_name)
                    self._originals[(module_path, class_name)] = original_class

                    wrapped_class = self._create_client_wrapper(original_class)
                    setattr(module, class_name, wrapped_class)
                    self._patched_modules.append(f"{module_path}.{class_name}")

            except ImportError:
                logger.debug(f"Module {module_path} not installed, skipping")

    def _create_connection_wrapper(
        self, original_connect: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Create a wrapper for connection factory functions with VCR interference recovery."""
        recorder = self.recorder
        is_replay = self.is_replay

        def wrapped_connect(*args: Any, **kwargs: Any) -> Any:
            if is_replay:
                # In replay mode, always return mock connection (even for SQLAlchemy)
                logger.debug("Returning replay connection (no real DB connection)")
                return ReplayConnection(recorder)

            # Check if this connection is being created for SQLAlchemy
            # SQLAlchemy needs special handling via event listeners during recording
            import inspect

            frame = inspect.currentframe()
            try:
                # Walk up the call stack to see if SQLAlchemy is calling us
                for _ in range(10):  # Check up to 10 frames
                    frame = frame.f_back  # type: ignore
                    if frame is None:
                        break
                    if "sqlalchemy" in str(frame.f_globals.get("__name__", "")):
                        # SQLAlchemy is creating this connection - don't wrap it
                        # SQLAlchemy event listeners will handle recording
                        logger.debug(
                            "SQLAlchemy detected - skipping connection proxy, "
                            "using event listeners only"
                        )
                        return original_connect(*args, **kwargs)
            finally:
                del frame

            # In recording mode, wrap the real connection
            try:
                logger.debug("Creating recording connection proxy")
                real_connection = original_connect(*args, **kwargs)
                logger.info("Database connection established (recording mode)")
                return ConnectionProxy(
                    connection=real_connection,
                    recorder=recorder,
                    is_replay=False,
                )
            except Exception as e:
                # Check if error might be from VCR interference
                # This can happen with Snowflake (vendored urllib3) or Databricks (Thrift client)
                if _is_vcr_interference_error(e):
                    logger.warning(
                        f"Database connection failed with VCR active. "
                        f"Error: {e}. "
                        f"This may be VCR interference with vendored/non-standard HTTP libraries. "
                        f"Retrying with temporary VCR bypass..."
                    )
                    # Retry with VCR temporarily disabled
                    from datahub.ingestion.recording.http_recorder import (
                        vcr_bypass_context,
                    )

                    try:
                        with vcr_bypass_context():
                            real_connection = original_connect(*args, **kwargs)
                            logger.info(
                                "Database connection succeeded with VCR bypassed. "
                                "SQL queries will still be recorded normally."
                            )
                            return ConnectionProxy(
                                connection=real_connection,
                                recorder=recorder,
                                is_replay=False,
                            )
                    except Exception as retry_error:
                        # Bypass didn't help - this was a real connection error
                        logger.error(
                            f"Database connection failed even with VCR bypassed. "
                            f"This is a real connection error, not VCR interference. "
                            f"Error: {retry_error}"
                        )
                        raise retry_error
                else:
                    # Not VCR-related, re-raise immediately
                    logger.error(f"Database connection failed: {e}")
                    raise

        return wrapped_connect

    def _create_engine_wrapper(  # noqa: C901
        self, original_create_engine: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Create a wrapper for SQLAlchemy create_engine.

        This is more complex because SQLAlchemy engines have their own
        connection pooling and cursor management. The complexity is inherent
        to properly handling SQLAlchemy's event system for recording/replay.
        """
        recorder = self.recorder
        is_replay = self.is_replay

        def wrapped_create_engine(*args: Any, **kwargs: Any) -> Any:  # noqa: C901
            if is_replay:
                # For SQLAlchemy replay, we still create an engine but
                # intercept at the connection level using events
                logger.debug("Creating SQLAlchemy engine for replay mode")
                # Fall through to create real engine but we'll intercept connections

            # Create the real engine
            engine = original_create_engine(*args, **kwargs)

            # Register event listeners for query recording/replay
            try:
                from sqlalchemy import event

                from datahub.ingestion.recording.db_proxy import QueryRecording

                # Store cursors and their query info for deferred result capture
                _cursor_queries: Dict[Any, Dict[str, Any]] = {}

                def before_cursor_execute(
                    conn: Any,
                    cursor: Any,
                    statement: str,
                    parameters: Any,
                    context: Any,
                    executemany: bool,
                ) -> None:
                    """Store query info and wrap cursor fetch methods to capture results."""
                    if is_replay:
                        # During replay, intercept queries and serve from recordings
                        if not hasattr(cursor, "_replay_wrapped"):
                            # Wrap execute to intercept and serve from recordings
                            original_execute = cursor.execute

                            def replay_execute(
                                query: str, *args: Any, **kwargs: Any
                            ) -> Any:
                                """Intercept execute to serve from recordings."""
                                # Get recording
                                recording = recorder.get_recording(
                                    query, kwargs.get("parameters")
                                )
                                if recording is None:
                                    # Check for common SQLAlchemy init queries
                                    query_lower = query.lower().strip()
                                    if (
                                        "select current_database(), current_schema()"
                                        in query_lower
                                    ):
                                        logger.debug(
                                            "Mocking SQLAlchemy init query: current_database/schema"
                                        )
                                        cursor.description = [
                                            (
                                                "CURRENT_DATABASE()",
                                                2,
                                                None,
                                                16777216,
                                                None,
                                                None,
                                                True,
                                            ),
                                            (
                                                "CURRENT_SCHEMA()",
                                                2,
                                                None,
                                                16777216,
                                                None,
                                                None,
                                                True,
                                            ),
                                        ]
                                        cursor._mock_results = [
                                            {
                                                "CURRENT_DATABASE()": None,
                                                "CURRENT_SCHEMA()": None,
                                            }
                                        ]
                                    elif "select 1" in query_lower:
                                        cursor.description = [
                                            ("1", 4, None, None, None, None, True)
                                        ]
                                        cursor._mock_results = [{"1": 1}]
                                    else:
                                        raise RuntimeError(
                                            f"Query not found in recordings: {query[:200]}..."
                                        )
                                else:
                                    # Set up cursor to return recorded results
                                    cursor.description = recording.description
                                    cursor._mock_results = recording.results

                                cursor._mock_index = 0
                                return original_execute(query, *args, **kwargs)

                            cursor.execute = replay_execute

                            # Wrap fetch methods to serve from mock results
                            original_fetchall = cursor.fetchall
                            original_fetchone = cursor.fetchone

                            def replay_fetchall(*args: Any, **kwargs: Any) -> Any:
                                if hasattr(cursor, "_mock_results"):
                                    results = cursor._mock_results
                                    cursor._mock_results = []  # Consume
                                    return results
                                return original_fetchall(*args, **kwargs)

                            def replay_fetchone(*args: Any, **kwargs: Any) -> Any:
                                if (
                                    hasattr(cursor, "_mock_results")
                                    and cursor._mock_results
                                ):
                                    return cursor._mock_results.pop(0)
                                return original_fetchone(*args, **kwargs)

                            cursor.fetchall = replay_fetchall
                            cursor.fetchone = replay_fetchone
                            cursor._replay_wrapped = True
                    else:
                        # Store query info for this cursor
                        query_info = {
                            "query": statement,
                            "parameters": parameters or {},
                            "description": None,
                            "results": None,
                            "captured": False,
                        }
                        _cursor_queries[id(cursor)] = query_info

                        # Wrap fetch methods to capture results and serve cached results
                        if not hasattr(cursor, "_recording_wrapped"):
                            original_fetchall = cursor.fetchall
                            original_fetchone = cursor.fetchone

                            # Cache for results that were pre-fetched in after_cursor_execute
                            cursor._recording_cache = None
                            cursor._recording_cache_consumed = False

                            def wrapped_fetchall(*args: Any, **kwargs: Any) -> Any:
                                """Intercept fetchall to capture or serve cached results."""
                                # If we have cached results, serve those
                                if (
                                    cursor._recording_cache is not None
                                    and not cursor._recording_cache_consumed
                                ):
                                    cursor._recording_cache_consumed = True
                                    return cursor._recording_cache

                                # Otherwise fetch from real cursor
                                fetched = original_fetchall(*args, **kwargs)

                                # Capture for recording
                                if not query_info["captured"]:
                                    query_info["description"] = cursor.description
                                    query_info["results"] = fetched
                                    query_info["captured"] = True

                                return fetched

                            def wrapped_fetchone(*args: Any, **kwargs: Any) -> Any:
                                """Intercept fetchone."""
                                # If we have cached results, serve from cache
                                if (
                                    cursor._recording_cache is not None
                                    and not cursor._recording_cache_consumed
                                ):
                                    if cursor._recording_cache:
                                        result = cursor._recording_cache.pop(0)
                                        if not cursor._recording_cache:
                                            cursor._recording_cache_consumed = True
                                        return result
                                    else:
                                        cursor._recording_cache_consumed = True
                                        return None

                                # Otherwise fetch from real cursor
                                result = original_fetchone(*args, **kwargs)
                                if not query_info["captured"] and result is not None:
                                    query_info["description"] = cursor.description
                                return result

                            cursor.fetchall = wrapped_fetchall
                            cursor.fetchone = wrapped_fetchone
                            cursor._recording_wrapped = True

                        logger.debug(f"Recording query: {statement[:100]}...")

                def after_cursor_execute(
                    conn: Any,
                    cursor: Any,
                    statement: str,
                    parameters: Any,
                    context: Any,
                    executemany: bool,
                ) -> None:
                    """Record query and results (captured by wrapped fetch methods)."""
                    if not is_replay:
                        try:
                            query_info = _cursor_queries.get(id(cursor), {})

                            results: List[Dict[str, Any]] = []
                            description = (
                                query_info.get("description") or cursor.description
                            )
                            error = None

                            # Get results captured by wrapped fetch methods
                            captured_results = query_info.get("results")

                            if captured_results is not None:
                                # Results were captured by fetchall wrapper
                                if description:
                                    col_names = (
                                        [d[0] for d in description]
                                        if description
                                        else []
                                    )
                                    if col_names and captured_results:
                                        if isinstance(
                                            captured_results[0], (tuple, list)
                                        ):
                                            results = [
                                                dict(zip(col_names, row))
                                                for row in captured_results
                                            ]
                                        elif isinstance(captured_results[0], dict):
                                            results = captured_results
                                        else:
                                            results = [
                                                {"value": row}
                                                for row in captured_results
                                            ]
                                    else:
                                        results = [
                                            {"row": list(row)}
                                            for row in captured_results
                                        ]
                                else:
                                    results = (
                                        captured_results
                                        if isinstance(captured_results, list)
                                        else []
                                    )
                            else:
                                # Results weren't captured by fetch wrapper yet
                                # Try to fetch immediately if cursor has results available
                                if description and hasattr(cursor, "fetchall"):
                                    try:
                                        # Try to fetch all results now
                                        # This works if results haven't been consumed yet
                                        fetched = cursor.fetchall()
                                        if fetched:
                                            col_names = (
                                                [d[0] for d in description]
                                                if description
                                                else []
                                            )
                                            if col_names:
                                                if isinstance(
                                                    fetched[0], (tuple, list)
                                                ):
                                                    results = [
                                                        dict(zip(col_names, row))
                                                        for row in fetched
                                                    ]
                                                elif isinstance(fetched[0], dict):
                                                    results = fetched
                                                else:
                                                    results = [
                                                        {"value": row}
                                                        for row in fetched
                                                    ]
                                            else:
                                                results = [
                                                    {"row": list(row)}
                                                    for row in fetched
                                                ]

                                            # Store for recording
                                            query_info["results"] = fetched
                                            query_info["captured"] = True

                                            # Cache results so wrapped fetch methods can serve them
                                            if hasattr(cursor, "_recording_cache"):
                                                cursor._recording_cache = fetched
                                                cursor._recording_cache_consumed = False
                                    except Exception as fetch_err:
                                        # Results already consumed or not available
                                        # Wrapped fetch methods will capture them if called later
                                        logger.debug(
                                            f"Could not pre-fetch results: {fetch_err}. "
                                            f"Will rely on fetch wrapper."
                                        )

                                # Get description if not set
                                if not description:
                                    try:
                                        description = cursor.description
                                        query_info["description"] = description
                                    except Exception:
                                        pass

                            # Create recording
                            recording = QueryRecording(
                                query=statement,
                                parameters=parameters or {},
                                results=results,
                                row_count=len(results),
                                description=description,
                                error=error,
                            )
                            recorder.record(recording)

                            # Clean up
                            _cursor_queries.pop(id(cursor), None)

                            logger.debug(
                                f"Recorded SQLAlchemy query: {statement[:60]}... "
                                f"({len(results)} rows)"
                            )
                        except Exception as e:
                            logger.warning(f"Failed to record SQLAlchemy query: {e}")
                            # Clean up on error
                            _cursor_queries.pop(id(cursor), None)

                # Use event.listen() instead of decorator for proper typing
                event.listen(engine, "before_cursor_execute", before_cursor_execute)
                event.listen(engine, "after_cursor_execute", after_cursor_execute)

            except Exception as e:
                logger.warning(f"Failed to register SQLAlchemy events: {e}")

            return engine

        return wrapped_create_engine

    def _create_client_wrapper(self, original_class: type) -> type:
        """Create a wrapper class for client-based connectors (e.g., BigQuery)."""
        recorder = self.recorder
        is_replay = self.is_replay

        class WrappedClient(original_class):  # type: ignore
            """Wrapped client that records/replays queries."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                if is_replay:
                    # In replay mode, don't initialize real client
                    self._recording_recorder = recorder
                    self._recording_is_replay = True
                    logger.debug("Created replay-mode client (no real connection)")
                else:
                    # In recording mode, initialize real client
                    super().__init__(*args, **kwargs)
                    self._recording_recorder = recorder
                    self._recording_is_replay = False
                    logger.debug("Created recording-mode client")

            def query(self, query: str, *args: Any, **kwargs: Any) -> Any:
                """Override query method for recording/replay."""
                if self._recording_is_replay:
                    # Serve from recordings
                    recording = self._recording_recorder.get_recording(query)
                    if recording is None:
                        raise RuntimeError(
                            f"Query not found in recordings: {query[:200]}..."
                        )
                    if recording.error:
                        raise RuntimeError(f"Recorded query error: {recording.error}")

                    # Return a mock result object
                    return MockQueryResult(recording.results)

                # Recording mode - execute and record
                from datahub.ingestion.recording.db_proxy import QueryRecording

                try:
                    result = super().query(query, *args, **kwargs)
                    # Materialize results for recording
                    rows = list(result)
                    results = [dict(row) for row in rows]

                    recording_obj = QueryRecording(
                        query=query,
                        results=results,
                        row_count=len(results),
                    )
                    self._recording_recorder.record(recording_obj)

                    # Return a new iterator over the results
                    return MockQueryResult(results)

                except Exception as e:
                    recording_obj = QueryRecording(
                        query=query,
                        error=str(e),
                    )
                    self._recording_recorder.record(recording_obj)
                    raise

        return WrappedClient


class MockQueryResult:
    """Mock query result for replay mode."""

    def __init__(self, results: List[Dict[str, Any]]) -> None:
        self.results = results
        self._index = 0

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    @property
    def total_rows(self) -> int:
        return len(self.results)

    def result(self) -> "MockQueryResult":
        """BigQuery-style result() method."""
        return self


@contextmanager
def patch_database_modules(
    recorder: QueryRecorder,
    is_replay: bool = False,
) -> Iterator[ModulePatcher]:
    """Context manager for patching database modules.

    Usage:
        recorder = QueryRecorder(Path("queries.jsonl"))
        with patch_database_modules(recorder, is_replay=False):
            # All database connections are now recorded
            conn = snowflake.connector.connect(...)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    patcher = ModulePatcher(recorder, is_replay=is_replay)
    with patcher:
        yield patcher
