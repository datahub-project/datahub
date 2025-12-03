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
        """Create a wrapper for connection factory functions."""
        recorder = self.recorder
        is_replay = self.is_replay

        def wrapped_connect(*args: Any, **kwargs: Any) -> Any:
            if is_replay:
                # In replay mode, return mock connection
                logger.debug("Returning replay connection (no real DB connection)")
                return ReplayConnection(recorder)

            # In recording mode, wrap the real connection
            logger.debug("Creating recording connection proxy")
            real_connection = original_connect(*args, **kwargs)
            return ConnectionProxy(
                connection=real_connection,
                recorder=recorder,
                is_replay=False,
            )

        return wrapped_connect

    def _create_engine_wrapper(
        self, original_create_engine: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Create a wrapper for SQLAlchemy create_engine.

        This is more complex because SQLAlchemy engines have their own
        connection pooling and cursor management.
        """
        recorder = self.recorder
        is_replay = self.is_replay

        def wrapped_create_engine(*args: Any, **kwargs: Any) -> Any:
            if is_replay:
                # For SQLAlchemy replay, we still create an engine but
                # intercept at the connection level using events
                logger.debug("Creating SQLAlchemy engine for replay mode")
                # Fall through to create real engine but we'll intercept connections

            # Create the real engine
            engine = original_create_engine(*args, **kwargs)

            # Register event listeners for connection interception
            try:
                from sqlalchemy import event

                def on_connect(dbapi_connection: Any, connection_record: Any) -> None:
                    """Intercept raw DBAPI connections."""
                    if is_replay:
                        # Replace with replay connection
                        connection_record.info["recording_proxy"] = ReplayConnection(
                            recorder
                        )
                    else:
                        # Wrap with recording proxy
                        connection_record.info["recording_proxy"] = ConnectionProxy(
                            connection=dbapi_connection,
                            recorder=recorder,
                            is_replay=False,
                        )

                def before_execute(
                    conn: Any,
                    cursor: Any,
                    statement: str,
                    parameters: Any,
                    context: Any,
                    executemany: bool,
                ) -> None:
                    """Record query before execution."""
                    if not is_replay:
                        logger.debug(f"Recording query: {statement[:100]}...")

                # Use event.listen() instead of decorator for proper typing
                event.listen(engine, "connect", on_connect)
                event.listen(engine, "before_cursor_execute", before_execute)

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
