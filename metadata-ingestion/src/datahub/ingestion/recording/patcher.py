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
from datahub.ingestion.recording.sqlalchemy_events import (
    attach_sqlalchemy_recorder,
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
        logger.debug(
            f"ModulePatcher.__init__() called with is_replay={is_replay}, "
            f"self.is_replay={self.is_replay}"
        )

    def __enter__(self) -> "ModulePatcher":
        """Apply patches to available modules."""
        logger.info(
            f"🔧 ModulePatcher.__enter__() called with is_replay={self.is_replay}"
        )
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
        """Restore all original functions and detach event listeners."""
        logger.debug(
            f"ModulePatcher.__exit__() called (is_replay={self.is_replay}), "
            f"restoring {len(self._originals)} patched functions"
        )
        for (module_path, func_name), original in self._originals.items():
            try:
                module = importlib.import_module(module_path)
                setattr(module, func_name, original)
                logger.debug(f"Restored {module_path}.{func_name}")
            except Exception as e:
                logger.warning(f"Failed to restore {module_path}.{func_name}: {e}")

        # Note: SQLAlchemy event listeners are automatically cleaned up when engines are garbage collected
        # We don't need to explicitly detach them here, but we could if needed for specific engines

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

                    current_func = getattr(module, func_name)
                    logger.debug(
                        f"Current function at {module_path}.{func_name}: "
                        f"{type(current_func).__name__}, "
                        f"id={id(current_func)}"
                    )

                    # Get the true original - check if we already stored it, otherwise use current
                    if (module_path, func_name) in self._originals:
                        # We've patched this before in this session, use stored original
                        original = self._originals[(module_path, func_name)]
                        logger.debug(
                            f"Module {module_path}.{func_name} already in _originals, "
                            f"using stored original (type={type(original).__name__}, id={id(original)})"
                        )
                    else:
                        # First time patching - store the current function as original
                        original = current_func
                        self._originals[(module_path, func_name)] = original
                        logger.debug(
                            f"Storing original for {module_path}.{func_name}: "
                            f"{type(original).__name__}, id={id(original)}"
                        )

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
                # In replay mode, return mock connection
                logger.debug("Returning replay connection (no real DB connection)")
                return ReplayConnection(recorder)

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

    def _create_engine_wrapper(
        self, original_create_engine: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Create a wrapper for SQLAlchemy create_engine.

        Uses event listeners for recording mode (avoids import reference issues).
        Uses raw_connection wrapper for replay mode (prevents actual DB connections).
        """
        recorder = self.recorder
        is_replay = self.is_replay

        def wrapped_create_engine(*args: Any, **kwargs: Any) -> Any:
            # Create the real engine
            engine = original_create_engine(*args, **kwargs)

            if is_replay:
                # Replay mode: replace raw_connection to prevent actual DB connections
                def replay_raw_connection() -> Any:
                    """Return replay connection for air-gapped mode."""
                    return ReplayConnection(recorder)

                engine.raw_connection = replay_raw_connection
                logger.debug(
                    "Wrapped SQLAlchemy engine.raw_connection() for replay mode"
                )
            else:
                # Recording mode: use event listeners (works even if create_engine was imported directly)
                attach_sqlalchemy_recorder(engine, recorder, is_replay=False)
                logger.debug("Attached SQLAlchemy recording event listeners to engine")

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
