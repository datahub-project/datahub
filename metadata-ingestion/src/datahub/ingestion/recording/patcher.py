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
from typing import Any, Callable, Dict, Iterator, KeysView, List, Optional, Tuple

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


def _convert_expiration_ms(value: Any, table_id: str = "unknown") -> Optional[int]:
    """Convert expiration_ms from JSON to int, with validation.

    Args:
        value: The expiration_ms value (may be str, float, int, or None)
        table_id: Table ID for error logging

    Returns:
        Integer value or None if conversion fails
    """
    if value is None:
        return None

    try:
        if isinstance(value, int):
            return value
        if isinstance(value, (str, float)):
            return int(value)
        return int(value)
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Failed to convert expiration_ms to int for table {table_id}: "
            f"{value} ({type(value)}): {e}. Setting to None."
        )
        return None


def _create_time_partitioning(
    tp_dict: Dict[str, Any], table_id: str = "unknown"
) -> Any:
    """Create TimePartitioning object from recorded dictionary.

    Args:
        tp_dict: Dictionary with time partitioning fields
        table_id: Table ID for error logging

    Returns:
        TimePartitioning object or None if creation fails
    """
    try:
        from google.cloud.bigquery.table import TimePartitioning

        expiration_ms = _convert_expiration_ms(tp_dict.get("expiration_ms"), table_id)

        require_partition_filter = tp_dict.get("require_partition_filter")
        if require_partition_filter is not None:
            require_partition_filter = bool(require_partition_filter)

        return TimePartitioning(
            field=tp_dict.get("field"),
            type_=tp_dict.get("type"),
            expiration_ms=expiration_ms,
            require_partition_filter=require_partition_filter,
        )
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to create TimePartitioning from {tp_dict}: {e}")
        return None


class MockTableListItem:
    """Mock TableListItem that mimics google.cloud.bigquery.table.TableListItem.

    Used during replay mode since TableListItem is not directly constructible.
    """

    def __init__(self, table_id: str, dataset_id: str, project: str) -> None:
        from datetime import datetime

        self._table_id = table_id
        self._dataset_id = dataset_id
        self._project = project
        self._table_type: Optional[str] = None
        self._labels: Optional[Dict[str, str]] = None
        self._expires: Optional[datetime] = None
        self._clustering_fields: Optional[List[str]] = None
        self._time_partitioning: Any = None
        self._properties: Dict[str, Any] = {}

    @property
    def table_id(self) -> str:
        return self._table_id

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @property
    def project(self) -> str:
        return self._project

    @property
    def table_type(self) -> Optional[str]:
        return self._table_type

    @table_type.setter
    def table_type(self, value: str) -> None:
        self._table_type = value

    @property
    def labels(self) -> Optional[Dict[str, str]]:
        return self._labels

    @labels.setter
    def labels(self, value: Dict[str, str]) -> None:
        self._labels = value

    @property
    def expires(self) -> Any:
        return self._expires

    @expires.setter
    def expires(self, value: Any) -> None:
        self._expires = value

    @property
    def clustering_fields(self) -> Optional[List[str]]:
        return self._clustering_fields

    @clustering_fields.setter
    def clustering_fields(self, value: Optional[List[str]]) -> None:
        self._clustering_fields = value

    @property
    def time_partitioning(self) -> Any:
        return self._time_partitioning

    @time_partitioning.setter
    def time_partitioning(self, value: Any) -> None:
        self._time_partitioning = value


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
                        original = self._originals[(module_path, func_name)]
                        logger.debug(
                            f"Module {module_path}.{func_name} already in _originals, "
                            f"using stored original (type={type(original).__name__}, id={id(original)})"
                        )
                    else:
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
                logger.debug("Returning replay connection (no real DB connection)")
                return ReplayConnection(recorder)

            try:
                real_connection = original_connect(*args, **kwargs)
                logger.info("Database connection established (recording mode)")
                return ConnectionProxy(
                    connection=real_connection,
                    recorder=recorder,
                    is_replay=False,
                )
            except Exception as e:
                # VCR can interfere with Snowflake (vendored urllib3) or Databricks (Thrift client)
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
                        logger.error(
                            f"Database connection failed even with VCR bypassed. "
                            f"This is a real connection error, not VCR interference. "
                            f"Error: {retry_error}"
                        )
                        raise retry_error
                else:
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

    def _create_client_wrapper(self, original_class: type) -> type:  # noqa: C901
        """Create a wrapper class for client-based connectors (e.g., BigQuery)."""
        recorder = self.recorder
        is_replay = self.is_replay

        class WrappedClient(original_class):  # type: ignore
            """Wrapped client that records/replays queries."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                if is_replay:
                    # In replay mode, don't initialize real client
                    # But we need to set some attributes that might be accessed
                    self._recording_recorder = recorder
                    self._recording_is_replay = True
                    # Set dummy attributes that BigQuery client methods might access
                    # This prevents AttributeError when methods like list_datasets() are called
                    self._connection = None
                    logger.debug("Created replay-mode client (no real connection)")
                else:
                    # In recording mode, initialize real client
                    super().__init__(*args, **kwargs)
                    self._recording_recorder = recorder
                    self._recording_is_replay = False
                    logger.debug("Created recording-mode client")

            def list_datasets(self, *args: Any, **kwargs: Any) -> Any:
                """Override list_datasets for recording/replay."""
                from datahub.ingestion.recording.db_proxy import QueryRecording

                # Create a key for this call based on arguments
                call_key = f"list_datasets({args}, {kwargs})"

                if self._recording_is_replay:
                    # In replay mode, serve from recordings
                    recording = self._recording_recorder.get_recording(call_key)
                    if recording is None:
                        logger.warning(
                            f"list_datasets() not found in recordings for {call_key} - returning empty list"
                        )
                        return []
                    if recording.error:
                        raise RuntimeError(
                            f"Recorded list_datasets error: {recording.error}"
                        )

                    # Reconstruct Dataset objects from recorded results
                    from google.cloud.bigquery import Dataset
                    from google.cloud.bigquery.dataset import DatasetReference

                    datasets = []
                    for result_dict in recording.results:
                        # Create a Dataset from the recorded data
                        dataset_ref = DatasetReference(
                            project=result_dict.get("project", ""),
                            dataset_id=result_dict.get("dataset_id", ""),
                        )
                        dataset = Dataset(dataset_ref)
                        # Initialize _properties first (required for dataset.dataset_id to work)
                        # _properties is used by BigQuery source to get location
                        if "_properties" in result_dict:
                            dataset._properties = result_dict["_properties"]  # type: ignore[attr-defined]
                        else:
                            # Ensure _properties exists even if not recorded
                            dataset._properties = {  # type: ignore[attr-defined]
                                "datasetReference": {
                                    "datasetId": result_dict.get("dataset_id", ""),
                                    "projectId": result_dict.get("project", ""),
                                },
                            }
                        # Set labels after _properties is initialized
                        if "labels" in result_dict:
                            dataset.labels = result_dict["labels"]
                            # Also store labels in _properties in case Dataset object requires it there
                            if dataset._properties:  # type: ignore[attr-defined]
                                dataset._properties["labels"] = result_dict["labels"]  # type: ignore[attr-defined]
                        datasets.append(dataset)

                    logger.debug(
                        f"Replaying list_datasets() - returning {len(datasets)} datasets"
                    )
                    return iter(datasets)

                # Recording mode - execute and record
                try:
                    datasets_iter = super().list_datasets(*args, **kwargs)
                    datasets_list = list(datasets_iter)

                    results = []
                    for dataset in datasets_list:
                        dataset_dict = {
                            "dataset_id": dataset.dataset_id,
                            "project": dataset.project,
                        }
                        # _properties is used by BigQuery source to get location
                        if hasattr(dataset, "_properties") and isinstance(
                            dataset._properties, dict
                        ):
                            dataset_dict["_properties"] = dataset._properties
                        if hasattr(dataset, "labels"):
                            dataset_dict["labels"] = dataset.labels
                        results.append(dataset_dict)

                    recording_obj = QueryRecording(
                        query=call_key,
                        results=results,
                        row_count=len(results),
                    )
                    self._recording_recorder.record(recording_obj)

                    logger.debug(f"Recorded list_datasets() - {len(results)} datasets")

                    return iter(datasets_list)

                except Exception as e:
                    recording_obj = QueryRecording(
                        query=call_key,
                        error=str(e),
                    )
                    self._recording_recorder.record(recording_obj)
                    raise

            def list_tables(self, *args: Any, **kwargs: Any) -> Any:  # noqa: C901
                """Override list_tables for recording/replay."""
                from datahub.ingestion.recording.db_proxy import QueryRecording

                # Create a key for this call based on arguments
                call_key = f"list_tables({args}, {kwargs})"

                if self._recording_is_replay:
                    # In replay mode, serve from recordings
                    recording = self._recording_recorder.get_recording(call_key)
                    if recording is None:
                        logger.warning(
                            f"list_tables() not found in recordings for {call_key} - returning empty list"
                        )
                        return []
                    if recording.error:
                        raise RuntimeError(
                            f"Recorded list_tables error: {recording.error}"
                        )

                    from datetime import datetime

                    tables = []
                    for result_dict in recording.results:
                        table_id = result_dict.get("table_id", "")
                        try:
                            # Use module-level MockTableListItem
                            table_item = MockTableListItem(
                                table_id=table_id,
                                dataset_id=result_dict.get("dataset_id", ""),
                                project=result_dict.get("project", ""),
                            )
                            if "table_type" in result_dict:
                                table_item.table_type = result_dict["table_type"]
                            if "labels" in result_dict:
                                table_item.labels = result_dict["labels"]
                            if "expires" in result_dict and result_dict["expires"]:
                                expires_value = result_dict["expires"]
                                if isinstance(expires_value, str):
                                    table_item.expires = datetime.fromisoformat(
                                        expires_value.replace("Z", "+00:00")
                                    )
                                elif isinstance(expires_value, datetime):
                                    table_item.expires = expires_value
                                elif isinstance(expires_value, (int, float)):
                                    table_item.expires = datetime.fromtimestamp(
                                        expires_value
                                    )
                                else:
                                    logger.warning(
                                        f"Unexpected type for expires: {type(expires_value)} for table {table_id}. Skipping."
                                    )
                            if "clustering_fields" in result_dict:
                                table_item.clustering_fields = result_dict[
                                    "clustering_fields"
                                ]
                            # Use helper function for time partitioning
                            if (
                                "time_partitioning" in result_dict
                                and result_dict["time_partitioning"]
                            ):
                                table_item.time_partitioning = (
                                    _create_time_partitioning(
                                        result_dict["time_partitioning"], table_id
                                    )
                                )
                            if "_properties" in result_dict:
                                table_item._properties = result_dict["_properties"]

                            tables.append(table_item)
                        except Exception as e:
                            logger.error(
                                f"Error creating MockTableListItem from {table_id}: {e}",
                                exc_info=True,
                            )
                            # Skip this table if we can't create it
                            continue

                    logger.debug(
                        f"Replaying list_tables() - returning {len(tables)} tables"
                    )
                    return iter(tables)

                # Recording mode - execute and record
                try:
                    tables_iter = super().list_tables(*args, **kwargs)
                    tables_list = list(tables_iter)

                    results = []
                    for table in tables_list:
                        table_dict = {
                            "table_id": table.table_id,
                            "dataset_id": table.dataset_id,
                            "project": table.project,
                        }
                        # Record additional properties if available
                        if hasattr(table, "table_type"):
                            table_dict["table_type"] = table.table_type
                        if hasattr(table, "labels"):
                            table_dict["labels"] = table.labels
                        if hasattr(table, "expires") and table.expires:
                            table_dict["expires"] = table.expires.isoformat()
                        if hasattr(table, "clustering_fields"):
                            table_dict["clustering_fields"] = table.clustering_fields
                        if (
                            hasattr(table, "time_partitioning")
                            and table.time_partitioning
                        ):
                            tp = table.time_partitioning
                            table_dict["time_partitioning"] = {
                                "field": tp.field,
                                "type": tp.type_,
                                "expiration_ms": tp.expiration_ms,
                                "require_partition_filter": tp.require_partition_filter,
                            }
                        if hasattr(table, "_properties"):
                            table_dict["_properties"] = table._properties
                        results.append(table_dict)

                    recording_obj = QueryRecording(
                        query=call_key,
                        results=results,
                        row_count=len(results),
                    )
                    self._recording_recorder.record(recording_obj)

                    logger.debug(f"Recorded list_tables() - {len(results)} tables")

                    # Return the original iterator
                    return iter(tables_list)

                except Exception as e:
                    recording_obj = QueryRecording(
                        query=call_key,
                        error=str(e),
                    )
                    self._recording_recorder.record(recording_obj)
                    raise

            def get_dataset(self, *args: Any, **kwargs: Any) -> Any:
                """Override get_dataset for recording/replay."""
                from datahub.ingestion.recording.db_proxy import QueryRecording

                # Create a key for this call based on arguments
                call_key = f"get_dataset({args}, {kwargs})"

                if self._recording_is_replay:
                    # In replay mode, serve from recordings
                    recording = self._recording_recorder.get_recording(call_key)
                    if recording is None:
                        raise RuntimeError(
                            f"get_dataset() not found in recordings for {call_key}"
                        )
                    if recording.error:
                        raise RuntimeError(
                            f"Recorded get_dataset error: {recording.error}"
                        )

                    # Reconstruct Dataset object from recorded results
                    # get_dataset returns a single Dataset object, not a list
                    if not recording.results:
                        raise RuntimeError(
                            f"get_dataset() recording has no results for {call_key}"
                        )

                    result_dict = recording.results[0]
                    from google.cloud.bigquery import Dataset
                    from google.cloud.bigquery.dataset import DatasetReference

                    # Create Dataset from recorded data
                    dataset_ref = DatasetReference(
                        project=result_dict.get("project", ""),
                        dataset_id=result_dict.get("dataset_id", ""),
                    )
                    dataset = Dataset(dataset_ref)
                    # _properties should already be recorded with datasetReference structure from real Dataset
                    if "_properties" in result_dict:
                        dataset._properties = result_dict["_properties"]  # type: ignore[attr-defined]

                    # Set additional properties if they were recorded
                    if "description" in result_dict:
                        dataset.description = result_dict["description"]
                    # Note: created and modified are read-only properties, so we can't set them
                    # They will be None in replay mode, which is acceptable
                    if "location" in result_dict:
                        dataset.location = result_dict["location"]
                    if "labels" in result_dict:
                        dataset.labels = result_dict["labels"]

                    logger.debug(f"Replaying get_dataset() for {dataset_ref}")
                    return dataset

                # Recording mode - execute and record
                try:
                    dataset = super().get_dataset(*args, **kwargs)

                    # Convert Dataset object to dict for recording
                    dataset_dict = {
                        "project": dataset.project,
                        "dataset_id": dataset.dataset_id,
                    }
                    # Record additional properties if available
                    if hasattr(dataset, "description"):
                        dataset_dict["description"] = dataset.description
                    if hasattr(dataset, "created"):
                        dataset_dict["created"] = dataset.created
                    if hasattr(dataset, "modified"):
                        dataset_dict["modified"] = dataset.modified
                    if hasattr(dataset, "location"):
                        dataset_dict["location"] = dataset.location

                    # Record the call (single result, not a list)
                    recording_obj = QueryRecording(
                        query=call_key,
                        results=[dataset_dict],
                        row_count=1,
                    )
                    self._recording_recorder.record(recording_obj)

                    logger.debug(
                        f"Recorded get_dataset() for {dataset.project}.{dataset.dataset_id}"
                    )

                    return dataset

                except Exception as e:
                    recording_obj = QueryRecording(
                        query=call_key,
                        error=str(e),
                    )
                    self._recording_recorder.record(recording_obj)
                    raise

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

                    # Return a mock result object with MockRow objects
                    return MockQueryResult(recording.results)

                # Recording mode - execute and record
                from datahub.ingestion.recording.db_proxy import QueryRecording

                try:
                    result = super().query(query, *args, **kwargs)
                    # Materialize results for recording
                    # Convert Row objects to dicts for JSON serialization
                    rows = list(result)
                    results = []
                    for row in rows:
                        if hasattr(row, "_mapping"):
                            # SQLAlchemy 2.x Row._mapping
                            results.append(dict(row._mapping))
                        elif hasattr(row, "keys"):
                            # BigQuery Row or dict-like object - convert to dict
                            # but preserve the original row for return
                            row_dict = dict(row)
                            results.append(row_dict)
                        else:
                            # Plain dict or other type
                            results.append(row if isinstance(row, dict) else dict(row))

                    recording_obj = QueryRecording(
                        query=query,
                        results=results,
                        row_count=len(results),
                    )
                    self._recording_recorder.record(recording_obj)

                    # Return MockQueryResult with MockRow objects that support attribute access
                    # This is necessary because we've consumed the original result
                    # MockRow provides both dict and attribute access like BigQuery Row objects
                    return MockQueryResult(results)

                except Exception as e:
                    recording_obj = QueryRecording(
                        query=query,
                        error=str(e),
                    )
                    self._recording_recorder.record(recording_obj)
                    raise

        return WrappedClient


class MockRow:
    """Mock BigQuery Row object that supports both dict and attribute access.

    BigQuery Row objects support attribute access (e.g., row.table_name)
    as well as dict access (e.g., row['table_name']). This class provides both.
    """

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def __getattr__(self, name: str) -> Any:
        """Support attribute access like row.table_name"""
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        return self._data.get(name)

    def __getitem__(self, key: str) -> Any:
        """Support dict access like row['table_name']"""
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Support dict.get() method"""
        return self._data.get(key, default)

    def keys(self) -> KeysView[str]:
        """Support dict.keys() method"""
        return self._data.keys()

    def __iter__(self) -> Iterator[Any]:
        """Support iteration over values"""
        return iter(self._data.values())

    def __repr__(self) -> str:
        return f"MockRow({self._data})"


class MockQueryResult:
    """Mock query result for replay mode.

    Returns MockRow objects that support both attribute and dict access,
    compatible with BigQuery Row objects.
    """

    def __init__(self, results: List[Dict[str, Any]]) -> None:
        # Convert dict results to MockRow objects for BigQuery compatibility
        self.results = [
            MockRow(row) if isinstance(row, dict) else row for row in results
        ]
        self._index = 0

    def __iter__(self) -> Iterator[Any]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)

    @property
    def total_rows(self) -> int:
        """Get the total number of rows in the result set."""
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
