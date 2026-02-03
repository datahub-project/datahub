import asyncio
import functools
import logging
import os
import threading
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)
from urllib.parse import urlparse, urlunparse

import airflow
from airflow.configuration import conf
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk import Connection
from openlineage.client.serde import Serde

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.emitter.composite_emitter import CompositeEmitter
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataFlowKeyClass,
    DataJobInputOutputClass,
    DataJobKeyClass,
    DataPlatformInstanceClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OperationClass,
    OperationTypeClass,
    StatusClass,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult
from datahub.telemetry import telemetry

# Import Airflow 3.x specific shims (clean, no cross-version complexity)
from datahub_airflow_plugin._airflow_asset_adapter import extract_urns_from_iolets
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin._version import __package_name__, __version__

# Import Airflow 3.x compatibility and patches before any Airflow imports
from datahub_airflow_plugin.airflow3 import _airflow_compat  # noqa: F401
from datahub_airflow_plugin.airflow3._shims import (
    OpenLineagePlugin,
    Operator,
    get_task_inlets,
    get_task_outlets,
    redact_with_exclusions,
)
from datahub_airflow_plugin.client.airflow_generator import (  # type: ignore[attr-defined]
    AirflowGenerator,
)
from datahub_airflow_plugin.entities import (
    entities_to_datajob_urn_list,
    entities_to_dataset_urn_list,
)

# Airflow 3.x always has these APIs
HAS_AIRFLOW_DAG_LISTENER_API: bool = True
HAS_AIRFLOW_DATASET_LISTENER_API: bool = True

# Airflow 3.0+: No extractors, use OpenLineage native integration
ExtractorManager = None  # type: ignore

_F = TypeVar("_F", bound=Callable[..., None])
if TYPE_CHECKING:
    from airflow.datasets import Dataset
    from airflow.models import DagRun, TaskInstance
    from airflow.sdk.definitions.dag import DAG

    # To placate mypy on Airflow versions that don't have the listener API,
    # we define a dummy hookimpl that's an identity function.

    def hookimpl(f: _F) -> _F:  # type: ignore[misc]
        return f

else:
    from airflow.listeners import hookimpl

logger = logging.getLogger(__name__)


def _get_dagrun_from_task_instance(task_instance: "TaskInstance") -> "DagRun":
    """
    Get a DagRun from a TaskInstance (Airflow 3.x).

    In Airflow 3.x, RuntimeTaskInstance doesn't have a dag_run attribute, so we create a
    proxy object with the attributes we need.
    """

    class DagRunProxy:
        """
        DagRun proxy for Airflow 3.x RuntimeTaskInstance.

        Provides minimal DagRun interface needed by the listener.
        """

        def __init__(self, ti: "TaskInstance"):
            self.ti = ti

        @property
        def dag(self) -> Any:
            """Get DAG from task.dag"""
            task = getattr(self.ti, "task", None)
            if task:
                return task.dag
            return None

        @property
        def dag_id(self) -> Any:
            """Get dag_id from task instance"""
            return getattr(self.ti, "dag_id", None)

        @property
        def run_id(self) -> Any:
            """Get run_id from task instance"""
            return getattr(self.ti, "run_id", None)

        def __repr__(self) -> str:
            return f"DagRunProxy(dag_id={self.dag_id!r}, run_id={self.run_id!r})"

    return DagRunProxy(task_instance)  # type: ignore[return-value]


_airflow_listener_initialized = False
_airflow_listener: Optional["DataHubListener"] = None
_airflow_listener_lock = threading.Lock()

# Threading is enabled by default for better performance
# It prevents slow lineage extraction from blocking task completion
# Can be disabled by setting DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD=false
_RUN_IN_THREAD = os.getenv("DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD", "true").lower() in (
    "true",
    "1",
)
_RUN_IN_THREAD_TIMEOUT = float(
    os.getenv("DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT", 10)
)
_DATAHUB_CLEANUP_DAG = "Datahub_Cleanup"

KILL_SWITCH_VARIABLE_NAME = "datahub_airflow_plugin_disable_listener"


def get_airflow_plugin_listener() -> Optional["DataHubListener"]:
    """
    Get or initialize the DataHub listener singleton.

    Uses double-checked locking pattern for thread-safe lazy initialization.
    This prevents race conditions when multiple worker threads try to initialize
    the listener simultaneously.
    """
    global _airflow_listener_initialized
    global _airflow_listener

    # Fast path: if already initialized, return immediately without acquiring lock
    if _airflow_listener_initialized:
        return _airflow_listener

    # Slow path: acquire lock for initialization
    with _airflow_listener_lock:
        # Double-check: another thread might have initialized while we waited for lock
        if _airflow_listener_initialized:
            return _airflow_listener

        # Now safe to initialize - we hold the lock and confirmed not initialized
        _airflow_listener_initialized = True

        plugin_config = get_lineage_config()

        if plugin_config.enabled:
            _airflow_listener = DataHubListener(config=plugin_config)
            logger.info(
                f"DataHub plugin v2 (package: {__package_name__} and version: {__version__}) listener initialized with config: {plugin_config}"
            )

            telemetry.telemetry_instance.ping(
                "airflow-plugin-init",
                {
                    "airflow-version": airflow.__version__,
                    "datahub-airflow-plugin": "v2",
                    "datahub-airflow-plugin-dag-events": HAS_AIRFLOW_DAG_LISTENER_API,
                    "capture_executions": plugin_config.capture_executions,
                    "capture_tags": plugin_config.capture_tags_info,
                    "capture_ownership": plugin_config.capture_ownership_info,
                    "enable_extractors": plugin_config.enable_extractors,
                    "render_templates": plugin_config.render_templates,
                    "disable_openlineage_plugin": plugin_config.disable_openlineage_plugin,
                },
            )

        # Debug: Log OpenLineage plugin state
        if OpenLineagePlugin is not None:
            logger.debug(
                f"OpenLineage plugin state: listeners={len(getattr(OpenLineagePlugin, 'listeners', []))} items, "
                f"disable_openlineage_plugin={plugin_config.disable_openlineage_plugin}"
            )

        if plugin_config.disable_openlineage_plugin and OpenLineagePlugin is not None:
            # Deactivate the OpenLineagePlugin listener to avoid conflicts/errors.
            OpenLineagePlugin.listeners = []
            logger.debug("Cleared OpenLineage plugin listeners")

    return _airflow_listener


def run_in_thread(f: _F) -> _F:
    # This is also responsible for catching exceptions and logging them.

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        def safe_target():
            """
            Wrapper for the thread target that catches and logs exceptions.

            Without this, exceptions raised inside the thread would be silently
            lost, making debugging production issues nearly impossible.
            """
            try:
                f(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Error in thread executing {f.__name__}: {e}",
                    exc_info=True,
                )

        try:
            if _RUN_IN_THREAD:
                # A poor-man's timeout mechanism.
                # This ensures that we don't hang the task if the extractors
                # are slow or the DataHub API is slow to respond.

                thread = threading.Thread(target=safe_target, daemon=True)
                thread.start()

                if _RUN_IN_THREAD_TIMEOUT > 0:
                    # If _RUN_IN_THREAD_TIMEOUT is 0, we just kick off the thread and move on.
                    # Because it's a daemon thread, it'll be automatically killed when the main
                    # thread exists.

                    start_time = time.time()
                    thread.join(timeout=_RUN_IN_THREAD_TIMEOUT)
                    if thread.is_alive():
                        logger.warning(
                            f"Thread for {f.__name__} is still running after {_RUN_IN_THREAD_TIMEOUT} seconds. "
                            "Continuing without waiting for it to finish."
                        )
                    else:
                        logger.debug(
                            f"Thread for {f.__name__} finished after {time.time() - start_time} seconds"
                        )
            else:
                f(*args, **kwargs)
        except Exception as e:
            logger.warning(
                f"Error setting up thread for {f.__name__}: {e}",
                exc_info=True,
            )

    return cast(_F, wrapper)


def _render_templates(task_instance: "TaskInstance") -> "TaskInstance":
    """
    Templates are already rendered in Airflow 3.x by the task execution system.

    RuntimeTaskInstance contains unpickleable thread locks, so we cannot use deepcopy.
    RuntimeTaskInstance.task contains the operator with rendered templates.
    """
    logger.debug(
        "Skipping template rendering for Airflow 3.0+ (already rendered by task worker)"
    )
    return task_instance


class DataHubListener:
    __name__ = "DataHubListener"

    def __init__(self, config: DatahubLineageConfig):
        self.config = config
        self._set_log_level()

        # Lazy-load emitter to avoid connection retrieval during plugin initialization
        # Connection retrieval via BaseHook.get_connection() only works during task execution
        # where SUPERVISOR_COMMS is available
        self._emitter: Optional[Emitter] = None
        self._graph: Optional[DataHubGraph] = None

        # For Airflow 3.0+, we don't need TaskHolder (dict is used as placeholder)
        self._task_holder: Dict[str, Any] = {}

        # Cache initial datajob objects to merge with completion events
        self._datajob_holder: Dict[str, DataJob] = {}

        # Airflow 3.0+ doesn't use extractors
        self.extractor_manager = None

        # This "inherits" from types.ModuleType to avoid issues with Airflow's listener plugin loader.
        # It previously (v2.4.x and likely other versions too) would throw errors if it was not a module.
        # https://github.com/apache/airflow/blob/e99a518970b2d349a75b1647f6b738c8510fa40e/airflow/listeners/listener.py#L56
        # self.__class__ = types.ModuleType

    def _get_emitter(self):
        """
        Lazy-load emitter on first use during task execution.

        This is a method (not a property) to avoid triggering during pluggy's
        attribute introspection when registering the listener.

        Uses database access to retrieve connection details, avoiding SUPERVISOR_COMMS
        limitations that prevent hook-based methods from working in listener context.
        """
        if self._emitter is None:
            try:
                self._emitter = self._create_emitter_from_connection()
                if self._emitter:
                    logger.debug(
                        f"DataHub plugin v2 using {repr(self._emitter)} (created via connection API)"
                    )
                else:
                    logger.debug(
                        "Could not create emitter via DB access - will retry during task execution"
                    )
                    return None
            except Exception as db_error:
                logger.debug(
                    f"Failed to create emitter via DB access: {db_error}. "
                    "Will retry during task execution.",
                    exc_info=True,
                )
                return None
        return self._emitter

    def _create_emitter_from_connection(self):
        """
        Create emitter by retrieving connection details using Airflow's connection API.

        Uses Connection.get() from SDK which works in all contexts:
        - Task execution (on_task_instance_running, on_task_instance_success, etc.)
        - DAG lifecycle hooks (on_dag_start, on_dag_run_running, etc.)
        - Listener hooks (where SUPERVISOR_COMMS is not available)

        This method works around the SUPERVISOR_COMMS limitation and Airflow 3.0's
        ORM restriction by using the proper Airflow APIs instead of direct database access.
        Supports datahub-rest, datahub-file, and datahub-kafka connection types.
        Handles multiple comma-separated connection IDs via CompositeEmitter.
        """
        try:
            # Parse comma-separated connection IDs
            connection_ids = self.config._datahub_connection_ids

            if len(connection_ids) > 1:
                # Multiple connections - use CompositeEmitter
                emitters = []
                for conn_id in connection_ids:
                    emitter = self._create_single_emitter_from_connection(conn_id)
                    if emitter:
                        emitters.append(emitter)

                if not emitters:
                    logger.warning(
                        f"Could not create any emitters from connection IDs: {connection_ids}"
                    )
                    return None

                logger.debug(
                    f"Created CompositeEmitter with {len(emitters)} emitters from connection IDs: {connection_ids}"
                )
                return CompositeEmitter(emitters)
            else:
                # Single connection
                return self._create_single_emitter_from_connection(connection_ids[0])

        except Exception as e:
            logger.debug(
                f"Failed to create emitter from connection: {e}", exc_info=True
            )
            return None

    def _create_single_emitter_from_connection(self, conn_id: str) -> Optional[Emitter]:
        """
        Create a single emitter from a connection ID.

        Uses Connection.get() from SDK which works in all contexts:
        - Task execution (on_task_instance_running, on_task_instance_success, etc.)
        - DAG lifecycle hooks (on_dag_start, on_dag_run_running, etc.)
        - Listener hooks (where SUPERVISOR_COMMS is not available)

        This method checks environment variables, secrets backends, and the database
        through the proper Airflow APIs, avoiding the ORM restriction in Airflow 3.0.
        """
        try:
            # In Airflow 3.0, direct ORM database access is not allowed during task execution.
            # Use Connection.get() from SDK which works in all contexts.
            # This method checks environment variables, secrets backends, and the database
            # through the proper Airflow APIs.
            conn = Connection.get(conn_id)
            if not conn:
                logger.warning(
                    f"Connection '{conn_id}' not found in secrets backend or environment variables"
                )
                return None

            # Normalize conn_type (handle both dashes and underscores)
            conn_type = (conn.conn_type or "").replace("_", "-")

            # Handle file-based emitter (used in tests)
            if conn_type == "datahub-file":
                import datahub.emitter.synchronized_file_emitter

                filename = conn.host
                if not filename:
                    logger.warning(
                        f"Connection '{conn_id}' is type datahub-file but has no host (filename) configured"
                    )
                    return None

                logger.debug(
                    f"Retrieved connection '{conn_id}' from secrets: type=datahub-file, filename={filename}"
                )
                return (
                    datahub.emitter.synchronized_file_emitter.SynchronizedFileEmitter(
                        filename=filename
                    )
                )

            # Handle Kafka-based emitter
            elif conn_type == "datahub-kafka":
                import datahub.emitter.kafka_emitter
                import datahub.ingestion.sink.datahub_kafka

                obj = conn.extra_dejson or {}
                obj.setdefault("connection", {})
                if conn.host:
                    bootstrap = ":".join(map(str, filter(None, [conn.host, conn.port])))
                    obj["connection"]["bootstrap"] = bootstrap

                config = datahub.ingestion.sink.datahub_kafka.KafkaSinkConfig.parse_obj(
                    obj
                )
                logger.debug(
                    f"Retrieved connection '{conn_id}' from connection API: type=datahub-kafka"
                )
                return datahub.emitter.kafka_emitter.DatahubKafkaEmitter(config)

            # Handle REST-based emitter (default)
            else:
                import datahub.emitter.rest_emitter
                from datahub.ingestion.graph.config import ClientMode

                # Build host URL with port if needed
                host = conn.host or ""
                if not host:
                    logger.warning(f"Connection '{conn_id}' has no host configured")
                    return None

                # Parse the URL using stdlib urlparse
                parsed = urlparse(host if "://" in host else f"http://{host}")

                # Add port if specified and not already in URL
                netloc = parsed.netloc
                if conn.port and not parsed.port:
                    netloc = f"{parsed.hostname}:{conn.port}"

                # Reconstruct the URL
                host = urlunparse(
                    (
                        parsed.scheme or "http",
                        netloc,
                        parsed.path,
                        parsed.params,
                        parsed.query,
                        parsed.fragment,
                    )
                )

                # Get token - check airflow.cfg first, then connection password
                token = conf.get("datahub", "token", fallback=None)
                if token is None:
                    token = conn.password

                # Get extra args
                extra_args = conn.extra_dejson or {}

                logger.debug(
                    f"Retrieved connection '{conn_id}' from connection API: type={conn_type or 'datahub-rest'}, host={host}, has_token={bool(token)}"
                )

                return datahub.emitter.rest_emitter.DataHubRestEmitter(
                    host,
                    token,
                    client_mode=ClientMode.INGESTION,
                    datahub_component="airflow-plugin",
                    **extra_args,
                )
        except Exception as e:
            logger.debug(
                f"Failed to create emitter from connection: {e}", exc_info=True
            )
            return None

    @property
    def emitter(self):
        """Compatibility property that delegates to _get_emitter()."""
        result = self._get_emitter()
        if result is None:
            # Retry emitter creation
            self._emitter = None  # Reset to force retry
            return self._get_emitter()
        return result

    @property
    def graph(self) -> Optional[DataHubGraph]:
        if self._graph:
            return self._graph

        # Use _get_emitter() method to ensure lazy-loading happens first
        emitter = self._get_emitter()
        if emitter is not None:
            import datahub.emitter.rest_emitter

            if isinstance(
                emitter, datahub.emitter.rest_emitter.DataHubRestEmitter
            ) and not isinstance(emitter, DataHubGraph):
                # This is lazy initialized to avoid throwing errors on plugin load.
                self._graph = emitter.to_graph()
                self._emitter = self._graph

        return self._graph

    def _set_log_level(self) -> None:
        """Set the log level for the plugin and its dependencies.

        This may need to be called multiple times, since Airflow sometimes
        messes with the logging configuration after the plugin is loaded.
        In particular, the loggers may get changed when the worker starts
        executing a task.
        """

        if self.config.log_level:
            logging.getLogger(__name__.split(".")[0]).setLevel(self.config.log_level)
        if self.config.debug_emitter:
            logging.getLogger("datahub.emitter").setLevel(logging.DEBUG)

    def _make_emit_callback(self) -> Callable[[Optional[Exception], str], None]:
        def emit_callback(err: Optional[Exception], msg: str) -> None:
            if err:
                logger.error(f"Error sending metadata to datahub: {msg}", exc_info=err)

        return emit_callback

    def _extract_lineage_from_airflow3(
        self,
        task: "Operator",
        task_instance: "TaskInstance",
        complete: bool,
    ) -> Tuple[List[str], List[str], Optional[SqlParsingResult]]:
        """Extract lineage using Airflow 3.x OpenLineage integration."""
        input_urns: List[str] = []
        output_urns: List[str] = []
        sql_parsing_result: Optional[SqlParsingResult] = None

        logger.debug(
            f"Extracting lineage for task {task.task_id} (complete={complete})"
        )
        logger.debug("Airflow 3.0+: Attempting to get lineage from OpenLineage")
        try:
            from datahub_airflow_plugin._datahub_ol_adapter import (
                translate_ol_to_datahub_urn,
            )

            # Check if the operator has OpenLineage support
            facet_method_name = (
                "get_openlineage_facets_on_complete"
                if complete
                else "get_openlineage_facets_on_start"
            )
            has_on_complete = hasattr(task, "get_openlineage_facets_on_complete")
            has_on_start = hasattr(task, "get_openlineage_facets_on_start")
            logger.debug(
                f"Task {task.task_id} OpenLineage support: on_complete={has_on_complete}, on_start={has_on_start}, operator_type={type(task).__name__}, required_method={facet_method_name}"
            )

            if not hasattr(task, facet_method_name):
                logger.debug(
                    f"Task {task.task_id} does not have OpenLineage support (missing {facet_method_name}) - SQL parsing will not be triggered"
                )
                return input_urns, output_urns, sql_parsing_result

            facet_method = getattr(task, facet_method_name)

            try:
                # Call the appropriate facet method
                operator_lineage = (
                    facet_method(task_instance) if complete else facet_method()
                )

                if not operator_lineage:
                    logger.debug(
                        f"OpenLineage facet method {facet_method_name} returned None for task {task.task_id} - this is expected for BigQuery when no job_id is found"
                    )
                    # Even if operator_lineage is None, we might have SQL parsing result from a patch
                    # that created a new OperatorLineage. But if it's None, there's nothing to process.
                    return input_urns, output_urns, sql_parsing_result

                logger.debug(
                    f"Got OpenLineage operator lineage for task {task.task_id}: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}, run_facets_keys={list(operator_lineage.run_facets.keys()) if hasattr(operator_lineage, 'run_facets') else 'N/A'}"
                )

                # Translate OpenLineage datasets to DataHub URNs
                for ol_dataset in operator_lineage.inputs:
                    urn = translate_ol_to_datahub_urn(ol_dataset)
                    input_urns.append(urn)
                    logger.debug(
                        f"  Input: {ol_dataset.namespace}/{ol_dataset.name} -> {urn}"
                    )

                for ol_dataset in operator_lineage.outputs:
                    urn = translate_ol_to_datahub_urn(ol_dataset)
                    output_urns.append(urn)
                    logger.debug(
                        f"  Output: {ol_dataset.namespace}/{ol_dataset.name} -> {urn}"
                    )

                # Check if DataHub SQL parsing result is in run_facets (from our patch)
                logger.debug(
                    f"Checking for SQL parsing result in OpenLineage run facets for task {task.task_id}. Key: {DATAHUB_SQL_PARSING_RESULT_KEY}"
                )
                if (
                    hasattr(operator_lineage, "run_facets")
                    and operator_lineage.run_facets
                ):
                    logger.debug(
                        f"Run facets available: {list(operator_lineage.run_facets.keys())}"
                    )
                    if DATAHUB_SQL_PARSING_RESULT_KEY in operator_lineage.run_facets:
                        sql_parsing_result = operator_lineage.run_facets[
                            DATAHUB_SQL_PARSING_RESULT_KEY
                        ]  # type: ignore
                        if sql_parsing_result is not None:
                            logger.debug(
                                f"✓ Found DataHub SQL parsing result for task {task.task_id} with {len(sql_parsing_result.column_lineage or [])} column lineages"
                            )
                        else:
                            logger.debug(
                                f"SQL parsing result key exists but value is None for task {task.task_id}"
                            )
                    else:
                        logger.debug(
                            f"SQL parsing result key '{DATAHUB_SQL_PARSING_RESULT_KEY}' not found in run_facets for task {task.task_id}"
                        )
                else:
                    logger.debug(
                        f"No run_facets available in operator_lineage for task {task.task_id}"
                    )

            except Exception as e:
                logger.debug(
                    f"Error calling OpenLineage facet method: {e}", exc_info=True
                )

        except Exception as e:
            logger.warning(
                f"Error extracting lineage from OpenLineage: {e}", exc_info=True
            )

        return input_urns, output_urns, sql_parsing_result

    def _process_sql_parsing_result(
        self,
        datajob: DataJob,
        sql_parsing_result: Optional[SqlParsingResult],
    ) -> Tuple[List[str], List[str], List[FineGrainedLineageClass]]:
        """Process SQL parsing result and return additional URNs and column lineage."""
        input_urns: List[str] = []
        output_urns: List[str] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        if not sql_parsing_result:
            logger.debug(
                f"No SQL parsing result available for task {datajob.urn} - lineage may be incomplete"
            )
            return input_urns, output_urns, fine_grained_lineages

        # Log parsing result summary for debugging
        logger.debug(
            f"Processing SQL parsing result for task {datajob.urn}: "
            f"in_tables={len(sql_parsing_result.in_tables)}, "
            f"out_tables={len(sql_parsing_result.out_tables)}, "
            f"column_lineage={len(sql_parsing_result.column_lineage or [])}, "
            f"table_error={sql_parsing_result.debug_info.table_error}, "
            f"error={sql_parsing_result.debug_info.error}"
        )

        if error := sql_parsing_result.debug_info.error:
            logger.warning(
                f"SQL parsing error for task {datajob.urn}: {error}", exc_info=error
            )
            datajob.properties["datahub_sql_parser_error"] = (
                f"{type(error).__name__}: {error}"
            )

        if not sql_parsing_result.debug_info.table_error:
            input_urns.extend(sql_parsing_result.in_tables)
            output_urns.extend(sql_parsing_result.out_tables)

            if sql_parsing_result.column_lineage:
                # Create FGLs from column_lineage items
                # Duplicates will be caught by sql_fine_grained_lineages deduplication below
                fine_grained_lineages.extend(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[
                            builder.make_schema_field_urn(
                                upstream.table, upstream.column
                            )
                            for upstream in column_lineage.upstreams
                        ],
                        downstreams=[
                            builder.make_schema_field_urn(
                                downstream.table, downstream.column
                            )
                            for downstream in [column_lineage.downstream]
                            if downstream.table
                        ],
                    )
                    for column_lineage in sql_parsing_result.column_lineage
                )
                logger.debug(
                    f"Created {len(fine_grained_lineages)} FGLs from {len(sql_parsing_result.column_lineage)} column_lineage items for task {datajob.urn}"
                )
        else:
            logger.warning(
                f"SQL parsing table error for task {datajob.urn}: {sql_parsing_result.debug_info.table_error}"
            )

        return input_urns, output_urns, fine_grained_lineages

    def _extract_lineage(
        self,
        datajob: DataJob,
        dagrun: "DagRun",
        task: "Operator",
        task_instance: "TaskInstance",
        complete: bool = False,
    ) -> None:
        """
        Combine lineage (including column lineage) from task inlets/outlets and
        extractor-generated task_metadata and write it to the datajob. This
        routine is also responsible for converting the lineage to DataHub URNs.
        """
        logger.debug(
            f"_extract_lineage called for task {task.task_id} (complete={complete}, enable_datajob_lineage={self.config.enable_datajob_lineage})"
        )
        if not self.config.enable_datajob_lineage:
            logger.debug(
                f"Skipping lineage extraction for task {task.task_id} - enable_datajob_lineage is False"
            )
            return

        input_urns: List[str] = []
        output_urns: List[str] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        # For completion events, start with empty FGLs to avoid accumulating duplicates
        if complete and datajob.fine_grained_lineages:
            datajob.fine_grained_lineages = []

        task_metadata = None
        sql_parsing_result: Optional[SqlParsingResult] = None

        # Extract lineage using Airflow 3.x OpenLineage integration
        logger.debug(f"Calling _extract_lineage_from_airflow3 for task {task.task_id}")
        extracted_input_urns, extracted_output_urns, sql_parsing_result = (
            self._extract_lineage_from_airflow3(task, task_instance, complete)
        )
        logger.debug(
            f"Lineage extraction result for task {task.task_id}: inputs={len(extracted_input_urns)}, outputs={len(extracted_output_urns)}, sql_parsing_result={'present' if sql_parsing_result else 'None'}"
        )
        input_urns.extend(extracted_input_urns)
        output_urns.extend(extracted_output_urns)

        # Process SQL parsing result
        sql_input_urns, sql_output_urns, sql_fine_grained_lineages = (
            self._process_sql_parsing_result(datajob, sql_parsing_result)
        )
        input_urns.extend(sql_input_urns)
        output_urns.extend(sql_output_urns)

        # Deduplicate within sql_fine_grained_lineages before adding to fine_grained_lineages
        # This prevents duplicates from SQL parsing result itself
        if sql_fine_grained_lineages:
            seen_sql_fgl_keys = {}
            unique_sql_fgls = []
            for fgl in sql_fine_grained_lineages:
                fgl_key = (
                    tuple(sorted(fgl.upstreams)) if fgl.upstreams else (),
                    tuple(sorted(fgl.downstreams)) if fgl.downstreams else (),
                    fgl.upstreamType,
                    fgl.downstreamType,
                )
                if fgl_key not in seen_sql_fgl_keys:
                    seen_sql_fgl_keys[fgl_key] = fgl
                    unique_sql_fgls.append(fgl)

            if len(unique_sql_fgls) != len(sql_fine_grained_lineages):
                logger.debug(
                    f"Deduplicated SQL parsing FGLs: {len(sql_fine_grained_lineages)} -> {len(unique_sql_fgls)} for task {datajob.urn}"
                )
            sql_fine_grained_lineages = unique_sql_fgls

        fine_grained_lineages.extend(sql_fine_grained_lineages)

        # Add DataHub-native inlets/outlets and Airflow Assets
        input_urns.extend(
            extract_urns_from_iolets(
                get_task_inlets(task),
                self.config.capture_airflow_assets,
                env=self.config.cluster,
            )
        )
        output_urns.extend(
            extract_urns_from_iolets(
                get_task_outlets(task),
                self.config.capture_airflow_assets,
                env=self.config.cluster,
            )
        )

        # Write the lineage to the datajob object
        datajob.inlets.extend(entities_to_dataset_urn_list(input_urns))
        datajob.outlets.extend(entities_to_dataset_urn_list(output_urns))
        datajob.upstream_urns.extend(entities_to_datajob_urn_list(input_urns))

        # Set fine_grained_lineages - already deduplicated (sql_fine_grained_lineages)
        datajob.fine_grained_lineages = fine_grained_lineages

        # Merge with datajob from task start (if this is task completion)
        if complete:
            original_datajob = self._datajob_holder.get(str(datajob.urn), None)
        else:
            self._datajob_holder[str(datajob.urn)] = datajob
            original_datajob = None

        if original_datajob:
            logger.debug("Merging start datajob into finish datajob")
            datajob.inlets.extend(original_datajob.inlets)
            datajob.outlets.extend(original_datajob.outlets)
            datajob.upstream_urns.extend(original_datajob.upstream_urns)
            # Don't merge fine_grained_lineages from start - completion lineage is complete and accurate
            # This avoids duplicates when SQLParser extracts lineage on both start and completion

            for k, v in original_datajob.properties.items():
                datajob.properties.setdefault(k, v)

        # Deduplicate inlets/outlets
        datajob.inlets = list(sorted(set(datajob.inlets), key=lambda x: str(x)))
        datajob.outlets = list(sorted(set(datajob.outlets), key=lambda x: str(x)))
        datajob.upstream_urns = list(
            sorted(set(datajob.upstream_urns), key=lambda x: str(x))
        )

        # Write all other OL facets as DataHub properties
        if task_metadata:
            for k, v in task_metadata.job_facets.items():
                datajob.properties[f"openlineage_job_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)  # type: ignore[arg-type]
                )

            for k, v in task_metadata.run_facets.items():
                datajob.properties[f"openlineage_run_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)  # type: ignore[arg-type]
                )

    def check_kill_switch(self) -> bool:
        """
        Check kill switch for Airflow 3.0+.

        Variable.get() cannot be called from listener hooks in Airflow 3.0+
        because it creates a database session commit which breaks HA locks.
        Use environment variable instead.
        """
        if (
            os.getenv(
                f"AIRFLOW_VAR_{KILL_SWITCH_VARIABLE_NAME}".upper(), "false"
            ).lower()
            == "true"
        ):
            logger.debug("DataHub listener disabled by kill switch (env var)")
            return True
        return False

    def _prepare_task_context(
        self, task_instance: "TaskInstance", for_completion: bool = False
    ) -> Optional[Tuple["DagRun", "Operator", "DAG"]]:
        """
        Prepare task context by extracting DAG run, task, and DAG from task instance.

        Args:
            task_instance: The Airflow task instance
            for_completion: If True, retrieves task from holder for completion events

        Returns:
            Tuple of (dagrun, task, dag) or None if context cannot be prepared
        """
        # Get dagrun in a version-compatible way (Airflow 2.x vs 3.x)
        dagrun: "DagRun" = _get_dagrun_from_task_instance(task_instance)

        if self.config.render_templates:
            task_instance = _render_templates(task_instance)

        # Get task - should be directly available on task_instance
        task = task_instance.task if hasattr(task_instance, "task") else None

        if task is None:
            return None

        dag: "DAG" = task.dag  # type: ignore[assignment]

        # Check if DAG is allowed by filter pattern
        if not self.config.dag_filter_pattern.allowed(dag.dag_id):
            logger.debug(f"DAG {dag.dag_id} is not allowed by the pattern")
            return None

        # Task type can vary between Airflow versions (MappedOperator, SerializedBaseOperator, etc.)
        return dagrun, task, dag  # type: ignore[return-value]

    def _generate_and_emit_datajob(
        self,
        dagrun: "DagRun",
        task: "Operator",
        dag: "DAG",
        task_instance: "TaskInstance",
        complete: bool = False,
    ) -> DataJob:
        """
        Generate DataJob with lineage and emit it to DataHub.

        Args:
            dagrun: The DAG run
            task: The task operator
            dag: The DAG
            task_instance: The task instance
            complete: Whether this is for task completion

        Returns:
            The generated DataJob
        """
        # Check if emitter is available
        emitter = self._get_emitter()
        if emitter is None:
            logger.warning(
                f"DataHub emitter not available for task {task.task_id}, skipping metadata emission"
            )
            # Still generate the datajob for tracking purposes, but don't emit
            datajob = AirflowGenerator.generate_datajob(
                cluster=self.config.cluster,
                task=task,  # type: ignore[arg-type]
                dag=dag,
                capture_tags=self.config.capture_tags_info,
                capture_owner=self.config.capture_ownership_info,
                config=self.config,
            )
            self._extract_lineage(
                datajob, dagrun, task, task_instance, complete=complete
            )  # type: ignore[arg-type]
            return datajob

        datajob = AirflowGenerator.generate_datajob(
            cluster=self.config.cluster,
            task=task,  # type: ignore[arg-type]
            dag=dag,
            capture_tags=self.config.capture_tags_info,
            capture_owner=self.config.capture_ownership_info,
            config=self.config,
        )

        # Add lineage info
        self._extract_lineage(datajob, dagrun, task, task_instance, complete=complete)  # type: ignore[arg-type]

        # Emit DataJob MCPs
        # Skip dataJobInputOutput aspects on task start to avoid file emitter merging duplicates
        # The file emitter merges aspects with the same entity URN and aspect name,
        # which causes FGLs from start and completion to be combined into duplicates.
        # We only emit the aspect on completion when lineage is complete and accurate.
        for mcp in datajob.generate_mcp(
            generate_lineage=self.config.enable_datajob_lineage,
            materialize_iolets=self.config.materialize_iolets,
        ):
            # Skip dataJobInputOutput aspects on task start
            if not complete:
                if isinstance(mcp.aspect, DataJobInputOutputClass):
                    logger.debug(
                        f"Skipping dataJobInputOutput for task {task.task_id} on start "
                        f"(will be emitted on completion to avoid file emitter merging duplicates)"
                    )
                    continue

            emitter.emit(mcp, self._make_emit_callback())

        status_text = f"finish w/ status {complete}" if complete else "start"
        logger.debug(f"Emitted DataHub Datajob {status_text}: {datajob}")

        return datajob

    @hookimpl
    @run_in_thread
    def on_task_instance_running(  # type: ignore[no-untyped-def]  # Airflow 3.0 removed previous_state parameter
        self, previous_state, task_instance: "TaskInstance", **kwargs
    ) -> None:
        # In Airflow 3.0, the session parameter was removed from the hook signature
        if self.check_kill_switch():
            return
        self._set_log_level()

        # This if statement mirrors the logic in https://github.com/OpenLineage/OpenLineage/pull/508.
        if not hasattr(task_instance, "task"):
            logger.warning(
                f"No task set for task_id: {task_instance.task_id} - "  # type: ignore[attr-defined]
                f"dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}"  # type: ignore[attr-defined]
            )
            return

        logger.debug(
            f"DataHub listener got notification about task instance start for {task_instance.task_id} of dag {task_instance.dag_id}"
        )

        # Check if DAG is allowed before doing any expensive operations
        if not self.config.dag_filter_pattern.allowed(task_instance.dag_id):
            logger.debug(f"DAG {task_instance.dag_id} is not allowed by the pattern")
            return

        # Handle async operators by skipping deferred state
        if (
            hasattr(task_instance, "next_method")
            and task_instance.next_method is not None
        ):
            return

        # Render templates and extract context
        if self.config.render_templates:
            task_instance = _render_templates(task_instance)

        dagrun: "DagRun" = _get_dagrun_from_task_instance(task_instance)
        task = task_instance.task
        assert task is not None
        dag: "DAG" = task.dag  # type: ignore[assignment]

        # Airflow 3.0+ doesn't need task holder

        # Always emit DataFlow from task instance handler as a fallback.
        # In distributed Airflow deployments (Kubernetes, Astronomer), the on_dag_run_running
        # hook runs on the scheduler process, but the DataHub listener is only initialized
        # on worker processes. This means the scheduler's on_dag_run_running never triggers
        # DataFlow emission. By emitting here, we ensure the DataFlow exists before the
        # DataJob references it. DataHub's UPSERT semantics make duplicate emissions safe.
        self.on_dag_start(dagrun)

        # Generate and emit datajob
        # Task type can vary between Airflow versions (MappedOperator from different modules)
        datajob = self._generate_and_emit_datajob(
            dagrun,
            task,  # type: ignore[arg-type]
            dag,
            task_instance,
            complete=False,
        )

        # Emit process instance if capturing executions
        emitter = self._get_emitter()
        if self.config.capture_executions and emitter:
            dpi = AirflowGenerator.run_datajob(
                emitter=emitter,
                config=self.config,
                ti=task_instance,
                dag=dag,
                dag_run=dagrun,
                datajob=datajob,
                emit_templates=False,
            )
            logger.debug(f"Emitted DataHub DataProcess Instance start: {dpi}")

        if emitter:
            emitter.flush()

        logger.debug(
            f"DataHub listener finished processing notification about task instance start for {task_instance.task_id}"
        )

        self.materialize_iolets(datajob)

    def materialize_iolets(self, datajob: DataJob) -> None:
        if self.config.materialize_iolets:
            emitter = self._get_emitter()
            if emitter is None:
                logger.warning(
                    "DataHub emitter not available, skipping iolet materialization"
                )
                return

            for outlet in datajob.outlets:
                reported_time: int = int(time.time() * 1000)
                operation = OperationClass(
                    timestampMillis=reported_time,
                    operationType=OperationTypeClass.CREATE,
                    lastUpdatedTimestamp=reported_time,
                    actor=builder.make_user_urn("airflow"),
                )

                operation_mcp = MetadataChangeProposalWrapper(
                    entityUrn=str(outlet), aspect=operation
                )

                emitter.emit(operation_mcp)
                logger.debug(f"Emitted Dataset Operation: {outlet}")
        else:
            if self.graph:
                for outlet in datajob.outlets:
                    if not self.graph.exists(str(outlet)):
                        logger.warning(f"Dataset {str(outlet)} not materialized")
                for inlet in datajob.inlets:
                    if not self.graph.exists(str(inlet)):
                        logger.warning(f"Dataset {str(inlet)} not materialized")

    def on_task_instance_finish(
        self, task_instance: "TaskInstance", status: InstanceRunResult
    ) -> None:
        logger.debug(
            f"on_task_instance_finish called for task {task_instance.task_id} (dag_id={task_instance.dag_id}, status={status})"
        )
        # Prepare task context (handles template rendering, task retrieval, DAG filtering)
        context = self._prepare_task_context(task_instance, for_completion=True)
        if context is None:
            logger.debug(
                f"Task context preparation returned None for task {task_instance.task_id}"
            )
            return

        dagrun, task, dag = context
        logger.debug(
            f"Task context prepared for task {task_instance.task_id}: task_type={type(task).__name__}"
        )

        # Generate and emit datajob with lineage
        logger.debug(
            f"Generating and emitting DataJob for task {task_instance.task_id} (complete=True)"
        )
        datajob = self._generate_and_emit_datajob(
            dagrun, task, dag, task_instance, complete=True
        )

        # Emit process instance if capturing executions
        emitter = self._get_emitter()
        if self.config.capture_executions and emitter:
            dpi = AirflowGenerator.complete_datajob(
                emitter=emitter,
                cluster=self.config.cluster,
                ti=task_instance,
                dag=dag,
                dag_run=dagrun,
                datajob=datajob,
                result=status,
                config=self.config,
            )
            logger.debug(
                f"Emitted DataHub DataProcess Instance with status {status}: {dpi}"
            )
            # Emit inlet/outlet aspects for DataProcessInstance (emit_process_end only emits run event)
            # This matches the behavior of emit_process_start which calls generate_mcp()
            for mcp in dpi.generate_inlet_outlet_mcp(materialize_iolets=False):
                emitter.emit(mcp, self._make_emit_callback())

        if emitter:
            emitter.flush()

    @hookimpl
    @run_in_thread
    def on_task_instance_success(  # type: ignore[no-untyped-def]  # Airflow 3.0 removed previous_state parameter
        self, previous_state, task_instance: "TaskInstance", **kwargs
    ) -> None:
        logger.debug(
            f"on_task_instance_success hook called for task {task_instance.task_id} (dag_id={task_instance.dag_id})"
        )
        if self.check_kill_switch():
            logger.debug(
                f"Skipping task {task_instance.task_id} - kill switch is enabled"
            )
            return

        self._set_log_level()

        logger.debug(
            f"DataHub listener got notification about task instance success for {task_instance.task_id}"
        )
        self.on_task_instance_finish(task_instance, status=InstanceRunResult.SUCCESS)
        logger.debug(
            f"DataHub listener finished processing task instance success for {task_instance.task_id}"
        )

    @hookimpl
    @run_in_thread
    def on_task_instance_failed(  # type: ignore[no-untyped-def]  # Airflow 3.0 removed previous_state parameter
        self, previous_state, task_instance: "TaskInstance", **kwargs
    ) -> None:
        if self.check_kill_switch():
            return

        self._set_log_level()

        logger.debug(
            f"DataHub listener got notification about task instance failure for {task_instance.task_id}"
        )

        # TODO: Handle UP_FOR_RETRY state.
        # TODO: Use the error parameter (available in kwargs for Airflow 3.0+) for better error reporting
        self.on_task_instance_finish(task_instance, status=InstanceRunResult.FAILURE)
        logger.debug(
            f"DataHub listener finished processing task instance failure for {task_instance.task_id}"
        )

    def on_dag_start(self, dag_run: "DagRun") -> None:  # type: ignore[no-untyped-def]
        logger.debug(
            f"DataHub on_dag_start called for dag_id={dag_run.dag_id}, run_id={dag_run.run_id}"
        )
        dag = dag_run.dag
        if not dag:
            logger.warning(
                f"DataHub listener could not find DAG for {dag_run.dag_id} - {dag_run.run_id}. Dag won't be captured"
            )
            return

        logger.debug(f"Generating DataFlow for DAG: {dag.dag_id}")
        dataflow = AirflowGenerator.generate_dataflow(
            config=self.config,
            dag=dag,  # type: ignore[arg-type]
        )
        logger.debug(
            f"Generated DataFlow URN: {dataflow.urn}, tags: {dataflow.tags}, description: {dataflow.description}"
        )

        # Ensure emitter is initialized
        emitter = self._get_emitter()
        if emitter is None:
            logger.warning("DataHub emitter not available, skipping DataFlow emission")
            return

        # Emit dataflow
        logger.debug(f"Emitting DataFlow MCPs for {dataflow.urn}")
        dataflow.emit(emitter, callback=self._make_emit_callback())

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=str(dataflow.urn), aspect=StatusClass(removed=False)
        )
        emitter.emit(event)

        for task in dag.tasks:
            task_urn = builder.make_data_job_urn_with_flow(
                str(dataflow.urn), task.task_id
            )
            event = MetadataChangeProposalWrapper(
                entityUrn=task_urn, aspect=StatusClass(removed=False)
            )
            emitter.emit(event)

        if self.config.platform_instance:
            instance = make_dataplatform_instance_urn(
                platform="airflow",
                instance=self.config.platform_instance,
            )
            event = MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn("airflow"),
                    instance=instance,
                ),
            )
            emitter.emit(event)

        # emit tags
        for tag in dataflow.tags:
            tag_urn = builder.make_tag_urn(tag)

            event = MetadataChangeProposalWrapper(
                entityUrn=tag_urn, aspect=StatusClass(removed=False)
            )
            emitter.emit(event)

        browsePaths: List[BrowsePathEntryClass] = []
        if self.config.platform_instance:
            urn = make_dataplatform_instance_urn(
                "airflow", self.config.platform_instance
            )
            browsePaths.append(BrowsePathEntryClass(self.config.platform_instance, urn))
        browsePaths.append(BrowsePathEntryClass(str(dag.dag_id)))
        browse_path_v2_event: MetadataChangeProposalWrapper = (
            MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=BrowsePathsV2Class(
                    path=browsePaths,
                ),
            )
        )
        logger.debug(
            f"Emitting BrowsePathsV2 MCP: entityUrn={browse_path_v2_event.entityUrn}, paths={[getattr(p, 'path', str(p)) for p in browsePaths]}"
        )
        emitter.emit(browse_path_v2_event)
        logger.debug(f"Completed emitting all DataFlow MCPs for {dataflow.urn}")

        if dag.dag_id == _DATAHUB_CLEANUP_DAG:
            assert self.graph

            logger.debug("Initiating the cleanup of obsolete data from datahub")

            # get all ingested dataflow and datajob
            ingested_dataflow_urns = list(
                self.graph.get_urns_by_filter(
                    platform="airflow",
                    entity_types=["dataFlow"],
                    platform_instance=self.config.platform_instance,
                )
            )
            ingested_datajob_urns = list(
                self.graph.get_urns_by_filter(
                    platform="airflow",
                    entity_types=["dataJob"],
                    platform_instance=self.config.platform_instance,
                )
            )

            # filter the ingested dataflow and datajob based on the cluster
            filtered_ingested_dataflow_urns: List = []
            filtered_ingested_datajob_urns: List = []

            for ingested_dataflow_urn in ingested_dataflow_urns:
                data_flow_aspect = self.graph.get_aspect(
                    entity_urn=ingested_dataflow_urn, aspect_type=DataFlowKeyClass
                )
                if (
                    data_flow_aspect is not None
                    and data_flow_aspect.flowId != _DATAHUB_CLEANUP_DAG
                    and data_flow_aspect is not None
                    and data_flow_aspect.cluster == self.config.cluster
                ):
                    filtered_ingested_dataflow_urns.append(ingested_dataflow_urn)

            for ingested_datajob_urn in ingested_datajob_urns:
                data_job_aspect = self.graph.get_aspect(
                    entity_urn=ingested_datajob_urn, aspect_type=DataJobKeyClass
                )
                if (
                    data_job_aspect is not None
                    and data_job_aspect.flow in filtered_ingested_dataflow_urns
                ):
                    filtered_ingested_datajob_urns.append(ingested_datajob_urn)

            # get all airflow dags
            all_airflow_dags = SerializedDagModel.read_all_dags().values()

            airflow_flow_urns: List = []
            airflow_job_urns: List = []

            for dag in all_airflow_dags:
                flow_urn = builder.make_data_flow_urn(
                    orchestrator="airflow",
                    flow_id=dag.dag_id,
                    cluster=self.config.cluster,
                    platform_instance=self.config.platform_instance,
                )
                airflow_flow_urns.append(flow_urn)

                for task in dag.tasks:
                    airflow_job_urns.append(
                        builder.make_data_job_urn_with_flow(str(flow_urn), task.task_id)
                    )

            obsolete_pipelines = set(filtered_ingested_dataflow_urns) - set(
                airflow_flow_urns
            )
            obsolete_tasks = set(filtered_ingested_datajob_urns) - set(airflow_job_urns)

            obsolete_urns = obsolete_pipelines.union(obsolete_tasks)

            asyncio.run(self._soft_delete_obsolete_urns(obsolete_urns=obsolete_urns))

            logger.debug(f"total pipelines removed = {len(obsolete_pipelines)}")
            logger.debug(f"total tasks removed = {len(obsolete_tasks)}")

    if HAS_AIRFLOW_DAG_LISTENER_API:

        @hookimpl
        @run_in_thread
        def on_dag_run_running(self, dag_run: "DagRun", msg: str) -> None:
            logger.debug(
                f"DataHub on_dag_run_running called for dag_id={dag_run.dag_id}, run_id={dag_run.run_id}, msg={msg}"
            )
            if self.check_kill_switch():
                return

            self._set_log_level()

            logger.debug(
                f"DataHub listener got notification about dag run start for {dag_run.dag_id}"
            )

            assert dag_run.dag_id
            if not self.config.dag_filter_pattern.allowed(dag_run.dag_id):
                logger.debug(f"DAG {dag_run.dag_id} is not allowed by the pattern")
                return

            self.on_dag_start(dag_run)
            emitter = self._get_emitter()
            if emitter:
                emitter.flush()

    # TODO: Add hooks for on_dag_run_success, on_dag_run_failed -> call AirflowGenerator.complete_dataflow

    if HAS_AIRFLOW_DATASET_LISTENER_API:

        @hookimpl
        @run_in_thread
        def on_dataset_created(self, dataset: "Dataset") -> None:  # type: ignore[no-untyped-def]
            self._set_log_level()

            logger.debug(
                f"DataHub listener got notification about dataset create for {dataset}"
            )

        @hookimpl
        @run_in_thread
        def on_dataset_changed(self, dataset: "Dataset") -> None:  # type: ignore[no-untyped-def]
            self._set_log_level()

            logger.debug(
                f"DataHub listener got notification about dataset change for {dataset}"
            )

    async def _soft_delete_obsolete_urns(self, obsolete_urns):
        delete_tasks = [self._delete_obsolete_data(urn) for urn in obsolete_urns]
        await asyncio.gather(*delete_tasks)

    async def _delete_obsolete_data(self, obsolete_urn):
        assert self.graph

        if self.graph.exists(str(obsolete_urn)):
            self.graph.soft_delete_entity(str(obsolete_urn))
