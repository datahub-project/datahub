import asyncio
import copy
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

import airflow

# Import Airflow 2.x specific shims (clean, no cross-version complexity)
import airflow.version
import packaging.version

# Note: We intentionally don't import Variable here to avoid DB access in listeners.
# See check_kill_switch() for details on why we use os.getenv() instead.
from airflow.models.serialized_dag import SerializedDagModel

# Import Airflow 2.x compatibility and patches before any Airflow imports
# Wrap in try-except to ensure listener can still load if compatibility module has issues
try:
    from datahub_airflow_plugin.airflow2 import _airflow_compat  # noqa: F401
except Exception as e:
    # Log but don't fail - compatibility patches are optional
    import logging

    logger = logging.getLogger(__name__)
    logger.warning(
        f"Could not import Airflow 2.x compatibility module: {e}. Some patches may not be applied."
    )

# Conditional import for OpenLineage (may not be installed)
try:
    from openlineage.client.serde import Serde

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    # Not available when openlineage packages aren't installed
    Serde = None  # type: ignore[assignment,misc]
    OPENLINEAGE_AVAILABLE = False

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
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
from datahub_airflow_plugin._airflow_asset_adapter import extract_urns_from_iolets
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin._constants import (
    DATAHUB_SQL_PARSING_RESULT_KEY,
    SQL_PARSING_RESULT_KEY,
)
from datahub_airflow_plugin._datahub_ol_adapter import translate_ol_to_datahub_urn
from datahub_airflow_plugin._version import __package_name__, __version__
from datahub_airflow_plugin.airflow2._extractors import ExtractorManager
from datahub_airflow_plugin.airflow2._shims import (
    OpenLineagePlugin,
    Operator,
    TaskHolder,
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

# Feature flags for Airflow 2.x
AIRFLOW_VERSION = packaging.version.parse(airflow.version.version)
HAS_AIRFLOW_DAG_LISTENER_API: bool = AIRFLOW_VERSION >= packaging.version.parse(
    "2.5.0.dev0"
)
HAS_AIRFLOW_DATASET_LISTENER_API: bool = AIRFLOW_VERSION >= packaging.version.parse(
    "2.8.0.dev0"
)

_F = TypeVar("_F", bound=Callable[..., None])
if TYPE_CHECKING:
    from airflow.datasets import Dataset
    from airflow.models import DAG, DagRun, TaskInstance

    # To placate mypy on Airflow versions that don't have the listener API,
    # we define a dummy hookimpl that's an identity function.

    def hookimpl(f: _F) -> _F:  # type: ignore[misc]
        return f

else:
    from airflow.listeners import hookimpl

logger = logging.getLogger(__name__)


def _get_dagrun_from_task_instance(task_instance: "TaskInstance") -> "DagRun":
    """
    Get a DagRun from a TaskInstance (Airflow 2.x).

    In Airflow 2.x, TaskInstance has a dag_run attribute.
    """
    return task_instance.dag_run  # type: ignore[return-value]


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
            logger.info(
                f"OpenLineage plugin state: listeners={len(getattr(OpenLineagePlugin, 'listeners', []))} items, "
                f"disable_openlineage_plugin={plugin_config.disable_openlineage_plugin}"
            )

        if plugin_config.disable_openlineage_plugin and OpenLineagePlugin is not None:
            # Deactivate the OpenLineagePlugin listener to avoid conflicts/errors.
            OpenLineagePlugin.listeners = []
            logger.info("Cleared OpenLineage plugin listeners")

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
    # Render templates in a copy of the task instance.
    # This is necessary to get the correct operator args in the extractors.

    try:
        task_instance_copy = copy.deepcopy(task_instance)
        task_instance_copy.render_templates()
        return task_instance_copy
    except Exception as e:
        logger.info(
            f"Error rendering templates in DataHub listener. Jinja-templated variables will not be extracted correctly: {e}. Template rendering improves SQL parsing accuracy. If this causes issues, you can disable it by setting `render_templates` to `false` in the DataHub plugin configuration."
        )
        return task_instance


class DataHubListener:
    __name__ = "DataHubListener"

    def __init__(self, config: DatahubLineageConfig):
        self.config = config
        self._set_log_level()

        self._emitter = config.make_emitter_hook().make_emitter()
        self._graph: Optional[DataHubGraph] = None
        logger.info(f"DataHub plugin v2 using {repr(self._emitter)}")

        # See discussion here https://github.com/OpenLineage/OpenLineage/pull/508 for
        # why we need to keep track of tasks ourselves.
        # Note: TaskHolder is only available in legacy openlineage-airflow package,
        # not in apache-airflow-providers-openlineage (where task_instance.task is directly available)
        self._task_holder: Any = TaskHolder() if TaskHolder is not None else None

        # In our case, we also want to cache the initial datajob object
        # so that we can add to it when the task completes.
        self._datajob_holder: Dict[str, DataJob] = {}

        # Create extractor_manager for Airflow 2.x with patch/extractor configuration
        self.extractor_manager = ExtractorManager(
            patch_sql_parser=self.config.patch_sql_parser,
            patch_snowflake_schema=self.config.patch_snowflake_schema,
            extract_athena_operator=self.config.extract_athena_operator,
            extract_bigquery_insert_job_operator=self.config.extract_bigquery_insert_job_operator,
            extract_teradata_operator=self.config.extract_teradata_operator,
        )

        # This "inherits" from types.ModuleType to avoid issues with Airflow's listener plugin loader.
        # It previously (v2.4.x and likely other versions too) would throw errors if it was not a module.
        # https://github.com/apache/airflow/blob/e99a518970b2d349a75b1647f6b738c8510fa40e/airflow/listeners/listener.py#L56
        # self.__class__ = types.ModuleType

    @property
    def emitter(self):
        return self._emitter

    @property
    def graph(self) -> Optional[DataHubGraph]:
        if self._graph:
            return self._graph

        if isinstance(self._emitter, DatahubRestEmitter) and not isinstance(
            self._emitter, DataHubGraph
        ):
            # This is lazy initialized to avoid throwing errors on plugin load.
            self._graph = self._emitter.to_graph()
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

    def _extract_lineage_from_airflow2(
        self,
        datajob: DataJob,
        dagrun: "DagRun",
        task: "Operator",
        task_instance: "TaskInstance",
        complete: bool,
    ) -> Tuple[List[str], List[str], Optional[SqlParsingResult], Optional[Any]]:
        """Extract lineage using Airflow 2.x extractor system."""
        input_urns: List[str] = []
        output_urns: List[str] = []

        task_metadata = self.extractor_manager.extract_metadata(  # type: ignore[union-attr]
            dagrun,
            task,
            complete=complete,
            task_instance=task_instance,
            task_uuid=str(datajob.urn),
            graph=self.graph,
        )
        logger.debug(f"Got task metadata: {task_metadata}")

        # Translate task_metadata.inputs/outputs to DataHub URNs.
        input_urns.extend(
            translate_ol_to_datahub_urn(dataset) for dataset in task_metadata.inputs
        )
        output_urns.extend(
            translate_ol_to_datahub_urn(dataset) for dataset in task_metadata.outputs
        )

        # Extract and remove DataHub's custom SQL parsing result from run_facets.
        # We use .pop() (not .get()) to remove the key so that when task_metadata.run_facets
        # are serialized as OpenLineage facets later, they don't include DataHub-specific
        # additions. This keeps the OpenLineage facets clean and standards-compliant.
        sql_parsing_result = task_metadata.run_facets.pop(SQL_PARSING_RESULT_KEY, None)
        # Also check for DATAHUB_SQL_PARSING_RESULT_KEY (used by provider mode patches)
        if DATAHUB_SQL_PARSING_RESULT_KEY in task_metadata.run_facets:
            if sql_parsing_result is None:
                sql_parsing_result = task_metadata.run_facets.pop(
                    DATAHUB_SQL_PARSING_RESULT_KEY, None
                )
            else:
                # If both keys exist, prefer DATAHUB_SQL_PARSING_RESULT_KEY and remove the other
                task_metadata.run_facets.pop(DATAHUB_SQL_PARSING_RESULT_KEY, None)

        return input_urns, output_urns, sql_parsing_result, task_metadata

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
            return input_urns, output_urns, fine_grained_lineages

        if error := sql_parsing_result.debug_info.error:
            logger.info(f"SQL parsing error: {error}", exc_info=error)
            datajob.properties["datahub_sql_parser_error"] = (
                f"{type(error).__name__}: {error}"
            )

        if not sql_parsing_result.debug_info.table_error:
            input_urns.extend(sql_parsing_result.in_tables)
            output_urns.extend(sql_parsing_result.out_tables)

            if sql_parsing_result.column_lineage:
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
        if not self.config.enable_datajob_lineage:
            return

        input_urns: List[str] = []
        output_urns: List[str] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        task_metadata = None
        sql_parsing_result: Optional[SqlParsingResult] = None

        # Extract lineage using Airflow 2.x extractors
        if self.config.enable_extractors:
            (
                extracted_input_urns,
                extracted_output_urns,
                sql_parsing_result,
                task_metadata,
            ) = self._extract_lineage_from_airflow2(
                datajob, dagrun, task, task_instance, complete
            )
            input_urns.extend(extracted_input_urns)
            output_urns.extend(extracted_output_urns)

        # Process SQL parsing result
        sql_input_urns, sql_output_urns, sql_fine_grained_lineages = (
            self._process_sql_parsing_result(datajob, sql_parsing_result)
        )
        input_urns.extend(sql_input_urns)
        output_urns.extend(sql_output_urns)
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
        datajob.fine_grained_lineages.extend(fine_grained_lineages)

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
            datajob.fine_grained_lineages.extend(original_datajob.fine_grained_lineages)

            for k, v in original_datajob.properties.items():
                datajob.properties.setdefault(k, v)

        # Deduplicate inlets/outlets
        datajob.inlets = list(sorted(set(datajob.inlets), key=lambda x: str(x)))
        datajob.outlets = list(sorted(set(datajob.outlets), key=lambda x: str(x)))
        datajob.upstream_urns = list(
            sorted(set(datajob.upstream_urns), key=lambda x: str(x))
        )

        # Write all other OL facets as DataHub properties
        if task_metadata and Serde is not None:
            for k, v in task_metadata.job_facets.items():
                # Redaction is only available with legacy openlineage-airflow package
                value_to_serialize = (
                    redact_with_exclusions(v)
                    if redact_with_exclusions is not None
                    else v
                )  # type: ignore[arg-type]
                datajob.properties[f"openlineage_job_facet_{k}"] = Serde.to_json(
                    value_to_serialize
                )

            for k, v in task_metadata.run_facets.items():
                # Skip DataHub-specific keys that can't be serialized by OpenLineage's Serde
                # These are SqlParsingResult objects, not attrs-decorated classes
                if k in (SQL_PARSING_RESULT_KEY, DATAHUB_SQL_PARSING_RESULT_KEY):
                    logger.debug(
                        f"Skipping serialization of DataHub-specific run_facet key: {k}"
                    )
                    continue
                # Redaction is only available with legacy openlineage-airflow package
                value_to_serialize = (
                    redact_with_exclusions(v)
                    if redact_with_exclusions is not None
                    else v
                )  # type: ignore[arg-type]
                datajob.properties[f"openlineage_run_facet_{k}"] = Serde.to_json(
                    value_to_serialize
                )

    def check_kill_switch(self) -> bool:
        """
        Check kill switch using environment variable.

        We use os.getenv() instead of Variable.get() because Variable.get()
        creates a new database session and commits it. When called from listener
        hooks (which execute during SQLAlchemy's after_flush event, before the
        main transaction commits), this nested commit can corrupt the outer
        transaction state and cause data loss.

        Specifically, this was causing TaskInstanceHistory records to not be
        persisted for retried tasks. See: https://github.com/apache/airflow/pull/48780

        Users can set the kill switch via environment variable:
            export AIRFLOW_VAR_DATAHUB_AIRFLOW_PLUGIN_DISABLE_LISTENER=true
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

        # Get task - different logic for start vs completion events
        if for_completion:
            # For completion: prefer task attribute, fallback to holder
            if getattr(task_instance, "task", None):
                task = task_instance.task
            elif hasattr(self._task_holder, "get_task"):
                task = self._task_holder.get_task(task_instance)
            else:
                task = None
        else:
            # For start: task should be directly available
            task = task_instance.task

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

            self.emitter.emit(mcp, self._make_emit_callback())

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

        # Handle async operators in Airflow 2.3 by skipping deferred state.
        # Inspired by https://github.com/OpenLineage/OpenLineage/pull/1601
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

        # Store task for later retrieval (only available with legacy openlineage-airflow package)
        if self._task_holder is not None:
            self._task_holder.set_task(task_instance)

        # If we don't have the DAG listener API, emit DAG start event
        if not HAS_AIRFLOW_DAG_LISTENER_API:
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
        if self.config.capture_executions:
            dpi = AirflowGenerator.run_datajob(
                emitter=self.emitter,
                config=self.config,
                ti=task_instance,
                dag=dag,
                dag_run=dagrun,
                datajob=datajob,
                emit_templates=False,
            )
            logger.debug(f"Emitted DataHub DataProcess Instance start: {dpi}")

        self.emitter.flush()

        logger.debug(
            f"DataHub listener finished processing notification about task instance start for {task_instance.task_id}"
        )

        self.materialize_iolets(datajob)

    def materialize_iolets(self, datajob: DataJob) -> None:
        if self.config.materialize_iolets:
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

                self.emitter.emit(operation_mcp)
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
        # Prepare task context (handles template rendering, task retrieval, DAG filtering)
        context = self._prepare_task_context(task_instance, for_completion=True)
        if context is None:
            return

        dagrun, task, dag = context

        # Generate and emit datajob with lineage
        datajob = self._generate_and_emit_datajob(
            dagrun, task, dag, task_instance, complete=True
        )

        # Emit process instance if capturing executions
        if self.config.capture_executions:
            dpi = AirflowGenerator.complete_datajob(
                emitter=self.emitter,
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
                self.emitter.emit(mcp, self._make_emit_callback())

        # Materialize iolets on completion (outlets may be populated during execution)
        # This ensures operation aspects are emitted for datasets created during task execution
        self.materialize_iolets(datajob)

        self.emitter.flush()

    @hookimpl
    @run_in_thread
    def on_task_instance_success(  # type: ignore[no-untyped-def]  # Airflow 3.0 removed previous_state parameter
        self, previous_state, task_instance: "TaskInstance", **kwargs
    ) -> None:
        if self.check_kill_switch():
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
        dag = dag_run.dag
        if not dag:
            logger.warning(
                f"DataHub listener could not find DAG for {dag_run.dag_id} - {dag_run.run_id}. Dag won't be captured"
            )
            return

        dataflow = AirflowGenerator.generate_dataflow(
            config=self.config,
            dag=dag,  # type: ignore[arg-type]
        )
        dataflow.emit(self.emitter, callback=self._make_emit_callback())
        logger.debug(f"Emitted DataHub DataFlow: {dataflow}")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=str(dataflow.urn), aspect=StatusClass(removed=False)
        )
        self.emitter.emit(event)

        for task in dag.tasks:
            task_urn = builder.make_data_job_urn_with_flow(
                str(dataflow.urn), task.task_id
            )
            event = MetadataChangeProposalWrapper(
                entityUrn=task_urn, aspect=StatusClass(removed=False)
            )
            self.emitter.emit(event)

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
            self.emitter.emit(event)

        # emit tags
        for tag in dataflow.tags:
            tag_urn = builder.make_tag_urn(tag)

            event = MetadataChangeProposalWrapper(
                entityUrn=tag_urn, aspect=StatusClass(removed=False)
            )
            self.emitter.emit(event)

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
        self.emitter.emit(browse_path_v2_event)

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
            self.emitter.flush()

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
