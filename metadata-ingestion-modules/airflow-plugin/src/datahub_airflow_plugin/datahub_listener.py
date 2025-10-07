import asyncio
import copy
import functools
import logging
import os
import platform
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
from airflow.models import Variable
from airflow.models.serialized_dag import SerializedDagModel
from openlineage.client.serde import Serde

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

# Import OpenLineage compatibility shims
from datahub_airflow_plugin._airflow_shims import (
    HAS_AIRFLOW_DAG_LISTENER_API,
    HAS_AIRFLOW_DATASET_LISTENER_API,
    OpenLineagePlugin,
    Operator,
    TaskHolder,
    get_task_inlets,
    get_task_outlets,
    redact_with_exclusions,
)

# Import version-specific utilities
from datahub_airflow_plugin._airflow_version_specific import IS_AIRFLOW_3_OR_HIGHER
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin._datahub_ol_adapter import translate_ol_to_datahub_urn
from datahub_airflow_plugin._version import __package_name__, __version__
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator

# Only import extractors on Airflow < 3.0
# On Airflow 3.0+, we use the SQLParser patch instead
if not IS_AIRFLOW_3_OR_HIGHER:
    from datahub_airflow_plugin._extractors import (
        SQL_PARSING_RESULT_KEY,
        ExtractorManager,
    )
else:
    # On Airflow 3.0+, extractors are not used
    SQL_PARSING_RESULT_KEY = None  # type: ignore
    ExtractorManager = None  # type: ignore

from datahub_airflow_plugin.entities import (
    _Entity,
    entities_to_datajob_urn_list,
    entities_to_dataset_urn_list,
)

# Check if we're on macOS with Airflow 3.0+ (for fork-safety workarounds)
_IS_MACOS_AIRFLOW3 = platform.system() == "Darwin" and IS_AIRFLOW_3_OR_HIGHER

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
    Get a DagRun from a TaskInstance, handling both Airflow 2.x and 3.x.

    In Airflow 2.x, TaskInstance has a dag_run attribute.
    In Airflow 3.x, RuntimeTaskInstance doesn't have dag_run, so we create a
    compatible object with the attributes we need.
    """
    # Try to get dag_run directly (Airflow 2.x)
    if hasattr(task_instance, "dag_run") and task_instance.dag_run is not None:
        return task_instance.dag_run  # type: ignore[return-value]

    # For Airflow 3.x RuntimeTaskInstance, create a dag_run-like object
    # with the attributes we need
    class DagRunProxy:
        """Proxy object that provides dag_run-like attributes from RuntimeTaskInstance."""

        def __init__(self, ti: "TaskInstance"):
            self.ti = ti

        @property
        def dag(self):
            """Get DAG from task.dag instead of dag_run.dag"""
            task = getattr(self.ti, "task", None)
            if task:
                return task.dag
            return None

        @property
        def dag_id(self):
            """Get dag_id directly from task instance"""
            return getattr(self.ti, "dag_id", None)

        @property
        def run_id(self):
            """Get run_id directly from task instance"""
            return getattr(self.ti, "run_id", None)

    return DagRunProxy(task_instance)  # type: ignore[return-value]


_airflow_listener_initialized = False
_airflow_listener: Optional["DataHubListener"] = None

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
    # Using globals instead of functools.lru_cache to make testing easier.
    global _airflow_listener_initialized
    global _airflow_listener

    if not _airflow_listener_initialized:
        _airflow_listener_initialized = True

        plugin_config = get_lineage_config()

        if plugin_config.enabled:
            _airflow_listener = DataHubListener(config=plugin_config)
            logger.info(
                f"DataHub plugin v2 (package: {__package_name__} and version: {__version__}) listener initialized with config: {plugin_config}"
            )

            # Skip telemetry on macOS with Airflow 3.0+ to avoid fork-unsafe network calls
            # that cause SIGSEGV crashes in task workers
            if not _IS_MACOS_AIRFLOW3:
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
            else:
                logger.debug(
                    "Skipping telemetry on macOS with Airflow 3.0+ to prevent SIGSEGV"
                )

        # Debug: Log OpenLineage plugin state
        if OpenLineagePlugin:
            logger.info(
                f"OpenLineage plugin state: listeners={len(getattr(OpenLineagePlugin, 'listeners', []))} items, "
                f"disable_openlineage_plugin={plugin_config.disable_openlineage_plugin}"
            )

        if plugin_config.disable_openlineage_plugin and OpenLineagePlugin:
            # Deactivate the OpenLineagePlugin listener to avoid conflicts/errors.
            OpenLineagePlugin.listeners = []
            logger.info("Cleared OpenLineage plugin listeners")

    return _airflow_listener


def run_in_thread(f: _F) -> _F:
    # This is also responsible for catching exceptions and logging them.

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            if _RUN_IN_THREAD:
                # A poor-man's timeout mechanism.
                # This ensures that we don't hang the task if the extractors
                # are slow or the DataHub API is slow to respond.

                thread = threading.Thread(
                    target=f, args=args, kwargs=kwargs, daemon=True
                )
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
            logger.warning(e, exc_info=True)

    return cast(_F, wrapper)


def _render_templates(task_instance: "TaskInstance") -> "TaskInstance":
    # Render templates in a copy of the task instance.
    # This is necessary to get the correct operator args in the extractors.

    # In Airflow 3.0+, RuntimeTaskInstance contains unpickleable thread locks,
    # so we cannot use deepcopy. Templates should already be rendered by Airflow
    # before the task executes, so we can skip this step.
    if IS_AIRFLOW_3_OR_HIGHER:
        # For Airflow 3.0+, templates are already rendered by the task execution system
        # RuntimeTaskInstance.task contains the operator with rendered templates
        logger.debug(
            "Skipping template rendering for Airflow 3.0+ (already rendered by task worker)"
        )
        return task_instance

    # For Airflow 2.x, we need to render templates manually
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
        # For Airflow 3.0+, TaskHolder may be a dict stub, so we need to handle it gracefully
        try:
            self._task_holder = TaskHolder()
        except (TypeError, AttributeError):
            # TaskHolder is just a dict stub in Airflow 3.0, use a real dict instead
            self._task_holder = {}  # type: ignore

        # In our case, we also want to cache the initial datajob object
        # so that we can add to it when the task completes.
        self._datajob_holder: Dict[str, DataJob] = {}

        # Only create extractor_manager on Airflow < 3.0
        if ExtractorManager is not None:
            self.extractor_manager = ExtractorManager()
        else:
            self.extractor_manager = None

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

        # Extract SQL parsing result from task_metadata
        sql_parsing_result = task_metadata.run_facets.pop(SQL_PARSING_RESULT_KEY, None)

        return input_urns, output_urns, sql_parsing_result, task_metadata

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
            if not hasattr(task, facet_method_name):
                logger.debug(f"Task {task.task_id} does not have OpenLineage support")
                return input_urns, output_urns, sql_parsing_result

            facet_method = getattr(task, facet_method_name)

            try:
                # Call the appropriate facet method
                operator_lineage = (
                    facet_method(task_instance) if complete else facet_method()
                )

                if not operator_lineage:
                    logger.debug("OpenLineage facet method returned None")
                    return input_urns, output_urns, sql_parsing_result

                logger.debug(
                    f"Got OpenLineage operator lineage: inputs={len(operator_lineage.inputs)}, outputs={len(operator_lineage.outputs)}"
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
                DATAHUB_SQL_PARSING_RESULT_KEY = "datahub_sql_parsing_result"
                if DATAHUB_SQL_PARSING_RESULT_KEY in operator_lineage.run_facets:
                    sql_parsing_result = operator_lineage.run_facets[
                        DATAHUB_SQL_PARSING_RESULT_KEY
                    ]  # type: ignore
                    logger.debug(
                        f"Found DataHub SQL parsing result with {len(sql_parsing_result.column_lineage or [])} column lineages"
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

        # Extract lineage using version-specific method
        if self.config.enable_extractors and self.extractor_manager is not None:
            # Airflow 2.x: Use extractors
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
        elif self.extractor_manager is None:
            # Airflow 3.0+: Get lineage from OpenLineage integration
            extracted_input_urns, extracted_output_urns, sql_parsing_result = (
                self._extract_lineage_from_airflow3(task, task_instance, complete)
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

        # Add DataHub-native inlets/outlets
        input_urns.extend(
            iolet.urn for iolet in get_task_inlets(task) if isinstance(iolet, _Entity)
        )
        output_urns.extend(
            iolet.urn for iolet in get_task_outlets(task) if isinstance(iolet, _Entity)
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
        if task_metadata:
            for k, v in task_metadata.job_facets.items():
                datajob.properties[f"openlineage_job_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)
                )

            for k, v in task_metadata.run_facets.items():
                datajob.properties[f"openlineage_run_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)
                )

    def check_kill_switch(self):
        # In Airflow 3.0+, Variable.get() cannot be called from listener hooks
        # because it creates a database session commit which breaks HA locks.
        # As a workaround, check an environment variable instead.
        if IS_AIRFLOW_3_OR_HIGHER:
            # For Airflow 3.0+, use environment variable
            if (
                os.getenv(
                    f"AIRFLOW_VAR_{KILL_SWITCH_VARIABLE_NAME}".upper(), "false"
                ).lower()
                == "true"
            ):
                logger.debug("DataHub listener disabled by kill switch (env var)")
                return True
        else:
            # For Airflow 2.x, use Variable.get()
            try:
                if Variable.get(KILL_SWITCH_VARIABLE_NAME, "false").lower() == "true":
                    logger.debug("DataHub listener disabled by kill switch")
                    return True
            except Exception as e:
                logger.debug(f"Error checking kill switch variable: {e}")
        return False

    @hookimpl
    @run_in_thread
    def on_task_instance_running(
        self, previous_state, task_instance: "TaskInstance", **kwargs
    ) -> None:
        # In Airflow 3.0, the session parameter was removed from the hook signature
        if self.check_kill_switch():
            return
        self._set_log_level()

        # This if statement mirrors the logic in https://github.com/OpenLineage/OpenLineage/pull/508.
        if not hasattr(task_instance, "task"):
            # The type ignore is to placate mypy on Airflow 2.1.x.
            logger.warning(
                f"No task set for task_id: {task_instance.task_id} - "  # type: ignore[attr-defined]
                f"dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}"  # type: ignore[attr-defined]
            )
            return

        logger.debug(
            f"DataHub listener got notification about task instance start for {task_instance.task_id} of dag {task_instance.dag_id}"
        )

        if not self.config.dag_filter_pattern.allowed(task_instance.dag_id):
            logger.debug(f"DAG {task_instance.dag_id} is not allowed by the pattern")
            return

        if self.config.render_templates:
            task_instance = _render_templates(task_instance)

        # Get dagrun in a version-compatible way (Airflow 2.x vs 3.x)
        dagrun: "DagRun" = _get_dagrun_from_task_instance(task_instance)
        task = task_instance.task
        assert task is not None
        dag: "DAG" = task.dag  # type: ignore[assignment]

        # Store task for later retrieval (only needed for Airflow < 3.0)
        if hasattr(self._task_holder, "set_task"):
            self._task_holder.set_task(task_instance)

        # Handle async operators in Airflow 2.3 by skipping deferred state.
        # Inspired by https://github.com/OpenLineage/OpenLineage/pull/1601
        # In Airflow 3.0, RuntimeTaskInstance doesn't have next_method attribute
        if (
            hasattr(task_instance, "next_method")
            and task_instance.next_method is not None
        ):  # type: ignore[attr-defined]
            return

        # If we don't have the DAG listener API, we just pretend that
        # the start of the task is the start of the DAG.
        # This generates duplicate events, but it's better than not
        # generating anything.
        if not HAS_AIRFLOW_DAG_LISTENER_API:
            self.on_dag_start(dagrun)

        datajob = AirflowGenerator.generate_datajob(
            cluster=self.config.cluster,
            task=task,
            dag=dag,
            capture_tags=self.config.capture_tags_info,
            capture_owner=self.config.capture_ownership_info,
            config=self.config,
        )

        # TODO: Make use of get_task_location to extract github urls.

        # Add lineage info.
        self._extract_lineage(datajob, dagrun, task, task_instance)

        # TODO: Add handling for Airflow mapped tasks using task_instance.map_index

        for mcp in datajob.generate_mcp(
            generate_lineage=self.config.enable_datajob_lineage,
            materialize_iolets=self.config.materialize_iolets,
        ):
            self.emitter.emit(mcp, self._make_emit_callback())
        logger.debug(f"Emitted DataHub Datajob start: {datajob}")

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
        # Get dagrun in a version-compatible way (Airflow 2.x vs 3.x)
        dagrun: "DagRun" = _get_dagrun_from_task_instance(task_instance)

        if self.config.render_templates:
            task_instance = _render_templates(task_instance)

        # We must prefer the task attribute, in case modifications to the task's inlets/outlets
        # were made by the execute() method.
        if getattr(task_instance, "task", None):
            task = task_instance.task
        elif hasattr(self._task_holder, "get_task"):
            # For Airflow < 3.0, retrieve task from TaskHolder if needed
            task = self._task_holder.get_task(task_instance)
        else:
            # For Airflow 3.0+, task should always be available on task_instance
            task = None
        assert task is not None

        dag: "DAG" = task.dag  # type: ignore[assignment]

        if not self.config.dag_filter_pattern.allowed(dag.dag_id):
            logger.debug(f"DAG {dag.dag_id} is not allowed by the pattern")
            return

        datajob = AirflowGenerator.generate_datajob(
            cluster=self.config.cluster,
            task=task,
            dag=dag,
            capture_tags=self.config.capture_tags_info,
            capture_owner=self.config.capture_ownership_info,
            config=self.config,
        )

        # Add lineage info.
        self._extract_lineage(datajob, dagrun, task, task_instance, complete=True)

        for mcp in datajob.generate_mcp(
            generate_lineage=self.config.enable_datajob_lineage,
            materialize_iolets=self.config.materialize_iolets,
        ):
            self.emitter.emit(mcp, self._make_emit_callback())
        logger.debug(f"Emitted DataHub Datajob finish w/ status {status}: {datajob}")

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

        self.emitter.flush()

    @hookimpl
    @run_in_thread
    def on_task_instance_success(
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
    def on_task_instance_failed(
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

    def on_dag_start(self, dag_run: "DagRun") -> None:
        dag = dag_run.dag
        if not dag:
            logger.warning(
                f"DataHub listener could not find DAG for {dag_run.dag_id} - {dag_run.run_id}. Dag won't be captured"
            )
            return

        dataflow = AirflowGenerator.generate_dataflow(
            config=self.config,
            dag=dag,
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
        def on_dataset_created(self, dataset: "Dataset") -> None:
            self._set_log_level()

            logger.debug(
                f"DataHub listener got notification about dataset create for {dataset}"
            )

        @hookimpl
        @run_in_thread
        def on_dataset_changed(self, dataset: "Dataset") -> None:
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
