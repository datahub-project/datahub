import asyncio
import copy
import functools
import logging
import os
from datetime import datetime
import threading
import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, TypeVar, cast

import airflow
from airflow.models import Variable
from airflow.models.operator import Operator
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import TaskInstanceState
from airflow.utils.timeout import timeout
from airflow.utils import timezone
from airflow.settings improt configure_orm
from airflow.stats import Stats
# TODO: to change to Airflow plugin
# from openlineage.airflow.listener import TaskHolder
# Ref: https://github.com/apache/airflow/blob/main/providers/openlineage/src/airflow/providers/openlineage/plugins/listener.py
from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
# TODO: to change to Airflow plugin
# from openlineage.airflow.utils import redact_with_exclusions
from airflow.providers.openlineage.utils.utils import (
    AIRFLOW_V_3_0_PLUS,
    get_airflow_dag_run_facet,
    get_airflow_debug_facet,
    get_airflow_job_facet,
    get_airflow_mapped_task_facet,
    get_airflow_run_facet,
    get_job_name,
    get_task_parent_run_facet,
    get_task_documentation,
    get_user_provided_run_facets,
    is_operator_disabled,
    is_selective_lineage_enabled,
    print_warning,
)
from airflow.providers.openlineage import conf
from airflow.providers.openlineage.extractors.manager import ExtractorManager
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter, RunState
# TODO: to change to Airflow plugin
# from openlineage.client.serde import Serde
from airflow.providers.openlineage.client.serde import Serde

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
from datahub_airflow_plugin._airflow_shims import (
    HAS_AIRFLOW_DAG_LISTENER_API,
    HAS_AIRFLOW_DATASET_LISTENER_API,
    get_task_inlets,
    get_task_outlets,
)
from datahub_airflow_plugin._config import DatahubLineageConfig, get_lineage_config
from datahub_airflow_plugin._datahub_ol_adapter import translate_ol_to_datahub_urn
from datahub_airflow_plugin._extractors import SQL_PARSING_RESULT_KEY, ExtractorManager
from datahub_airflow_plugin._version import __package_name__, __version__
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator
from datahub_airflow_plugin.entities import (
    _Entity,
    entities_to_datajob_urn_list,
    entities_to_dataset_urn_list,
)

_F = TypeVar("_F", bound=Callable[..., None])
if TYPE_CHECKING:
    from airflow.datasets import Dataset
    from airflow.models import DAG, DagRun, TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.settings import Session

    # To placate mypy on Airflow versions that don't have the listener API,
    # we define a dummy hookimpl that's an identity function.

    def hookimpl(f: _F) -> _F:  # type: ignore[misc]
        return f

else:
    from airflow.listeners import hookimpl

logger = logging.getLogger(__name__)

_airflow_listener_initialized = False
_airflow_listener: Optional["DataHubListener"] = None
_RUN_IN_THREAD = os.getenv("DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD", "true").lower() in (
    "true",
    "1",
)
_RUN_IN_THREAD_TIMEOUT = float(
    os.getenv("DATAHUB_AIRFLOW_PLUGIN_RUN_IN_THREAD_TIMEOUT", 10)
)
_DATAHUB_CLEANUP_DAG = "Datahub_Cleanup"

KILL_SWITCH_VARIABLE_NAME = "datahub_airflow_plugin_disable_listener"

# verified - airflow openlineage dependencies
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
            telemetry.telemetry_instance.ping(
                "airflow-plugin-init",
                {
                    "airflow-version": airflow.__version__,
                    "datahub-airflow-plugin": "v2",
                    "datahub-airflow-plugin-dag-events": HAS_AIRFLOW_DAG_LISTENER_API,
                    "datahub-airflow-plugin-dataset-events": HAS_AIRFLOW_DATASET_LISTENER_API,
                    "capture_executions": plugin_config.capture_executions,
                    "capture_tags": plugin_config.capture_tags_info,
                    "capture_ownership": plugin_config.capture_ownership_info,
                    "enable_extractors": plugin_config.enable_extractors,
                    "render_templates": plugin_config.render_templates,
                    "disable_openlineage_plugin": plugin_config.disable_openlineage_plugin,
                },
            )

        if plugin_config.disable_openlineage_plugin:
            # Deactivate the OpenLineagePlugin listener to avoid conflicts/errors.
            from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin

            OpenLineageProviderPlugin.listeners = []

    return _airflow_listener

# verified - airflow openlineage dependencies
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
                    # thread exits.

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

# verified - airflow openlineage dependencies
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

    # verified - airflow openlineage dependencies
    def __init__(self, config: DatahubLineageConfig):
        self.config = config
        self._set_log_level()

        self._emitter = config.make_emitter_hook().make_emitter()
        self._graph: Optional[DataHubGraph] = None
        logger.info(f"DataHub plugin v2 using {repr(self._emitter)}")

        # See discussion here https://github.com/OpenLineage/OpenLineage/pull/508 for
        # why we need to keep track of tasks ourselves.
        self._open_lineage_listener = get_openlineage_listener()

        # In our case, we also want to cache the initial datajob object
        # so that we can add to it when the task completes.
        self._datajob_holder: Dict[str, DataJob] = {}

        self.extractor_manager = ExtractorManager()

        # This "inherits" from types.ModuleType to avoid issues with Airflow's listener plugin loader.
        # It previously (v2.4.x and likely other versions too) would throw errors if it was not a module.
        # https://github.com/apache/airflow/blob/e99a518970b2d349a75b1647f6b738c8510fa40e/airflow/listeners/listener.py#L56
        # self.__class__ = types.ModuleType

    # verified - airflow openlineage dependencies
    @property
    def emitter(self):
        return self._emitter

    # verified - airflow openlineage dependencies
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

    # verified - airflow openlineage dependencies
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

    # verified - airflow openlineage dependencies
    def _make_emit_callback(self) -> Callable[[Optional[Exception], str], None]:
        def emit_callback(err: Optional[Exception], msg: str) -> None:
            if err:
                logger.error(f"Error sending metadata to datahub: {msg}", exc_info=err)

        return emit_callback

    # verified - airflow openlineage dependencies
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
        if self.config.enable_extractors:
            task_metadata = self.extractor_manager.extract_metadata(
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
                translate_ol_to_datahub_urn(dataset)
                for dataset in task_metadata.outputs
            )

        # Add DataHub-native SQL parser results.
        sql_parsing_result: Optional[SqlParsingResult] = None
        if task_metadata:
            sql_parsing_result = task_metadata.run_facets.pop(
                SQL_PARSING_RESULT_KEY, None
            )
        if sql_parsing_result:
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

        # Add DataHub-native inlets/outlets.
        # These are filtered out by the extractor, so we need to add them manually.
        input_urns.extend(
            iolet.urn for iolet in get_task_inlets(task) if isinstance(iolet, _Entity)
        )
        output_urns.extend(
            iolet.urn for iolet in get_task_outlets(task) if isinstance(iolet, _Entity)
        )

        # Write the lineage to the datajob object.
        datajob.inlets.extend(entities_to_dataset_urn_list(input_urns))
        datajob.outlets.extend(entities_to_dataset_urn_list(output_urns))
        datajob.upstream_urns.extend(entities_to_datajob_urn_list(input_urns))
        datajob.fine_grained_lineages.extend(fine_grained_lineages)

        # Merge in extra stuff that was present in the DataJob we constructed
        # at the start of the task.
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

        # Deduplicate inlets/outlets.
        datajob.inlets = list(sorted(set(datajob.inlets), key=lambda x: str(x)))
        datajob.outlets = list(sorted(set(datajob.outlets), key=lambda x: str(x)))
        datajob.upstream_urns = list(
            sorted(set(datajob.upstream_urns), key=lambda x: str(x))
        )

        # Write all other OL facets as DataHub properties.
        if task_metadata:
            for k, v in task_metadata.job_facets.items():
                datajob.properties[f"openlineage_job_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)
                )

            for k, v in task_metadata.run_facets.items():
                datajob.properties[f"openlineage_run_facet_{k}"] = Serde.to_json(
                    redact_with_exclusions(v)
                )

    # verified - airflow openlineage dependencies
    def check_kill_switch(self):
        if Variable.get(KILL_SWITCH_VARIABLE_NAME, "false").lower() == "true":
            logger.debug("DataHub listener disabled by kill switch")
            return True
        return False

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        @run_in_thread
        def on_task_instance_running(self, previous_state: TaskInstanceState, task_instance: "TaskInstance", session: "Session") -> None:
            self.log.debug("DataHub listener got notification about task instance start")
            context = task_instance.get_template_context()
            task = context["task"]

            if TYPE_CHECKING:
                assert task
            dagrun = context["dag_run"]
            dag = context["dag"]
            start_date = task_instance.start_date
            self._on_task_instance_running(task_instance, dag, dagrun, task, start_date)
    else:

        @hookimpl
        @run_in_thread
        def on_task_instance_running(self, previous_state: TaskInstance, task_instance: "TaskInstance", session: "Session") -> None:
            from airflow.providers.openlineage.utils.utils import is_ti_rescheduled_already

            if not getattr(task_instance, "task", None) is not None:
                logger.warning(
                    "No task set for TI object task_id: %s - dag_id: %s - run_id %s",
                    task_instance.task_id,
                    task_instance.dag_id,
                    task_instance.run_id,
                )
                return

            logger.debug("OpenLineage listener got notification about task instance start")
            task = task_instance.task
            if TYPE_CHECKING:
                assert task
            start_date = task_instance.start_date if task_instance.start_date else timezone.utcnow()

            if is_ti_rescheduled_already(task_instance):
                self.log.debug("Skipping this instance of rescheduled task - START event was emitted already")
                return
            self._on_task_instance_running(task_instance, task.dag, task_instance.dag_run, task, start_date)

    def _on_task_instance_running(self, task_instance: RuntimeTaskInstance | TaskInstance, dag, dagrun, task, start_date: datetime):
        if is_operator_disabled(task):
            self.log.debug("Skipping OpenLineage event emission for operator `%s` "
            "due to its presence in [openlineage] disabled_for_operators.",
            task.task_type,
        )
            return

        if not is_selective_lineage_enabled(task):
            self.log.debug(
                "Skipping OpenLineage event emission for task `%s` "
                "due to lack of explicit lineage enablement for task or DAG while "
                "[openlineage] selective_enable is on.",
                task_instance.task_id,
            )
            return

        # Needs to be calculated outside of inner method so that it gets cached for usage in fork processes
        debug_facet = get_airflow_debug_facet()

        @print_warning(self.log)
        def on_running():
            context = task_instance.get_template_context()
            if hasattr(context, "task_reschedule_count") and context["task_reschedule_count"] > 0:
                self.log.debug("Skipping this instance of rescheduled task - START event was emitted already")
                return

            date = dagrun.logical_date
            if AIRFLOW_V_3_0_PLUS and date is None:
                date = dagrun.run_after

            clear_number = 0
            if hasattr(dagrun, "clear_number"):
                clear_number = dagrun.clear_number

            parent_run_id = self.adapter.build_dag_run_id(
                dag_id=task_instance.dag_id,
                logical_date=date,
                clear_number=clear_number,
            )

            task_uuid = self.adapter.build_task_instance_run_id(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                try_number=task_instance.try_number,
                logical_date=date,
                map_index=task_instance.map_index,
            )
            event_type = RunState.RUNNING.value.lower()
            operator_name = task.task_type.lower()

            data_interval_start = dagrun.data_interval_start
            if isinstance(data_interval_start, datetime):
                data_interval_start = data_interval_start.isoformat()
            data_interval_end = dagrun.data_interval_end
            if isinstance(data_interval_end, datetime):
                data_interval_end = data_interval_end.isoformat()

            doc, doc_type = get_task_documentation(task)
            if not doc:
                doc, doc_type = get_dag_documentation(dag)

            with Stats.timer(f"ol.extract.{event_type}.{operator_name}"):
                task_metadata = self.extractor_manager.extract_metadata(
                    dagrun=dagrun, task=task, task_instance_state=TaskInstanceState.RUNNING
                )

            redacted_event = self.adapter.start_task(
                run_id=task_uuid,
                job_name=get_job_name(task_instance),
                job_description=doc,
                job_description_type=doc_type,
                event_time=start_date.isoformat(),
                nominal_start_time=data_interval_start,
                nominal_end_time=data_interval_end,
                # If task owner is default ("airflow"), use DAG owner instead that may have more details
                owners=[x.strip() for x in (task if task.owner != "airflow" else dag).owner.split(",")],
                tags=dag.tags,
                task=task_metadata,
                run_facets={
                    **get_task_parent_run_facet(parent_run_id=parent_run_id, parent_job_name=dag.dag_id),
                    **get_user_provided_run_facets(task_instance, TaskInstanceState.RUNNING),
                    **get_airflow_mapped_task_facet(task_instance),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid),
                    **debug_facet,
                },
            )
            Stats.gauge(
                f"ol.event.size.{event_type}.{operator_name}",
                len(Serde.to_json(redacted_event).encode("utf-8")),
            )

        self._execute(on_running, "on_running", use_fork=True)


    @hookimpl
    @run_in_thread
    def on_task_instance_running(
        self,
        previous_state: None,
        task_instance: "TaskInstance",
        session: "Session",  # This will always be QUEUED
    ) -> None:
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

        # The type ignore is to placate mypy on Airflow 2.1.x.
        dagrun: "DagRun" = task_instance.dag_run  # type: ignore[attr-defined]
        task = task_instance.task
        assert task is not None
        dag: "DAG" = task.dag  # type: ignore[assignment]

        # TODO: Replace with airflow openlineage plugin equivalent
        # TODO: Implement each step for Airflow < 3.0 and Airflow >= 3.0
        self._task_holder.set_task(task_instance)

        # Handle async operators in Airflow 2.3 by skipping deferred state.
        # Inspired by https://github.com/OpenLineage/OpenLineage/pull/1601
        if task_instance.next_method is not None:  # type: ignore[attr-defined]
            return

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
        dagrun: "DagRun" = task_instance.dag_run  # type: ignore[attr-defined]

        if self.config.render_templates:
            task_instance = _render_templates(task_instance)

        # We must prefer the task attribute, in case modifications to the task's inlets/outlets
        # were made by the execute() method.
        if getattr(task_instance, "task", None):
            task = task_instance.task
        else:
            task = self._task_holder.get_task(task_instance)
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
        self, previous_state: None, task_instance: "TaskInstance", session: "Session"
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
        self, previous_state: None, task_instance: "TaskInstance", session: "Session"
    ) -> None:
        if self.check_kill_switch():
            return

        self._set_log_level()

        logger.debug(
            f"DataHub listener got notification about task instance failure for {task_instance.task_id}"
        )

        # TODO: Handle UP_FOR_RETRY state.
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
