import contextlib
import logging
import traceback
from typing import Any, Callable, Iterable, List, Optional, Union

import airflow
from airflow.lineage import PIPELINE_OUTLETS
from airflow.models.baseoperator import BaseOperator
from airflow.utils.module_loading import import_string

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.telemetry import telemetry
from datahub_airflow_plugin._airflow_shims import (
    MappedOperator,
    get_task_inlets,
    get_task_outlets,
)
from datahub_airflow_plugin._config import get_lineage_config
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator
from datahub_airflow_plugin.entities import (
    entities_to_datajob_urn_list,
    entities_to_dataset_urn_list,
)
from datahub_airflow_plugin.hooks.datahub import DatahubGenericHook
from datahub_airflow_plugin.lineage.datahub import DatahubLineageConfig

TASK_ON_FAILURE_CALLBACK = "on_failure_callback"
TASK_ON_SUCCESS_CALLBACK = "on_success_callback"
TASK_ON_RETRY_CALLBACK = "on_retry_callback"


def load_config_v22():
    plugin_config = get_lineage_config()
    return plugin_config


def get_task_inlets_advanced(task: BaseOperator, context: Any) -> Iterable[Any]:
    # TODO: Fix for https://github.com/apache/airflow/commit/1b1f3fabc5909a447a6277cafef3a0d4ef1f01ae
    # in Airflow 2.4.
    # TODO: ignore/handle airflow's dataset type in our lineage

    inlets: List[Any] = []
    task_inlets = get_task_inlets(task)
    # From Airflow 2.3 this should be AbstractOperator but due to compatibility reason lets use BaseOperator
    if isinstance(task_inlets, (str, BaseOperator)):
        inlets = [
            task_inlets,
        ]

    if task_inlets and isinstance(task_inlets, list):
        inlets = []
        task_ids = {o for o in task_inlets if isinstance(o, str)}.union(
            op.task_id for op in task_inlets if isinstance(op, BaseOperator)
        ).intersection(task.get_flat_relative_ids(upstream=True))

        from airflow.lineage import AUTO
        from cattr import structure

        # pick up unique direct upstream task_ids if AUTO is specified
        if AUTO.upper() in task_inlets or AUTO.lower() in task_inlets:
            print("Picking up unique direct upstream task_ids as AUTO is specified")
            task_ids = task_ids.union(
                task_ids.symmetric_difference(task.upstream_task_ids)
            )

        inlets = task.xcom_pull(
            context, task_ids=list(task_ids), dag_id=task.dag_id, key=PIPELINE_OUTLETS
        )

        # re-instantiate the obtained inlets
        inlets = [
            structure(item["data"], import_string(item["type_name"]))
            # _get_instance(structure(item, Metadata))
            for sublist in inlets
            if sublist
            for item in sublist
        ]

        for inlet in task_inlets:
            if not isinstance(inlet, str):
                inlets.append(inlet)

    return inlets


def _make_emit_callback(
    logger: logging.Logger,
) -> Callable[[Optional[Exception], str], None]:
    def emit_callback(err: Optional[Exception], msg: str) -> None:
        if err:
            logger.error(f"Error sending metadata to datahub: {msg}", exc_info=err)

    return emit_callback


def datahub_task_status_callback(context, status):
    ti = context["ti"]
    task: "BaseOperator" = ti.task
    dag = context["dag"]
    config: DatahubLineageConfig = context["_datahub_config"]

    # This code is from the original airflow lineage code ->
    # https://github.com/apache/airflow/blob/main/airflow/lineage/__init__.py
    task_inlets = get_task_inlets_advanced(task, context)
    task_outlets = get_task_outlets(task)

    emitter = config.make_emitter_hook().make_emitter()

    dataflow = AirflowGenerator.generate_dataflow(
        config=config,
        dag=dag,
    )
    task.log.info(f"Emitting Datahub Dataflow: {dataflow}")
    dataflow.emit(emitter, callback=_make_emit_callback(task.log))

    datajob = AirflowGenerator.generate_datajob(
        cluster=config.cluster,
        task=task,
        dag=dag,
        capture_tags=config.capture_tags_info,
        capture_owner=config.capture_ownership_info,
        config=config,
    )
    datajob.inlets.extend(
        entities_to_dataset_urn_list([let.urn for let in task_inlets])
    )
    datajob.outlets.extend(
        entities_to_dataset_urn_list([let.urn for let in task_outlets])
    )
    datajob.upstream_urns.extend(
        entities_to_datajob_urn_list([let.urn for let in task_inlets])
    )

    task.log.info(f"Emitting Datahub Datajob: {datajob}")
    for mcp in datajob.generate_mcp(materialize_iolets=config.materialize_iolets):
        emitter.emit(mcp, _make_emit_callback(task.log))

    if config.capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            config=config,
            ti=ti,
            dag=dag,
            dag_run=context["dag_run"],
            datajob=datajob,
            start_timestamp_millis=int(ti.start_date.timestamp() * 1000),
        )

        task.log.info(f"Emitted Start Datahub Dataprocess Instance: {dpi}")

        dpi = AirflowGenerator.complete_datajob(
            emitter=emitter,
            cluster=config.cluster,
            ti=ti,
            dag_run=context["dag_run"],
            result=status,
            dag=dag,
            datajob=datajob,
            end_timestamp_millis=int(ti.end_date.timestamp() * 1000),
        )
        task.log.info(f"Emitted Completed Data Process Instance: {dpi}")

    emitter.flush()


def datahub_pre_execution(context):
    ti = context["ti"]
    task: "BaseOperator" = ti.task
    dag = context["dag"]
    config: DatahubLineageConfig = context["_datahub_config"]

    task.log.info("Running Datahub pre_execute method")

    emitter = (
        DatahubGenericHook(config.datahub_conn_id).get_underlying_hook().make_emitter()
    )

    # This code is from the original airflow lineage code ->
    # https://github.com/apache/airflow/blob/main/airflow/lineage/__init__.py
    task_inlets = get_task_inlets_advanced(task, context)
    task_outlets = get_task_outlets(task)

    datajob = AirflowGenerator.generate_datajob(
        cluster=config.cluster,
        task=ti.task,
        dag=dag,
        capture_tags=config.capture_tags_info,
        capture_owner=config.capture_ownership_info,
        config=config,
    )
    datajob.inlets.extend(
        entities_to_dataset_urn_list([let.urn for let in task_inlets])
    )
    datajob.outlets.extend(
        entities_to_dataset_urn_list([let.urn for let in task_outlets])
    )
    datajob.upstream_urns.extend(
        entities_to_datajob_urn_list([let.urn for let in task_inlets])
    )

    task.log.info(f"Emitting Datahub dataJob {datajob}")
    for mcp in datajob.generate_mcp(materialize_iolets=config.materialize_iolets):
        emitter.emit(mcp, _make_emit_callback(task.log))

    if config.capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            config=config,
            ti=ti,
            dag=dag,
            dag_run=context["dag_run"],
            datajob=datajob,
            start_timestamp_millis=int(ti.start_date.timestamp() * 1000),
        )

        task.log.info(f"Emitting Datahub Dataprocess Instance: {dpi}")

    emitter.flush()


def _wrap_pre_execution(pre_execution):
    def custom_pre_execution(context):
        config = load_config_v22()
        if config.enabled:
            context["_datahub_config"] = config
            datahub_pre_execution(context)

        # Call original policy
        if pre_execution:
            pre_execution(context)

    return custom_pre_execution


def _wrap_on_failure_callback(on_failure_callback):
    def custom_on_failure_callback(context):
        config = load_config_v22()
        if config.enabled:
            context["_datahub_config"] = config
            try:
                datahub_task_status_callback(context, status=InstanceRunResult.FAILURE)
            except Exception as e:
                if not config.graceful_exceptions:
                    raise e
                else:
                    print(f"Exception: {traceback.format_exc()}")

        # Call original policy
        if on_failure_callback:
            on_failure_callback(context)

    return custom_on_failure_callback


def _wrap_on_success_callback(on_success_callback):
    def custom_on_success_callback(context):
        config = load_config_v22()
        if config.enabled:
            context["_datahub_config"] = config
            try:
                datahub_task_status_callback(context, status=InstanceRunResult.SUCCESS)
            except Exception as e:
                if not config.graceful_exceptions:
                    raise e
                else:
                    print(f"Exception: {traceback.format_exc()}")

        # Call original policy
        if on_success_callback:
            on_success_callback(context)

    return custom_on_success_callback


def _wrap_on_retry_callback(on_retry_callback):
    def custom_on_retry_callback(context):
        config = load_config_v22()

        if config.enabled:
            context["_datahub_config"] = config
            try:
                datahub_task_status_callback(
                    context, status=InstanceRunResult.UP_FOR_RETRY
                )
            except Exception as e:
                if not config.graceful_exceptions:
                    raise e
                else:
                    print(f"Exception: {traceback.format_exc()}")

        # Call original policy
        if on_retry_callback:
            on_retry_callback(context)

    return custom_on_retry_callback


def task_policy(task: Union[BaseOperator, MappedOperator]) -> None:
    task.log.debug(f"Setting task policy for Dag: {task.dag_id} Task: {task.task_id}")
    # task.add_inlets(["auto"])
    # task.pre_execute = _wrap_pre_execution(task.pre_execute)

    # MappedOperator's callbacks don't have setters until Airflow 2.X.X
    # https://github.com/apache/airflow/issues/24547
    # We can bypass this by going through partial_kwargs for now
    if MappedOperator and isinstance(task, MappedOperator):  # type: ignore
        on_failure_callback_prop: property = getattr(
            MappedOperator, TASK_ON_FAILURE_CALLBACK
        )
        on_success_callback_prop: property = getattr(
            MappedOperator, TASK_ON_SUCCESS_CALLBACK
        )
        on_retry_callback_prop: property = getattr(
            MappedOperator, TASK_ON_RETRY_CALLBACK
        )
        if (
            not on_failure_callback_prop.fset
            or not on_success_callback_prop.fset
            or not on_retry_callback_prop.fset
        ):
            task.log.debug(
                "Using MappedOperator's partial_kwargs instead of callback properties"
            )
            task.partial_kwargs[TASK_ON_FAILURE_CALLBACK] = _wrap_on_failure_callback(
                task.on_failure_callback
            )
            task.partial_kwargs[TASK_ON_SUCCESS_CALLBACK] = _wrap_on_success_callback(
                task.on_success_callback
            )
            task.partial_kwargs[TASK_ON_RETRY_CALLBACK] = _wrap_on_retry_callback(
                task.on_retry_callback
            )
            return

    task.on_failure_callback = _wrap_on_failure_callback(task.on_failure_callback)  # type: ignore
    task.on_success_callback = _wrap_on_success_callback(task.on_success_callback)  # type: ignore
    task.on_retry_callback = _wrap_on_retry_callback(task.on_retry_callback)  # type: ignore
    # task.pre_execute = _wrap_pre_execution(task.pre_execute)


def _wrap_task_policy(policy):
    if policy and hasattr(policy, "_task_policy_patched_by"):
        return policy

    def custom_task_policy(task):
        policy(task)
        task_policy(task)

    # Add a flag to the policy to indicate that we've patched it.
    custom_task_policy._task_policy_patched_by = "datahub_plugin"  # type: ignore[attr-defined]
    return custom_task_policy


def _patch_policy(settings):
    if hasattr(settings, "task_policy"):
        datahub_task_policy = _wrap_task_policy(settings.task_policy)
        settings.task_policy = datahub_task_policy


def _patch_datahub_policy():
    with contextlib.suppress(ImportError):
        import airflow_local_settings

        _patch_policy(airflow_local_settings)

    from airflow.models.dagbag import settings

    _patch_policy(settings)

    plugin_config = load_config_v22()
    telemetry.telemetry_instance.ping(
        "airflow-plugin-init",
        {
            "airflow-version": airflow.__version__,
            "datahub-airflow-plugin": "v1",
            "capture_executions": plugin_config.capture_executions,
            "capture_tags": plugin_config.capture_tags_info,
            "capture_ownership": plugin_config.capture_ownership_info,
        },
    )


_patch_datahub_policy()
