import contextlib
import traceback
from typing import Any, Iterable

import attr
from airflow.configuration import conf
from airflow.lineage import PIPELINE_OUTLETS
from airflow.models.baseoperator import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.module_loading import import_string
from cattr import structure
from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub_provider.client.airflow_generator import AirflowGenerator
from datahub_provider.hooks.datahub import DatahubGenericHook
from datahub_provider.lineage.datahub import DatahubLineageConfig


def get_lineage_config() -> DatahubLineageConfig:
    """Load the lineage config from airflow.cfg."""

    enabled = conf.get("datahub", "enabled", fallback=True)
    datahub_conn_id = conf.get("datahub", "conn_id", fallback="datahub_rest_default")
    cluster = conf.get("datahub", "cluster", fallback="prod")
    graceful_exceptions = conf.get("datahub", "graceful_exceptions", fallback=True)
    capture_tags_info = conf.get("datahub", "capture_tags_info", fallback=True)
    capture_ownership_info = conf.get(
        "datahub", "capture_ownership_info", fallback=True
    )
    capture_executions = conf.get("datahub", "capture_executions", fallback=True)
    return DatahubLineageConfig(
        enabled=enabled,
        datahub_conn_id=datahub_conn_id,
        cluster=cluster,
        graceful_exceptions=graceful_exceptions,
        capture_ownership_info=capture_ownership_info,
        capture_tags_info=capture_tags_info,
        capture_executions=capture_executions,
    )


def get_inlets_from_task(task: BaseOperator, context: Any) -> Iterable[Any]:
    inlets = []
    if isinstance(task._inlets, (str, BaseOperator)) or attr.has(task._inlets):  # type: ignore
        inlets = [
            task._inlets,
        ]

    if task._inlets and isinstance(task._inlets, list):
        inlets = []
        task_ids = (
            {o for o in task._inlets if isinstance(o, str)}
            .union(op.task_id for op in task._inlets if isinstance(op, BaseOperator))
            .intersection(task.get_flat_relative_ids(upstream=True))
        )

        from airflow.lineage import AUTO

        # pick up unique direct upstream task_ids if AUTO is specified
        if AUTO.upper() in task._inlets or AUTO.lower() in task._inlets:
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

        for inlet in task._inlets:
            if type(inlet) != str:
                inlets.append(inlet)

    return inlets


def datahub_on_failure_callback(context, *args, **kwargs):
    ti = context["ti"]
    task: "BaseOperator" = ti.task
    dag = context["dag"]

    # This code is from the original airflow lineage code ->
    # https://github.com/apache/airflow/blob/main/airflow/lineage/__init__.py
    inlets = get_inlets_from_task(task, context)

    emitter = (
        DatahubGenericHook(context["_datahub_config"].datahub_conn_id)
        .get_underlying_hook()
        .make_emitter()
    )

    dataflow = AirflowGenerator.generate_dataflow(
        cluster=context["_datahub_config"].cluster,
        dag=dag,
        capture_tags=context["_datahub_config"].capture_tags_info,
        capture_owner=context["_datahub_config"].capture_ownership_info,
    )
    dataflow.emit(emitter)

    task.log.info(f"Emitted Datahub DataFlow: {dataflow}")

    datajob = AirflowGenerator.generate_datajob(
        cluster=context["_datahub_config"].cluster,
        task=context["ti"].task,
        dag=dag,
        capture_tags=context["_datahub_config"].capture_tags_info,
        capture_owner=context["_datahub_config"].capture_ownership_info,
    )

    for inlet in inlets:
        datajob.inlets.append(inlet.urn)

    for outlet in task._outlets:
        datajob.outlets.append(outlet.urn)

    task.log.info(f"Emitted Datahub DataJob: {datajob}")
    datajob.emit(emitter)

    if context["_datahub_config"].capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            cluster=context["_datahub_config"].cluster,
            ti=context["ti"],
            dag=dag,
            dag_run=context["dag_run"],
            datajob=datajob,
            start_timestamp_millis=int(ti.start_date.timestamp() * 1000),
        )

        task.log.info(f"Emitted Start Datahub Dataprocess Instance: {dpi}")

        dpi = AirflowGenerator.complete_datajob(
            emitter=emitter,
            cluster=context["_datahub_config"].cluster,
            ti=context["ti"],
            dag_run=context["dag_run"],
            result=InstanceRunResult.FAILURE,
            dag=dag,
            datajob=datajob,
            end_timestamp_millis=int(ti.end_date.timestamp() * 1000),
        )
        task.log.info(f"Emitted Completed Datahub Dataprocess Instance: {dpi}")


def datahub_on_success_callback(context, *args, **kwargs):
    ti = context["ti"]
    task: "BaseOperator" = ti.task
    dag = context["dag"]

    # This code is from the original airflow lineage code ->
    # https://github.com/apache/airflow/blob/main/airflow/lineage/__init__.py
    inlets = get_inlets_from_task(task, context)

    emitter = (
        DatahubGenericHook(context["_datahub_config"].datahub_conn_id)
        .get_underlying_hook()
        .make_emitter()
    )

    dataflow = AirflowGenerator.generate_dataflow(
        cluster=context["_datahub_config"].cluster,
        dag=dag,
        capture_tags=context["_datahub_config"].capture_tags_info,
        capture_owner=context["_datahub_config"].capture_ownership_info,
    )
    dataflow.emit(emitter)

    task.log.info(f"Emitted Datahub DataFlow: {dataflow}")

    datajob = AirflowGenerator.generate_datajob(
        cluster=context["_datahub_config"].cluster,
        task=task,
        dag=dag,
        capture_tags=context["_datahub_config"].capture_tags_info,
        capture_owner=context["_datahub_config"].capture_ownership_info,
    )

    for inlet in inlets:
        datajob.inlets.append(inlet.urn)

    # We have to use _outlets because outlets is empty
    for outlet in task._outlets:
        datajob.outlets.append(outlet.urn)

    task.log.info(f"Emitted Datahub dataJob: {datajob}")
    datajob.emit(emitter)

    if context["_datahub_config"].capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            cluster=context["_datahub_config"].cluster,
            ti=context["ti"],
            dag=dag,
            dag_run=context["dag_run"],
            datajob=datajob,
            start_timestamp_millis=int(ti.start_date.timestamp() * 1000),
        )

        task.log.info(f"Emitted Start Datahub Dataprocess Instance: {dpi}")

        dpi = AirflowGenerator.complete_datajob(
            emitter=emitter,
            cluster=context["_datahub_config"].cluster,
            ti=context["ti"],
            dag_run=context["dag_run"],
            result=InstanceRunResult.SUCCESS,
            dag=dag,
            datajob=datajob,
            end_timestamp_millis=int(ti.end_date.timestamp() * 1000),
        )
        task.log.info(f"Emitted Completed Data Process Instance: {dpi}")


def datahub_pre_execution(context):
    ti = context["ti"]
    task: "BaseOperator" = ti.task
    dag = context["dag"]

    task.log.info("Running Datahub pre_execute method")

    emitter = (
        DatahubGenericHook(context["_datahub_config"].datahub_conn_id)
        .get_underlying_hook()
        .make_emitter()
    )

    # This code is from the original airflow lineage code ->
    # https://github.com/apache/airflow/blob/main/airflow/lineage/__init__.py
    inlets = get_inlets_from_task(task, context)

    datajob = AirflowGenerator.generate_datajob(
        cluster=context["_datahub_config"].cluster,
        task=context["ti"].task,
        dag=dag,
        capture_tags=context["_datahub_config"].capture_tags_info,
        capture_owner=context["_datahub_config"].capture_ownership_info,
    )

    for inlet in inlets:
        datajob.inlets.append(inlet.urn)

    for outlet in task._outlets:
        datajob.outlets.append(outlet.urn)

    datajob.emit(emitter)
    task.log.info(f"Emitting Datahub DataJob: {datajob}")

    if context["_datahub_config"].capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            cluster=context["_datahub_config"].cluster,
            ti=context["ti"],
            dag=dag,
            dag_run=context["dag_run"],
            datajob=datajob,
            start_timestamp_millis=int(ti.start_date.timestamp() * 1000),
        )

        task.log.info(f"Emitting Datahub Dataprocess Instance: {dpi}")


def _wrap_pre_execution(pre_execution):
    def custom_pre_execution(context):
        config = get_lineage_config()
        context["_datahub_config"] = config
        datahub_pre_execution(context)

        # Call original policy
        if pre_execution:
            pre_execution(context)

    return custom_pre_execution


def _wrap_on_failure_callback(on_failure_callback):
    def custom_on_failure_callback(context):
        config = get_lineage_config()
        context["_datahub_config"] = config
        try:
            datahub_on_failure_callback(context)
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
        config = get_lineage_config()
        context["_datahub_config"] = config
        try:
            datahub_on_success_callback(context)
        except Exception as e:
            if not config.graceful_exceptions:
                raise e
            else:
                print(f"Exception: {traceback.format_exc()}")

        if on_success_callback:
            on_success_callback(context)

    return custom_on_success_callback


def task_policy(task: BaseOperator) -> None:
    print(f"Setting task policy for Dag: {task.dag_id} Task: {task.task_id}")
    # task.add_inlets(["auto"])
    # task.pre_execute = _wrap_pre_execution(task.pre_execute)
    task.on_failure_callback = _wrap_on_failure_callback(task.on_failure_callback)
    task.on_success_callback = _wrap_on_success_callback(task.on_success_callback)
    # task.pre_execute = _wrap_pre_execution(task.pre_execute)


def _wrap_task_policy(policy):
    if policy and hasattr(policy, "_task_policy_patched_by"):
        return policy

    def custom_task_policy(task):
        policy(task)
        task_policy(task)

    setattr(custom_task_policy, "_task_policy_patched_by", "datahub_plugin")
    return custom_task_policy


def _patch_policy(settings):
    print("Patching datahub policy")
    if hasattr(settings, "task_policy"):
        datahub_task_policy = _wrap_task_policy(settings.task_policy)
        settings.task_policy = datahub_task_policy


def _patch_datahub_policy():
    with contextlib.suppress(ImportError):
        import airflow_local_settings

        _patch_policy(airflow_local_settings)
    from airflow.models.dagbag import settings

    _patch_policy(settings)


_patch_datahub_policy()


class DatahubPlugin(AirflowPlugin):
    name = "datahub_plugin"
