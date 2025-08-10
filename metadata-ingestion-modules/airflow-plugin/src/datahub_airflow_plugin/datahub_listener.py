# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import threading
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Callable, Optional

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import (
    DagUtils,
    get_airflow_run_facet,
    get_custom_facets,
    get_dagrun_start_end,
    get_job_name,
    get_task_location,
    getboolean,
    is_airflow_version_enough,
)

from airflow.listeners import hookimpl
from airflow.utils import timezone

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import BaseOperator, DagRun, TaskInstance


class TaskHolder:
    """Class that stores run data - run_id and task in-memory. This is needed because Airflow
    does not always pass all runtime info to on_task_instance_success and
    on_task_instance_failed that is needed to emit events. This is not a big problem since
    we're only running on worker - in separate process that is always spawned (or forked) on
    execution, just like old PHP runtime model.
    """

    def __init__(self):
        self.run_data = {}

    def set_task(self, task_instance: "TaskInstance"):
        self.run_data[self._pk(task_instance)] = task_instance.task

    def get_task(self, task_instance: "TaskInstance") -> Optional["BaseOperator"]:
        return self.run_data.get(self._pk(task_instance))

    @staticmethod
    def _pk(ti: "TaskInstance"):
        return ti.dag_id + ti.task_id + ti.run_id


log = logging.getLogger(__name__)
# TODO: move task instance runs to executor
executor: Optional[Executor] = None


def execute_in_thread(target: Callable, kwargs=None):
    if kwargs is None:
        kwargs = {}
    thread = threading.Thread(target=target, kwargs=kwargs, daemon=True)
    thread.start()

    # Join, but ignore checking if thread stopped. If it did, then we shouldn't do anything.
    # This basically gives this thread 5 seconds to complete work, then it can be killed,
    # as daemon=True. We don't want to deadlock Airflow if our code hangs.

    # This will hang if this timeouts, and extractor is running non-daemon thread inside,
    # since it will never be cleaned up. Ex. SnowflakeOperator
    thread.join(timeout=10)


task_holder = TaskHolder()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()


def direct_execution():
    return is_airflow_version_enough("2.6.0") or getboolean(
        "OPENLINEAGE_AIRFLOW_ENABLE_DIRECT_EXECUTION", False
    )


def execute(_callable):
    try:
        if direct_execution():
            _callable()
        else:
            execute_in_thread(_callable)
    except Exception:
        # Make sure we're not failing task, even for things we think can't happen
        log.exception("Failed to emit OpenLineage event due to exception")


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, "task"):
        log.warning(
            "No task set for TI object task_id: %s - dag_id: %s - run_id %s",
            task_instance.task_id,
            task_instance.dag_id,
            task_instance.run_id,
        )
        return

    log.debug("OpenLineage listener got notification about task instance start")
    dagrun = task_instance.dag_run

    def on_running():
        nonlocal task_instance
        try:
            ti = copy.deepcopy(task_instance)
        except Exception as err:
            log.debug(
                f"Creating a task instance copy failed; proceeding without rendering templates. Error: {err}"
            )
            ti = task_instance
        else:
            ti.render_templates()

        task = ti.task
        dag = task.dag
        task_holder.set_task(ti)
        # that's a workaround to detect task running from deferred state
        # we return here because Airflow 2.3 needs task from deferred state
        if ti.next_method is not None:
            return

        parent_run_id = OpenLineageAdapter.build_dag_run_id(
            dag_id=dag.dag_id,
            execution_date=dagrun.execution_date,
        )
        task_uuid = OpenLineageAdapter.build_task_instance_run_id(
            dag_id=dag.dag_id,
            task_id=task.task_id,
            try_number=ti._try_number,
            execution_date=ti.execution_date,
        )

        task_metadata = extractor_manager.extract_metadata(dagrun, task, task_uuid=task_uuid)

        ti_start_time = ti.start_date if ti.start_date else timezone.utcnow()
        start, end = get_dagrun_start_end(dagrun=dagrun, dag=dag)

        adapter.start_task(
            run_id=task_uuid,
            job_name=get_job_name(task),
            job_description=dag.description,
            event_time=DagUtils.get_start_time(ti_start_time),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            code_location=get_task_location(task),
            nominal_start_time=DagUtils.get_start_time(start),
            nominal_end_time=DagUtils.to_iso_8601(end),
            owners=dag.owner.split(", "),
            task=task_metadata,
            run_facets={
                **get_custom_facets(dagrun, task, dagrun.external_trigger, ti),
                **get_airflow_run_facet(dagrun, dag, ti, task, task_uuid),
            },
        )

    execute(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance success")
    task = task_holder.get_task(task_instance) or task_instance.task
    dag = task.dag
    dagrun = task_instance.dag_run

    parent_run_id = OpenLineageAdapter.build_dag_run_id(
        dag_id=dag.dag_id,
        execution_date=dagrun.execution_date,
    )
    task_uuid = OpenLineageAdapter.build_task_instance_run_id(
        dag_id=dag.dag_id,
        task_id=task.task_id,
        try_number=task_instance._try_number,
        execution_date=task_instance.execution_date,
    )

    def on_success():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )
        adapter.complete_task(
            run_id=task_uuid,
            job_name=get_job_name(task),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
        )

    execute(on_success)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance failure")
    task = task_holder.get_task(task_instance) or task_instance.task
    dag = task.dag
    dagrun = task_instance.dag_run

    parent_run_id = OpenLineageAdapter.build_dag_run_id(
        dag_id=dag.dag_id,
        execution_date=dagrun.execution_date,
    )
    task_uuid = OpenLineageAdapter.build_task_instance_run_id(
        dag_id=dag.dag_id,
        task_id=task.task_id,
        try_number=task_instance._try_number,
        execution_date=task_instance.execution_date,
    )

    def on_failure():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )

        end_date = task_instance.end_date if task_instance.end_date else timezone.utcnow()

        adapter.fail_task(
            run_id=task_uuid,
            job_name=get_job_name(task),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            end_time=DagUtils.to_iso_8601(end_date),
            task=task_metadata,
        )

    execute(on_failure)


@hookimpl
def on_starting(component):
    global executor
    executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="openlineage_")


@hookimpl
def before_stopping(component):
    if executor:
        # stom accepting new events
        executor.shutdown(wait=False)
    # block until all pending events are processed
    adapter.close()


@hookimpl
def on_dag_run_running(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_running`")
        return
    start, end = get_dagrun_start_end(dag_run, dag_run.dag)
    executor.submit(
        adapter.dag_started,
        dag_run=dag_run,
        msg=msg,
        nominal_start_time=DagUtils.get_start_time(start),
        nominal_end_time=DagUtils.to_iso_8601(end),
    )


@hookimpl
def on_dag_run_success(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_success`")
        return
    executor.submit(adapter.dag_success, dag_run=dag_run, msg=msg)


@hookimpl
def on_dag_run_failed(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_failed`")
        return
    executor.submit(adapter.dag_failed, dag_run=dag_run, msg=msg)
