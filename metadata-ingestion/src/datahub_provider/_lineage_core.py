from datahub_provider._airflow_compat import Operator

from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.configuration.common import ConfigModel
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.client.airflow_generator import AirflowGenerator
from datahub_provider.entities import _Entity

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    from datahub_provider.hooks.datahub import DatahubGenericHook


def _entities_to_urn_list(iolets: List[_Entity]) -> List[DatasetUrn]:
    return [DatasetUrn.create_from_string(let.urn) for let in iolets]


class DatahubBasicLineageConfig(ConfigModel):
    enabled: bool = True

    # DataHub hook connection ID.
    datahub_conn_id: str

    # Cluster to associate with the pipelines and tasks. Defaults to "prod".
    cluster: str = builder.DEFAULT_FLOW_CLUSTER

    # If true, the owners field of the DAG will be capture as a DataHub corpuser.
    capture_ownership_info: bool = True

    # If true, the tags field of the DAG will be captured as DataHub tags.
    capture_tags_info: bool = True

    capture_executions: bool = False

    def make_emitter_hook(self) -> "DatahubGenericHook":
        # This is necessary to avoid issues with circular imports.
        from datahub_provider.hooks.datahub import DatahubGenericHook

        return DatahubGenericHook(self.datahub_conn_id)


def _task_underscore_inlets(operator: "Operator") -> Optional[List]:
    if hasattr(operator, "_inlets"):
        return operator._inlets  # type: ignore[attr-defined,union-attr]
    return None


def _task_underscore_outlets(operator: "Operator") -> Optional[List]:
    if hasattr(operator, "_outlets"):
        return operator._outlets  # type: ignore[attr-defined,union-attr]
    return None


def send_lineage_to_datahub(
    config: DatahubBasicLineageConfig,
    operator: "Operator",
    inlets: List[_Entity],
    outlets: List[_Entity],
    context: Dict,
) -> None:
    if not config.enabled:
        return

    dag: "DAG" = context["dag"]
    task: "Operator" = context["task"]
    ti: "TaskInstance" = context["task_instance"]

    hook = config.make_emitter_hook()
    emitter = hook.make_emitter()

    dataflow = AirflowGenerator.generate_dataflow(
        cluster=config.cluster,
        dag=dag,
        capture_tags=config.capture_tags_info,
        capture_owner=config.capture_ownership_info,
    )
    dataflow.emit(emitter)
    operator.log.info(f"Emitted from Lineage: {dataflow}")

    datajob = AirflowGenerator.generate_datajob(
        cluster=config.cluster,
        task=task,
        dag=dag,
        capture_tags=config.capture_tags_info,
        capture_owner=config.capture_ownership_info,
    )
    datajob.inlets.extend(_entities_to_urn_list(inlets))
    datajob.outlets.extend(_entities_to_urn_list(outlets))

    datajob.emit(emitter)
    operator.log.info(f"Emitted from Lineage: {datajob}")

    if config.capture_executions:
        dag_run: "DagRun" = context["dag_run"]

        dpi = AirflowGenerator.run_datajob(
            emitter=emitter,
            cluster=config.cluster,
            ti=ti,
            dag=dag,
            dag_run=dag_run,
            datajob=datajob,
            emit_templates=False,
        )

        operator.log.info(f"Emitted from Lineage: {dpi}")

        dpi = AirflowGenerator.complete_datajob(
            emitter=emitter,
            cluster=config.cluster,
            ti=ti,
            dag=dag,
            dag_run=dag_run,
            datajob=datajob,
            result=InstanceRunResult.SUCCESS,
            end_timestamp_millis=int(datetime.utcnow().timestamp() * 1000),
        )
        operator.log.info(f"Emitted from Lineage: {dpi}")


def preprocess_task_iolets(task: "Operator", context: Dict) -> None:
    # This is necessary to avoid issues with circular imports.
    from airflow.lineage import prepare_lineage

    from datahub_provider.hooks.datahub import AIRFLOW_1

    # Detect Airflow 1.10.x inlet/outlet configurations in Airflow 2.x, and
    # convert to the newer version. This code path will only be triggered
    # when 2.x receives a 1.10.x inlet/outlet config.
    needs_repeat_preparation = False

    # Translate inlets.
    previous_inlets = _task_underscore_inlets(task)
    if (
        not AIRFLOW_1
        and previous_inlets is not None
        and isinstance(previous_inlets, list)
        and len(previous_inlets) == 1
        and isinstance(previous_inlets[0], dict)
    ):
        from airflow.lineage import AUTO

        task._inlets = [  # type: ignore[attr-defined,union-attr]
            # See https://airflow.apache.org/docs/apache-airflow/1.10.15/lineage.html.
            *previous_inlets[0].get("datasets", []),  # assumes these are attr-annotated
            *previous_inlets[0].get("task_ids", []),
            *([AUTO] if previous_inlets[0].get("auto", False) else []),
        ]
        needs_repeat_preparation = True

    # Translate outlets.
    previous_outlets = _task_underscore_outlets(task)
    if (
        not AIRFLOW_1
        and previous_inlets is not None
        and isinstance(previous_outlets, list)
        and len(previous_outlets) == 1
        and isinstance(previous_outlets[0], dict)
    ):
        task._outlets = [*previous_outlets[0].get("datasets", [])]  # type: ignore[attr-defined,union-attr]
        needs_repeat_preparation = True

    # Rerun the lineage preparation routine, now that the old format has been translated to the new one.
    if needs_repeat_preparation:
        prepare_lineage(lambda self, ctx: None)(task, context)
