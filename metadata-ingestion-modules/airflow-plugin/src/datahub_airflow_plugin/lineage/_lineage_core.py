from datetime import datetime
from typing import TYPE_CHECKING, Dict, List

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type

from datahub_airflow_plugin._config import DatahubLineageConfig
from datahub_airflow_plugin.client.airflow_generator import AirflowGenerator
from datahub_airflow_plugin.entities import _Entity

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

    from datahub_airflow_plugin._airflow_shims import Operator


def _entities_to_dataset_urn_list(iolets: List[_Entity]) -> List[DatasetUrn]:
    dataset_urn_list: List[DatasetUrn] = []
    for let in iolets:
        if guess_entity_type(let.urn) == "dataset":
            dataset_urn_list.append(DatasetUrn.create_from_string(let.urn))
    return dataset_urn_list


def _entities_to_datajob_urn_list(inlets: List[_Entity]) -> List[DataJobUrn]:
    datajob_urn_list: List[DataJobUrn] = []
    for let in inlets:
        if guess_entity_type(let.urn) == "dataJob":
            datajob_urn_list.append(DataJobUrn.create_from_string(let.urn))
    return datajob_urn_list


def send_lineage_to_datahub(
    config: DatahubLineageConfig,
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
    datajob.inlets.extend(_entities_to_dataset_urn_list(inlets))
    datajob.outlets.extend(_entities_to_dataset_urn_list(outlets))
    datajob.upstream_urns.extend(_entities_to_datajob_urn_list(inlets))

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

    emitter.flush()
