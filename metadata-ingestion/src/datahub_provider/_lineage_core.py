from datetime import datetime
from typing import TYPE_CHECKING, Dict, List

import datahub.emitter.mce_builder as builder
from datahub.api.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.configuration.common import ConfigModel
from datahub_provider.client.airflow_generator import AirflowGenerator
from datahub_provider.entities import _Entity

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.taskinstance import TaskInstance

    from datahub_provider.hooks.datahub import DatahubGenericHook


def _entities_to_urn_list(iolets: List[_Entity]) -> List[str]:
    return [let.urn for let in iolets]


class DatahubBasicLineageConfig(ConfigModel):
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


def send_lineage_to_datahub(
    config: DatahubBasicLineageConfig,
    operator: "BaseOperator",
    inlets: List[_Entity],
    outlets: List[_Entity],
    context: Dict,
) -> None:
    dag: "DAG" = context["dag"]
    task: "BaseOperator" = context["task"]
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
    print(f"Emitted from Lineage: {dataflow}")

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

    if config.capture_executions:
        dpi = AirflowGenerator.run_datajob(
            emitter=emitter, cluster=config.cluster, ti=ti, dag=dag, datajob=datajob
        )

        print(f"Emitted from Lineage: {dpi}")

        dpi = AirflowGenerator.complete_datajob(
            emitter=emitter,
            cluster=config.cluster,
            ti=ti,
            dag=dag,
            datajob=datajob,
            result=InstanceRunResult.SUCCESS,
            end_timestamp_millis=int(datetime.utcnow().timestamp() * 1000),
        )
        print(f"Emitted from Lineage: {dpi}")
