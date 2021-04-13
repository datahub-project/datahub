import json
from typing import TYPE_CHECKING, Dict, List, Optional

import dateutil.parser
from airflow.lineage.backend import LineageBackend

import datahub.emitter.mce_builder as builder
import datahub.metadata as models

if TYPE_CHECKING:
    from airflow import DAG

    # from airflow.taskinstance import TaskInstance
    from airflow.models.baseoperator import (
        BaseOperator,
    )  # pylint: disable=cyclic-import

from airflow.configuration import conf


def _entities_to_urn_list(iolets: List):
    return [let.urn for let in iolets]


def make_emitter_hook():
    # This is necessary to avoid issues with circular imports.
    from datahub.integrations.airflow.hooks import DatahubGenericHook

    _datahub_conn_id = conf.get("lineage", "datahub_conn_id")
    return DatahubGenericHook(_datahub_conn_id)


class DatahubAirflowLineageBackend(LineageBackend):
    # With Airflow 2.0, this can be an instance method. However, with Airflow 1.10.x, this
    # method is used statically, even though LineageBackend declares it as an instance variable.
    @staticmethod
    def send_lineage(
        operator: "BaseOperator",
        inlets: Optional[List] = None,
        outlets: Optional[List] = None,
        context: Dict = None,
    ):
        context = context or {}  # ensure not None to satisfy mypy

        dag: "DAG" = context["dag"]
        task = context["task"]
        # task_instance: "TaskInstance" = context["task_instance"]

        # TODO: verify if task and operator are the same?
        # TODO: use dag serialization to just save the whole thing.
        # TODO: save context.get("conf")
        # TODO: save DAG tags
        # TODO: save context.get("dag_run")
        # TODO: save all the data from task_instance
        # TODO: capture raw sql from db operators

        flow_urn = builder.make_data_flow_urn("airflow", dag.dag_id)
        job_urn = builder.make_data_job_urn_with_flow(flow_urn, task.task_id)

        timestamp = int(dateutil.parser.parse(context["ts"]).timestamp() * 1000)
        ownership = models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner=dag.owner,
                    type=models.OwnershipTypeClass.DEVELOPER,
                    source=models.OwnershipSourceClass(
                        type=models.OwnershipSourceTypeClass.SERVICE,
                        url=dag.filepath,
                    ),
                )
            ],
            lastModified=models.AuditStampClass(
                time=timestamp, actor=builder.make_user_urn("airflow")
            ),
        )

        flow_mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataFlowSnapshotClass(
                urn=flow_urn,
                aspects=[
                    models.DataFlowInfoClass(
                        name=dag.dag_id,
                        description=f"{dag.description}\n\n{dag.doc_md}",
                    ),
                    ownership,
                ],
            )
        )

        job_mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataJobSnapshotClass(
                urn=job_urn,
                aspects=[
                    models.DataJobInfoClass(
                        name=task.task_id,
                        type=models.AzkabanJobTypeClass.COMMAND,
                        description=None,  # TODO: add datajob description
                    ),
                    models.DataJobInputOutputClass(
                        inputDatasets=_entities_to_urn_list(inlets or []),
                        outputDatasets=_entities_to_urn_list(outlets or []),
                    ),
                    ownership,
                ],
            )
        )

        lineage_mces = [
            builder.make_lineage_mce(_entities_to_urn_list(inlets or []), outlet)
            for outlet in _entities_to_urn_list(outlets or [])
        ]

        hook = make_emitter_hook()

        mces = [
            flow_mce,
            job_mce,
            *lineage_mces,
        ]
        operator.log.info(
            "DataHub lineage backend - emitting metadata:\n"
            + "\n".join(json.dumps(mce.to_obj()) for mce in mces)
        )
        hook.emit_mces(mces)
