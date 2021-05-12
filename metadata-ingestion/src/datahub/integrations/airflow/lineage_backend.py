import json
from typing import TYPE_CHECKING, Dict, List, Optional

import dateutil.parser
from airflow.lineage.backend import LineageBackend

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models

if TYPE_CHECKING:
    from airflow import DAG

    # from airflow.taskinstance import TaskInstance
    from airflow.models.baseoperator import (
        BaseOperator,
    )  # pylint: disable=cyclic-import

    from datahub.integrations.airflow.hooks import (
        DatahubGenericHook,
    )  # pylint: disable=cyclic-import

from airflow.configuration import conf


def _entities_to_urn_list(iolets: List) -> List[str]:
    return [let.urn for let in iolets]


def make_emitter_hook() -> "DatahubGenericHook":
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
    ) -> None:
        # This is necessary to avoid issues with circular imports.
        from airflow.lineage import prepare_lineage
        from airflow.serialization.serialized_objects import (
            SerializedBaseOperator,
            SerializedDAG,
        )

        from datahub.integrations.airflow.hooks import AIRFLOW_1

        # Detect Airflow 1.10.x inlet/outlet configurations in Airflow 2.x, and
        # convert to the newer version. This code path will only be triggered
        # when 2.x receives a 1.10.x inlet/outlet config.
        needs_repeat_preparation = False
        if (
            not AIRFLOW_1
            and isinstance(operator._inlets, list)
            and len(operator._inlets) == 1
            and isinstance(operator._inlets[0], dict)
        ):
            from airflow.lineage import AUTO

            operator._inlets = [
                # See https://airflow.apache.org/docs/apache-airflow/1.10.15/lineage.html.
                *operator._inlets[0].get(
                    "datasets", []
                ),  # assumes these are attr-annotated
                *operator._inlets[0].get("task_ids", []),
                *([AUTO] if operator._inlets[0].get("auto", False) else []),
            ]
            needs_repeat_preparation = True
        if (
            not AIRFLOW_1
            and isinstance(operator._outlets, list)
            and len(operator._outlets) == 1
            and isinstance(operator._outlets[0], dict)
        ):
            operator._outlets = [*operator._outlets[0].get("datasets", [])]
            needs_repeat_preparation = True
        if needs_repeat_preparation:
            # Rerun the lineage preparation routine, now that the old format has been translated to the new one.
            prepare_lineage(lambda self, ctx: None)(operator, context)

        context = context or {}  # ensure not None to satisfy mypy

        dag: "DAG" = context["dag"]
        task = context["task"]

        # TODO: capture context
        # context dag_run
        # task_instance: "TaskInstance" = context["task_instance"]
        # TODO: capture raw sql from db operators

        flow_urn = builder.make_data_flow_urn("airflow", dag.dag_id)
        job_urn = builder.make_data_job_urn_with_flow(flow_urn, task.task_id)

        base_url = conf.get("webserver", "base_url")
        flow_url = f"{base_url}/tree?dag_id={dag.dag_id}"
        job_url = f"{base_url}/taskinstance/list/?flt1_dag_id_equals={dag.dag_id}&_flt_3_task_id={task.task_id}"
        # operator.log.info(f"{flow_url=}")
        # operator.log.info(f"{job_url=}")
        # operator.log.info(f"{dag.get_serialized_fields()=}")
        # operator.log.info(f"{task.get_serialized_fields()=}")
        # operator.log.info(f"{SerializedDAG.serialize_dag(dag)=}")

        flow_property_bag: Dict[str, str] = {
            key: repr(value)
            for (key, value) in SerializedDAG.serialize_dag(dag).items()
        }
        for key in dag.get_serialized_fields():
            if key not in flow_property_bag:
                flow_property_bag[key] = repr(getattr(dag, key))
        job_property_bag: Dict[str, str] = {
            key: repr(value)
            for (key, value) in SerializedBaseOperator.serialize_operator(task).items()
        }
        for key in task.get_serialized_fields():
            if key not in job_property_bag:
                job_property_bag[key] = repr(getattr(task, key))
        # operator.log.info(f"{flow_property_bag=}")
        # operator.log.info(f"{job_property_bag=}")

        timestamp = int(dateutil.parser.parse(context["ts"]).timestamp() * 1000)
        ownership = models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner=builder.make_user_urn(dag.owner),
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
        # operator.log.info(f"{ownership=}")

        tags = models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(tag=builder.make_tag_urn(f"airflow_{tag}"))
                for tag in (dag.tags or [])
            ]
        )
        # operator.log.info(f"{tags=}")

        flow_mce = models.MetadataChangeEventClass(
            proposedSnapshot=models.DataFlowSnapshotClass(
                urn=flow_urn,
                aspects=[
                    models.DataFlowInfoClass(
                        name=dag.dag_id,
                        description=f"{dag.description}\n\n{dag.doc_md or ''}",
                        customProperties=flow_property_bag,
                        externalUrl=flow_url,
                    ),
                    ownership,
                    tags,
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
                        description=None,
                        customProperties=job_property_bag,
                        externalUrl=job_url,
                    ),
                    models.DataJobInputOutputClass(
                        inputDatasets=_entities_to_urn_list(inlets or []),
                        outputDatasets=_entities_to_urn_list(outlets or []),
                    ),
                    ownership,
                    tags,
                ],
            )
        )

        force_entity_materialization = [
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DatasetSnapshotClass(
                    urn=iolet,
                    aspects=[
                        models.StatusClass(removed=False),
                    ],
                )
            )
            for iolet in _entities_to_urn_list((inlets or []) + (outlets or []))
        ]

        hook = make_emitter_hook()

        mces = [
            flow_mce,
            job_mce,
            *force_entity_materialization,
        ]
        operator.log.info(
            "DataHub lineage backend - emitting metadata:\n"
            + "\n".join(json.dumps(mce.to_obj()) for mce in mces)
        )
        hook.emit_mces(mces)
