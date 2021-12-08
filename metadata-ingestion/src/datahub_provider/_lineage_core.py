import json
from typing import TYPE_CHECKING, Dict, List

from airflow.configuration import conf

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigModel
from datahub_provider.entities import _Entity

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

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
    # This is necessary to avoid issues with circular imports.
    from airflow.serialization.serialized_objects import (
        SerializedBaseOperator,
        SerializedDAG,
    )

    dag: "DAG" = context["dag"]
    task: "BaseOperator" = context["task"]

    # resolve URNs for upstream nodes in subdags upstream of the current task.
    upstream_subdag_task_urns: List[str] = []

    for upstream_task_id in task.upstream_task_ids:
        upstream_task = dag.task_dict[upstream_task_id]

        # if upstream task is not a subdag, then skip it
        if upstream_task.subdag is None:
            continue

        # else, link the leaf tasks of the upstream subdag as upstream tasks
        upstream_subdag = upstream_task.subdag

        upstream_subdag_flow_urn = builder.make_data_flow_urn(
            "airflow", upstream_subdag.dag_id, config.cluster
        )

        for upstream_subdag_task_id in upstream_subdag.task_dict:
            upstream_subdag_task = upstream_subdag.task_dict[upstream_subdag_task_id]

            upstream_subdag_task_urn = builder.make_data_job_urn_with_flow(
                upstream_subdag_flow_urn, upstream_subdag_task_id
            )

            # if subdag task is a leaf task, then link it as an upstream task
            if len(upstream_subdag_task._downstream_task_ids) == 0:

                upstream_subdag_task_urns.append(upstream_subdag_task_urn)

    # resolve URNs for upstream nodes that trigger the subdag containing the current task.
    # (if it is in a subdag at all)
    upstream_subdag_triggers: List[str] = []

    # subdags are always named with 'parent.child' style or Airflow won't run them
    # add connection from subdag trigger(s) if subdag task has no upstreams
    if (
        dag.is_subdag
        and dag.parent_dag is not None
        and len(task._upstream_task_ids) == 0
    ):

        # filter through the parent dag's tasks and find the subdag trigger(s)
        subdags = [x for x in dag.parent_dag.task_dict.values() if x.subdag is not None]
        matched_subdags = [
            x for x in subdags if getattr(getattr(x, "subdag"), "dag_id") == dag.dag_id
        ]

        # id of the task containing the subdag
        subdag_task_id = matched_subdags[0].task_id

        parent_dag_urn = builder.make_data_flow_urn(
            "airflow", dag.parent_dag.dag_id, config.cluster
        )

        # iterate through the parent dag's tasks and find the ones that trigger the subdag
        for upstream_task_id in dag.parent_dag.task_dict:
            upstream_task = dag.parent_dag.task_dict[upstream_task_id]

            upstream_task_urn = builder.make_data_job_urn_with_flow(
                parent_dag_urn, upstream_task_id
            )

            # if the task triggers the subdag, link it to this node in the subdag
            if subdag_task_id in upstream_task._downstream_task_ids:
                upstream_subdag_triggers.append(upstream_task_urn)

    # TODO: capture context
    # context dag_run
    # task_instance: "TaskInstance" = context["task_instance"]
    # TODO: capture raw sql from db operators

    flow_urn = builder.make_data_flow_urn("airflow", dag.dag_id, config.cluster)
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
        key: repr(value) for (key, value) in SerializedDAG.serialize_dag(dag).items()
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
    allowed_task_keys = [
        "_downstream_task_ids",
        "_inlets",
        "_outlets",
        "_task_type",
        "_task_module",
        "depends_on_past",
        "email",
        "label",
        "execution_timeout",
        "end_date",
        "start_date",
        "sla",
        "sql",
        "task_id",
        "trigger_rule",
        "wait_for_downstream",
    ]
    job_property_bag = {
        k: v for (k, v) in job_property_bag.items() if k in allowed_task_keys
    }
    allowed_flow_keys = [
        "_access_control",
        "_concurrency",
        "_default_view",
        "catchup",
        "fileloc",
        "is_paused_upon_creation",
        "start_date",
        "tags",
        "timezone",
    ]
    flow_property_bag = {
        k: v for (k, v) in flow_property_bag.items() if k in allowed_flow_keys
    }

    if config.capture_ownership_info:
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
                time=0, actor=builder.make_user_urn("airflow")
            ),
        )
        # operator.log.info(f"{ownership=}")
        ownership_aspect = [ownership]
    else:
        ownership_aspect = []

    if config.capture_tags_info:
        tags = models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(tag=builder.make_tag_urn(tag))
                for tag in (dag.tags or [])
            ]
        )
        # operator.log.info(f"{tags=}")
        tags_aspect = [tags]
    else:
        tags_aspect = []

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
                *ownership_aspect,
                *tags_aspect,
            ],
        )
    )

    # exclude subdag operator tasks since these are not emitted, resulting in empty metadata
    upstream_tasks = (
        [
            builder.make_data_job_urn_with_flow(flow_urn, task_id)
            for task_id in task.upstream_task_ids
            if dag.task_dict[task_id].subdag is None
        ]
        + upstream_subdag_task_urns
        + upstream_subdag_triggers
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
                    inputDatajobs=upstream_tasks,
                ),
                *ownership_aspect,
                *tags_aspect,
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

    hook = config.make_emitter_hook()

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
