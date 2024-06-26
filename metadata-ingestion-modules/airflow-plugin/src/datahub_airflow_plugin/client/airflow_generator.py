from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union, cast

from airflow.configuration import conf
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.generic_emitter import Emitter
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn

from datahub_airflow_plugin._airflow_compat import AIRFLOW_PATCHED
from datahub_airflow_plugin._config import DatahubLineageConfig, DatajobUrl

assert AIRFLOW_PATCHED

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models import DagRun, TaskInstance

    from datahub_airflow_plugin._airflow_shims import Operator


def _task_downstream_task_ids(operator: "Operator") -> Set[str]:
    if hasattr(operator, "downstream_task_ids"):
        return operator.downstream_task_ids
    return operator._downstream_task_id  # type: ignore[attr-defined,union-attr]


class AirflowGenerator:
    @staticmethod
    def _get_dependencies(
        task: "Operator", dag: "DAG", flow_urn: DataFlowUrn
    ) -> List[DataJobUrn]:
        from datahub_airflow_plugin._airflow_shims import ExternalTaskSensor

        # resolve URNs for upstream nodes in subdags upstream of the current task.
        upstream_subdag_task_urns: List[DataJobUrn] = []

        for upstream_task_id in task.upstream_task_ids:
            upstream_task = dag.task_dict[upstream_task_id]

            # if upstream task is not a subdag, then skip it
            upstream_subdag = getattr(upstream_task, "subdag", None)
            if upstream_subdag is None:
                continue

            # else, link the leaf tasks of the upstream subdag as upstream tasks
            for upstream_subdag_task_id in upstream_subdag.task_dict:
                upstream_subdag_task = upstream_subdag.task_dict[
                    upstream_subdag_task_id
                ]

                upstream_subdag_task_urn = DataJobUrn.create_from_ids(
                    job_id=upstream_subdag_task_id, data_flow_urn=str(flow_urn)
                )

                # if subdag task is a leaf task, then link it as an upstream task
                if len(_task_downstream_task_ids(upstream_subdag_task)) == 0:
                    upstream_subdag_task_urns.append(upstream_subdag_task_urn)

        # resolve URNs for upstream nodes that trigger the subdag containing the current task.
        # (if it is in a subdag at all)
        upstream_subdag_triggers: List[DataJobUrn] = []

        # subdags are always named with 'parent.child' style or Airflow won't run them
        # add connection from subdag trigger(s) if subdag task has no upstreams
        if (
            dag.is_subdag
            and dag.parent_dag is not None
            and len(task.upstream_task_ids) == 0
        ):
            # filter through the parent dag's tasks and find the subdag trigger(s)
            subdags = [
                x for x in dag.parent_dag.task_dict.values() if x.subdag is not None
            ]
            matched_subdags = [
                x for x in subdags if x.subdag and x.subdag.dag_id == dag.dag_id
            ]

            # id of the task containing the subdag
            subdag_task_id = matched_subdags[0].task_id

            # iterate through the parent dag's tasks and find the ones that trigger the subdag
            for upstream_task_id in dag.parent_dag.task_dict:
                upstream_task = dag.parent_dag.task_dict[upstream_task_id]
                upstream_task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(flow_urn), job_id=upstream_task_id
                )

                # if the task triggers the subdag, link it to this node in the subdag
                if subdag_task_id in sorted(_task_downstream_task_ids(upstream_task)):
                    upstream_subdag_triggers.append(upstream_task_urn)

        # If the operator is an ExternalTaskSensor then we set the remote task as upstream.
        # It is possible to tie an external sensor to DAG if external_task_id is omitted but currently we can't tie
        # jobflow to anothet jobflow.
        external_task_upstreams = []
        if isinstance(task, ExternalTaskSensor):
            task = cast(ExternalTaskSensor, task)
            if hasattr(task, "external_task_id") and task.external_task_id is not None:
                external_task_upstreams = [
                    DataJobUrn.create_from_ids(
                        job_id=task.external_task_id,
                        data_flow_urn=str(
                            DataFlowUrn.create_from_ids(
                                orchestrator=flow_urn.orchestrator,
                                flow_id=task.external_dag_id,
                                env=flow_urn.cluster,
                            )
                        ),
                    )
                ]
        # exclude subdag operator tasks since these are not emitted, resulting in empty metadata
        upstream_tasks = (
            [
                DataJobUrn.create_from_ids(job_id=task_id, data_flow_urn=str(flow_urn))
                for task_id in task.upstream_task_ids
                if getattr(dag.task_dict[task_id], "subdag", None) is None
            ]
            + upstream_subdag_task_urns
            + upstream_subdag_triggers
            + external_task_upstreams
        )
        return upstream_tasks

    @staticmethod
    def generate_dataflow(
        config: DatahubLineageConfig,
        dag: "DAG",
    ) -> DataFlow:
        """
        Generates a Dataflow object from an Airflow DAG
        :param cluster: str - name of the cluster
        :param dag: DAG -
        :param capture_tags:
        :param capture_owner:
        :return: DataFlow - Data generated dataflow
        """
        id = dag.dag_id
        orchestrator = "airflow"
        description = "\n\n".join(filter(None, [dag.description, dag.doc_md])) or None
        data_flow = DataFlow(
            env=config.cluster,
            id=id,
            orchestrator=orchestrator,
            description=description,
        )

        flow_property_bag: Dict[str, str] = {}

        allowed_flow_keys = [
            "_access_control",
            "_concurrency",
            # "_default_view",
            "catchup",
            "description",
            "doc_md",
            "fileloc",
            "is_paused_upon_creation",
            "start_date",
            "tags",
            "timezone",
        ]

        for key in allowed_flow_keys:
            if hasattr(dag, key):
                flow_property_bag[key] = repr(getattr(dag, key))

        data_flow.properties = flow_property_bag
        base_url = conf.get("webserver", "base_url")
        data_flow.url = f"{base_url}/tree?dag_id={dag.dag_id}"

        if config.capture_ownership_info and dag.owner:
            owners = [owner.strip() for owner in dag.owner.split(",")]
            if config.capture_ownership_as_group:
                data_flow.group_owners.update(owners)
            else:
                data_flow.owners.update(owners)

        if config.capture_tags_info and dag.tags:
            data_flow.tags.update(dag.tags)

        return data_flow

    @staticmethod
    def _get_description(task: "Operator") -> Optional[str]:
        from airflow.models.baseoperator import BaseOperator

        if not isinstance(task, BaseOperator):
            # TODO: Get docs for mapped operators.
            return None

        if hasattr(task, "doc") and task.doc:
            return task.doc
        elif hasattr(task, "doc_md") and task.doc_md:
            return task.doc_md
        elif hasattr(task, "doc_json") and task.doc_json:
            return task.doc_json
        elif hasattr(task, "doc_yaml") and task.doc_yaml:
            return task.doc_yaml
        elif hasattr(task, "doc_rst") and task.doc_yaml:
            return task.doc_yaml
        return None

    @staticmethod
    def generate_datajob(
        cluster: str,
        task: "Operator",
        dag: "DAG",
        set_dependencies: bool = True,
        capture_owner: bool = True,
        capture_tags: bool = True,
        config: Optional[DatahubLineageConfig] = None,
    ) -> DataJob:
        """

        :param cluster: str
        :param task: TaskIntance
        :param dag: DAG
        :param set_dependencies: bool - whether to extract dependencies from airflow task
        :param capture_owner: bool - whether to extract owner from airflow task
        :param capture_tags: bool - whether to set tags automatically from airflow task
        :param config: DatahubLineageConfig
        :return: DataJob - returns the generated DataJob object
        """
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator="airflow", env=cluster, flow_id=dag.dag_id
        )
        datajob = DataJob(id=task.task_id, flow_urn=dataflow_urn)

        # TODO add support for MappedOperator
        datajob.description = AirflowGenerator._get_description(task)

        job_property_bag: Dict[str, str] = {}

        allowed_task_keys: List[Union[str, Tuple[str, ...]]] = [
            "_task_type",
            "_task_module",
            "depends_on_past",
            "email",
            "label",
            "execution_timeout",
            "sla",
            "sql",
            "task_id",
            "trigger_rule",
            "wait_for_downstream",
            # In Airflow 2.3, _downstream_task_ids was renamed to downstream_task_ids
            ("downstream_task_ids", "_downstream_task_ids"),
            # In Airflow 2.4, _inlets and _outlets were removed in favor of non-private versions.
            ("inlets", "_inlets"),
            ("outlets", "_outlets"),
        ]

        for key in allowed_task_keys:
            if isinstance(key, tuple):
                out_key: str = key[0]
                try_keys = key
            else:
                out_key = key
                try_keys = (key,)

            for k in try_keys:
                if hasattr(task, k):
                    v = getattr(task, k)
                    if out_key == "downstream_task_ids":
                        # Generate these in a consistent order.
                        v = list(sorted(v))
                    job_property_bag[out_key] = repr(v)
                    break

        datajob.properties = job_property_bag
        base_url = conf.get("webserver", "base_url")

        if config and config.datajob_url_link == DatajobUrl.GRID:
            datajob.url = f"{base_url}/dags/{datajob.flow_urn.get_flow_id()}/grid?task_id={task.task_id}"
        else:
            datajob.url = f"{base_url}/taskinstance/list/?flt1_dag_id_equals={datajob.flow_urn.flow_id}&_flt_3_task_id={task.task_id}"

        if capture_owner and dag.owner:
            if config and config.capture_ownership_as_group:
                datajob.group_owners.add(dag.owner)
            else:
                datajob.owners.add(dag.owner)

        if capture_tags and dag.tags:
            datajob.tags.update(dag.tags)

        if set_dependencies:
            datajob.upstream_urns.extend(
                AirflowGenerator._get_dependencies(
                    task=task, dag=dag, flow_urn=datajob.flow_urn
                )
            )

        return datajob

    @staticmethod
    def create_datajob_instance(
        cluster: str,
        task: "Operator",
        dag: "DAG",
        data_job: Optional[DataJob] = None,
        config: Optional[DatahubLineageConfig] = None,
    ) -> DataProcessInstance:
        if data_job is None:
            data_job = AirflowGenerator.generate_datajob(
                cluster, task=task, dag=dag, config=config
            )
        dpi = DataProcessInstance.from_datajob(
            datajob=data_job, id=task.task_id, clone_inlets=True, clone_outlets=True
        )
        return dpi

    @staticmethod
    def run_dataflow(
        emitter: Emitter,
        config: DatahubLineageConfig,
        dag_run: "DagRun",
        start_timestamp_millis: Optional[int] = None,
        dataflow: Optional[DataFlow] = None,
    ) -> None:
        if dataflow is None:
            assert dag_run.dag
            dataflow = AirflowGenerator.generate_dataflow(config, dag_run.dag)

        if start_timestamp_millis is None:
            assert dag_run.execution_date
            start_timestamp_millis = int(dag_run.execution_date.timestamp() * 1000)

        assert dag_run.run_id
        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=dag_run.run_id)

        # This property only exists in Airflow2
        if hasattr(dag_run, "run_type"):
            from airflow.utils.types import DagRunType

            if dag_run.run_type == DagRunType.SCHEDULED:
                dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
            elif dag_run.run_type == DagRunType.MANUAL:
                dpi.type = DataProcessTypeClass.BATCH_AD_HOC
        else:
            if dag_run.run_id.startswith("scheduled__"):
                dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
            else:
                dpi.type = DataProcessTypeClass.BATCH_AD_HOC

        property_bag: Dict[str, str] = {}
        property_bag["run_id"] = str(dag_run.run_id)
        property_bag["execution_date"] = str(dag_run.execution_date)
        property_bag["end_date"] = str(dag_run.end_date)
        property_bag["start_date"] = str(dag_run.start_date)
        property_bag["creating_job_id"] = str(dag_run.creating_job_id)
        # These properties only exists in Airflow>=2.2.0
        if hasattr(dag_run, "data_interval_start") and hasattr(
            dag_run, "data_interval_end"
        ):
            property_bag["data_interval_start"] = str(dag_run.data_interval_start)
            property_bag["data_interval_end"] = str(dag_run.data_interval_end)
        property_bag["external_trigger"] = str(dag_run.external_trigger)
        dpi.properties.update(property_bag)

        dpi.emit_process_start(
            emitter=emitter,
            start_timestamp_millis=start_timestamp_millis,
            materialize_iolets=config.materialize_iolets,
        )

    @staticmethod
    def complete_dataflow(
        emitter: Emitter,
        config: DatahubLineageConfig,
        dag_run: "DagRun",
        end_timestamp_millis: Optional[int] = None,
        dataflow: Optional[DataFlow] = None,
    ) -> None:
        """

        :param emitter: Emitter - the datahub emitter to emit the generated mcps
        :param cluster: str - name of the cluster
        :param dag_run: DagRun
        :param end_timestamp_millis: Optional[int] - the completion time in milliseconds if not set the current time will be used.
        :param dataflow: Optional[Dataflow]
        """
        if dataflow is None:
            assert dag_run.dag
            dataflow = AirflowGenerator.generate_dataflow(config, dag_run.dag)

        assert dag_run.run_id
        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=dag_run.run_id)
        if end_timestamp_millis is None:
            if dag_run.end_date is None:
                raise Exception(
                    f"Dag {dag_run.dag_id}_{dag_run.run_id} is still running and unable to get end_date..."
                )
            end_timestamp_millis = int(dag_run.end_date.timestamp() * 1000)

        # We should use DagRunState but it is not available in Airflow 1
        if dag_run.state == "success":
            result = InstanceRunResult.SUCCESS
        elif dag_run.state == "failed":
            result = InstanceRunResult.FAILURE
        else:
            raise Exception(
                f"Result should be either success or failure and it was {dag_run.state}"
            )

        dpi.emit_process_end(
            emitter=emitter,
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type="airflow",
        )

    @staticmethod
    def run_datajob(
        emitter: Emitter,
        ti: "TaskInstance",
        dag: "DAG",
        dag_run: "DagRun",
        config: DatahubLineageConfig,
        start_timestamp_millis: Optional[int] = None,
        datajob: Optional[DataJob] = None,
        attempt: Optional[int] = None,
        emit_templates: bool = True,
    ) -> DataProcessInstance:
        if datajob is None:
            assert ti.task is not None
            datajob = AirflowGenerator.generate_datajob(
                config.cluster, ti.task, dag, config=config
            )

        assert dag_run.run_id
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{dag.dag_id}_{ti.task_id}_{dag_run.run_id}",
            clone_inlets=True,
            clone_outlets=True,
        )
        job_property_bag: Dict[str, str] = {}
        job_property_bag["run_id"] = str(dag_run.run_id)
        job_property_bag["duration"] = str(ti.duration)
        job_property_bag["start_date"] = str(ti.start_date)
        job_property_bag["end_date"] = str(ti.end_date)
        job_property_bag["execution_date"] = str(ti.execution_date)
        job_property_bag["try_number"] = str(ti.try_number - 1)
        job_property_bag["max_tries"] = str(ti.max_tries)
        # Not compatible with Airflow 1
        if hasattr(ti, "external_executor_id"):
            job_property_bag["external_executor_id"] = str(ti.external_executor_id)
        job_property_bag["state"] = str(ti.state)
        job_property_bag["operator"] = str(ti.operator)
        job_property_bag["priority_weight"] = str(ti.priority_weight)
        job_property_bag["log_url"] = ti.log_url
        job_property_bag["orchestrator"] = "airflow"
        job_property_bag["dag_id"] = str(dag.dag_id)
        job_property_bag["task_id"] = str(ti.task_id)
        dpi.properties.update(job_property_bag)
        dpi.url = ti.log_url

        # This property only exists in Airflow2
        if hasattr(ti, "dag_run") and hasattr(ti.dag_run, "run_type"):
            from airflow.utils.types import DagRunType

            if ti.dag_run.run_type == DagRunType.SCHEDULED:
                dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
            elif ti.dag_run.run_type == DagRunType.MANUAL:
                dpi.type = DataProcessTypeClass.BATCH_AD_HOC
        else:
            if dag_run.run_id.startswith("scheduled__"):
                dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
            else:
                dpi.type = DataProcessTypeClass.BATCH_AD_HOC

        if start_timestamp_millis is None:
            if ti.start_date:
                start_timestamp_millis = int(ti.start_date.timestamp() * 1000)
            else:
                start_timestamp_millis = int(datetime.now().timestamp() * 1000)

        if attempt is None:
            attempt = ti.try_number

        dpi.emit_process_start(
            emitter=emitter,
            start_timestamp_millis=start_timestamp_millis,
            attempt=attempt,
            emit_template=emit_templates,
            materialize_iolets=config.materialize_iolets,
        )
        return dpi

    @staticmethod
    def complete_datajob(
        emitter: Emitter,
        cluster: str,
        ti: "TaskInstance",
        dag: "DAG",
        dag_run: "DagRun",
        end_timestamp_millis: Optional[int] = None,
        result: Optional[InstanceRunResult] = None,
        datajob: Optional[DataJob] = None,
        config: Optional[DatahubLineageConfig] = None,
    ) -> DataProcessInstance:
        """

        :param emitter: Emitter - the datahub emitter to emit the generated mcps
        :param cluster: str
        :param ti: TaskInstance
        :param dag: DAG
        :param dag_run: DagRun
        :param end_timestamp_millis: Optional[int]
        :param result: Optional[str] One of the result from datahub.metadata.schema_class.RunResultTypeClass
        :param datajob: Optional[DataJob]
        :param config: Optional[DatahubLineageConfig]
        :return: DataProcessInstance
        """
        if datajob is None:
            assert ti.task is not None
            datajob = AirflowGenerator.generate_datajob(
                cluster, ti.task, dag, config=config
            )

        if end_timestamp_millis is None:
            if ti.end_date:
                end_timestamp_millis = int(ti.end_date.timestamp() * 1000)
            else:
                end_timestamp_millis = int(datetime.now().timestamp() * 1000)

        if result is None:
            # We should use TaskInstanceState but it is not available in Airflow 1
            if ti.state == "success":
                result = InstanceRunResult.SUCCESS
            elif ti.state == "failed":
                result = InstanceRunResult.FAILURE
            else:
                raise Exception(
                    f"Result should be either success or failure and it was {ti.state}"
                )

        assert datajob is not None
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{dag.dag_id}_{ti.task_id}_{dag_run.run_id}",
            clone_inlets=True,
            clone_outlets=True,
        )
        dpi.emit_process_end(
            emitter=emitter,
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type="airflow",
        )
        return dpi
