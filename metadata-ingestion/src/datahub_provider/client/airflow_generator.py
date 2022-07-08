from typing import TYPE_CHECKING, Dict, List, Optional, Union

from airflow.configuration import conf

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models import BaseOperator, DagRun, TaskInstance

    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
    from datahub.emitter.rest_emitter import DatahubRestEmitter


class AirflowGenerator:
    @staticmethod
    def _get_dependencies(
        task: "BaseOperator", dag: "DAG", flow_urn: DataFlowUrn
    ) -> List[DataJobUrn]:

        # resolve URNs for upstream nodes in subdags upstream of the current task.
        upstream_subdag_task_urns: List[DataJobUrn] = []

        for upstream_task_id in task.upstream_task_ids:
            upstream_task = dag.task_dict[upstream_task_id]

            # if upstream task is not a subdag, then skip it
            if upstream_task.subdag is None:
                continue

            # else, link the leaf tasks of the upstream subdag as upstream tasks
            upstream_subdag = upstream_task.subdag

            for upstream_subdag_task_id in upstream_subdag.task_dict:
                upstream_subdag_task = upstream_subdag.task_dict[
                    upstream_subdag_task_id
                ]

                upstream_subdag_task_urn = DataJobUrn.create_from_ids(
                    job_id=upstream_subdag_task_id, data_flow_urn=str(flow_urn)
                )

                # if subdag task is a leaf task, then link it as an upstream task
                if len(upstream_subdag_task._downstream_task_ids) == 0:
                    upstream_subdag_task_urns.append(upstream_subdag_task_urn)

        # resolve URNs for upstream nodes that trigger the subdag containing the current task.
        # (if it is in a subdag at all)
        upstream_subdag_triggers: List[DataJobUrn] = []

        # subdags are always named with 'parent.child' style or Airflow won't run them
        # add connection from subdag trigger(s) if subdag task has no upstreams
        if (
            dag.is_subdag
            and dag.parent_dag is not None
            and len(task._upstream_task_ids) == 0
        ):

            # filter through the parent dag's tasks and find the subdag trigger(s)
            subdags = [
                x for x in dag.parent_dag.task_dict.values() if x.subdag is not None
            ]
            matched_subdags = [
                x
                for x in subdags
                if getattr(getattr(x, "subdag"), "dag_id") == dag.dag_id
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
                if subdag_task_id in upstream_task._downstream_task_ids:
                    upstream_subdag_triggers.append(upstream_task_urn)

        # exclude subdag operator tasks since these are not emitted, resulting in empty metadata
        upstream_tasks = (
            [
                DataJobUrn.create_from_ids(job_id=task_id, data_flow_urn=str(flow_urn))
                for task_id in task.upstream_task_ids
                if dag.task_dict[task_id].subdag is None
            ]
            + upstream_subdag_task_urns
            + upstream_subdag_triggers
        )
        return upstream_tasks

    @staticmethod
    def generate_dataflow(
        cluster: str,
        dag: "DAG",
        capture_owner: bool = True,
        capture_tags: bool = True,
    ) -> DataFlow:
        """
        Generates a Dataflow object from an Airflow DAG
        :param cluster: str - name of the cluster
        :param dag: DAG -
        :param capture_tags:
        :param capture_owner:
        :return: DataFlow - Data generated dataflow
        """
        from airflow.serialization.serialized_objects import SerializedDAG

        id = dag.dag_id
        orchestrator = "airflow"
        description = f"{dag.description}\n\n{dag.doc_md or ''}"
        data_flow = DataFlow(
            cluster=cluster, id=id, orchestrator=orchestrator, description=description
        )

        flow_property_bag: Dict[str, str] = {
            key: repr(value)
            for (key, value) in SerializedDAG.serialize_dag(dag).items()
        }
        for key in dag.get_serialized_fields():
            if key not in flow_property_bag:
                flow_property_bag[key] = repr(getattr(dag, key))

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

        data_flow.properties = flow_property_bag
        base_url = conf.get("webserver", "base_url")
        data_flow.url = f"{base_url}/tree?dag_id={dag.dag_id}"

        if capture_owner and dag.owner:
            data_flow.owners.add(dag.owner)

        if capture_tags and dag.tags:
            data_flow.tags.update(dag.tags)

        return data_flow

    @staticmethod
    def _get_description(task: "BaseOperator") -> Optional[str]:
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
        task: "BaseOperator",
        dag: "DAG",
        set_dependendecies: bool = True,
        capture_owner: bool = True,
        capture_tags: bool = True,
    ) -> DataJob:
        """

        :param cluster: str
        :param task: TaskIntance
        :param dag: DAG
        :param set_dependendecies: bool - whether to extract dependencies from airflow task
        :param capture_owner: bool - whether to extract owner from airflow task
        :param capture_tags: bool - whether to set tags automatically from airflow task
        :return: DataJob - returns the generated DataJob object
        """
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator="airflow", env=cluster, flow_id=dag.dag_id
        )
        datajob = DataJob(id=task.task_id, flow_urn=dataflow_urn)
        datajob.description = AirflowGenerator._get_description(task)

        job_property_bag: Dict[str, str] = {
            key: repr(value)
            for (key, value) in SerializedBaseOperator.serialize_operator(task).items()
        }
        for key in task.get_serialized_fields():
            if key not in job_property_bag:
                job_property_bag[key] = repr(getattr(task, key))

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
            "sla",
            "sql",
            "task_id",
            "trigger_rule",
            "wait_for_downstream",
        ]
        job_property_bag = {
            k: v for (k, v) in job_property_bag.items() if k in allowed_task_keys
        }

        datajob.properties = job_property_bag
        base_url = conf.get("webserver", "base_url")
        datajob.url = f"{base_url}/taskinstance/list/?flt1_dag_id_equals={datajob.flow_urn.get_flow_id()}&_flt_3_task_id={task.task_id}"

        if capture_owner and dag.owner:
            datajob.owners.add(dag.owner)

        if capture_tags and dag.tags:
            datajob.tags.update(dag.tags)

        if set_dependendecies:
            datajob.upstream_urns.extend(
                AirflowGenerator._get_dependencies(
                    task=task, dag=dag, flow_urn=datajob.flow_urn
                )
            )

        return datajob

    @staticmethod
    def create_datajob_instance(
        cluster: str,
        task: "BaseOperator",
        dag: "DAG",
        data_job: Optional[DataJob] = None,
    ) -> DataProcessInstance:

        if data_job is None:
            data_job = AirflowGenerator.generate_datajob(cluster, task=task, dag=dag)
        dpi = DataProcessInstance.from_datajob(
            datajob=data_job, id=task.task_id, clone_inlets=True, clone_outlets=True
        )
        return dpi

    @staticmethod
    def run_dataflow(
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        cluster: str,
        dag_run: "DagRun",
        start_timestamp_millis: Optional[int] = None,
        dataflow: Optional[DataFlow] = None,
    ) -> None:

        if dataflow is None:
            assert dag_run.dag
            dataflow = AirflowGenerator.generate_dataflow(cluster, dag_run.dag)

        if start_timestamp_millis is None:
            start_timestamp_millis = int(dag_run.execution_date.timestamp() * 1000)

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
        property_bag["data_interval_start"] = str(dag_run.data_interval_start)
        property_bag["data_interval_end"] = str(dag_run.data_interval_end)
        property_bag["external_trigger"] = str(dag_run.external_trigger)
        dpi.properties.update(property_bag)

        dpi.emit_process_start(
            emitter=emitter, start_timestamp_millis=start_timestamp_millis
        )

    @staticmethod
    def complete_dataflow(
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        cluster: str,
        dag_run: "DagRun",
        end_timestamp_millis: Optional[int] = None,
        dataflow: Optional[DataFlow] = None,
    ) -> None:
        """

        :param emitter: DatahubRestEmitter - the datahub rest emitter to emit the generated mcps
        :param cluster: str - name of the cluster
        :param dag_run: DagRun
        :param end_timestamp_millis: Optional[int] - the completion time in milliseconds if not set the current time will be used.
        :param dataflow: Optional[Dataflow]
        """
        if dataflow is None:
            assert dag_run.dag
            dataflow = AirflowGenerator.generate_dataflow(cluster, dag_run.dag)

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
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        cluster: str,
        ti: "TaskInstance",
        dag: "DAG",
        dag_run: "DagRun",
        start_timestamp_millis: Optional[int] = None,
        datajob: Optional[DataJob] = None,
        attempt: Optional[int] = None,
        emit_templates: bool = True,
    ) -> DataProcessInstance:
        if datajob is None:
            datajob = AirflowGenerator.generate_datajob(cluster, ti.task, dag)

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
        job_property_bag["hostname"] = str(ti.hostname)
        job_property_bag["max_tries"] = str(ti.max_tries)
        # Not compatible with Airflow 1
        if hasattr(ti, "external_executor_id"):
            job_property_bag["external_executor_id"] = str(ti.external_executor_id)
        job_property_bag["pid"] = str(ti.pid)
        job_property_bag["state"] = str(ti.state)
        job_property_bag["operator"] = str(ti.operator)
        job_property_bag["priority_weight"] = str(ti.priority_weight)
        job_property_bag["unixname"] = str(ti.unixname)
        job_property_bag["log_url"] = ti.log_url
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
            assert ti.start_date
            start_timestamp_millis = int(ti.start_date.timestamp() * 1000)

        if attempt is None:
            attempt = ti.try_number

        dpi.emit_process_start(
            emitter=emitter,
            start_timestamp_millis=start_timestamp_millis,
            attempt=attempt,
            emit_template=emit_templates,
        )
        return dpi

    @staticmethod
    def complete_datajob(
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        cluster: str,
        ti: "TaskInstance",
        dag: "DAG",
        dag_run: "DagRun",
        end_timestamp_millis: Optional[int] = None,
        result: Optional[InstanceRunResult] = None,
        datajob: Optional[DataJob] = None,
    ) -> DataProcessInstance:
        """

        :param emitter: DatahubRestEmitter
        :param cluster: str
        :param ti: TaskInstance
        :param dag: DAG
        :param dag_run: DagRun
        :param end_timestamp_millis: Optional[int]
        :param result: Optional[str] One of the result from datahub.metadata.schema_class.RunResultTypeClass
        :param datajob: Optional[DataJob]
        :return: DataProcessInstance
        """
        if datajob is None:
            datajob = AirflowGenerator.generate_datajob(cluster, ti.task, dag)

        if end_timestamp_millis is None:
            assert ti.end_date
            end_timestamp_millis = int(ti.end_date.timestamp() * 1000)

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
