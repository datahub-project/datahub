import json
from datetime import datetime, tzinfo
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

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
from datahub_airflow_plugin._airflow_version_specific import (
    get_task_instance_attributes,
)
from datahub_airflow_plugin._config import DatahubLineageConfig, DatajobUrl

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models import DagRun, TaskInstance
    from airflow.sdk import DAG as SdkDAG

    from datahub_airflow_plugin._airflow_shims import Operator

    try:
        from airflow.serialization.serialized_objects import (
            SerializedBaseOperator,
            SerializedDAG,
        )

        # Accept both the core `airflow.models` DAG and the Task SDK `airflow.sdk`
        # DAG. The listener's task hooks pass SDK DAGs (`task.dag`) while the
        # DAG-run hooks pass core DAGs. On Airflow 3.0 these are distinct classes
        # (they converge in 3.1+), so the union keeps both callers typed without
        # per-call-site `# type: ignore[arg-type]`s.
        DagType = Union[DAG, SdkDAG, SerializedDAG]
        OperatorType = Union[Operator, SerializedBaseOperator]
    except ImportError:
        DagType = Union[DAG, SdkDAG]  # type: ignore[misc]
        OperatorType = Operator  # type: ignore[misc]

    # `ti.task` may be a MappedOperator whose module path differs across Airflow 3
    # minor versions (see _shims.py); use `Any` to keep callsites tidy.
    TaskType = Union[OperatorType, Any]  # type: ignore[misc]


def _get_base_url() -> str:
    """Return the base URL for Airflow web UI links.

    Prefers the Airflow 3 `[api] base_url`; falls back to `[webserver] base_url`
    for users who haven't migrated their `airflow.cfg`, then to localhost.
    """
    api_base_url = conf.get("api", "base_url", fallback=None)
    if api_base_url:
        return api_base_url

    base_url = conf.get("webserver", "base_url", fallback=None)
    if base_url:
        return base_url

    return "http://localhost:8080"


def _serialize_iolets_for_properties(iolets: List[Any]) -> List[str]:
    """Serialize inlets/outlets to a list of URIs for stable custom properties.

    This function extracts the URI from various inlet/outlet object types:
    - Airflow Asset / AssetDefinition / deprecated-Dataset objects (have `.uri`)
    - DataHub Dataset entities (have __repr__ that returns URN)
    - Strings (used as-is)

    This avoids using repr() on complex objects which would include memory addresses
    and other unstable information.
    """
    result = []
    for iolet in iolets:
        if hasattr(iolet, "uri"):
            # Airflow Dataset/Asset/AssetDefinition objects
            result.append(str(iolet.uri))
        elif hasattr(iolet, "urn"):
            # DataHub entities with URN
            result.append(str(iolet.urn))
        elif isinstance(iolet, str):
            result.append(iolet)
        else:
            # Fallback to string representation for unknown types
            result.append(str(iolet))
    return result


class AirflowGenerator:
    @staticmethod
    def _get_dependencies(
        task: "OperatorType",
        dag: "DagType",
        flow_urn: DataFlowUrn,
        config: Optional[DatahubLineageConfig] = None,
    ) -> List[DataJobUrn]:
        from datahub_airflow_plugin._airflow_shims import ExternalTaskSensor

        # If the operator is an ExternalTaskSensor then we set the remote task as upstream.
        # It is possible to tie an external sensor to DAG if external_task_id is omitted but currently we can't tie
        # jobflow to another jobflow.
        external_task_upstreams = []
        if isinstance(task, ExternalTaskSensor):
            task = cast(ExternalTaskSensor, task)
            external_task_id = getattr(task, "external_task_id", None)
            external_dag_id = getattr(task, "external_dag_id", None)
            if external_task_id is not None and external_dag_id is not None:
                external_task_upstreams = [
                    DataJobUrn.create_from_ids(
                        job_id=external_task_id,
                        data_flow_urn=str(
                            DataFlowUrn.create_from_ids(
                                orchestrator=flow_urn.orchestrator,
                                flow_id=external_dag_id,
                                env=flow_urn.cluster,
                                platform_instance=config.platform_instance
                                if config
                                else None,
                            )
                        ),
                    )
                ]
        return [
            DataJobUrn.create_from_ids(job_id=task_id, data_flow_urn=str(flow_urn))
            for task_id in task.upstream_task_ids
        ] + external_task_upstreams

    @staticmethod
    def _extract_owners(dag: "DagType") -> List[str]:
        return [owner.strip() for owner in dag.owner.split(",")]

    @staticmethod
    def generate_dataflow(
        config: DatahubLineageConfig,
        dag: "DagType",
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
            platform_instance=config.platform_instance,
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

        def _serialize_dag_property(value: Any) -> str:
            """Serialize DAG property values to string format (JSON-compatible when possible)."""
            if value is None:
                return ""
            elif isinstance(value, bool):
                return "true" if value else "false"
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, (set, frozenset)):
                # Convert set to JSON array string
                return json.dumps(sorted(list(value)))
            elif isinstance(value, tzinfo):
                return str(value.tzname(None))
            elif isinstance(value, (int, float)):
                return str(value)
            elif isinstance(value, str):
                return value
            else:
                # For other types, convert to string but avoid repr() format
                return str(value)

        for key in allowed_flow_keys:
            if hasattr(dag, key):
                value = getattr(dag, key)
                flow_property_bag[key] = _serialize_dag_property(value)

        data_flow.properties = flow_property_bag
        base_url = _get_base_url()
        data_flow.url = f"{base_url}/tree?dag_id={dag.dag_id}"

        if config.capture_ownership_info and dag.owner:
            owners = AirflowGenerator._extract_owners(dag)
            if config.capture_ownership_as_group:
                data_flow.group_owners.update(owners)
            else:
                data_flow.owners.update(owners)

        if config.capture_tags_info and dag.tags:
            data_flow.tags.update(dag.tags)

        return data_flow

    @staticmethod
    def _get_description(task: "OperatorType") -> Optional[str]:
        from datahub_airflow_plugin._airflow_shims import BaseOperator

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
        task: "OperatorType",
        dag: "DagType",
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
            orchestrator="airflow",
            env=cluster,
            flow_id=dag.dag_id,
            platform_instance=config.platform_instance if config else None,
        )

        datajob = DataJob(
            id=task.task_id,
            flow_urn=dataflow_urn,
            platform_instance=config.platform_instance if config else None,
        )

        # TODO add support for MappedOperator
        datajob.description = AirflowGenerator._get_description(task)

        job_property_bag: Dict[str, str] = {}

        allowed_task_keys: List[str] = [
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
            "downstream_task_ids",
            "inlets",
            "outlets",
        ]

        for key in allowed_task_keys:
            if hasattr(task, key):
                v = getattr(task, key)
                if key == "downstream_task_ids":
                    # Generate these in a consistent order.
                    v = list(sorted(v))
                if key in ("inlets", "outlets"):
                    # Serialize inlets/outlets as list of URIs for stable representation.
                    # This avoids including memory addresses from repr() of complex objects.
                    v = _serialize_iolets_for_properties(v)
                job_property_bag[key] = repr(v)

        datajob.properties = job_property_bag
        base_url = _get_base_url()

        if config and config.datajob_url_link == DatajobUrl.GRID:
            datajob.url = f"{base_url}/dags/{dag.dag_id}/grid?task_id={task.task_id}"
        else:
            datajob.url = f"{base_url}/dags/{dag.dag_id}/tasks/{task.task_id}"

        if capture_owner and dag.owner:
            if config and config.capture_ownership_info:
                owners = AirflowGenerator._extract_owners(dag)
                if config.capture_ownership_as_group:
                    datajob.group_owners.update(owners)
                else:
                    datajob.owners.update(owners)

        if capture_tags and dag.tags:
            datajob.tags.update(dag.tags)

        if set_dependencies:
            datajob.upstream_urns.extend(
                AirflowGenerator._get_dependencies(
                    task=task, dag=dag, flow_urn=datajob.flow_urn, config=config
                )
            )

        return datajob

    @staticmethod
    def create_datajob_instance(
        cluster: str,
        task: "Operator",
        dag: "DagType",
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
            # logical_date is nullable in Airflow 3 (manual / asset-triggered runs),
            # so fall back to the run's start_date, then now(), instead of asserting.
            started_at = dag_run.logical_date or dag_run.start_date or datetime.now()
            start_timestamp_millis = int(started_at.timestamp() * 1000)

        assert dag_run.run_id
        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=dag_run.run_id)

        from airflow.utils.types import DagRunType

        if dag_run.run_type == DagRunType.SCHEDULED:
            dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
        elif dag_run.run_type == DagRunType.MANUAL:
            dpi.type = DataProcessTypeClass.BATCH_AD_HOC

        property_bag: Dict[str, str] = {}
        property_bag["run_id"] = str(dag_run.run_id)
        property_bag["logical_date"] = str(dag_run.logical_date)
        property_bag["end_date"] = str(dag_run.end_date)
        property_bag["start_date"] = str(dag_run.start_date)
        property_bag["creating_job_id"] = str(dag_run.creating_job_id)
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
        dag: "DagType",
        dag_run: "DagRun",
        config: DatahubLineageConfig,
        start_timestamp_millis: Optional[int] = None,
        datajob: Optional[DataJob] = None,
        attempt: Optional[int] = None,
        emit_templates: bool = True,
    ) -> DataProcessInstance:
        if datajob is None:
            assert ti.task is not None
            # ti.task may be a MappedOperator whose module differs across Airflow 3 minors.
            datajob = AirflowGenerator.generate_datajob(
                config.cluster,
                ti.task,  # type: ignore[arg-type]
                dag,
                config=config,
            )

        assert dag_run.run_id
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{dag.dag_id}_{ti.task_id}_{dag_run.run_id}",
            clone_inlets=True,
            clone_outlets=True,
        )

        job_property_bag = get_task_instance_attributes(ti)

        # Add orchestrator and DAG/task IDs
        job_property_bag["orchestrator"] = "airflow"
        if "dag_id" not in job_property_bag:
            job_property_bag["dag_id"] = str(dag.dag_id)
        if "task_id" not in job_property_bag:
            job_property_bag["task_id"] = str(ti.task_id)
        if "run_id" not in job_property_bag:
            job_property_bag["run_id"] = str(dag_run.run_id)

        dpi.properties.update(job_property_bag)

        # Set URL if log_url is available
        if "log_url" in job_property_bag:
            dpi.url = job_property_bag["log_url"]

        from airflow.utils.types import DagRunType

        if dag_run.run_type == DagRunType.SCHEDULED:
            dpi.type = DataProcessTypeClass.BATCH_SCHEDULED
        elif dag_run.run_type == DagRunType.MANUAL:
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
        dag: "DagType",
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
            # ti.task may be a MappedOperator whose module differs across Airflow 3 minors.
            datajob = AirflowGenerator.generate_datajob(
                cluster,
                ti.task,  # type: ignore[arg-type]
                dag,
                config=config,
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

        job_property_bag = get_task_instance_attributes(ti)

        # Add orchestrator and DAG/task IDs
        job_property_bag["orchestrator"] = "airflow"
        if "dag_id" not in job_property_bag:
            job_property_bag["dag_id"] = str(dag.dag_id)
        if "task_id" not in job_property_bag:
            job_property_bag["task_id"] = str(ti.task_id)
        if "run_id" not in job_property_bag:
            job_property_bag["run_id"] = str(dag_run.run_id)

        dpi.properties.update(job_property_bag)

        # Set URL if log_url is available
        if "log_url" in job_property_bag:
            dpi.url = job_property_bag["log_url"]

        dpi.emit_process_end(
            emitter=emitter,
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type="airflow",
        )
        return dpi
