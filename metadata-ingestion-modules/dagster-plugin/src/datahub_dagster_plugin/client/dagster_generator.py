import os
from typing import Dict, List, Optional

import pydantic
from dagster._core.execution.stats import RunStepKeyStatsSnapshot, StepEventStatus
from dagster._core.snap import JobSnapshot
from dagster._core.snap.node import OpDefSnap
from dagster._core.storage.dagster_run import (
    DagsterRun,
    DagsterRunStatsSnapshot,
    DagsterRunStatus,
)
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn


class Constant:
    """
    keys used in dagster plugin
    """

    ORCHESTRATOR = "dagster"

    # Default config constants
    DEFAULT_DATAHUB_REST_URL = "http://localhost:8080"

    # Environment variable contants
    DATAHUB_REST_URL = "DATAHUB_REST_URL"
    DATAHUB_ENV = "DATAHUB_ENV"
    DATAHUB_PLATFORM_INSTANCE = "DATAHUB_PLATFORM_INSTANCE"

    # Datahub inputs/outputs constant
    DATAHUB_INPUTS = "datahub.inputs"
    DATAHUB_OUTPUTS = "datahub.outputs"

    # Job run constant
    JOB_SNAPSHOT_ID = "job_snapshot_id"
    EXECUTION_PLAN_SNAPSHOT_ID = "execution_plan_snapshot_id"
    ROOT_RUN_ID = "root_run_id"
    PARENT_RUN_ID = "parent_run_id"
    HAS_REPOSITORY_LOAD_DATA = "has_repository_load_data"
    TAGS = "tags"
    STEPS_SUCCEEDED = "steps_succeeded"
    STEPS_FAILED = "steps_failed"
    MATERIALIZATIONS = "materializations"
    EXPECTATIONS = "expectations"
    ENQUEUED_TIME = "enqueued_time"
    LAUNCH_TIME = "launch_time"
    START_TIME = "start_time"
    END_TIME = "end_time"

    # Op run contants
    STEP_KEY = "step_key"
    ATTEMPTS = "attempts"


class DagsterSourceConfig(DatasetSourceConfigMixin):
    datahub_rest_url: str = pydantic.Field(
        default=Constant.DEFAULT_DATAHUB_REST_URL,
        description="Datahub GMS Rest URL. Default to http://localhost:8080",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datahub_rest_url: str = os.getenv(
            Constant.DATAHUB_REST_URL, Constant.DEFAULT_DATAHUB_REST_URL
        )
        self.env = os.getenv(Constant.DATAHUB_ENV, DEFAULT_ENV)
        self.platform_instance = os.getenv(Constant.DATAHUB_PLATFORM_INSTANCE)


def _str_urn_to_dataset_urn(urns: List[str]) -> List[DatasetUrn]:
    return [DatasetUrn.create_from_string(urn) for urn in urns]


class DagsterGenerator:
    @staticmethod
    def generate_dataflow(
        job_snapshot: JobSnapshot, env: str, platform_instance: Optional[str] = None
    ) -> DataFlow:
        """
        Generates a Dataflow object from an Dagster Job Snapshot
        :param job_snapshot: JobSnapshot - Job snapshot object
        :param env: str
        :param platform_instance: Optional[str]
        :return: DataFlow - Data generated dataflow
        """
        dataflow = DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=job_snapshot.name,
            env=env,
            name=job_snapshot.name,
            platform_instance=platform_instance,
        )
        dataflow.description = job_snapshot.description
        dataflow.tags = set(job_snapshot.tags.keys())
        flow_property_bag: Dict[str, str] = {}
        for key in job_snapshot.metadata.keys():
            flow_property_bag[key] = str(job_snapshot.metadata[key])
        dataflow.properties = flow_property_bag

        return dataflow

    @staticmethod
    def generate_datajob(
        job_snapshot: JobSnapshot,
        step_deps: Dict[str, List],
        op_def_snap: OpDefSnap,
        env: str,
        platform_instance: Optional[str] = None,
    ) -> DataJob:
        """
        Generates a Datajob object from an Dagster op snapshot
        :param job_snapshot: JobSnapshot - Job snapshot object
        :param op_def_snap: OpDefSnap - Op def snapshot object
        :param env: str
        :param platform_instance: Optional[str]
        :return: DataJob - Data generated datajob
        """
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=job_snapshot.name,
            env=env,
            platform_instance=platform_instance,
        )
        datajob = DataJob(
            id=op_def_snap.name,
            flow_urn=dataflow_urn,
            name=op_def_snap.name,
        )

        datajob.description = op_def_snap.description
        datajob.tags = set(op_def_snap.tags.keys())

        # Add upstream dependencies for this op
        for upstream_op_name in step_deps[op_def_snap.name]:
            upstream_op_urn = DataJobUrn.create_from_ids(
                data_flow_urn=str(dataflow_urn),
                job_id=upstream_op_name,
            )
            datajob.upstream_urns.extend([upstream_op_urn])

        job_property_bag: Dict[str, str] = {}

        # For all op inputs/outputs:
        # Add input/output details like its type, description, metadata etc in datajob properties.
        # Also, add datahub inputs/outputs if present in input/output metatdata.
        for input_def_snap in op_def_snap.input_def_snaps:
            job_property_bag[f"input.{input_def_snap.name}"] = str(
                input_def_snap._asdict()
            )
            if Constant.DATAHUB_INPUTS in input_def_snap.metadata:
                datajob.inlets.extend(
                    _str_urn_to_dataset_urn(
                        input_def_snap.metadata[Constant.DATAHUB_INPUTS].value
                    )
                )

        for output_def_snap in op_def_snap.output_def_snaps:
            job_property_bag[f"output_{output_def_snap.name}"] = str(
                output_def_snap._asdict()
            )
            if Constant.DATAHUB_OUTPUTS in output_def_snap.metadata:
                datajob.outlets.extend(
                    _str_urn_to_dataset_urn(
                        output_def_snap.metadata[Constant.DATAHUB_OUTPUTS].value
                    )
                )

        datajob.properties = job_property_bag

        return datajob

    @staticmethod
    def emit_job_run(
        emitter: DatahubRestEmitter,
        dataflow: DataFlow,
        run: DagsterRun,
        run_stats: DagsterRunStatsSnapshot,
    ) -> None:
        """
        Emit a latest job run
        :param emitter: DatahubRestEmitter
        :param dataflow: DataFlow - DataFlow object
        :param run: DagsterRun - Dagster Run object
        :param run_stats: DagsterRunStatsSnapshot - latest job run stats
        """
        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=run_stats.run_id)

        # Add below details in dpi properties
        dpi_property_bag: Dict[str, str] = {}
        allowed_job_run_keys = [
            Constant.JOB_SNAPSHOT_ID,
            Constant.EXECUTION_PLAN_SNAPSHOT_ID,
            Constant.ROOT_RUN_ID,
            Constant.PARENT_RUN_ID,
            Constant.HAS_REPOSITORY_LOAD_DATA,
            Constant.TAGS,
            Constant.STEPS_SUCCEEDED,
            Constant.STEPS_FAILED,
            Constant.MATERIALIZATIONS,
            Constant.EXPECTATIONS,
            Constant.ENQUEUED_TIME,
            Constant.LAUNCH_TIME,
            Constant.START_TIME,
            Constant.END_TIME,
        ]
        for key in allowed_job_run_keys:
            if hasattr(run, key) and getattr(run, key) is not None:
                dpi_property_bag[key] = str(getattr(run, key))
            if hasattr(run_stats, key) and getattr(run_stats, key) is not None:
                dpi_property_bag[key] = str(getattr(run_stats, key))
        dpi.properties.update(dpi_property_bag)

        status_result_map = {
            DagsterRunStatus.SUCCESS: InstanceRunResult.SUCCESS,
            DagsterRunStatus.FAILURE: InstanceRunResult.FAILURE,
            DagsterRunStatus.CANCELED: InstanceRunResult.SKIPPED,
        }

        if run.status not in status_result_map:
            raise Exception(
                f"Job run status should be either complete, failed or cancelled and it was "
                f"{run.status }"
            )

        if run_stats.start_time is not None:
            dpi.emit_process_start(
                emitter=emitter,
                start_timestamp_millis=int(run_stats.start_time * 1000),
            )

        if run_stats.end_time is not None:
            dpi.emit_process_end(
                emitter=emitter,
                end_timestamp_millis=int(run_stats.end_time * 1000),
                result=status_result_map[run.status],
                result_type=Constant.ORCHESTRATOR,
            )

    @staticmethod
    def emit_op_run(
        emitter: DatahubRestEmitter,
        datajob: DataJob,
        run_step_stats: RunStepKeyStatsSnapshot,
    ) -> None:
        """
        Emit an op run
        :param emitter: DatahubRestEmitter
        :param datajob: DataJob - DataJob object
        :param run_step_stats: RunStepKeyStatsSnapshot - step(op) run stats
        """
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{run_step_stats.run_id}.{datajob.id}",
            clone_inlets=True,
            clone_outlets=True,
        )

        # Add below details in dpi properties
        dpi_property_bag: Dict[str, str] = {}
        allowed_op_run_keys = [
            Constant.STEP_KEY,
            Constant.ATTEMPTS,
            Constant.START_TIME,
            Constant.END_TIME,
        ]
        for key in allowed_op_run_keys:
            if (
                hasattr(run_step_stats, key)
                and getattr(run_step_stats, key) is not None
            ):
                dpi_property_bag[key] = str(getattr(run_step_stats, key))
        dpi.properties.update(dpi_property_bag)

        status_result_map = {
            StepEventStatus.SUCCESS: InstanceRunResult.SUCCESS,
            StepEventStatus.FAILURE: InstanceRunResult.FAILURE,
            StepEventStatus.SKIPPED: InstanceRunResult.SKIPPED,
        }

        if run_step_stats.status not in status_result_map:
            raise Exception(
                f"Step run status should be either complete, failed or cancelled and it was "
                f"{run_step_stats.status }"
            )

        if run_step_stats.start_time is not None:
            dpi.emit_process_start(
                emitter=emitter,
                start_timestamp_millis=int(run_step_stats.start_time * 1000),
            )

        if run_step_stats.end_time is not None:
            dpi.emit_process_end(
                emitter=emitter,
                end_timestamp_millis=int(run_step_stats.end_time * 1000),
                result=status_result_map[run_step_stats.status],
                result_type=Constant.ORCHESTRATOR,
            )
