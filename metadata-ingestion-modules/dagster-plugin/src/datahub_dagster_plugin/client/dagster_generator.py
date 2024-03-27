from dataclasses import dataclass
from logging import Logger
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence, Set
from urllib.parse import urlsplit

import pydantic
from dagster import DagsterRunStatus, PathMetadataValue, RunStatusSensorContext
from dagster._core.execution.stats import RunStepKeyStatsSnapshot, StepEventStatus
from dagster._core.snap import JobSnapshot
from dagster._core.snap.node import OpDefSnap
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatsSnapshot
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.api.entities.dataset.dataset import Dataset
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import DataPlatformInstanceClass, SubTypesClass
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
    DAGSTER_UI_URL = "DAGSTER_UI_URL"

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


class DatasetLineage(NamedTuple):
    inputs: Set[str]
    outputs: Set[str]


class DatahubDagsterSourceConfig(DatasetSourceConfigMixin):
    datahub_client_config: DatahubClientConfig = pydantic.Field(
        default=DatahubClientConfig(),
        description="Datahub client config",
    )

    dagster_url: Optional[str] = pydantic.Field(
        default=None,
        description="Dagster UI URL. Like: https://myDagsterCloudEnvironment.dagster.cloud/prod",
    )

    capture_asset_materialization: bool = pydantic.Field(
        default=True,
        description="Whether to capture asset keys as Dataset on AssetMaterialization event",
    )

    capture_input_output: bool = pydantic.Field(
        default=False,
        description="Whether to capture and try to parse input and output from HANDLED_OUTPUT, LOADED_INPUT event. (currently only filepathvalue metadata supported",
    )

    asset_lineage_extractor: Optional[
        Callable[
            [RunStatusSensorContext, "DagsterGenerator", DataHubGraph],
            Dict[str, DatasetLineage],
        ]
    ] = pydantic.Field(
        default=None,
        description="Custom asset lineage extractor function. See details at [https://datahubproject.io/docs/lineage/dagster/#define-your-custom-logic-to-capture-asset-lineage-information]",
    )


def _str_urn_to_dataset_urn(urns: List[str]) -> List[DatasetUrn]:
    return [DatasetUrn.create_from_string(urn) for urn in urns]


@dataclass
class DagsterEnvironment:
    repository: Optional[str]
    is_cloud: bool = True
    is_branch_deployment: bool = False
    branch: Optional[str] = "prod"
    module: Optional[str] = None


def job_url_generator(dagster_url: str, dagster_environment: DagsterEnvironment) -> str:
    if dagster_environment.is_cloud:
        base_url = f"{dagster_url}/{dagster_environment.branch}"
    else:
        base_url = dagster_url

    if dagster_environment.module:
        base_url = f"{base_url}/locations/{dagster_environment.module}"

    return base_url


class DagsterGenerator:
    def __init__(
        self,
        logger: Logger,
        config: DatahubDagsterSourceConfig,
        dagster_environment: DagsterEnvironment,
    ):
        self.logger = logger
        self.config = config
        self.dagster_environment = dagster_environment

    def path_metadata_resolver(self, value: PathMetadataValue) -> Optional[DatasetUrn]:
        """
        Resolve path metadata to dataset urn
        """
        path = value.value
        if not path:
            return None

        if "://" in path:
            url = urlsplit(path)
            scheme = url.scheme

            # Need to adjust some these schemes
            if scheme in ["s3a", "s3n"]:
                scheme = "s3"
            elif scheme in ["gs"]:
                scheme = "gcs"

            return DatasetUrn(platform=scheme, name=url.path)
        else:
            return DatasetUrn(platform="file", name=path)

    def metadata_resolver(self, metadata: Any) -> Optional[DatasetUrn]:
        """
        Resolve metadata to dataset urn
        """
        if isinstance(metadata, PathMetadataValue):
            return self.path_metadata_resolver(metadata)
        else:
            self.logger.info(f"Unknown Metadata: {metadata} of type {type(metadata)}")
        return None

    def generate_dataflow(
        self,
        job_snapshot: JobSnapshot,
        env: str,
        platform_instance: Optional[str] = None,
    ) -> DataFlow:
        """
        Generates a Dataflow object from an Dagster Job Snapshot
        :param job_snapshot: JobSnapshot - Job snapshot object
        :param env: str
        :param platform_instance: Optional[str]
        :return: DataFlow - Data generated dataflow
        """
        if self.dagster_environment.is_cloud:
            id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{job_snapshot.name}"
        else:
            id = f"{self.dagster_environment.module}/{job_snapshot.name}"

        dataflow = DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=id,
            env=env,
            name=job_snapshot.name,
            platform_instance=platform_instance,
        )
        dataflow.description = job_snapshot.description
        dataflow.tags = set(job_snapshot.tags.keys())
        if self.config.dagster_url:
            dataflow.url = f"{job_url_generator(dagster_url=self.config.dagster_url, dagster_environment=self.dagster_environment)}/jobs/{job_snapshot.name}"
        flow_property_bag: Dict[str, str] = {}
        for key in job_snapshot.metadata.keys():
            flow_property_bag[key] = str(job_snapshot.metadata[key])
        dataflow.properties = flow_property_bag
        return dataflow

    def generate_datajob(
        self,
        job_snapshot: JobSnapshot,
        step_deps: Dict[str, List],
        op_def_snap: OpDefSnap,
        env: str,
        input_datasets: Dict[str, Set[DatasetUrn]],
        output_datasets: Dict[str, Set[DatasetUrn]],
        platform_instance: Optional[str] = None,
    ) -> DataJob:
        """
        Generates a Datajob object from an Dagster op snapshot
        :param job_snapshot: JobSnapshot - Job snapshot object
        :param op_def_snap: OpDefSnap - Op def snapshot object
        :param env: str
        :param platform_instance: Optional[str]
        :param output_datasets: dict[str, Set[DatasetUrn]] - output datasets for each op
        :return: DataJob - Data generated datajob
        """

        if self.dagster_environment.is_cloud:
            flow_id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{job_snapshot.name}"
            job_id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{op_def_snap.name}"
        else:
            flow_id = f"{self.dagster_environment.module}/{job_snapshot.name}"
            job_id = f"{self.dagster_environment.module}/{op_def_snap.name}"

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=flow_id,
            env=env,
            platform_instance=platform_instance,
        )
        datajob = DataJob(
            id=job_id,
            flow_urn=dataflow_urn,
            name=op_def_snap.name,
        )

        if self.config.dagster_url:
            datajob.url = f"{job_url_generator(dagster_url=self.config.dagster_url, dagster_environment=self.dagster_environment)}/jobs/{job_snapshot.name}/{op_def_snap.name}"

        datajob.description = op_def_snap.description
        datajob.tags = set(op_def_snap.tags.keys())

        # Add upstream dependencies for this op
        for upstream_op_name in step_deps[op_def_snap.name]:
            if self.dagster_environment.is_cloud:
                upstream_job_id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{upstream_op_name}"
            else:
                upstream_job_id = (
                    f"{self.dagster_environment.module}/{upstream_op_name}"
                )
            upstream_op_urn = DataJobUrn.create_from_ids(
                data_flow_urn=str(dataflow_urn),
                job_id=upstream_job_id,
            )
            datajob.upstream_urns.extend([upstream_op_urn])
        job_property_bag: Dict[str, str] = {}
        if input_datasets:
            self.logger.info(
                f"Input datasets for {op_def_snap.name} are { list(input_datasets.get(op_def_snap.name, []))}"
            )
            datajob.inlets = list(input_datasets.get(op_def_snap.name, []))

        if output_datasets:
            self.logger.info(
                f"Output datasets for {op_def_snap.name} are { list(output_datasets.get(op_def_snap.name, []))}"
            )
            datajob.outlets = list(output_datasets.get(op_def_snap.name, []))

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
                        input_def_snap.metadata[Constant.DATAHUB_INPUTS].value  # type: ignore
                    )
                )

        for output_def_snap in op_def_snap.output_def_snaps:
            job_property_bag[f"output_{output_def_snap.name}"] = str(
                output_def_snap._asdict()
            )
            if Constant.DATAHUB_OUTPUTS in output_def_snap.metadata:
                datajob.outlets.extend(
                    _str_urn_to_dataset_urn(
                        output_def_snap.metadata[Constant.DATAHUB_OUTPUTS].value  # type: ignore
                    )
                )

        datajob.properties = job_property_bag

        return datajob

    def emit_job_run(
        self,
        graph: DataHubGraph,
        dataflow: DataFlow,
        run: DagsterRun,
        run_stats: DagsterRunStatsSnapshot,
    ) -> None:
        """
        Emit a latest job run
        :param graph: DatahubRestEmitter
        :param dataflow: DataFlow - DataFlow object
        :param run: DagsterRun - Dagster Run object
        :param run_stats: DagsterRunStatsSnapshot - latest job run stats
        """
        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=run_stats.run_id)
        if self.config.dagster_url:
            if self.dagster_environment.is_cloud:
                dpi.url = f"{self.config.dagster_url}/{self.dagster_environment.branch}/runs/{run.run_id}"
            else:
                dpi.url = f"{self.config.dagster_url}/runs/{run.run_id}"

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
                emitter=graph,
                start_timestamp_millis=int(run_stats.start_time * 1000),
            )

        if run_stats.end_time is not None:
            dpi.emit_process_end(
                emitter=graph,
                end_timestamp_millis=int(run_stats.end_time * 1000),
                result=status_result_map[run.status],
                result_type=Constant.ORCHESTRATOR,
            )

    def emit_op_run(
        self,
        graph: DataHubGraph,
        datajob: DataJob,
        run_step_stats: RunStepKeyStatsSnapshot,
    ) -> None:
        """
        Emit an op run
        :param graph: DataHubGraph
        :param datajob: DataJob - DataJob object
        :param run_step_stats: RunStepKeyStatsSnapshot - step(op) run stats
        """
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{run_step_stats.run_id}.{datajob.id}",
            clone_inlets=True,
            clone_outlets=True,
        )
        if self.config.dagster_url:
            dpi.url = f"{self.config.dagster_url}/runs/{run_step_stats.run_id}"
            if self.dagster_environment.is_cloud:
                dpi.url = f"{self.config.dagster_url}/{self.dagster_environment.branch}/runs/{run_step_stats.run_id}"
            else:
                dpi.url = f"{self.config.dagster_url}/runs/{run_step_stats.run_id}"

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
                emitter=graph,
                start_timestamp_millis=int(run_step_stats.start_time * 1000),
            )

        if run_step_stats.end_time is not None:
            dpi.emit_process_end(
                emitter=graph,
                end_timestamp_millis=int(run_step_stats.end_time * 1000),
                result=status_result_map[run_step_stats.status],
                result_type=Constant.ORCHESTRATOR,
            )

    def dataset_urn_from_asset(self, asset_key: Sequence[str]) -> DatasetUrn:
        """
        Generate dataset urn from asset key
        """
        return DatasetUrn(
            platform="dagster", env=self.config.env, name="/".join(asset_key)
        )

    def emit_asset(
        self,
        graph: DataHubGraph,
        asset_key: Sequence[str],
        description: Optional[str],
        properties: Optional[Dict[str, str]],
    ) -> str:
        """
        Emit asset to datahub
        """
        dataset_urn = self.dataset_urn_from_asset(asset_key)
        dataset = Dataset(
            id=None,
            urn=dataset_urn.urn(),
            platform="dagster",
            name=asset_key[-1],
            schema=None,
            downstreams=None,
            subtype="Asset",
            subtypes=None,
            description=description,
            env=self.config.env,
            properties=properties,
        )
        for mcp in dataset.generate_mcp():
            graph.emit_mcp(mcp)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn.urn(),
            aspect=SubTypesClass(typeNames=["Asset"]),
        )
        graph.emit_mcp(mcp)

        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn.urn(),
                aspect=DataPlatformInstanceClass(
                    instance=make_dataplatform_instance_urn(
                        instance=self.config.platform_instance,
                        platform="dagster",
                    ),
                    platform=make_data_platform_urn("dagster"),
                ),
            )
            graph.emit_mcp(mcp)
        return dataset_urn.urn()
