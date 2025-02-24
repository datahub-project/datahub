from dataclasses import dataclass
from datetime import datetime, timezone
from logging import Logger
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence, Set
from urllib.parse import urlsplit

import pydantic
from dagster import (
    DagsterRunStatus,
    MultiAssetSensorEvaluationContext,
    PathMetadataValue,
    RunStatusSensorContext,
    TableSchemaMetadataValue,
)
from dagster._core.execution.stats import RunStepKeyStatsSnapshot, StepEventStatus

from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

try:
    from dagster._core.snap import JobSnapshot  # type: ignore[attr-defined]
except ImportError:
    # Import changed since Dagster 1.8.12  to this -> https://github.com/dagster-io/dagster/commit/29a37d1f0260cfd112849633d1096ffc916d6c95
    from dagster._core.snap import JobSnap as JobSnapshot

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
    make_tag_urn,
    make_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DatasetKeyClass,
    DatasetLineageTypeClass,
    GlobalTagsClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

ASSET_SUBTYPE = "Asset"

DAGSTER_PLATFORM = "dagster"

_DEFAULT_USER_URN = CorpUserUrn("_ingestion")


class Constant:
    """
    keys used in dagster plugin
    """

    ORCHESTRATOR = "dagster"

    # Default config constants
    DEFAULT_DATAHUB_REST_URL = "http://localhost:8080"

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

    SNOWFLAKE_FIELD_TYPE_MAPPINGS = {
        "DATE": DateType,
        "BIGINT": NumberType,
        "BINARY": BytesType,
        # 'BIT': BIT,
        "BOOLEAN": BooleanType,
        "CHAR": NullType,
        "CHARACTER": NullType,
        "DATETIME": TimeType,
        "DEC": NumberType,
        "DECIMAL": NumberType,
        "DOUBLE": NumberType,
        "FIXED": NumberType,
        "FLOAT": NumberType,
        "FLOAT64": NumberType,
        "INT": NumberType,
        "INTEGER": NumberType,
        "NUMBER": NumberType,
        # 'OBJECT': ?
        "REAL": NumberType,
        "BYTEINT": NumberType,
        "SMALLINT": NumberType,
        "STRING": StringType,
        "TEXT": StringType,
        "TIME": TimeType,
        "TIMESTAMP": TimeType,
        "TIMESTAMP_TZ": TimeType,
        "TIMESTAMP_LTZ": TimeType,
        "TIMESTAMP_NTZ": TimeType,
        "TINYINT": NumberType,
        "VARBINARY": BytesType,
        "VARCHAR": StringType,
        "VARIANT": RecordType,
        "OBJECT": NullType,
        "ARRAY": ArrayType,
        "GEOGRAPHY": NullType,
    }


class DatasetLineage(NamedTuple):
    inputs: Set[DatasetUrn]
    outputs: Set[DatasetUrn]


class DatahubDagsterSourceConfig(DatasetSourceConfigMixin):
    datahub_client_config: DatahubClientConfig = pydantic.Field(
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

    connect_ops_to_ops: bool = pydantic.Field(
        default=False,
        description="Whether to connect ops to ops based on the order of execution",
    )

    enable_asset_query_metadata_parsing: bool = pydantic.Field(
        default=True,
        description="Whether to enable parsing query from asset metadata",
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

    capture_dataset_from_asset_key: Optional[bool] = pydantic.Field(
        default=True,
        description="Whether to capture dataset from asset key",
    )

    asset_keys_to_dataset_urn_converter: Optional[
        Callable[
            [Sequence[str]],
            DatasetUrn,
        ]
    ] = pydantic.Field(
        default=None,
        description="Custom asset key to urn converter function. See details at [https://datahubproject.io/docs/lineage/dagster/#define-your-custom-logic-to-capture-asset-lineage-information]",
    )

    materialize_dependencies: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to materialize asset dependency in DataHub. It emits a datasetKey for each dependencies. Default is False.",
    )

    emit_queries: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to emit queries aspects. Default is False.",
    )

    emit_assets: Optional[bool] = pydantic.Field(
        default=True,
        description="Whether to emit assets aspects. Default is True.",
    )

    debug_mode: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to enable debug mode",
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


DATAHUB_ASSET_GROUP_NAME_CACHE: Dict[str, str] = {}


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

    def dataset_urn_from_asset(self, asset_key: Sequence[str]) -> DatasetUrn:
        """
        Generate dataset urn from asset key
        """
        return DatasetUrn(
            #  A key/name can only contain letters, numbers, and _ in Dagster
            platform="dagster",
            env=self.config.env,
            name=".".join(asset_key),
        )

    def asset_keys_to_dataset_urn_converter(
        self, asset_key: Sequence[str]
    ) -> Optional[DatasetUrn]:
        """
        Convert asset key to dataset urn

        By default we assume the following asset key structure:
        key_prefix=["prod", "snowflake", "db_name", "schema_name"]
        """
        if self.config.asset_keys_to_dataset_urn_converter:
            return self.config.asset_keys_to_dataset_urn_converter(asset_key)

        if len(asset_key) >= 3:
            return DatasetUrn(
                platform=asset_key[1],
                env=asset_key[0],
                name=".".join(asset_key[2:]),
            )
        else:
            return None

    def update_asset_group_name_cache(
        self, asset_context: MultiAssetSensorEvaluationContext
    ) -> None:
        """
        Update asset group name cache
        """
        for _, asset_def in asset_context.assets_defs_by_key.items():
            if asset_def:
                for key, group_name in asset_def.group_names_by_key.items():
                    asset_urn = self.dataset_urn_from_asset(key.path)
                    DATAHUB_ASSET_GROUP_NAME_CACHE[asset_urn.urn()] = group_name
                    if self.config.debug_mode:
                        self.logger.debug(
                            f"Asset group name cache updated: {asset_urn.urn()} -> {group_name}"
                        )
        if self.config.debug_mode:
            self.logger.debug(
                f"Asset group name cache: {DATAHUB_ASSET_GROUP_NAME_CACHE}"
            )
        self.logger.info(f"Asset group name cache: {DATAHUB_ASSET_GROUP_NAME_CACHE}")

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
        remove_double_underscores: bool = True,
        add_asset_group_tag: bool = True,
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
            module_name = (
                self.dagster_environment.module
                if self.dagster_environment.module
                else self.dagster_environment.branch
            )
            id = f"{module_name}/{job_snapshot.name}"

        flow_name = job_snapshot.name
        if remove_double_underscores and flow_name.split("__"):
            flow_name = flow_name.split("__")[-1]

        dataflow = DataFlow(
            orchestrator=Constant.ORCHESTRATOR,
            id=id,
            env=env,
            name=flow_name,
            platform_instance=platform_instance,
        )

        dataflow.description = job_snapshot.description
        dataflow.tags = set(job_snapshot.tags.keys())
        if add_asset_group_tag:
            asset_group = self.get_asset_group_from_op_name(
                job_snapshot.name.split("__")
            )
            if asset_group:
                dataflow.tags.add(f"asset_group:{asset_group}")

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
        remove_double_underscores: bool = True,
        add_asset_group_tag: bool = True,
    ) -> DataJob:
        """
        Generates a Datajob object from an Dagster op snapshot
        :param job_snapshot: JobSnapshot - Job snapshot object
        :param step_deps: Dict[str, List] - step dependencies
        :param op_def_snap: OpDefSnap - Op def snapshot object
        :param env: str
        :param platform_instance: Optional[str]
        :param output_datasets: dict[str, Set[DatasetUrn]] - output datasets for each op
        :param input_datasets: dict[str, Set[DatasetUrn]] - input datasets for each op
        :return: DataJob - Data generated datajob
        """
        self.logger.info(f"Generating datajob for Op Def Snap: {op_def_snap}")
        if self.dagster_environment.is_cloud:
            flow_id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{job_snapshot.name}"
            job_id = f"{self.dagster_environment.branch}/{self.dagster_environment.module}/{op_def_snap.name}"
        else:
            module_name = (
                self.dagster_environment.module
                if self.dagster_environment.module
                else self.dagster_environment.branch
            )
            flow_id = f"{module_name}/{job_snapshot.name}"
            job_id = f"{module_name}/{op_def_snap.name}"

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=flow_id,
            env=env,
            platform_instance=platform_instance,
        )

        job_name = op_def_snap.name
        if remove_double_underscores and job_name.split("__"):
            job_name = job_name.split("__")[-1]

        datajob = DataJob(
            id=job_id,
            flow_urn=dataflow_urn,
            name=job_name,
        )

        if self.config.dagster_url:
            datajob.url = f"{job_url_generator(dagster_url=self.config.dagster_url, dagster_environment=self.dagster_environment)}/jobs/{job_snapshot.name}/{op_def_snap.name}"

        datajob.description = op_def_snap.description
        datajob.tags = set(op_def_snap.tags.keys())

        if add_asset_group_tag:
            asset_group = self.get_asset_group_from_op_name(
                op_def_snap.name.split("__")
            )
            if asset_group:
                datajob.tags.add(f"asset_group:{asset_group}")

        inlets: Set[DatasetUrn] = set()
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
            if self.config.connect_ops_to_ops:
                datajob.upstream_urns.extend([upstream_op_urn])
            inlets.update(output_datasets.get(upstream_op_name, []))
        job_property_bag: Dict[str, str] = {}
        if input_datasets:
            self.logger.info(
                f"Input datasets for {op_def_snap.name} are {list(input_datasets.get(op_def_snap.name, []))}"
            )
            inlets.update(input_datasets.get(op_def_snap.name, []))

        datajob.inlets = list(inlets)

        if output_datasets:
            self.logger.info(
                f"Output datasets for {op_def_snap.name} are {list(output_datasets.get(op_def_snap.name, []))}"
            )
            datajob.outlets = list(output_datasets.get(op_def_snap.name, []))

        # For all op inputs/outputs:
        # Add input/output details like its type, description, metadata etc in datajob properties.
        # Also, add datahub inputs/outputs if present in input/output metatdata.
        for input_def_snap in op_def_snap.input_def_snaps:
            job_property_bag[f"input.{input_def_snap.name}"] = str(
                input_def_snap.__dict__
            )
            if Constant.DATAHUB_INPUTS in input_def_snap.metadata:
                datajob.inlets.extend(
                    _str_urn_to_dataset_urn(
                        input_def_snap.metadata[Constant.DATAHUB_INPUTS].value  # type: ignore
                    )
                )

        for output_def_snap in op_def_snap.output_def_snaps:
            job_property_bag[f"output_{output_def_snap.name}"] = str(
                output_def_snap.__dict__
            )
            if (
                Constant.DATAHUB_OUTPUTS in output_def_snap.metadata
                and self.config.connect_ops_to_ops
            ):
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
                f"{run.status}"
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
                f"{run_step_stats.status}"
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

    def convert_table_schema_to_schema_metadata(
        self, table_schema: TableSchemaMetadataValue, parent_urn: DatasetUrn
    ) -> MetadataChangeProposalWrapper:
        """
        Convert TableSchemaMetadataValue to SchemaSpecification
        """
        self.logger.info(f"Converting table schema to schema metadata: {table_schema}")
        fields = []
        for column in table_schema.schema.columns:
            fields.append(
                SchemaField(
                    fieldPath=column.name,
                    nativeDataType=column.type,
                    type=SchemaFieldDataType(
                        Constant.SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
                            column.type.upper(), NullType
                        )()
                    ),
                    nullable=column.constraints.nullable,
                    description=column.description,
                )
            )
        self.logger.info(f"Fields: {fields}")
        schema_metadata = SchemaMetadata(
            schemaName="",
            platform=parent_urn.platform,
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=fields,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=parent_urn.urn(), aspect=schema_metadata
        )

    def generate_asset_group_tag(
        self, graph: DataHubGraph, asset_urn: DatasetUrn, target_urn: Optional[Urn]
    ) -> Optional[MetadataChangeProposalWrapper]:
        if not target_urn:
            target_urn = asset_urn
        self.logger.info(
            f"Getting {asset_urn.urn()} from Asset Cache: {DATAHUB_ASSET_GROUP_NAME_CACHE}"
        )
        group_name = DATAHUB_ASSET_GROUP_NAME_CACHE.get(asset_urn.urn())
        if group_name:
            current_tags: Optional[GlobalTagsClass] = graph.get_aspect(
                entity_urn=target_urn.urn(),
                aspect_type=GlobalTagsClass,
            )

            tag_to_add = make_tag_urn(f"asset_group:{group_name}")
            tag_association_to_add = TagAssociationClass(
                tag=make_tag_urn(f"asset_group:{group_name}")
            )
            need_write = False
            if current_tags:
                if tag_to_add not in [x.tag for x in current_tags.tags]:
                    current_tags.tags.append(tag_association_to_add)
                    need_write = True
            else:
                current_tags = GlobalTagsClass(tags=[tag_association_to_add])
                need_write = True

            if need_write:
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=target_urn.urn(), aspect=current_tags
                )
                self.logger.info(f"tag_mcp: {mcp}")
                return mcp

        return None

    def _gen_entity_browsepath_aspect(
        self,
        entity_urn: str,
        paths: List[str],
    ) -> MetadataWorkUnit:
        entries = [BrowsePathEntryClass(id=path) for path in paths]
        if self.config.platform_instance:
            urn = make_dataplatform_instance_urn("asset", self.config.platform_instance)
            entries = [BrowsePathEntryClass(id=urn, urn=urn)] + entries
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(entries),
        ).as_workunit()

    def emit_asset(
        self,
        graph: DataHubGraph,
        asset_key: Sequence[str],
        description: Optional[str],
        properties: Optional[Dict[str, str]],
        downstreams: Optional[Set[str]] = None,
        upstreams: Optional[Set[str]] = None,
        schema: Optional[TableSchemaMetadataValue] = None,
        materialize_dependencies: Optional[bool] = False,
    ) -> DatasetUrn:
        """
        Emit asset to datahub
        """
        dataset_urn = self.dataset_urn_from_asset(asset_key)
        dataset = Dataset(
            id=None,
            urn=dataset_urn.urn(),
            platform=DAGSTER_PLATFORM,
            name=asset_key[-1],
            schema=None,
            downstreams=None,
            subtype=ASSET_SUBTYPE,
            subtypes=None,
            description=description,
            env=self.config.env,
            properties=properties,
        )
        for mcp in dataset.generate_mcp():
            graph.emit_mcp(mcp)

        if schema:
            mcp = self.convert_table_schema_to_schema_metadata(
                table_schema=schema, parent_urn=dataset_urn
            )
            graph.emit_mcp(mcp)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn.urn(),
            aspect=SubTypesClass(typeNames=[ASSET_SUBTYPE]),
        )
        graph.emit_mcp(mcp)

        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn.urn(),
                aspect=DataPlatformInstanceClass(
                    instance=make_dataplatform_instance_urn(
                        instance=self.config.platform_instance,
                        platform=DAGSTER_PLATFORM,
                    ),
                    platform=make_data_platform_urn(DAGSTER_PLATFORM),
                ),
            )
            graph.emit_mcp(mcp)

        tag_mcp = self.generate_asset_group_tag(
            graph, asset_urn=dataset_urn, target_urn=dataset_urn
        )
        if tag_mcp:
            graph.emit_mcp(tag_mcp)

        if downstreams:
            for downstream in downstreams:
                if materialize_dependencies:
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=downstream,
                        aspect=DatasetKeyClass(
                            name=DatasetUrn.create_from_string(downstream).name,
                            platform=DatasetUrn.create_from_string(downstream).platform,
                            origin=DatasetUrn.create_from_string(downstream).env,
                        ),
                    )
                    if self.config.debug_mode:
                        self.logger.info(f"mcp: {mcp}")
                    graph.emit_mcp(mcp)

                patch_builder = DatasetPatchBuilder(downstream)
                patch_builder.add_upstream_lineage(
                    UpstreamClass(
                        dataset=dataset_urn.urn(),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )
                for patch_event in patch_builder.build():
                    graph.emit_mcp(patch_event)

        if upstreams:
            for upstream in upstreams:
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=upstream,
                    aspect=DatasetKeyClass(
                        name=DatasetUrn.create_from_string(upstream).name,
                        platform=DatasetUrn.create_from_string(upstream).platform,
                        origin=DatasetUrn.create_from_string(upstream).env,
                    ),
                )
                self.logger.info(f"mcp: {mcp}")
                graph.emit_mcp(mcp)

                patch_builder = DatasetPatchBuilder(dataset_urn.urn())
                patch_builder.add_upstream_lineage(
                    UpstreamClass(
                        dataset=upstream,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )
                for patch_event in patch_builder.build():
                    graph.emit_mcp(patch_event)

        if self.config.debug_mode:
            self.logger.info(f"asset_key: {asset_key}")
        self.generate_browse_path(asset_key=asset_key, urn=dataset_urn, graph=graph)
        return dataset_urn

    def get_asset_group_from_op_name(self, asset_key: Sequence[str]) -> Optional[str]:
        """
        Get asset group name from op name
        """
        asset_urn = self.dataset_urn_from_asset(asset_key).urn()
        asset_group_name = DATAHUB_ASSET_GROUP_NAME_CACHE.get(asset_urn)
        if asset_group_name:
            self.logger.info(
                f"asset_key: {asset_key}, urn: {asset_urn}, asset_group_name: {asset_group_name}"
            )
            return asset_group_name
        else:
            self.logger.info(
                f"asset_key: {asset_key}, urn: {asset_urn} not in {DATAHUB_ASSET_GROUP_NAME_CACHE}, asset_group_name: None"
            )

        return None

    def generate_browse_path(
        self, asset_key: Sequence[str], urn: Urn, graph: DataHubGraph
    ) -> None:
        """
        Generate browse path from asset key
        """
        browsePaths: List[BrowsePathEntryClass] = []

        asset_group_name = self.get_asset_group_from_op_name(asset_key)
        if asset_group_name:
            browsePaths.append(BrowsePathEntryClass(asset_group_name))

        for key in asset_key[:-1]:
            browsePaths.append(BrowsePathEntryClass(key))

        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn.urn(),
            aspect=BrowsePathsV2Class(
                path=browsePaths,
            ),
        )
        graph.emit_mcp(mcp)

    def gen_query_aspect(
        self,
        graph: DataHubGraph,
        platform: str,
        query_subject_urns: List[str],
        query: str,
        job_urn: Optional[str] = None,
    ) -> None:
        """
        Generate query aspect for lineage
        """
        query_id = get_query_fingerprint(query, platform)

        aspects = [
            QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=query,
                    language=QueryLanguageClass.SQL,
                ),
                source=QuerySourceClass.SYSTEM,
                origin=job_urn if job_urn else None,
                created=AuditStampClass(
                    make_ts_millis(datetime.now(tz=timezone.utc)),
                    actor=_DEFAULT_USER_URN.urn(),
                ),
                lastModified=AuditStampClass(
                    make_ts_millis(datetime.now(tz=timezone.utc)),
                    actor=_DEFAULT_USER_URN.urn(),
                ),
            ),
            QuerySubjectsClass(
                subjects=[QuerySubjectClass(entity=urn) for urn in query_subject_urns]
            ),
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(platform),
            ),
            SubTypesClass(
                typeNames=["Query"],
            ),
        ]

        mcps = MetadataChangeProposalWrapper.construct_many(
            entityUrn=f"urn:li:query:dagster_{query_id}",
            aspects=aspects,
        )
        for mcp in mcps:
            graph.emit_mcp(mcp)
