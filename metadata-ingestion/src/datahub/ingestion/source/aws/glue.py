import logging
from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import botocore.exceptions
import yaml
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    get_sys_time,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.ingestion_job_state_provider import JobId
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws import s3_util
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.glue_profiling_config import GlueProfilingConfig
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataFlowInfoClass,
    DataFlowSnapshotClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobSnapshotClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    JobStatusClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    PartitionSpecClass,
    PartitionTypeClass,
    StatusClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

logger = logging.getLogger(__name__)


DEFAULT_PLATFORM = "glue"
VALID_PLATFORMS = [DEFAULT_PLATFORM, "athena"]


class GlueStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the GlueSourceConfig.
    """

    _entity_types: List[str] = Field(default=["table"])


class GlueSourceConfig(
    AwsSourceConfig, GlueProfilingConfig, StatefulIngestionConfigBase
):
    extract_owners: Optional[bool] = Field(
        default=True,
        description="When enabled, extracts ownership from Glue directly and overwrites existing owners. When disabled, ownership is left empty for datasets.",
    )
    extract_transforms: Optional[bool] = Field(
        default=True, description="Whether to extract Glue transform jobs."
    )
    underlying_platform: Optional[str] = Field(
        default=None,
        description="@deprecated(Use `platform`) Override for platform name. Allowed values - `glue`, `athena`",
    )
    ignore_unsupported_connectors: Optional[bool] = Field(
        default=True,
        description="Whether to ignore unsupported connectors. If disabled, an error will be raised.",
    )
    emit_s3_lineage: bool = Field(
        default=False, description=" Whether to emit S3-to-Glue lineage."
    )
    glue_s3_lineage_direction: str = Field(
        default="upstream",
        description="If `upstream`, S3 is upstream to Glue. If `downstream` S3 is downstream to Glue.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description="regex patterns for tables to filter to assign domain_key. ",
    )
    catalog_id: Optional[str] = Field(
        default=None,
        description="The aws account id where the target glue catalog lives. If None, datahub will ingest glue in aws caller's account.",
    )
    use_s3_bucket_tags: Optional[bool] = Field(
        default=False,
        description="If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files.",
    )
    use_s3_object_tags: Optional[bool] = Field(
        default=False,
        description="If an S3 Objects Tags should be created for the Tables ingested by Glue.",
    )
    profiling: Optional[GlueProfilingConfig] = Field(
        default=None,
        description="Configs to ingest data profiles from glue table",
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[GlueStatefulIngestionConfig] = Field(
        default=None, description=""
    )

    @property
    def glue_client(self):
        return self.get_glue_client()

    @property
    def s3_client(self):
        return self.get_s3_client()

    @validator("glue_s3_lineage_direction")
    def check_direction(cls, v: str) -> str:
        if v.lower() not in ["upstream", "downstream"]:
            raise ConfigurationError(
                "glue_s3_lineage_direction must be either upstream or downstream"
            )
        return v.lower()

    @validator("underlying_platform")
    def underlying_platform_validator(cls, v: str) -> str:
        if not v or v in VALID_PLATFORMS:
            return v
        else:
            raise ConfigurationError(
                f"'underlying_platform' can only take following values: {VALID_PLATFORMS}"
            )

    @validator("platform")
    def platform_validator(cls, v: str) -> str:
        if not v or v in VALID_PLATFORMS:
            return v
        else:
            raise ConfigurationError(
                f"'platform' can only take following values: {VALID_PLATFORMS}"
            )


@dataclass
class GlueSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


@platform_name("Glue")
@config_class(GlueSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on.",
)
class GlueSource(StatefulIngestionSourceBase):
    """
    Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](../../../../docs/generated/ingestion/sources/s3.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

    This plugin extracts the following:

    - Tables in the Glue catalog
    - Column types associated with each table
    - Table metadata, such as owner, description and parameters
    - Jobs and their component transformations, data sources, and data sinks

    ## IAM permissions

    For ingesting datasets, the following IAM permissions are required:
    ```json
    {
        "Effect": "Allow",
        "Action": [
            "glue:GetDatabases",
            "glue:GetTables"
        ],
        "Resource": [
            "arn:aws:glue:$region-id:$account-id:catalog",
            "arn:aws:glue:$region-id:$account-id:database/*",
            "arn:aws:glue:$region-id:$account-id:table/*"
        ]
    }
    ```

    For ingesting jobs (`extract_transforms: True`), the following additional permissions are required:
    ```json
    {
        "Effect": "Allow",
        "Action": [
            "glue:GetDataflowGraph",
            "glue:GetJobs",
        ],
        "Resource": "*"
    }
    ```

    plus `s3:GetObject` for the job script locations.

    """

    source_config: GlueSourceConfig
    report = GlueSourceReport()

    def __init__(self, config: GlueSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.extract_owners = config.extract_owners
        self.source_config = config
        self.report = GlueSourceReport()
        self.glue_client = config.glue_client
        self.s3_client = config.s3_client
        self.extract_transforms = config.extract_transforms
        self.env = config.env

        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=BaseSQLAlchemyCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def get_glue_arn(
        self, account_id: str, database: str, table: Optional[str] = None
    ) -> str:
        prefix = f"arn:aws:glue:{self.source_config.aws_region}:{account_id}"
        if table:
            return f"{prefix}:table/{database}/{table}"
        return f"{prefix}:database/{database}"

    @classmethod
    def create(cls, config_dict, ctx):
        config = GlueSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @property
    def platform(self) -> str:
        """
        This deprecates "underlying_platform" field in favour of the standard "platform" one, which has
        more priority when both are defined.
        :return: platform, otherwise underlying_platform, otherwise "glue"
        """
        return (
            self.source_config.platform
            or self.source_config.underlying_platform
            or DEFAULT_PLATFORM
        )

    def get_all_jobs(self):
        """
        List all jobs in Glue. boto3 get_jobs api call doesn't support cross account access.
        """

        jobs = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_jobs
        paginator = self.glue_client.get_paginator("get_jobs")
        for page in paginator.paginate():
            jobs += page["Jobs"]

        return jobs

    def get_dataflow_graph(self, script_path: str) -> Optional[Dict[str, Any]]:
        """
        Get the DAG of transforms and data sources/sinks for a job.

        Parameters
        ----------
            script_path:
                S3 path to the job's Python script.
        """

        # handle a bug in AWS where script path has duplicate prefixes
        if script_path.lower().startswith("s3://s3://"):
            script_path = script_path[5:]

        # catch any other cases where the script path is invalid
        if not script_path.startswith("s3://"):

            self.report.report_warning(
                script_path,
                f"Error parsing DAG for Glue job. The script {script_path} is not a valid S3 path.",
            )

            return None

        # extract the script's bucket and key
        url = urlparse(script_path, allow_fragments=False)
        bucket = url.netloc
        key = url.path[1:]

        # download the script contents
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            self.report.report_failure(
                script_path,
                f"Unable to download DAG for Glue job from {script_path}, so job subtasks and lineage will be missing: {e}",
            )
            return None
        script = obj["Body"].read().decode("utf-8")

        try:
            # extract the job DAG from the script
            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_dataflow_graph
            return self.glue_client.get_dataflow_graph(PythonScript=script)

        # sometimes the Python script can be user-modified and the script is not valid for graph extraction
        except self.glue_client.exceptions.InvalidInputException as e:

            self.report.report_warning(
                script_path,
                f"Error parsing DAG for Glue job. The script {script_path} cannot be processed by Glue (this usually occurs when it has been user-modified): {e}",
            )

            return None

    def get_s3_uri(self, node_args):
        s3_uri = node_args.get("connection_options", {}).get("path")

        # sometimes the path is a single element in a list rather than a single one
        if s3_uri is None:
            s3_uri = node_args.get("connection_options", {}).get("paths")[0]

        return s3_uri

    def get_dataflow_s3_names(
        self, dataflow_graph: Dict[str, Any]
    ) -> Iterator[Tuple[str, Optional[str]]]:

        # iterate through each node to populate processed nodes
        for node in dataflow_graph["DagNodes"]:

            node_type = node["NodeType"]

            # for nodes representing datasets, we construct a dataset URN accordingly
            if node_type in ["DataSource", "DataSink"]:

                node_args = {
                    x["Name"]: yaml.safe_load(x["Value"]) for x in node["Args"]
                }

                # if data object is S3 bucket
                if node_args.get("connection_type") == "s3":

                    s3_uri = self.get_s3_uri(node_args)

                    if s3_uri is None:
                        continue

                    extension = node_args.get("format")

                    yield s3_uri, extension

    def process_dataflow_node(
        self,
        node: Dict[str, Any],
        flow_urn: str,
        new_dataset_ids: List[str],
        new_dataset_mces: List[MetadataChangeEvent],
        s3_formats: DefaultDict[str, Set[Union[str, None]]],
    ) -> Optional[Dict[str, Any]]:

        node_type = node["NodeType"]

        # for nodes representing datasets, we construct a dataset URN accordingly
        if node_type in ["DataSource", "DataSink"]:

            node_args = {x["Name"]: yaml.safe_load(x["Value"]) for x in node["Args"]}

            # if data object is Glue table
            if "database" in node_args and "table_name" in node_args:

                full_table_name = f"{node_args['database']}.{node_args['table_name']}"

                # we know that the table will already be covered when ingesting Glue tables
                node_urn = make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=full_table_name,
                    env=self.env,
                    platform_instance=self.source_config.platform_instance,
                )

            # if data object is S3 bucket
            elif node_args.get("connection_type") == "s3":

                s3_uri = self.get_s3_uri(node_args)

                if s3_uri is None:
                    self.report.report_warning(
                        f"{node['Nodetype']}-{node['Id']}",
                        f"Could not find script path for job {node['Nodetype']}-{node['Id']} in flow {flow_urn}. Skipping",
                    )
                    return None

                # append S3 format if different ones exist
                if len(s3_formats[s3_uri]) > 1:
                    node_urn = make_s3_urn(
                        f"{s3_uri}.{node_args.get('format')}",
                        self.env,
                    )

                else:
                    node_urn = make_s3_urn(s3_uri, self.env)

                dataset_snapshot = DatasetSnapshot(
                    urn=node_urn,
                    aspects=[],
                )

                dataset_snapshot.aspects.append(Status(removed=False))
                dataset_snapshot.aspects.append(
                    DatasetPropertiesClass(
                        customProperties={k: str(v) for k, v in node_args.items()},
                        tags=[],
                    )
                )

                new_dataset_mces.append(
                    MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                )
                new_dataset_ids.append(f"{node['NodeType']}-{node['Id']}")

            else:

                if self.source_config.ignore_unsupported_connectors:

                    logger.info(
                        flow_urn,
                        f"Unrecognized Glue data object type: {node_args}. Skipping.",
                    )
                    return None
                else:

                    raise ValueError(f"Unrecognized Glue data object type: {node_args}")

        # otherwise, a node represents a transformation
        else:
            node_urn = mce_builder.make_data_job_urn_with_flow(
                flow_urn, job_id=f'{node["NodeType"]}-{node["Id"]}'
            )

        return {
            **node,
            "urn": node_urn,
            # to be filled in after traversing edges
            "inputDatajobs": [],
            "inputDatasets": [],
            "outputDatasets": [],
        }

    def process_dataflow_graph(
        self,
        dataflow_graph: Dict[str, Any],
        flow_urn: str,
        s3_formats: DefaultDict[str, Set[Union[str, None]]],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[MetadataChangeEvent]]:
        """
        Prepare a job's DAG for ingestion.
        Parameters
        ----------
            dataflow_graph:
                Job DAG returned from get_dataflow_graph()
            flow_urn:
                URN of the flow (i.e. the AWS Glue job itself).
            s3_formats:
                Map from s3 URIs to formats used (for deduplication purposes)
        """

        new_dataset_ids: List[str] = []
        new_dataset_mces: List[MetadataChangeEvent] = []

        nodes: dict = {}

        # iterate through each node to populate processed nodes
        for node in dataflow_graph["DagNodes"]:

            processed_node = self.process_dataflow_node(
                node, flow_urn, new_dataset_ids, new_dataset_mces, s3_formats
            )

            if processed_node is not None:
                nodes[node["Id"]] = processed_node

        # traverse edges to fill in node properties
        for edge in dataflow_graph["DagEdges"]:

            source_node = nodes.get(edge["Source"])
            target_node = nodes.get(edge["Target"])

            # Currently, in case of unsupported connectors,
            # Source and Target for some edges is not available
            # in nodes. this may lead to broken edge in lineage.
            if source_node is None or target_node is None:
                logger.warning(
                    f"{flow_urn}: Unrecognized source or target node in edge: {edge}. Skipping.\
                        This may lead to broken edge in lineage",
                )
                continue

            source_node_type = source_node["NodeType"]
            target_node_type = target_node["NodeType"]

            # note that source nodes can't be data sinks
            if source_node_type == "DataSource":
                target_node["inputDatasets"].append(source_node["urn"])
            # keep track of input data jobs (as defined in schemas)
            else:
                target_node["inputDatajobs"].append(source_node["urn"])
            # track output datasets (these can't be input datasets)
            if target_node_type == "DataSink":
                source_node["outputDatasets"].append(target_node["urn"])

        return nodes, new_dataset_ids, new_dataset_mces

    def get_dataflow_wu(self, flow_urn: str, job: Dict[str, Any]) -> MetadataWorkUnit:
        """
        Generate a DataFlow workunit for a Glue job.

        Parameters
        ----------
            flow_urn:
                URN for the flow
            job:
                Job object from get_all_jobs()
        """

        region = self.source_config.aws_region

        custom_props = {
            "role": job["Role"],
        }

        if job.get("CreatedOn") is not None:
            custom_props["created"] = str(job["CreatedOn"])

        if job.get("LastModifiedOn") is not None:
            custom_props["modified"] = str(job["LastModifiedOn"])

        command = job.get("Command", {}).get("ScriptLocation")
        if command is not None:
            custom_props["command"] = command

        mce = MetadataChangeEventClass(
            proposedSnapshot=DataFlowSnapshotClass(
                urn=flow_urn,
                aspects=[
                    DataFlowInfoClass(
                        name=job["Name"],
                        description=job.get("Description"),
                        externalUrl=f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}#/editor/job/{job['Name']}/graph",
                        # specify a few Glue-specific properties
                        customProperties=custom_props,
                    ),
                ],
            )
        )

        return MetadataWorkUnit(id=job["Name"], mce=mce)

    def get_datajob_wu(self, node: Dict[str, Any], job_name: str) -> MetadataWorkUnit:
        """
        Generate a DataJob workunit for a component (node) in a Glue job.

        Parameters
        ----------
            node:
                Node from process_dataflow_graph()
            job:
                Job object from get_all_jobs()
        """

        region = self.source_config.aws_region

        mce = MetadataChangeEventClass(
            proposedSnapshot=DataJobSnapshotClass(
                urn=node["urn"],
                aspects=[
                    DataJobInfoClass(
                        name=f"{job_name}:{node['NodeType']}-{node['Id']}",
                        type="GLUE",
                        # there's no way to view an individual job node by link, so just show the graph
                        externalUrl=f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}#/editor/job/{job_name}/graph",
                        customProperties={
                            **{x["Name"]: x["Value"] for x in node["Args"]},
                            "transformType": node["NodeType"],
                            "nodeId": node["Id"],
                        },
                    ),
                    DataJobInputOutputClass(
                        inputDatasets=node["inputDatasets"],
                        outputDatasets=node["outputDatasets"],
                        inputDatajobs=node["inputDatajobs"],
                    ),
                ],
            )
        )

        return MetadataWorkUnit(id=f'{job_name}-{node["Id"]}', mce=mce)

    def get_all_tables_and_databases(
        self,
    ) -> Tuple[Dict, List[Dict]]:
        def get_tables_from_database(database_name: str) -> List[dict]:
            new_tables = []

            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_tables
            paginator = self.glue_client.get_paginator("get_tables")

            if self.source_config.catalog_id:
                paginator_response = paginator.paginate(
                    DatabaseName=database_name, CatalogId=self.source_config.catalog_id
                )
            else:
                paginator_response = paginator.paginate(DatabaseName=database_name)

            for page in paginator_response:
                new_tables += page["TableList"]

            return new_tables

        def get_databases() -> List[Mapping[str, Any]]:
            databases = []

            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_databases
            paginator = self.glue_client.get_paginator("get_databases")

            if self.source_config.catalog_id:
                paginator_response = paginator.paginate(
                    CatalogId=self.source_config.catalog_id
                )
            else:
                paginator_response = paginator.paginate()

            for page in paginator_response:
                for db in page["DatabaseList"]:
                    if self.source_config.database_pattern.allowed(db["Name"]):
                        databases.append(db)

            return databases

        all_databases = get_databases()

        databases = {
            database["Name"]: database
            for database in all_databases
            if self.source_config.database_pattern.allowed(database["Name"])
        }

        all_tables: List[dict] = [
            table
            for databaseName in databases.keys()
            for table in get_tables_from_database(databaseName)
        ]

        return databases, all_tables

    def get_lineage_if_enabled(
        self, mce: MetadataChangeEventClass
    ) -> Optional[MetadataChangeProposalWrapper]:
        if self.source_config.emit_s3_lineage:
            # extract dataset properties aspect
            dataset_properties: Optional[
                DatasetPropertiesClass
            ] = mce_builder.get_aspect_if_available(mce, DatasetPropertiesClass)
            if dataset_properties and "Location" in dataset_properties.customProperties:
                location = dataset_properties.customProperties["Location"]
                if location.startswith("s3://"):
                    s3_dataset_urn = make_s3_urn(location, self.source_config.env)
                    if self.source_config.glue_s3_lineage_direction == "upstream":
                        upstream_lineage = UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=s3_dataset_urn,
                                    type=DatasetLineageTypeClass.COPY,
                                )
                            ]
                        )
                        mcp = MetadataChangeProposalWrapper(
                            entityType="dataset",
                            entityUrn=mce.proposedSnapshot.urn,
                            changeType=ChangeTypeClass.UPSERT,
                            aspectName="upstreamLineage",
                            aspect=upstream_lineage,
                        )
                        return mcp
                    else:
                        # Need to mint the s3 dataset with upstream lineage from it to glue
                        upstream_lineage = UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=mce.proposedSnapshot.urn,
                                    type=DatasetLineageTypeClass.COPY,
                                )
                            ]
                        )
                        mcp = MetadataChangeProposalWrapper(
                            entityType="dataset",
                            entityUrn=s3_dataset_urn,
                            changeType=ChangeTypeClass.UPSERT,
                            aspectName="upstreamLineage",
                            aspect=upstream_lineage,
                        )
                        return mcp
        return None

    def _create_profile_mcp(
        self,
        mce: MetadataChangeEventClass,
        table_stats: dict,
        column_stats: dict,
        partition_spec: Optional[str] = None,
    ) -> MetadataChangeProposalWrapper:
        assert self.source_config.profiling

        # instantiate profile class
        dataset_profile = DatasetProfileClass(timestampMillis=get_sys_time())

        # Inject table level stats
        if self.source_config.profiling.row_count in table_stats:
            dataset_profile.rowCount = int(
                float(table_stats[self.source_config.profiling.row_count])
            )
        if self.source_config.profiling.column_count in table_stats:
            dataset_profile.columnCount = int(
                float(table_stats[self.source_config.profiling.column_count])
            )

        # inject column level stats
        dataset_profile.fieldProfiles = []
        for profile in column_stats:
            column_name = profile["Name"]

            # some columns may not be profiled
            if "Parameters" in profile:
                column_params = profile["Parameters"]
            else:
                continue

            logger.debug(f"column_name: {column_name}")
            # instantiate column profile class for each column
            column_profile = DatasetFieldProfileClass(fieldPath=column_name)

            if self.source_config.profiling.unique_count in column_params:
                column_profile.uniqueCount = int(
                    float(column_params[self.source_config.profiling.unique_count])
                )
            if self.source_config.profiling.unique_proportion in column_params:
                column_profile.uniqueProportion = float(
                    column_params[self.source_config.profiling.unique_proportion]
                )
            if self.source_config.profiling.null_count in column_params:
                column_profile.nullCount = int(
                    float(column_params[self.source_config.profiling.null_count])
                )
            if self.source_config.profiling.null_proportion in column_params:
                column_profile.nullProportion = float(
                    column_params[self.source_config.profiling.null_proportion]
                )
            if self.source_config.profiling.min in column_params:
                column_profile.min = column_params[self.source_config.profiling.min]
            if self.source_config.profiling.max in column_params:
                column_profile.max = column_params[self.source_config.profiling.max]
            if self.source_config.profiling.mean in column_params:
                column_profile.mean = column_params[self.source_config.profiling.mean]
            if self.source_config.profiling.median in column_params:
                column_profile.median = column_params[
                    self.source_config.profiling.median
                ]
            if self.source_config.profiling.stdev in column_params:
                column_profile.stdev = column_params[self.source_config.profiling.stdev]

            dataset_profile.fieldProfiles.append(column_profile)

        if partition_spec:
            # inject partition level stats
            dataset_profile.partitionSpec = PartitionSpecClass(
                partition=partition_spec,
                type=PartitionTypeClass.PARTITION,
            )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=mce.proposedSnapshot.urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="datasetProfile",
            aspect=dataset_profile,
        )
        return mcp

    def get_profile_if_enabled(
        self, mce: MetadataChangeEventClass, database_name: str, table_name: str
    ) -> List[MetadataChangeProposalWrapper]:
        if self.source_config.profiling:
            # for cross-account ingestion
            kwargs = dict(
                DatabaseName=database_name,
                Name=table_name,
                CatalogId=self.source_config.catalog_id,
            )
            response = self.glue_client.get_table(
                **{k: v for k, v in kwargs.items() if v}
            )

            partition_keys = response["Table"]["PartitionKeys"]

            # check if this table is partitioned
            if partition_keys:
                # ingest data profile with partitions
                # for cross-account ingestion
                kwargs = dict(
                    DatabaseName=database_name,
                    TableName=table_name,
                    CatalogId=self.source_config.catalog_id,
                )
                response = self.glue_client.get_partitions(
                    **{k: v for k, v in kwargs.items() if v}
                )

                partitions = response["Partitions"]
                partition_keys = [k["Name"] for k in partition_keys]

                mcps = []
                for p in partitions:
                    table_stats = p["Parameters"]
                    column_stats = p["StorageDescriptor"]["Columns"]

                    # only support single partition key
                    partition_spec = str({partition_keys[0]: p["Values"][0]})

                    if self.source_config.profiling.partition_patterns.allowed(
                        partition_spec
                    ):
                        mcps.append(
                            self._create_profile_mcp(
                                mce, table_stats, column_stats, partition_spec
                            )
                        )
                    else:
                        continue
                return mcps
            else:
                # ingest data profile without partition
                table_stats = response["Table"]["Parameters"]
                column_stats = response["Table"]["StorageDescriptor"]["Columns"]
                return [self._create_profile_mcp(mce, table_stats, column_stats)]

        return []

    def gen_database_key(self, database: str) -> DatabaseKey:
        return DatabaseKey(
            database=database,
            platform=self.platform,
            instance=self.source_config.platform_instance
            # keeps backward compatibility when platform instance is missed
            if self.source_config.platform_instance is not None
            else self.source_config.env,
        )

    def gen_database_containers(
        self, database: Mapping[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database["Name"])
        database_container_key = self.gen_database_key(database["Name"])
        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database["Name"],
            sub_types=["Database"],
            domain_urn=domain_urn,
            description=database.get("Description"),
            qualified_name=self.get_glue_arn(
                account_id=database["CatalogId"], database=database["Name"]
            ),
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def add_table_to_database_container(
        self, dataset_urn: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_database_key(db_name)
        container_workunits = add_dataset_to_container(
            container_key=database_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                return make_domain_urn(domain)

        return None

    def _get_domain_wu(
        self, dataset_name: str, entity_urn: str, entity_type: str
    ) -> Iterable[MetadataWorkUnit]:

        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type=entity_type,
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        database_seen = set()
        databases, tables = self.get_all_tables_and_databases()

        for table in tables:
            database_name = table["DatabaseName"]
            table_name = table["Name"]
            full_table_name = f"{database_name}.{table_name}"
            self.report.report_table_scanned()
            if not self.source_config.database_pattern.allowed(
                database_name
            ) or not self.source_config.table_pattern.allowed(full_table_name):
                self.report.report_table_dropped(full_table_name)
                continue
            if database_name not in database_seen:
                database_seen.add(database_name)
                yield from self.gen_database_containers(databases[database_name])

            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=full_table_name,
                env=self.env,
                platform_instance=self.source_config.platform_instance,
            )

            mce = self._extract_record(dataset_urn, table, full_table_name)
            workunit = MetadataWorkUnit(full_table_name, mce=mce)
            self.report.report_workunit(workunit)
            yield workunit

            # We also want to assign "table" subType to the dataset representing glue table - unfortunately it is not
            # possible via Dataset snapshot embedded in a mce, so we have to generate a mcp.
            workunit = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypes(typeNames=["table"]),
            ).as_workunit()
            self.report.report_workunit(workunit)
            yield workunit

            yield from self._get_domain_wu(
                dataset_name=full_table_name,
                entity_urn=dataset_urn,
                entity_type="dataset",
            )
            yield from self.add_table_to_database_container(
                dataset_urn=dataset_urn, db_name=database_name
            )

            # Add table to the checkpoint state.
            self.stale_entity_removal_handler.add_entity_to_state("table", dataset_urn)

            mcp = self.get_lineage_if_enabled(mce)
            if mcp:
                mcp_wu = MetadataWorkUnit(
                    id=f"{full_table_name}-upstreamLineage", mcp=mcp
                )
                self.report.report_workunit(mcp_wu)
                yield mcp_wu

            mcps_profiling = self.get_profile_if_enabled(mce, database_name, table_name)
            if mcps_profiling:
                for mcp_index, mcp in enumerate(mcps_profiling):
                    mcp_wu = MetadataWorkUnit(
                        id=f"profile-{full_table_name}-partition-{mcp_index}",
                        mcp=mcps_profiling[mcp_index],
                    )
                    self.report.report_workunit(mcp_wu)
                    yield mcp_wu

        if self.extract_transforms:
            yield from self._transform_extraction()

        # Clean up stale entities.
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def _transform_extraction(self) -> Iterable[MetadataWorkUnit]:
        dags: Dict[str, Optional[Dict[str, Any]]] = {}
        flow_names: Dict[str, str] = {}
        for job in self.get_all_jobs():

            flow_urn = mce_builder.make_data_flow_urn(
                self.platform, job["Name"], self.env
            )

            flow_wu = self.get_dataflow_wu(flow_urn, job)
            self.report.report_workunit(flow_wu)
            yield flow_wu

            job_script_location = job.get("Command", {}).get("ScriptLocation")

            dag: Optional[Dict[str, Any]] = None

            if job_script_location is not None:
                dag = self.get_dataflow_graph(job_script_location)

            dags[flow_urn] = dag
            flow_names[flow_urn] = job["Name"]
        # run a first pass to pick up s3 bucket names and formats
        # in Glue, it's possible for two buckets to have files of different extensions
        # if this happens, we append the extension in the URN so the sources can be distinguished
        # see process_dataflow_node() for details
        s3_formats: DefaultDict[str, Set[Optional[str]]] = defaultdict(lambda: set())
        for dag in dags.values():
            if dag is not None:
                for s3_name, extension in self.get_dataflow_s3_names(dag):
                    s3_formats[s3_name].add(extension)
        # run second pass to generate node workunits
        for flow_urn, dag in dags.items():

            if dag is None:
                continue

            nodes, new_dataset_ids, new_dataset_mces = self.process_dataflow_graph(
                dag, flow_urn, s3_formats
            )

            for node in nodes.values():

                if node["NodeType"] not in ["DataSource", "DataSink"]:
                    job_wu = self.get_datajob_wu(node, flow_names[flow_urn])
                    self.report.report_workunit(job_wu)
                    yield job_wu

            for dataset_id, dataset_mce in zip(new_dataset_ids, new_dataset_mces):
                dataset_wu = MetadataWorkUnit(id=dataset_id, mce=dataset_mce)
                self.report.report_workunit(dataset_wu)
                yield dataset_wu

    # flake8: noqa: C901
    def _extract_record(
        self, dataset_urn: str, table: Dict, table_name: str
    ) -> MetadataChangeEvent:
        def get_owner() -> Optional[OwnershipClass]:
            owner = table.get("Owner")
            if owner:
                owners = [
                    OwnerClass(
                        owner=f"urn:li:corpuser:{owner}",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
                return OwnershipClass(
                    owners=owners,
                )
            return None

        def get_dataset_properties() -> DatasetPropertiesClass:
            return DatasetPropertiesClass(
                description=table.get("Description"),
                customProperties={
                    **table.get("Parameters", {}),
                    **{
                        k: str(v)
                        for k, v in table.get("StorageDescriptor", {}).items()
                        if k not in ["Columns", "Parameters"]
                    },
                },
                uri=table.get("Location"),
                tags=[],
                qualifiedName=self.get_glue_arn(
                    account_id=table["CatalogId"],
                    database=table["DatabaseName"],
                    table=table["Name"],
                ),
            )

        def get_s3_tags() -> Optional[GlobalTagsClass]:
            # when TableType=VIRTUAL_VIEW the Location can be empty and we should
            # return no tags rather than fail the entire ingestion
            if table.get("StorageDescriptor", {}).get("Location") is None:
                return None
            bucket_name = s3_util.get_bucket_name(
                table["StorageDescriptor"]["Location"]
            )
            tags_to_add = []
            if self.source_config.use_s3_bucket_tags:
                try:
                    bucket_tags = self.s3_client.get_bucket_tagging(Bucket=bucket_name)
                    tags_to_add.extend(
                        [
                            make_tag_urn(f"""{tag["Key"]}:{tag["Value"]}""")
                            for tag in bucket_tags["TagSet"]
                        ]
                    )
                except self.s3_client.exceptions.ClientError:
                    logger.warning(f"No tags found for bucket={bucket_name}")
            if self.source_config.use_s3_object_tags:
                key_prefix = s3_util.get_key_prefix(
                    table["StorageDescriptor"]["Location"]
                )
                object_tagging = self.s3_client.get_object_tagging(
                    Bucket=bucket_name, Key=key_prefix
                )
                tag_set = object_tagging["TagSet"]
                if tag_set:
                    tags_to_add.extend(
                        [
                            make_tag_urn(f"""{tag["Key"]}:{tag["Value"]}""")
                            for tag in tag_set
                        ]
                    )
                else:
                    # Unlike bucket tags, if an object does not have tags, it will just return an empty array
                    # as opposed to an exception.
                    logger.warning(
                        f"No tags found for bucket={bucket_name} key={key_prefix}"
                    )
            if len(tags_to_add) == 0:
                return None
            if self.ctx.graph is not None:
                logger.debug(
                    "Connected to DatahubApi, grabbing current tags to maintain."
                )
                current_tags: Optional[GlobalTagsClass] = self.ctx.graph.get_aspect_v2(
                    entity_urn=dataset_urn,
                    aspect="globalTags",
                    aspect_type=GlobalTagsClass,
                )
                if current_tags:
                    tags_to_add.extend(
                        [current_tag.tag for current_tag in current_tags.tags]
                    )
            else:
                logger.warning(
                    "Could not connect to DatahubApi. No current tags to maintain"
                )

            # Remove duplicate tags
            tags_to_add = list(set(tags_to_add))
            new_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
            )
            return new_tags

        def get_schema_metadata() -> Optional[SchemaMetadata]:
            if not table.get("StorageDescriptor"):
                return None

            schema = table["StorageDescriptor"]["Columns"]
            fields: List[SchemaField] = []
            for field in schema:
                schema_fields = get_schema_fields_for_hive_column(
                    hive_column_name=field["Name"],
                    hive_column_type=field["Type"],
                    description=field.get("Comment"),
                    default_nullable=True,
                )
                assert schema_fields
                fields.extend(schema_fields)

            partition_keys = table.get("PartitionKeys", [])
            for partition_key in partition_keys:
                schema_fields = get_schema_fields_for_hive_column(
                    hive_column_name=partition_key["Name"],
                    hive_column_type=partition_key["Type"],
                    default_nullable=False,
                )
                assert schema_fields
                fields.extend(schema_fields)

            return SchemaMetadata(
                schemaName=table_name,
                version=0,
                fields=fields,
                platform=f"urn:li:dataPlatform:{self.platform}",
                hash="",
                platformSchema=MySqlDDL(tableSchema=""),
            )

        def get_data_platform_instance() -> DataPlatformInstanceClass:
            return DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.source_config.platform_instance
                )
                if self.source_config.platform_instance
                else None,
            )

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[
                Status(removed=False),
                get_dataset_properties(),
            ],
        )

        schema_metadata = get_schema_metadata()
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)

        dataset_snapshot.aspects.append(get_data_platform_instance())

        if self.extract_owners:
            optional_owner_aspect = get_owner()
            if optional_owner_aspect is not None:
                dataset_snapshot.aspects.append(optional_owner_aspect)

        if (
            self.source_config.use_s3_bucket_tags
            or self.source_config.use_s3_object_tags
        ):
            s3_tags = get_s3_tags()
            if s3_tags is not None:
                dataset_snapshot.aspects.append(s3_tags)

        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return metadata_record

    def get_report(self):
        return self.report

    def get_platform_instance_id(self) -> str:
        return self.source_config.platform_instance or self.platform

    def close(self):
        self.prepare_for_commit()
