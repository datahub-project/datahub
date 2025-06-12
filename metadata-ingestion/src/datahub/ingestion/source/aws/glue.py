import datetime
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from functools import lru_cache
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

from datahub.api.entities.dataset.dataset import Dataset
from datahub.api.entities.external.external_entities import (
    PlatformResourceRepository,
)
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
)
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
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
from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws import s3_util
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import (
    is_s3_uri,
    make_s3_urn,
    make_s3_urn_for_lineage,
)
from datahub.ingestion.source.aws.tag_entities import (
    LakeFormationTagPlatformResource,
    LakeFormationTagPlatformResourceId,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.glue_profiling_config import GlueProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled
from datahub.metadata.com.linkedin.pegasus2avro.common import Status, SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
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
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    PartitionSpecClass,
    PartitionTypeClass,
    SchemaMetadataClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.delta import delta_type_to_hive_type
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)

DEFAULT_PLATFORM = "glue"
VALID_PLATFORMS = [DEFAULT_PLATFORM, "athena"]


class GlueSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, AwsSourceConfig
):
    platform: str = Field(
        default=DEFAULT_PLATFORM,
        description=f"The platform to use for the dataset URNs. Must be one of {VALID_PLATFORMS}.",
    )

    # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-glue-table-tableinput.html#cfn-glue-table-tableinput-owner
    extract_owners: Optional[bool] = Field(
        default=True,
        description="When enabled, extracts ownership from Glue table property and overwrites existing owners (DATAOWNER). When disabled, ownership is left empty for datasets. Expects a corpGroup urn, a corpuser urn or only the identifier part for the latter. Not used in the normal course of AWS Glue operations.",
    )
    extract_transforms: Optional[bool] = Field(
        default=True, description="Whether to extract Glue transform jobs."
    )
    ignore_unsupported_connectors: Optional[bool] = Field(
        default=True,
        description="Whether to ignore unsupported connectors. If disabled, an error will be raised.",
    )
    emit_s3_lineage: bool = Field(
        default=False, description="Whether to emit S3-to-Glue lineage."
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
    ignore_resource_links: Optional[bool] = Field(
        default=False,
        description="If set to True, ignore database resource links.",
    )
    use_s3_bucket_tags: Optional[bool] = Field(
        default=False,
        description="If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files.",
    )
    use_s3_object_tags: Optional[bool] = Field(
        default=False,
        description="If an S3 Objects Tags should be created for the Tables ingested by Glue.",
    )

    extract_lakeformation_tags: Optional[bool] = Field(
        default=False,
        description="When True, extracts Lake Formation tags directly assigned to Glue tables/databases. Note: Tags inherited from databases or other parent resources are excluded.",
    )

    profiling: GlueProfilingConfig = Field(
        default_factory=GlueProfilingConfig,
        description="Configs to ingest data profiles from glue table",
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )

    extract_delta_schema_from_parameters: Optional[bool] = Field(
        default=False,
        description="If enabled, delta schemas can be alternatively fetched from table parameters.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="When enabled, column-level lineage will be extracted from the s3.",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @property
    def glue_client(self):
        return self.get_glue_client()

    @property
    def s3_client(self):
        return self.get_s3_client()

    @property
    def lakeformation_client(self):
        return self.get_lakeformation_client()

    @validator("glue_s3_lineage_direction")
    def check_direction(cls, v: str) -> str:
        if v.lower() not in ["upstream", "downstream"]:
            raise ValueError(
                "glue_s3_lineage_direction must be either upstream or downstream"
            )
        return v.lower()

    @validator("platform")
    def platform_validator(cls, v: str) -> str:
        if not v or v in VALID_PLATFORMS:
            return v
        else:
            raise ValueError(
                f"'platform' can only take following values: {VALID_PLATFORMS}"
            )


@dataclass
class GlueSourceReport(StaleEntityRemovalSourceReport):
    catalog_id: Optional[str] = None
    tables_scanned = 0
    filtered: LossyList[str] = dataclass_field(default_factory=LossyList)
    databases: EntityFilterReport = EntityFilterReport.field(type="database")

    num_job_script_location_missing: int = 0
    num_job_script_location_invalid: int = 0
    num_job_script_failed_download: int = 0
    num_job_script_failed_parsing: int = 0
    num_job_without_nodes: int = 0
    num_dataset_to_dataset_edges_in_job: int = 0
    num_dataset_invalid_delta_schema: int = 0
    num_dataset_valid_delta_schema: int = 0

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
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE, "Support via the `emit_s3_lineage` config field"
)
class GlueSource(StatefulIngestionSourceBase):
    """
    Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](../../../../docs/generated/ingestion/sources/s3.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

    This plugin extracts the following:

    - Tables in the Glue catalog
    - Column types associated with each table
    - Table metadata, such as owner, description and parameters
    - Jobs and their component transformations, data sources, and data sinks

    ### IAM permissions

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
            "s3:GetObject",
        ],
        "Resource": "*"
    }
    ```

    For profiling datasets, the following additional permissions are required:
    ```json
        {
        "Effect": "Allow",
        "Action": [
            "glue:GetPartitions",
        ],
        "Resource": "*"
    }
    ```

    """

    source_config: GlueSourceConfig
    report: GlueSourceReport

    lf_tag_cache: Dict[str, Dict[str, List[str]]] = {}

    def __init__(self, config: GlueSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.extract_owners = config.extract_owners
        self.source_config = config
        self.report = GlueSourceReport()
        self.report.catalog_id = self.source_config.catalog_id
        self.glue_client = config.glue_client
        self.s3_client = config.s3_client
        # Initialize Lake Formation client
        self.lf_client = config.lakeformation_client
        self.extract_transforms = config.extract_transforms
        self.env = config.env

        self.platform_resource_repository: Optional[PlatformResourceRepository] = None
        if self.ctx.graph:
            self.platform_resource_repository = PlatformResourceRepository(
                self.ctx.graph
            )

    def get_database_lf_tags(
        self,
        catalog_id: str,
        database_name: str,
    ) -> List[LakeFormationTag]:
        """Get all LF tags for a specific table."""
        try:
            # Get LF tags for the specified table
            response = self.lf_client.get_resource_lf_tags(
                CatalogId=catalog_id,
                Resource={
                    "Database": {
                        "CatalogId": catalog_id,
                        "Name": database_name,
                    }
                },
                ShowAssignedLFTags=True,
            )

            if response:
                logger.info(f"LF tags for database {database_name}: {response}")
            # Extract and return the LF tags
            lf_tags = response.get("LFTagOnDatabase", [])

            tags = []
            for lf_tag in lf_tags:
                catalog_id = lf_tag.get("CatalogId")
                tag_key = lf_tag.get("TagKey")
                for tag_value in lf_tag.get("TagValues", []):
                    t = LakeFormationTag(
                        key=tag_key,
                        value=tag_value,
                        catalog_id=catalog_id,
                    )
                    tags.append(t)
            return tags

        except Exception as e:
            print(
                f"Error getting LF tags for table {catalog_id}.{database_name}: {str(e)}"
            )
            return []

    def get_table_lf_tags(
        self,
        catalog_id: str,
        database_name: str,
        table_name: str,
    ) -> List[LakeFormationTag]:
        """Get all LF tags for a specific table."""
        try:
            # Get LF tags for the specified table
            response = self.lf_client.get_resource_lf_tags(
                CatalogId=catalog_id,
                Resource={
                    "Table": {
                        "CatalogId": catalog_id,
                        "DatabaseName": database_name,
                        "Name": table_name,
                    },
                },
                ShowAssignedLFTags=True,
            )

            # Extract and return the LF tags
            lf_tags = response.get("LFTagsOnTable", [])

            tags = []
            for lf_tag in lf_tags:
                catalog_id = lf_tag.get("CatalogId")
                tag_key = lf_tag.get("TagKey")
                for tag_value in lf_tag.get("TagValues", []):
                    t = LakeFormationTag(
                        key=tag_key,
                        value=tag_value,
                        catalog_id=catalog_id,
                    )
                    tags.append(t)
            return tags

        except Exception:
            return []

    def get_all_lf_tags(self) -> List:
        # 1. Get all LF-Tags in your account (metadata only)
        response = self.lf_client.list_lf_tags(
            MaxResults=50  # Adjust as needed
        )
        all_lf_tags = response["LFTags"]
        # Continue pagination if necessary
        while "NextToken" in response:
            response = self.lf_client.list_lf_tags(
                NextToken=response["NextToken"], MaxResults=50
            )
            all_lf_tags.extend(response["LFTags"])
        return all_lf_tags

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
        return self.source_config.platform

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

    def get_dataflow_graph(
        self, script_path: str, flow_urn: str
    ) -> Optional[Dict[str, Any]]:
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
            self.report_warning(
                flow_urn,
                f"Error parsing DAG for Glue job. The script {script_path} is not a valid S3 path.",
            )
            self.report.num_job_script_location_invalid += 1

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
            self.report_warning(
                flow_urn,
                f"Unable to download DAG for Glue job from {script_path}, so job subtasks and lineage will be missing: {e}",
            )
            self.report.num_job_script_failed_download += 1
            return None
        script = obj["Body"].read().decode("utf-8")

        try:
            # extract the job DAG from the script
            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_dataflow_graph
            return self.glue_client.get_dataflow_graph(PythonScript=script)

        # sometimes the Python script can be user-modified and the script is not valid for graph extraction
        except self.glue_client.exceptions.InvalidInputException as e:
            self.report_warning(
                flow_urn,
                f"Error parsing DAG for Glue job. The script {script_path} cannot be processed by Glue (this usually occurs when it has been user-modified): {e}",
            )
            self.report.num_job_script_failed_parsing += 1

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
                    self.report_warning(
                        flow_urn,
                        f"Could not find script path for job {node['NodeType']}-{node['Id']} in flow {flow_urn}. Skipping",
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
                    self.report_warning(
                        flow_urn,
                        f"Unrecognized node {node['NodeType']}-{node['Id']} in flow {flow_urn}. Args: {node_args} Skipping",
                    )
                    return None
                else:
                    raise ValueError(f"Unrecognized Glue data object type: {node_args}")

        # otherwise, a node represents a transformation
        else:
            node_urn = mce_builder.make_data_job_urn_with_flow(
                flow_urn, job_id=f"{node['NodeType']}-{node['Id']}"
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
                self.report_warning(
                    flow_urn,
                    f"Unrecognized source or target node in edge: {edge}. Skipping."
                    "This may lead to missing lineage",
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

        return MetadataWorkUnit(id=f"{job_name}-{node['Id']}", mce=mce)

    def get_all_databases(self) -> Iterable[Mapping[str, Any]]:
        logger.debug("Getting all databases")
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/paginator/GetDatabases.html
        paginator = self.glue_client.get_paginator("get_databases")

        if self.source_config.catalog_id:
            paginator_response = paginator.paginate(
                CatalogId=self.source_config.catalog_id
            )
        else:
            paginator_response = paginator.paginate()

        pattern = "DatabaseList"
        if self.source_config.ignore_resource_links:
            # exclude resource links by using a JMESPath conditional query against the TargetDatabase struct key
            pattern += "[?!TargetDatabase]"

        for database in paginator_response.search(pattern):
            if (not self.source_config.database_pattern.allowed(database["Name"])) or (
                self.source_config.catalog_id
                and database.get("CatalogId")
                and database.get("CatalogId") != self.source_config.catalog_id
            ):
                self.report.databases.dropped(database["Name"])
            else:
                self.report.databases.processed(database["Name"])
                yield database

    def get_tables_from_database(self, database: Mapping[str, Any]) -> Iterable[Dict]:
        logger.debug(f"Getting tables from database {database['Name']}")
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/paginator/GetTables.html
        paginator = self.glue_client.get_paginator("get_tables")
        database_name = database["Name"]

        if self.source_config.catalog_id:
            paginator_response = paginator.paginate(
                DatabaseName=database_name, CatalogId=self.source_config.catalog_id
            )
        else:
            paginator_response = paginator.paginate(DatabaseName=database_name)

        for table in paginator_response.search("TableList"):
            # if resource links are detected, re-use database names from the current catalog
            # otherwise, external names are picked up instead of aliased ones when creating full table names later
            # This will cause an incoherent situation when creating full table names later
            # Note: use an explicit source_config check but it is useless actually (filtering has already been done)
            if (
                not self.source_config.ignore_resource_links
                and "TargetDatabase" in database
            ):
                table["DatabaseName"] = database["Name"]
            yield table

    def get_all_databases_and_tables(
        self,
    ) -> Tuple[List[Mapping[str, Any]], List[Dict]]:
        all_databases = [*self.get_all_databases()]
        all_tables = []
        for database in all_databases:
            try:
                for tables in self.get_tables_from_database(database):
                    all_tables.append(tables)
            except Exception as e:
                self.report.warning(
                    message="Failed to get tables from database",
                    context=database["Name"],
                    exc=e,
                )
        return all_databases, all_tables

    def get_lineage_if_enabled(
        self, mce: MetadataChangeEventClass
    ) -> Optional[MetadataWorkUnit]:
        if self.source_config.emit_s3_lineage:
            # extract dataset properties aspect
            dataset_properties: Optional[DatasetPropertiesClass] = (
                mce_builder.get_aspect_if_available(mce, DatasetPropertiesClass)
            )
            # extract dataset schema aspect
            schema_metadata: Optional[SchemaMetadataClass] = (
                mce_builder.get_aspect_if_available(mce, SchemaMetadataClass)
            )

            if dataset_properties and "Location" in dataset_properties.customProperties:
                location = dataset_properties.customProperties["Location"]
                if is_s3_uri(location):
                    s3_dataset_urn = make_s3_urn_for_lineage(
                        location, self.source_config.env
                    )
                    assert self.ctx.graph
                    schema_metadata_for_s3: Optional[SchemaMetadataClass] = (
                        self.ctx.graph.get_schema_metadata(s3_dataset_urn)
                    )

                    if self.source_config.glue_s3_lineage_direction == "upstream":
                        fine_grained_lineages = None
                        if (
                            self.source_config.include_column_lineage
                            and schema_metadata
                            and schema_metadata_for_s3
                        ):
                            fine_grained_lineages = self.get_fine_grained_lineages(
                                mce.proposedSnapshot.urn,
                                s3_dataset_urn,
                                schema_metadata,
                                schema_metadata_for_s3,
                            )
                        upstream_lineage = UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=s3_dataset_urn,
                                    type=DatasetLineageTypeClass.COPY,
                                )
                            ],
                            fineGrainedLineages=fine_grained_lineages or None,
                        )
                        return MetadataChangeProposalWrapper(
                            entityUrn=mce.proposedSnapshot.urn,
                            aspect=upstream_lineage,
                        ).as_workunit()
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
                        return MetadataChangeProposalWrapper(
                            entityUrn=s3_dataset_urn,
                            aspect=upstream_lineage,
                        ).as_workunit()
        return None

    def get_fine_grained_lineages(
        self,
        dataset_urn: str,
        s3_dataset_urn: str,
        schema_metadata: SchemaMetadata,
        schema_metadata_for_s3: SchemaMetadata,
    ) -> Optional[List[FineGrainedLineageClass]]:
        def simplify_field_path(field_path):
            return Dataset._simplify_field_path(field_path)

        if schema_metadata and schema_metadata_for_s3:
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            for field in schema_metadata.fields:
                field_path_v1 = simplify_field_path(field.fieldPath)
                matching_s3_field = next(
                    (
                        f
                        for f in schema_metadata_for_s3.fields
                        if simplify_field_path(f.fieldPath) == field_path_v1
                    ),
                    None,
                )
                if matching_s3_field:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                mce_builder.make_schema_field_urn(
                                    dataset_urn, field_path_v1
                                )
                            ],
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                mce_builder.make_schema_field_urn(
                                    s3_dataset_urn,
                                    simplify_field_path(matching_s3_field.fieldPath),
                                )
                            ],
                        )
                    )
            return fine_grained_lineages
        return None

    def _create_profile_mcp(
        self,
        mce: MetadataChangeEventClass,
        table_stats: dict,
        column_stats: dict,
        partition_spec: Optional[str] = None,
    ) -> Optional[MetadataChangeProposalWrapper]:
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

            if not self.source_config.profiling.profile_table_level_only:
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
                    column_profile.mean = column_params[
                        self.source_config.profiling.mean
                    ]
                if self.source_config.profiling.median in column_params:
                    column_profile.median = column_params[
                        self.source_config.profiling.median
                    ]
                if self.source_config.profiling.stdev in column_params:
                    column_profile.stdev = column_params[
                        self.source_config.profiling.stdev
                    ]

            dataset_profile.fieldProfiles.append(column_profile)

        # if no stats are available, skip ingestion
        if (
            not dataset_profile.fieldProfiles
            and dataset_profile.rowCount is None
            and dataset_profile.columnCount is None
        ):
            return None

        if partition_spec:
            # inject partition level stats
            dataset_profile.partitionSpec = PartitionSpecClass(
                partition=partition_spec,
                type=PartitionTypeClass.PARTITION,
            )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=mce.proposedSnapshot.urn,
            aspect=dataset_profile,
        )
        return mcp

    def get_profile_if_enabled(
        self, mce: MetadataChangeEventClass, database_name: str, table_name: str
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.is_profiling_enabled():
            # for cross-account ingestion
            kwargs = dict(
                DatabaseName=database_name,
                Name=table_name,
                CatalogId=self.source_config.catalog_id,
            )
            response = self.glue_client.get_table(
                **{k: v for k, v in kwargs.items() if v}
            )

            # check if this table is partitioned
            partition_keys = response["Table"].get("PartitionKeys")
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

                for p in partitions:
                    table_stats = p.get("Parameters", {})
                    column_stats = p["StorageDescriptor"]["Columns"]

                    # only support single partition key
                    partition_spec = str({partition_keys[0]: p["Values"][0]})

                    if self.source_config.profiling.partition_patterns.allowed(
                        partition_spec
                    ):
                        profile_mcp = self._create_profile_mcp(
                            mce, table_stats, column_stats, partition_spec
                        )
                        if profile_mcp:
                            yield profile_mcp.as_workunit()
                    else:
                        continue
            else:
                # ingest data profile without partition
                table_stats = response["Table"]["Parameters"]
                column_stats = response["Table"]["StorageDescriptor"]["Columns"]
                profile_mcp = self._create_profile_mcp(mce, table_stats, column_stats)
                if profile_mcp:
                    yield profile_mcp.as_workunit()

    def gen_database_key(self, database: str) -> DatabaseKey:
        return DatabaseKey(
            database=database,
            platform=self.platform,
            instance=self.source_config.platform_instance,
            env=self.source_config.env,
            backcompat_env_as_instance=True,
        )

    def gen_platform_resource(
        self, tag: LakeFormationTag
    ) -> Iterable[MetadataWorkUnit]:
        if self.ctx.graph and self.platform_resource_repository:
            platform_resource_id = LakeFormationTagPlatformResourceId.from_tag(
                platform_instance=self.source_config.platform_instance,
                platform_resource_repository=self.platform_resource_repository,
                catalog=tag.catalog,
                tag=tag,
            )
            logger.info(f"Created platform resource {platform_resource_id}")

            lf_tag = LakeFormationTagPlatformResource.get_from_datahub(
                platform_resource_id, self.platform_resource_repository, False
            )
            if (
                tag.to_datahub_tag_urn().urn()
                not in lf_tag.datahub_linked_resources().urns
            ):
                try:
                    lf_tag.datahub_linked_resources().add(
                        tag.to_datahub_tag_urn().urn()
                    )
                    platform_resource = lf_tag.as_platform_resource()
                    for mcp in platform_resource.to_mcps():
                        yield MetadataWorkUnit(
                            id=f"platform_resource-{platform_resource.id}",
                            mcp=mcp,
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to create platform resource for tag {tag}: {e}",
                        exc_info=True,
                    )
                    self.report.report_warning(
                        context="Failed to create platform resource",
                        message=f"Failed to create platform resource for Tag: {tag}",
                    )

    def gen_database_containers(
        self, database: Mapping[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        container_tags: Optional[List] = None
        if self.source_config.extract_lakeformation_tags:
            try:
                tags = self.get_database_lf_tags(
                    catalog_id=database["CatalogId"], database_name=database["Name"]
                )
                container_tags = []
                for tag in tags:
                    try:
                        container_tags.append(tag.to_datahub_tag_urn().name)
                        yield from self.gen_platform_resource(tag)
                    except InvalidUrnError:
                        continue
            except Exception:
                self.report_warning(
                    reason="Failed to extract Lake Formation tags for database",
                    key=database["Name"],
                )
        domain_urn = self._gen_domain_urn(database["Name"])
        database_container_key = self.gen_database_key(database["Name"])
        parameters = database.get("Parameters", {})
        if database.get("LocationUri") is not None:
            parameters["LocationUri"] = database["LocationUri"]
        if database.get("CreateTime") is not None:
            create_time: datetime.datetime = database["CreateTime"]
            parameters["CreateTime"] = create_time.strftime("%B %d, %Y at %H:%M:%S")
        yield from gen_containers(
            container_key=database_container_key,
            name=database["Name"],
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_urn=domain_urn,
            description=database.get("Description"),
            qualified_name=self.get_glue_arn(
                account_id=database["CatalogId"], database=database["Name"]
            ),
            tags=container_tags,
            extra_properties=parameters,
        )

    def add_table_to_database_container(
        self, dataset_urn: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_database_key(db_name)
        yield from add_dataset_to_container(
            container_key=database_container_key,
            dataset_urn=dataset_urn,
        )

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                return make_domain_urn(domain)

        return None

    def _get_domain_wu(
        self, dataset_name: str, entity_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        databases, tables = self.get_all_databases_and_tables()

        for database in databases:
            yield from self.gen_database_containers(database)

        for table in tables:
            table_name = table["Name"]
            try:
                yield from self._gen_table_wu(table=table)
            except KeyError as e:
                self.report.report_failure(
                    message="Failed to extract workunit for table",
                    context=f"Table: {table_name}",
                    exc=e,
                )
        if self.extract_transforms:
            yield from self._transform_extraction()

    def _gen_table_wu(self, table: Dict) -> Iterable[MetadataWorkUnit]:
        database_name = table["DatabaseName"]
        table_name = table["Name"]
        full_table_name = f"{database_name}.{table_name}"
        self.report.report_table_scanned()
        if not self.source_config.database_pattern.allowed(
            database_name
        ) or not self.source_config.table_pattern.allowed(full_table_name):
            self.report.report_table_dropped(full_table_name)
            return

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=full_table_name,
            env=self.env,
            platform_instance=self.source_config.platform_instance,
        )

        yield from self._extract_record(dataset_urn, table, full_table_name)
        # generate a Dataset snapshot
        # We also want to assign "table" subType to the dataset representing glue table - unfortunately it is not
        # possible via Dataset snapshot embedded in a mce, so we have to generate a mcp.
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.TABLE]),
        ).as_workunit()

        yield from self._get_domain_wu(
            dataset_name=full_table_name,
            entity_urn=dataset_urn,
        )
        yield from self.add_table_to_database_container(
            dataset_urn=dataset_urn, db_name=database_name
        )

    def _transform_extraction(self) -> Iterable[MetadataWorkUnit]:
        dags: Dict[str, Optional[Dict[str, Any]]] = {}
        flow_names: Dict[str, str] = {}
        for job in self.get_all_jobs():
            flow_urn = mce_builder.make_data_flow_urn(
                self.platform, job["Name"], self.env
            )

            yield self.get_dataflow_wu(flow_urn, job)

            job_script_location = job.get("Command", {}).get("ScriptLocation")

            dag: Optional[Dict[str, Any]] = None

            if job_script_location is not None:
                dag = self.get_dataflow_graph(job_script_location, flow_urn)
            else:
                self.report.num_job_script_location_missing += 1

            dags[flow_urn] = dag
            flow_names[flow_urn] = job["Name"]
        # run a first pass to pick up s3 bucket names and formats
        # in Glue, it's possible for two buckets to have files of different extensions
        # if this happens, we append the extension in the URN so the sources can be distinguished
        # see process_dataflow_node() for details
        s3_formats: DefaultDict[str, Set[Optional[str]]] = defaultdict(set)
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

            if not nodes:
                self.report.num_job_without_nodes += 1

            for node in nodes.values():
                if node["NodeType"] not in ["DataSource", "DataSink"]:
                    yield self.get_datajob_wu(node, flow_names[flow_urn])
                elif (node["NodeType"] == "DataSource" and node["outputDatasets"]) or (
                    node["NodeType"] == "DataSink" and node["inputDatasets"]
                ):
                    # Not common, but capturing counts here for reporting
                    self.report.num_dataset_to_dataset_edges_in_job += 1

            for dataset_id, dataset_mce in zip(new_dataset_ids, new_dataset_mces):
                yield MetadataWorkUnit(id=dataset_id, mce=dataset_mce)

    def _extract_record(
        self, dataset_urn: str, table: Dict, table_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Extract and yield metadata work units for a Glue table."""
        logger.debug(
            f"extract record from table={table_name} for dataset={dataset_urn}"
        )

        # Create the main dataset snapshot
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[
                Status(removed=False),
                self._get_dataset_properties(table),
            ],
        )

        # Add schema metadata if available
        schema_metadata = self._get_schema_metadata(table, table_name, dataset_urn)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)

        # Add platform instance
        dataset_snapshot.aspects.append(self._get_data_platform_instance())

        # Add ownership if enabled
        if self.extract_owners:
            ownership = GlueSource._get_ownership(table.get("Owner"))
            if ownership:
                dataset_snapshot.aspects.append(ownership)

        # Add S3 tags if enabled
        s3_tags = self._get_s3_tags(table, dataset_urn)
        if s3_tags:
            dataset_snapshot.aspects.append(s3_tags)

        # Add Lake Formation tags if enabled
        if self.source_config.extract_lakeformation_tags:
            tags = self.get_table_lf_tags(
                catalog_id=table["CatalogId"],
                database_name=table["DatabaseName"],
                table_name=table["Name"],
            )

            global_tags = self._get_lake_formation_tags(tags)
            if global_tags:
                dataset_snapshot.aspects.append(global_tags)
                # Generate platform resources for LF tags
                for tag in tags:
                    yield from self.gen_platform_resource(tag)

        # Create and yield the main metadata work unit
        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(table_name, mce=metadata_record)

        # Add lineage if enabled
        lineage_wu = self.get_lineage_if_enabled(metadata_record)
        if lineage_wu:
            yield lineage_wu

        # Add profile if enabled
        try:
            yield from self.get_profile_if_enabled(
                metadata_record, table["DatabaseName"], table["Name"]
            )
        except KeyError as e:
            self.report.report_failure(
                message="Failed to extract profile for table",
                context=f"Table: {dataset_urn}",
                exc=e,
            )

    def _get_dataset_properties(self, table: Dict) -> DatasetPropertiesClass:
        """Extract dataset properties from Glue table."""
        storage_descriptor = table.get("StorageDescriptor", {})
        custom_properties = {
            **table.get("Parameters", {}),
            **{
                k: str(v)
                for k, v in storage_descriptor.items()
                if k not in ["Columns", "Parameters"]
            },
        }

        return DatasetPropertiesClass(
            description=table.get("Description"),
            customProperties=custom_properties,
            uri=table.get("Location"),
            tags=[],
            name=table["Name"],
            qualifiedName=self.get_glue_arn(
                account_id=table["CatalogId"],
                database=table["DatabaseName"],
                table=table["Name"],
            ),
        )

    def _get_schema_metadata(
        self, table: Dict, table_name: str, dataset_urn: str
    ) -> Optional[SchemaMetadata]:
        """Extract schema metadata from Glue table."""
        if not table.get("StorageDescriptor"):
            return None

        # Check if this is a delta table with schema in parameters
        if self._is_delta_schema(table):
            return self._get_delta_schema_metadata(table, table_name, dataset_urn)
        else:
            return self._get_glue_schema_metadata(table, table_name)

    def _is_delta_schema(self, table: Dict) -> bool:
        """Check if table uses delta format with schema in parameters."""
        if not self.source_config.extract_delta_schema_from_parameters:
            return False

        provider = table.get("Parameters", {}).get("spark.sql.sources.provider", "")
        num_parts = int(
            table.get("Parameters", {}).get("spark.sql.sources.schema.numParts", "0")
        )
        columns = table.get("StorageDescriptor", {}).get("Columns", [])

        return (
            provider == "delta"
            and num_parts > 0
            and columns
            and len(columns) == 1
            and columns[0].get("Name", "") == "col"
            and columns[0].get("Type", "") == "array<string>"
        )

    def _get_glue_schema_metadata(
        self, table: Dict, table_name: str
    ) -> Optional[SchemaMetadata]:
        """Extract schema metadata from Glue table columns."""
        schema = table["StorageDescriptor"]["Columns"]
        fields: List[SchemaField] = []

        # Process regular columns
        for field in schema:
            schema_fields = get_schema_fields_for_hive_column(
                hive_column_name=field["Name"],
                hive_column_type=field["Type"],
                description=field.get("Comment"),
                default_nullable=True,
            )
            if schema_fields:
                fields.extend(schema_fields)

        # Process partition keys
        partition_keys = table.get("PartitionKeys", [])
        for partition_key in partition_keys:
            schema_fields = get_schema_fields_for_hive_column(
                hive_column_name=partition_key["Name"],
                hive_column_type=partition_key.get("Type", "unknown"),
                description=partition_key.get("Comment"),
                default_nullable=False,
            )
            if schema_fields:
                fields.extend(schema_fields)

        return SchemaMetadata(
            schemaName=table_name,
            version=0,
            fields=fields,
            platform=f"urn:li:dataPlatform:{self.platform}",
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
        )

    def _get_delta_schema_metadata(
        self, table: Dict, table_name: str, dataset_urn: str
    ) -> Optional[SchemaMetadata]:
        """Extract schema metadata from Delta table parameters."""
        try:
            # Reconstruct schema from parameters
            num_parts = int(table["Parameters"]["spark.sql.sources.schema.numParts"])
            schema_str = "".join(
                table["Parameters"][f"spark.sql.sources.schema.part.{i}"]
                for i in range(num_parts)
            )
            schema_json = json.loads(schema_str)

            fields: List[SchemaField] = []
            for field in schema_json["fields"]:
                field_type = delta_type_to_hive_type(field.get("type", "unknown"))
                schema_fields = get_schema_fields_for_hive_column(
                    hive_column_name=field["name"],
                    hive_column_type=field_type,
                    description=field.get("description"),
                    default_nullable=bool(field.get("nullable", True)),
                )
                if schema_fields:
                    fields.extend(schema_fields)

            self.report.num_dataset_valid_delta_schema += 1
            return SchemaMetadata(
                schemaName=table_name,
                version=0,
                fields=fields,
                platform=f"urn:li:dataPlatform:{self.platform}",
                hash="",
                platformSchema=MySqlDDL(tableSchema=""),
            )

        except Exception as e:
            self.report_warning(
                dataset_urn,
                f"Could not parse schema for {table_name} because of {type(e).__name__}: {e}",
            )
            self.report.num_dataset_invalid_delta_schema += 1
            return None

    def _get_data_platform_instance(self) -> DataPlatformInstanceClass:
        """Get data platform instance aspect."""
        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=(
                make_dataplatform_instance_urn(
                    self.platform, self.source_config.platform_instance
                )
                if self.source_config.platform_instance
                else None
            ),
        )

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_ownership(owner: str) -> Optional[OwnershipClass]:
        """Get ownership aspect for a given owner."""
        if not owner:
            return None

        owners = [
            OwnerClass(
                owner=mce_builder.make_user_urn(owner),
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]
        return OwnershipClass(owners=owners)

    def _get_s3_tags(self, table: Dict, dataset_urn: str) -> Optional[GlobalTagsClass]:
        """Extract S3 tags if enabled."""
        if not (
            self.source_config.use_s3_bucket_tags
            or self.source_config.use_s3_object_tags
        ):
            return None

        # Check if table has a location (VIRTUAL_VIEW tables may not)
        location = table.get("StorageDescriptor", {}).get("Location")
        if not location:
            return None

        bucket_name = s3_util.get_bucket_name(location)
        tags_to_add: List[str] = []

        # Get bucket tags
        if self.source_config.use_s3_bucket_tags:
            try:
                bucket_tags = self.s3_client.get_bucket_tagging(Bucket=bucket_name)
                tags_to_add.extend(
                    make_tag_urn(f"{tag['Key']}:{tag['Value']}")
                    for tag in bucket_tags["TagSet"]
                )
            except self.s3_client.exceptions.ClientError:
                logger.warning(f"No tags found for bucket={bucket_name}")

        # Get object tags
        if self.source_config.use_s3_object_tags:
            key_prefix = s3_util.get_key_prefix(location)
            try:
                object_tagging = self.s3_client.get_object_tagging(
                    Bucket=bucket_name, Key=key_prefix
                )
                if object_tagging["TagSet"]:
                    tags_to_add.extend(
                        make_tag_urn(f"{tag['Key']}:{tag['Value']}")
                        for tag in object_tagging["TagSet"]
                    )
                else:
                    logger.warning(
                        f"No tags found for bucket={bucket_name} key={key_prefix}"
                    )
            except Exception as e:
                logger.warning(f"Failed to get object tags: {e}")

        if not tags_to_add:
            return None

        # Merge with existing tags if connected to DataHub API
        if self.ctx.graph:
            logger.debug("Connected to DatahubApi, grabbing current tags to maintain.")
            current_tags: Optional[GlobalTagsClass] = self.ctx.graph.get_aspect(
                entity_urn=dataset_urn, aspect_type=GlobalTagsClass
            )
            if current_tags:
                tags_to_add.extend(current_tag.tag for current_tag in current_tags.tags)
        else:
            logger.warning(
                "Could not connect to DatahubApi. No current tags to maintain"
            )

        # Remove duplicates and create tags
        unique_tags = sorted(set(tags_to_add))
        return GlobalTagsClass(tags=[TagAssociationClass(tag) for tag in unique_tags])

    def _get_lake_formation_tags(
        self, tags: List[LakeFormationTag]
    ) -> Optional[GlobalTagsClass]:
        """Extract Lake Formation tags if enabled."""
        tag_urns: List[str] = []
        for tag in tags:
            try:
                tag_urns.append(tag.to_datahub_tag_urn().urn())
            except InvalidUrnError as e:
                logger.warning(
                    f"Invalid Lake Formation tag URN for {tag}: {e}", exc_info=True
                )
                continue  # Skip invalid tags

        tag_urns.sort()  # Sort to maintain consistent order
        return (
            GlobalTagsClass(tags=[TagAssociationClass(tag_urn) for tag_urn in tag_urns])
            if tag_urns
            else None
        )

    def get_report(self):
        return self.report

    def report_warning(self, key: str, reason: str) -> None:
        logger.warning(f"{key}: {reason}")
        self.report.report_warning(key, reason)
