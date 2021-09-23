import json
import typing
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig, make_s3_urn
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataFlowSnapshotClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobSnapshotClass,
    DatasetPropertiesClass,
    MapTypeClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class GlueSourceConfig(AwsSourceConfig):

    extract_transforms: Optional[bool] = True
    underlying_platform: Optional[str] = None

    @property
    def glue_client(self):
        return self.get_glue_client()

    @property
    def s3_client(self):
        return self.get_s3_client()


@dataclass
class GlueSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


class GlueSource(Source):
    source_config: GlueSourceConfig
    report = GlueSourceReport()

    def __init__(self, config: GlueSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = GlueSourceReport()
        self.glue_client = config.glue_client
        self.s3_client = config.s3_client
        self.extract_transforms = config.extract_transforms
        self.underlying_platform = config.underlying_platform
        self.env = config.env

    @classmethod
    def create(cls, config_dict, ctx):
        config = GlueSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_underlying_platform(self):
        if self.underlying_platform in ["athena"]:
            return self.underlying_platform
        return "glue"

    def get_all_jobs(self):
        """
        List all jobs in Glue.
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
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
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

    def get_dataflow_s3_names(
        self, dataflow_graph: Dict[str, Any]
    ) -> Iterator[Tuple[str, Optional[str]]]:

        # iterate through each node to populate processed nodes
        for node in dataflow_graph["DagNodes"]:

            node_type = node["NodeType"]

            # for nodes representing datasets, we construct a dataset URN accordingly
            if node_type in ["DataSource", "DataSink"]:

                node_args = {x["Name"]: json.loads(x["Value"]) for x in node["Args"]}

                # if data object is S3 bucket
                if node_args.get("connection_type") == "s3":

                    s3_uri = node_args["connection_options"]["path"]

                    extension = node_args.get("format")

                    yield s3_uri, extension

    def process_dataflow_node(
        self,
        node: Dict[str, Any],
        flow_urn: str,
        new_dataset_ids: List[str],
        new_dataset_mces: List[MetadataChangeEvent],
        s3_formats: typing.DefaultDict[str, Set[Union[str, None]]],
    ) -> Dict[str, Any]:

        node_type = node["NodeType"]

        # for nodes representing datasets, we construct a dataset URN accordingly
        if node_type in ["DataSource", "DataSink"]:

            node_args = {x["Name"]: json.loads(x["Value"]) for x in node["Args"]}

            # if data object is Glue table
            if "database" in node_args and "table_name" in node_args:

                full_table_name = f"{node_args['database']}.{node_args['table_name']}"

                # we know that the table will already be covered when ingesting Glue tables
                node_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.get_underlying_platform()},{full_table_name},{self.env})"

            # if data object is S3 bucket
            elif node_args.get("connection_type") == "s3":

                s3_uri = node_args["connection_options"]["path"]

                # append S3 format if different ones exist
                if len(s3_formats[s3_uri]) > 1:
                    node_urn = make_s3_urn(
                        s3_uri,
                        self.env,
                        suffix=node_args.get("format"),
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
        s3_formats: typing.DefaultDict[str, Set[Union[str, None]]],
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

            nodes[node["Id"]] = self.process_dataflow_node(
                node, flow_urn, new_dataset_ids, new_dataset_mces, s3_formats
            )

        # traverse edges to fill in node properties
        for edge in dataflow_graph["DagEdges"]:

            source_node = nodes[edge["Source"]]
            target_node = nodes[edge["Target"]]

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

    def get_datajob_wu(
        self, node: Dict[str, Any], job: Dict[str, Any]
    ) -> MetadataWorkUnit:
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
                        name=f"{job['Name']}:{node['NodeType']}-{node['Id']}",
                        type="GLUE",
                        # there's no way to view an individual job node by link, so just show the graph
                        externalUrl=f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}#/editor/job/{job['Name']}/graph",
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

        return MetadataWorkUnit(id=f'{job["Name"]}-{node["Id"]}', mce=mce)

    def get_all_tables(self) -> List[dict]:
        def get_tables_from_database(database_name: str) -> List[dict]:
            new_tables = []

            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_tables
            paginator = self.glue_client.get_paginator("get_tables")
            for page in paginator.paginate(DatabaseName=database_name):
                new_tables += page["TableList"]

            return new_tables

        def get_database_names() -> List[str]:
            database_names = []

            # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_databases
            paginator = self.glue_client.get_paginator("get_databases")
            for page in paginator.paginate():
                for db in page["DatabaseList"]:
                    if self.source_config.database_pattern.allowed(db["Name"]):
                        database_names.append(db["Name"])

            return database_names

        if self.source_config.database_pattern.is_fully_specified_allow_list():
            database_names = self.source_config.database_pattern.get_allowed_list()
        else:
            database_names = get_database_names()

        all_tables: List[dict] = []
        for database in database_names:
            all_tables += get_tables_from_database(database)
        return all_tables

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        tables = self.get_all_tables()

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

            mce = self._extract_record(table, full_table_name)
            workunit = MetadataWorkUnit(full_table_name, mce=mce)
            self.report.report_workunit(workunit)
            yield workunit

        if self.extract_transforms:

            dags = {}

            for job in self.get_all_jobs():

                flow_urn = mce_builder.make_data_flow_urn(
                    self.get_underlying_platform(), job["Name"], self.env
                )

                flow_wu = self.get_dataflow_wu(flow_urn, job)
                self.report.report_workunit(flow_wu)
                yield flow_wu

                job_script_location = job.get("Command", {}).get("ScriptLocation")

                dag: Optional[Dict[str, Any]] = None

                if job_script_location is not None:

                    dag = self.get_dataflow_graph(job_script_location)

                dags[flow_urn] = dag

            # run a first pass to pick up s3 bucket names and formats
            # in Glue, it's possible for two buckets to have files of different extensions
            # if this happens, we append the extension in the URN so the sources can be distinguished
            # see process_dataflow_node() for details

            s3_formats: typing.DefaultDict[str, Set[Union[str, None]]] = defaultdict(
                lambda: set()
            )

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
                        job_wu = self.get_datajob_wu(node, job)
                        self.report.report_workunit(job_wu)
                        yield job_wu

                for dataset_id, dataset_mce in zip(new_dataset_ids, new_dataset_mces):

                    dataset_wu = MetadataWorkUnit(id=dataset_id, mce=dataset_mce)
                    self.report.report_workunit(dataset_wu)
                    yield dataset_wu

    def _extract_record(self, table: Dict, table_name: str) -> MetadataChangeEvent:
        def get_owner() -> OwnershipClass:
            owner = table.get("Owner")
            if owner:
                owners = [
                    OwnerClass(
                        owner=f"urn:li:corpuser:{owner}",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            else:
                owners = []
            return OwnershipClass(
                owners=owners,
            )

        def get_dataset_properties() -> DatasetPropertiesClass:
            return DatasetPropertiesClass(
                description=table.get("Description"),
                customProperties={
                    **table.get("Parameters", {}),
                    **{
                        k: str(v)
                        for k, v in table["StorageDescriptor"].items()
                        if k not in ["Columns", "Parameters"]
                    },
                },
                uri=table.get("Location"),
                tags=[],
            )

        def get_schema_metadata(glue_source: GlueSource) -> SchemaMetadata:
            schema = table["StorageDescriptor"]["Columns"]
            fields: List[SchemaField] = []
            for field in schema:
                schema_field = SchemaField(
                    fieldPath=field["Name"],
                    nativeDataType=field["Type"],
                    type=get_column_type(
                        glue_source, field["Type"], table_name, field["Name"]
                    ),
                    description=field.get("Comment"),
                    recursive=False,
                    nullable=True,
                )
                fields.append(schema_field)

            partition_keys = table["PartitionKeys"]
            for partition_key in partition_keys:
                schema_field = SchemaField(
                    fieldPath=partition_key["Name"],
                    nativeDataType=partition_key["Type"],
                    type=get_column_type(
                        glue_source,
                        partition_key["Type"],
                        table_name,
                        partition_key["Name"],
                    ),
                    recursive=False,
                    nullable=False,
                )
                fields.append(schema_field)

            return SchemaMetadata(
                schemaName=table_name,
                version=0,
                fields=fields,
                platform=f"urn:li:dataPlatform:{self.get_underlying_platform()}",
                hash="",
                platformSchema=MySqlDDL(tableSchema=""),
            )

        dataset_snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.get_underlying_platform()},{table_name},{self.env})",
            aspects=[],
        )

        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(get_owner())
        dataset_snapshot.aspects.append(get_dataset_properties())
        dataset_snapshot.aspects.append(get_schema_metadata(self))

        metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return metadata_record

    def get_report(self):
        return self.report

    def close(self):
        pass


def get_column_type(
    glue_source: GlueSource, field_type: str, table_name: str, field_name: str
) -> SchemaFieldDataType:
    field_type_mapping = {
        "array": ArrayTypeClass,
        "bigint": NumberTypeClass,
        "binary": BytesTypeClass,
        "boolean": BooleanTypeClass,
        "char": StringTypeClass,
        "date": DateTypeClass,
        "decimal": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "int": NumberTypeClass,
        "integer": NumberTypeClass,
        "interval": TimeTypeClass,
        "long": NumberTypeClass,
        "map": MapTypeClass,
        "null": NullTypeClass,
        "set": ArrayTypeClass,
        "smallint": NumberTypeClass,
        "string": StringTypeClass,
        "struct": MapTypeClass,
        "timestamp": TimeTypeClass,
        "tinyint": NumberTypeClass,
        "union": UnionTypeClass,
        "varchar": StringTypeClass,
    }

    field_starts_type_mapping = {
        "array": ArrayTypeClass,
        "set": ArrayTypeClass,
        "map": MapTypeClass,
        "struct": MapTypeClass,
        "varchar": StringTypeClass,
        "decimal": NumberTypeClass,
    }

    type_class = None
    if field_type in field_type_mapping:
        type_class = field_type_mapping[field_type]
    else:
        for key in field_starts_type_mapping:
            if field_type.startswith(key):
                type_class = field_starts_type_mapping[key]
                break

    if type_class is None:
        glue_source.report.report_warning(
            field_type,
            f"The type '{field_type}' is not recognised for field '{field_name}' in table '{table_name}', setting as StringTypeClass.",
        )
        type_class = StringTypeClass
    data_type = SchemaFieldDataType(type=type_class())
    return data_type
