import dataclasses
import functools
import json
import logging
import os
import pathlib
import re
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from deltalake import DeltaTable
from more_itertools import peekable
from smart_open import open as smart_open

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_boto_utils import get_s3_tags, list_folders
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    get_key_prefix,
    strip_s3_prefix,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.delta_lake_utils import (
    get_file_count,
    read_delta_table,
)
from datahub.ingestion.source.delta_lake.report import DeltaLakeSourceReport
from datahub.ingestion.source.schema_inference import (
    avro,
    csv_tsv,
    json as schema_json,
    parquet,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    _Aspect,
)
from datahub.telemetry import telemetry
from datahub.utilities.delta import delta_type_to_hive_type
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

config_options_to_report = [
    "platform",
]

OPERATION_STATEMENT_TYPES = {
    "INSERT": OperationTypeClass.INSERT,
    "UPDATE": OperationTypeClass.UPDATE,
    "DELETE": OperationTypeClass.DELETE,
    "MERGE": OperationTypeClass.UPDATE,
    "CREATE": OperationTypeClass.CREATE,
    "CREATE_TABLE_AS_SELECT": OperationTypeClass.CREATE,
    "CREATE_SCHEMA": OperationTypeClass.CREATE,
    "DROP_TABLE": OperationTypeClass.DROP,
    "REPLACE TABLE AS SELECT": OperationTypeClass.UPDATE,
    "COPY INTO": OperationTypeClass.UPDATE,
}

PAGE_SIZE = 1000


def partitioned_folder_comparator(folder1: str, folder2: str) -> int:
    # Try to convert to number and compare if the folder name is a number
    try:
        # Stripping = from the folder names as it most probably partition name part like year=2021
        if "=" in folder1 and "=" in folder2:
            if folder1.rsplit("=", 1)[0] == folder2.rsplit("=", 1)[0]:
                folder1 = folder1.rsplit("=", 1)[-1]
                folder2 = folder2.rsplit("=", 1)[-1]

        num_folder1 = int(folder1)
        num_folder2 = int(folder2)
        if num_folder1 == num_folder2:
            return 0
        else:
            return 1 if num_folder1 > num_folder2 else -1
    except Exception:
        # If folder name is not a number then do string comparison
        if folder1 == folder2:
            return 0
        else:
            return 1 if folder1 > folder2 else -1


@dataclasses.dataclass
class TableData:
    id: str
    display_name: str
    is_s3: bool
    full_path: str
    partitions: Optional[OrderedDict]
    timestamp: datetime
    table_path: str
    size_in_bytes: int
    number_of_files: int


@platform_name("Delta Lake", id="delta-lake")
@config_class(DeltaLakeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TAGS, "Can extract S3 object/bucket tags if enabled")
class DeltaLakeSource(Source):
    """
    This plugin extracts:
    - Column types and schema associated with each delta table
    - Custom properties: number_of_files, partition_columns, table_creation_time, location, version etc.

    :::caution

    If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

    :::

    """

    source_config: DeltaLakeSourceConfig
    report: DeltaLakeSourceReport
    profiling_times_taken: List[float]
    container_WU_creator: ContainerWUCreator
    storage_options: Dict[str, str]

    def __init__(self, config: DeltaLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = DeltaLakeSourceReport()
        if self.source_config.is_s3:
            if (
                self.source_config.s3 is None
                or self.source_config.s3.aws_config is None
            ):
                raise ValueError("AWS Config must be provided for S3 base path.")
            self.s3_client = self.source_config.s3.aws_config.get_s3_client()

        # self.profiling_times_taken = []
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }

        telemetry.telemetry_instance.ping(
            "delta_lake_config",
            config_report,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = DeltaLakeSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _parse_datatype(self, raw_field_json_str: str) -> List[SchemaFieldClass]:
        raw_field_json = json.loads(raw_field_json_str)

        # get the parent field name and type
        field_name = raw_field_json.get("name")
        field_type = delta_type_to_hive_type(raw_field_json.get("type"))

        return get_schema_fields_for_hive_column(field_name, field_type)

    def get_fields(self, delta_table: DeltaTable) -> List[SchemaField]:
        fields: List[SchemaField] = []

        for raw_field in delta_table.schema().fields:
            parsed_data_list = self._parse_datatype(raw_field.to_json())
            fields = fields + parsed_data_list

        fields = sorted(fields, key=lambda f: f.fieldPath)
        return fields

    def _create_operation_aspect_wu(
        self, delta_table: DeltaTable, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        for hist in delta_table.history(
            limit=self.source_config.version_history_lookback
        ):
            # History schema picked up from https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history
            reported_time: int = int(time.time() * 1000)
            last_updated_timestamp: int = hist["timestamp"]
            statement_type = OPERATION_STATEMENT_TYPES.get(
                hist.get("operation", "UNKNOWN"), OperationTypeClass.CUSTOM
            )
            custom_type = (
                hist.get("operation")
                if statement_type == OperationTypeClass.CUSTOM
                else None
            )

            operation_custom_properties = dict()
            for key, val in sorted(hist.items()):
                if val is not None:
                    if isinstance(val, dict):
                        for k, v in sorted(val.items()):
                            if v is not None:
                                operation_custom_properties[f"{key}_{k}"] = str(v)
                    else:
                        operation_custom_properties[key] = str(val)
            operation_custom_properties.pop("timestamp", None)
            operation_custom_properties.pop("operation", None)
            operation_aspect = OperationClass(
                timestampMillis=reported_time,
                lastUpdatedTimestamp=last_updated_timestamp,
                operationType=statement_type,
                customOperationType=custom_type,
                customProperties=operation_custom_properties,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=operation_aspect,
            ).as_workunit()

    def ingest_table(
        self, delta_table: DeltaTable, path: str
    ) -> Iterable[MetadataWorkUnit]:
        table_name = (
            delta_table.metadata().name
            if delta_table.metadata().name
            else path.split("/")[-1]
        )
        if not self.source_config.table_pattern.allowed(table_name):
            logger.debug(
                f"Skipping table ({table_name}) present at location {path} as table pattern does not match"
            )

        logger.debug(f"Ingesting table {table_name} from location {path}")
        if self.source_config.relative_path is None:
            browse_path: str = (
                strip_s3_prefix(path) if self.source_config.is_s3 else path.strip("/")
            )
        else:
            browse_path = path.split(self.source_config.base_path)[1].strip("/")

        data_platform_urn = make_data_platform_urn(self.source_config.platform)
        logger.info(f"Creating dataset urn with name: {browse_path}")
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.source_config.platform,
            browse_path,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],
        )

        customProperties = {
            "number_of_files": str(get_file_count(delta_table)),
            "partition_columns": str(delta_table.metadata().partition_columns),
            "table_creation_time": str(delta_table.metadata().created_time),
            "id": str(delta_table.metadata().id),
            "version": str(delta_table.version()),
            "location": self.source_config.complete_path,
        }
        if not self.source_config.require_files:
            del customProperties["number_of_files"]  # always 0

        dataset_properties = DatasetPropertiesClass(
            description=delta_table.metadata().description,
            name=table_name,
            customProperties=customProperties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        fields = self.get_fields(delta_table)
        schema_metadata = SchemaMetadata(
            schemaName=table_name,
            platform=data_platform_urn,
            version=delta_table.version(),
            hash="",
            fields=fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )
        dataset_snapshot.aspects.append(schema_metadata)

        if (
            self.source_config.is_s3
            and self.source_config.s3
            and (
                self.source_config.s3.use_s3_bucket_tags
                or self.source_config.s3.use_s3_object_tags
            )
        ):
            bucket = get_bucket_name(path)
            key_prefix = get_key_prefix(path)
            s3_tags = get_s3_tags(
                bucket,
                key_prefix,
                dataset_urn,
                self.source_config.s3.aws_config,
                self.ctx,
                self.source_config.s3.use_s3_bucket_tags,
                self.source_config.s3.use_s3_object_tags,
            )
            if s3_tags is not None:
                dataset_snapshot.aspects.append(s3_tags)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield MetadataWorkUnit(id=str(delta_table.metadata().id), mce=mce)

        yield from self.container_WU_creator.create_container_hierarchy(
            browse_path, dataset_urn
        )

        yield from self._create_operation_aspect_wu(delta_table, dataset_urn)

    def get_storage_options(self) -> Dict[str, str]:
        if (
            self.source_config.is_s3
            and self.source_config.s3 is not None
            and self.source_config.s3.aws_config is not None
        ):
            aws_config = self.source_config.s3.aws_config
            creds = aws_config.get_credentials()
            opts = {
                "AWS_ACCESS_KEY_ID": creds.get("aws_access_key_id") or "",
                "AWS_SECRET_ACCESS_KEY": creds.get("aws_secret_access_key") or "",
                "AWS_SESSION_TOKEN": creds.get("aws_session_token") or "",
                # Allow http connections, this is required for minio
                "AWS_STORAGE_ALLOW_HTTP": "true",  # for delta-lake < 0.11.0
                "AWS_ALLOW_HTTP": "true",  # for delta-lake >= 0.11.0
            }
            if aws_config.aws_region:
                opts["AWS_REGION"] = aws_config.aws_region
            if aws_config.aws_endpoint_url:
                opts["AWS_ENDPOINT_URL"] = aws_config.aws_endpoint_url
            return opts
        else:
            return {}

    def process_folder(self, path: str) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Processing folder: {path}")
        delta_table = read_delta_table(path, self.storage_options, self.source_config)
        if delta_table:
            logger.debug(f"Delta table found at: {path}")
            yield from self.ingest_table(delta_table, path.rstrip("/"))
        else:
            for folder in self.get_folders(path):
                yield from self.process_folder(folder)

    def get_folders(self, path: str) -> Iterable[str]:
        if self.source_config.is_s3:
            return self.s3_get_folders(path)
        else:
            return self.local_get_folders(path)

    def s3_get_folders(self, path: str) -> Iterable[str]:
        parse_result = urlparse(path)
        for page in self.s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=parse_result.netloc, Prefix=parse_result.path[1:], Delimiter="/"
        ):
            for o in page.get("CommonPrefixes", []):
                yield f"{parse_result.scheme}://{parse_result.netloc}/{o.get('Prefix')}"

    def local_get_folders(self, path: str) -> Iterable[str]:
        if not os.path.isdir(path):
            raise FileNotFoundError(
                f"{path} does not exist or is not a directory. Please check base_path configuration."
            )
        for folder in os.listdir(path):
            yield os.path.join(path, folder)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        self.storage_options = self.get_storage_options()
        if self.source_config.path_spec:
            assert self.source_config.path_spec.include
            file_browser = self.s3_browser(
                self.source_config.path_spec,
                self.source_config.number_of_files_to_sample,
            )
            table_dict: Dict[str, TableData] = {}
            for file, timestamp, size in file_browser:
                if not self.source_config.path_spec.allowed(file):
                    continue
                table_data = self.extract_table_data(
                    self.source_config.path_spec, file, timestamp, size
                )
                if table_data.table_path not in table_dict:
                    table_dict[table_data.table_path] = table_data
                else:
                    table_dict[table_data.table_path].number_of_files = (
                        table_dict[table_data.table_path].number_of_files + 1
                    )
                    table_dict[table_data.table_path].size_in_bytes = (
                        table_dict[table_data.table_path].size_in_bytes
                        + table_data.size_in_bytes
                    )
                    if (
                        table_dict[table_data.table_path].timestamp
                        < table_data.timestamp
                    ) and (table_data.size_in_bytes > 0):
                        table_dict[
                            table_data.table_path
                        ].full_path = table_data.full_path
                        table_dict[
                            table_data.table_path
                        ].timestamp = table_data.timestamp

            for guid, table_data in table_dict.items():
                yield from self.ingest_table_from_path_spec(
                    table_data, self.source_config.path_spec
                )
        else:
            yield from self.process_folder(self.source_config.complete_path)

    def get_report(self) -> SourceReport:
        return self.report

    def extract_table_data(
        self, path_spec: PathSpec, path: str, timestamp: datetime, size: int
    ) -> TableData:
        logger.debug(f"Getting table data for path: {path}")
        _, table_path = path_spec.extract_table_name_and_path(path)
        table_name = get_key_prefix(path).split("/")[0]
        table_data = TableData(
            id=str(uuid.uuid4()),
            display_name=table_name,
            is_s3=self.source_config.is_s3,
            full_path=path,
            partitions=None,
            timestamp=timestamp,
            table_path=table_path,
            number_of_files=1,
            size_in_bytes=size,
        )
        return table_data

    def resolve_templated_folders(self, bucket_name: str, prefix: str) -> Iterable[str]:
        folder_split: List[str] = prefix.split("*", 1)
        if len(folder_split) == 1:
            return [prefix]
        pre_folder, post_folder = folder_split
        if not pre_folder:
            pre_folder = bucket_name
        return [pre_folder + "*" + post_folder]

    def get_dir_to_process(
        self, bucket_name: str, folder: str, path_spec: PathSpec, protocol: str
    ) -> str:
        assert self.source_config.s3
        iterator = list_folders(
            bucket_name=bucket_name,
            prefix=folder,
            aws_config=self.source_config.s3.aws_config,
        )
        iterator = peekable(iterator)
        if iterator:
            sorted_dirs = sorted(
                iterator,
                key=functools.cmp_to_key(partitioned_folder_comparator),
                reverse=True,
            )
            for dir in sorted_dirs:
                if path_spec.dir_allowed(f"{protocol}{bucket_name}/{dir}/"):
                    return self.get_dir_to_process(
                        bucket_name=bucket_name,
                        folder=dir + "/",
                        path_spec=path_spec,
                        protocol=protocol,
                    )
            return folder
        else:
            return folder

    def s3_browser(
        self, path_spec: PathSpec, sample_size: int
    ) -> Iterable[Tuple[str, datetime, int]]:
        assert self.source_config.s3
        if self.source_config.s3.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.source_config.s3.aws_config.get_s3_resource(
            self.source_config.verify_ssl
        )
        bucket_name = get_bucket_name(path_spec.include)
        logger.debug(f"Scanning bucket: {bucket_name}")
        bucket = s3.Bucket(bucket_name)
        prefix = self.get_prefix(get_bucket_relative_path(path_spec.include))
        logger.debug(f"Scanning objects with prefix:{prefix}")
        matches = re.finditer(r"{\s*\w+\s*}", path_spec.include, re.MULTILINE)
        matches_list = list(matches)
        if matches_list and path_spec.sample_files:
            # Replace the patch_spec include's templates with star because later we want to resolve all the stars
            # to actual directories.
            # For example:
            # "s3://my-test-bucket/*/{dept}/*/{table}/*/*.*" -> "s3://my-test-bucket/*/*/*/{table}/*/*.*"
            # We only keep the last template as a marker to know the point util we need to resolve path.
            # After the marker we can safely get sample files for sampling because it is not used in the
            # table name, so we don't need all the files.
            # This speed up processing but we won't be able to get a precise modification date/size/number of files.
            max_start: int = -1
            include: str = path_spec.include
            max_match: str = ""
            for match in matches_list:
                pos = include.find(match.group())
                if pos > max_start:
                    if max_match:
                        include = include.replace(max_match, "*")
                    max_start = match.start()
                    max_match = match.group()

            table_index = include.find(max_match)
            for folder in self.resolve_templated_folders(
                bucket_name, get_bucket_relative_path(include[:table_index])
            ):
                try:
                    for f in list_folders(
                        bucket_name, f"{folder}", self.source_config.s3.aws_config
                    ):
                        logger.info(f"Processing folder: {f}")
                        protocol = ContainerWUCreator.get_protocol(path_spec.include)
                        dir_to_process = self.get_dir_to_process(
                            bucket_name=bucket_name,
                            folder=f + "/",
                            path_spec=path_spec,
                            protocol=protocol,
                        )
                        logger.info(f"Getting files from folder: {dir_to_process}")
                        dir_to_process = dir_to_process.rstrip("\\")
                        for obj in (
                            bucket.objects.filter(Prefix=f"{dir_to_process}")
                            .page_size(PAGE_SIZE)
                            .limit(sample_size)
                        ):
                            s3_path = self.create_s3_path(obj.bucket_name, obj.key)
                            logger.debug(f"Sampling file: {s3_path}")
                            yield s3_path, obj.last_modified, obj.size,
                except Exception as e:
                    # This odd check if being done because boto does not have a proper exception to catch
                    # The exception that appears in stacktrace cannot actually be caught without a lot more work
                    # https://github.com/boto/boto3/issues/1195
                    if "NoSuchBucket" in repr(e):
                        logger.debug(f"Got NoSuchBucket exception for {bucket_name}", e)
                        self.get_report().report_warning(
                            "Missing bucket", f"No bucket found {bucket_name}"
                        )
                    else:
                        raise e
        else:
            logger.debug(
                "No template in the pathspec can't do sampling, fallbacking to do full scan"
            )
            path_spec.sample_files = False
            for obj in bucket.objects.filter(Prefix=prefix).page_size(PAGE_SIZE):
                s3_path = self.create_s3_path(obj.bucket_name, obj.key)
                logger.debug(f"Path: {s3_path}")
                yield s3_path, obj.last_modified, obj.size,

    def get_prefix(self, relative_path: str) -> str:
        index = re.search(r"[\*|\{]", relative_path)
        if index:
            return relative_path[: index.start()]
        else:
            return relative_path

    def create_s3_path(self, bucket_name: str, key: str) -> str:
        return f"s3://{bucket_name}/{key}"

    def ingest_table_from_path_spec(
        self, table_data: TableData, path_spec: PathSpec
    ) -> Iterable[MetadataWorkUnit]:
        aspects: List[Optional[_Aspect]] = []

        if not self.source_config.table_pattern.allowed(table_data.display_name):
            logger.debug(
                f"Skipping table ({table_data.display_name}) present at location {table_data.table_path} as table pattern does not match"
            )

        logger.debug(
            f"Ingesting table {table_data.display_name} from location {table_data.table_path}"
        )

        browse_path: str = strip_s3_prefix(table_data.table_path)

        data_platform_urn = make_data_platform_urn(self.source_config.platform)
        logger.info(f"Creating dataset urn with name: {browse_path}")
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.source_config.platform,
            browse_path,
            self.source_config.platform_instance,
            self.source_config.env,
        )

        if self.source_config.platform_instance:
            data_platform_instance = DataPlatformInstanceClass(
                platform=data_platform_urn,
                instance=make_dataplatform_instance_urn(
                    self.source_config.platform, self.source_config.platform_instance
                ),
            )
            aspects.append(data_platform_instance)

        customProperties = {"schema_inferred_from": str(table_data.full_path)}

        if not path_spec.sample_files:
            customProperties.update(
                {
                    "number_of_files": str(table_data.number_of_files),
                    "size_in_bytes": str(table_data.size_in_bytes),
                }
            )

        dataset_properties = DatasetPropertiesClass(
            description="",
            name=table_data.display_name,
            customProperties=customProperties,
        )
        aspects.append(dataset_properties)
        if table_data.size_in_bytes > 0:
            try:
                fields = self.get_fields_from_path_spec(table_data)
                schema_metadata = SchemaMetadata(
                    schemaName=table_data.display_name,
                    platform=data_platform_urn,
                    version=0,
                    hash="",
                    fields=fields,
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                aspects.append(schema_metadata)
            except Exception as e:
                logger.error(
                    f"Failed to extract schema from file {table_data.full_path}. The error was:{e}"
                )
        else:
            logger.info(
                f"Skipping schema extraction for empty file {table_data.full_path}"
            )

        if (
            self.source_config.is_s3
            and self.source_config.s3
            and (
                self.source_config.s3.use_s3_bucket_tags
                or self.source_config.s3.use_s3_object_tags
            )
        ):
            bucket = get_bucket_name(table_data.table_path)
            key_prefix = (
                get_key_prefix(table_data.table_path)
                if table_data.full_path == table_data.table_path
                else None
            )
            s3_tags = get_s3_tags(
                bucket,
                key_prefix,
                dataset_urn,
                self.source_config.s3.aws_config,
                self.ctx,
                self.source_config.s3.use_s3_bucket_tags,
                self.source_config.s3.use_s3_object_tags,
                self.source_config.verify_ssl,
            )
            if s3_tags:
                aspects.append(s3_tags)

        operation = self._create_table_operation_aspect(table_data)
        aspects.append(operation)
        for mcp in MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=aspects,
        ):
            yield mcp.as_workunit()

        yield from self.container_WU_creator.create_container_hierarchy(
            table_data.table_path, dataset_urn
        )

    def _create_table_operation_aspect(self, table_data: TableData) -> OperationClass:
        reported_time = int(time.time() * 1000)

        operation = OperationClass(
            timestampMillis=reported_time,
            lastUpdatedTimestamp=int(table_data.timestamp.timestamp() * 1000),
            # actor=make_user_urn(table_data.created_by),
            operationType=OperationTypeClass.UPDATE,
        )

        return operation

    def get_fields_from_path_spec(self, table_data: TableData) -> List:
        assert self.source_config.path_spec
        assert self.source_config.s3

        if self.source_config.s3.aws_config is None:
            raise ValueError("AWS config is required for S3 file sources")

        s3_client = self.source_config.s3.aws_config.get_s3_client(
            self.source_config.verify_ssl
        )

        file = smart_open(
            table_data.full_path, "rb", transport_params={"client": s3_client}
        )

        fields = []

        extension = pathlib.Path(table_data.full_path).suffix
        from datahub.ingestion.source.data_lake_common.path_spec import (
            SUPPORTED_COMPRESSIONS,
        )

        if self.source_config.path_spec.enable_compression and (
            extension[1:] in SUPPORTED_COMPRESSIONS
        ):
            # Removing the compression extension and using the one before that like .json.gz -> .json
            extension = pathlib.Path(table_data.full_path).with_suffix("").suffix
        if extension == "" and self.source_config.path_spec.default_extension:
            extension = f".{self.source_config.path_spec.default_extension}"

        try:
            if extension == ".parquet":
                fields = parquet.ParquetInferrer().infer_schema(file)
            elif extension == ".csv":
                fields = csv_tsv.CsvInferrer(
                    max_rows=self.source_config.max_rows
                ).infer_schema(file)
            elif extension == ".tsv":
                fields = csv_tsv.TsvInferrer(
                    max_rows=self.source_config.max_rows
                ).infer_schema(file)
            elif extension == ".jsonl":
                fields = schema_json.JsonInferrer(
                    max_rows=self.source_config.max_rows, format="jsonl"
                ).infer_schema(file)
            elif extension == ".json":
                fields = schema_json.JsonInferrer().infer_schema(file)
            elif extension == ".avro":
                fields = avro.AvroInferrer().infer_schema(file)
            else:
                self.report.report_warning(
                    table_data.full_path,
                    f"file {table_data.full_path} has unsupported extension",
                )
            file.close()
        except Exception as e:
            self.report.report_warning(
                table_data.full_path,
                f"could not infer schema for file {table_data.full_path}: {e}",
            )
            file.close()
        logger.debug(f"Extracted fields in schema: {fields}")
        if self.source_config.sort_schema_fields:
            fields = sorted(fields, key=lambda f: f.fieldPath)

        if self.source_config.add_partition_columns_to_schema:
            self.add_partition_columns_to_schema(
                fields=fields,
                path_spec=self.source_config.path_spec,
                full_path=table_data.full_path,
            )

        return fields

    def add_partition_columns_to_schema(
        self, path_spec: Optional[PathSpec], full_path: str, fields: List[SchemaField]
    ) -> None:
        is_fieldpath_v2 = False
        for field in fields:
            if field.fieldPath.startswith("[version=2.0]"):
                is_fieldpath_v2 = True
                break
        if path_spec is None:
            return None
        vars = path_spec.get_named_vars(full_path)
        if vars is not None and "partition_key" in vars:
            for partition_key in vars["partition_key"].values():
                fields.append(
                    SchemaField(
                        fieldPath=f"{partition_key}"
                        if not is_fieldpath_v2
                        else f"[version=2.0].[type=string].{partition_key}",
                        nativeDataType="string",
                        type=SchemaFieldDataType(StringTypeClass())
                        if not is_fieldpath_v2
                        else SchemaFieldDataTypeClass(type=StringTypeClass()),
                        isPartitioningKey=True,
                        nullable=True,
                        recursive=False,
                    )
                )
