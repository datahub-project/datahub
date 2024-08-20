import dataclasses
import functools
import logging
import os
import pathlib
import re
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import PurePath
from typing import Dict, Iterable, List, Optional, Tuple

import smart_open.compression as so_compression
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.abs.config import DataLakeSourceConfig, PathSpec
from datahub.ingestion.source.abs.report import DataLakeSourceReport
from datahub.ingestion.source.azure.abs_folder_utils import (
    get_abs_properties,
    get_abs_tags,
    list_folders,
)
from datahub.ingestion.source.azure.abs_utils import (
    get_container_name,
    get_container_relative_path,
    get_key_prefix,
    strip_abs_prefix,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
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
    _Aspect,
)
from datahub.telemetry import telemetry
from datahub.utilities.perf_timer import PerfTimer

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

PAGE_SIZE = 1000

# Hack to support the .gzip extension with smart_open.
so_compression.register_compressor(".gzip", so_compression._COMPRESSOR_REGISTRY[".gz"])


# config flags to emit telemetry for
config_options_to_report = [
    "platform",
    "use_relative_path",
    "ignore_dotfiles",
]


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
    display_name: str
    is_abs: bool
    full_path: str
    rel_path: str
    partitions: Optional[OrderedDict]
    timestamp: datetime
    table_path: str
    size_in_bytes: int
    number_of_files: int


@platform_name("ABS Data Lake", id="abs")
@config_class(DataLakeSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.TAGS, "Can extract ABS object/container tags if enabled")
class ABSSource(StatefulIngestionSourceBase):
    source_config: DataLakeSourceConfig
    report: DataLakeSourceReport
    profiling_times_taken: List[float]
    container_WU_creator: ContainerWUCreator

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = DataLakeSourceReport()
        self.profiling_times_taken = []
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }
        config_report = {
            **config_report,
            "profiling_enabled": config.is_profiling_enabled(),
        }

        telemetry.telemetry_instance.ping(
            "data_lake_config",
            config_report,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataLakeSourceConfig.parse_obj(config_dict)

        return cls(config, ctx)

    def get_fields(self, table_data: TableData, path_spec: PathSpec) -> List:
        if self.is_abs_platform():
            if self.source_config.azure_config is None:
                raise ValueError("Azure config is required for ABS file sources")

            abs_client = self.source_config.azure_config.get_blob_service_client()
            file = smart_open(
                f"azure://{self.source_config.azure_config.container_name}/{table_data.rel_path}",
                "rb",
                transport_params={"client": abs_client},
            )
        else:
            # We still use smart_open here to take advantage of the compression
            # capabilities of smart_open.
            file = smart_open(table_data.full_path, "rb")

        fields = []

        extension = pathlib.Path(table_data.full_path).suffix
        from datahub.ingestion.source.data_lake_common.path_spec import (
            SUPPORTED_COMPRESSIONS,
        )

        if path_spec.enable_compression and (extension[1:] in SUPPORTED_COMPRESSIONS):
            # Removing the compression extension and using the one before that like .json.gz -> .json
            extension = pathlib.Path(table_data.full_path).with_suffix("").suffix
        if extension == "" and path_spec.default_extension:
            extension = f".{path_spec.default_extension}"

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
            elif extension == ".json":
                fields = json.JsonInferrer().infer_schema(file)
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
        fields = sorted(fields, key=lambda f: f.fieldPath)

        if self.source_config.add_partition_columns_to_schema:
            self.add_partition_columns_to_schema(
                fields=fields, path_spec=path_spec, full_path=table_data.full_path
            )

        return fields

    def add_partition_columns_to_schema(
        self, path_spec: PathSpec, full_path: str, fields: List[SchemaField]
    ) -> None:
        vars = path_spec.get_named_vars(full_path)
        if vars is not None and "partition" in vars:
            for partition in vars["partition"].values():
                partition_arr = partition.split("=")
                if len(partition_arr) != 2:
                    logger.debug(
                        f"Could not derive partition key from partition field {partition}"
                    )
                    continue
                partition_key = partition_arr[0]
                fields.append(
                    SchemaField(
                        fieldPath=f"{partition_key}",
                        nativeDataType="string",
                        type=SchemaFieldDataType(StringTypeClass()),
                        isPartitioningKey=True,
                        nullable=True,
                        recursive=False,
                    )
                )

    def _create_table_operation_aspect(self, table_data: TableData) -> OperationClass:
        reported_time = int(time.time() * 1000)

        operation = OperationClass(
            timestampMillis=reported_time,
            lastUpdatedTimestamp=int(table_data.timestamp.timestamp() * 1000),
            operationType=OperationTypeClass.UPDATE,
        )

        return operation

    def ingest_table(
        self, table_data: TableData, path_spec: PathSpec
    ) -> Iterable[MetadataWorkUnit]:
        aspects: List[Optional[_Aspect]] = []

        logger.info(f"Extracting table schema from file: {table_data.full_path}")
        browse_path: str = (
            strip_abs_prefix(table_data.table_path)
            if self.is_abs_platform()
            else table_data.table_path.strip("/")
        )

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

        container = get_container_name(table_data.table_path)
        key_prefix = (
            get_key_prefix(table_data.table_path)
            if table_data.full_path == table_data.table_path
            else None
        )

        custom_properties = get_abs_properties(
            container,
            key_prefix,
            full_path=str(table_data.full_path),
            number_of_files=table_data.number_of_files,
            size_in_bytes=table_data.size_in_bytes,
            sample_files=path_spec.sample_files,
            azure_config=self.source_config.azure_config,
            use_abs_container_properties=self.source_config.use_abs_container_properties,
            use_abs_blob_properties=self.source_config.use_abs_blob_properties,
        )

        dataset_properties = DatasetPropertiesClass(
            description="",
            name=table_data.display_name,
            customProperties=custom_properties,
        )
        aspects.append(dataset_properties)
        if table_data.size_in_bytes > 0:
            try:
                fields = self.get_fields(table_data, path_spec)
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
            self.source_config.use_abs_container_properties
            or self.source_config.use_abs_blob_tags
        ):
            abs_tags = get_abs_tags(
                container,
                key_prefix,
                dataset_urn,
                self.source_config.azure_config,
                self.ctx,
                self.source_config.use_abs_blob_tags,
            )
            if abs_tags:
                aspects.append(abs_tags)

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

    def get_prefix(self, relative_path: str) -> str:
        index = re.search(r"[\*|\{]", relative_path)
        if index:
            return relative_path[: index.start()]
        else:
            return relative_path

    def extract_table_name(self, path_spec: PathSpec, named_vars: dict) -> str:
        if path_spec.table_name is None:
            raise ValueError("path_spec.table_name is not set")
        return path_spec.table_name.format_map(named_vars)

    def extract_table_data(
        self,
        path_spec: PathSpec,
        path: str,
        rel_path: str,
        timestamp: datetime,
        size: int,
    ) -> TableData:
        logger.debug(f"Getting table data for path: {path}")
        table_name, table_path = path_spec.extract_table_name_and_path(path)
        table_data = TableData(
            display_name=table_name,
            is_abs=self.is_abs_platform(),
            full_path=path,
            rel_path=rel_path,
            partitions=None,
            timestamp=timestamp,
            table_path=table_path,
            number_of_files=1,
            size_in_bytes=size,
        )
        return table_data

    def resolve_templated_folders(
        self, container_name: str, prefix: str
    ) -> Iterable[str]:
        folder_split: List[str] = prefix.split("*", 1)
        # If the len of split is 1 it means we don't have * in the prefix
        if len(folder_split) == 1:
            yield prefix
            return

        folders: Iterable[str] = list_folders(
            container_name, folder_split[0], self.source_config.azure_config
        )
        for folder in folders:
            yield from self.resolve_templated_folders(
                container_name, f"{folder}{folder_split[1]}"
            )

    def get_dir_to_process(
        self,
        container_name: str,
        folder: str,
        path_spec: PathSpec,
        protocol: str,
    ) -> str:
        iterator = list_folders(
            container_name=container_name,
            prefix=folder,
            azure_config=self.source_config.azure_config,
        )
        iterator = peekable(iterator)
        if iterator:
            sorted_dirs = sorted(
                iterator,
                key=functools.cmp_to_key(partitioned_folder_comparator),
                reverse=True,
            )
            for dir in sorted_dirs:
                if path_spec.dir_allowed(f"{protocol}{container_name}/{dir}/"):
                    return self.get_dir_to_process(
                        container_name=container_name,
                        folder=dir + "/",
                        path_spec=path_spec,
                        protocol=protocol,
                    )
            return folder
        else:
            return folder

    def abs_browser(
        self, path_spec: PathSpec, sample_size: int
    ) -> Iterable[Tuple[str, str, datetime, int]]:
        if self.source_config.azure_config is None:
            raise ValueError("azure_config not set. Cannot browse Azure Blob Storage")
        abs_blob_service_client = (
            self.source_config.azure_config.get_blob_service_client()
        )
        container_client = abs_blob_service_client.get_container_client(
            self.source_config.azure_config.container_name
        )

        container_name = self.source_config.azure_config.container_name
        logger.debug(f"Scanning container: {container_name}")

        prefix = self.get_prefix(get_container_relative_path(path_spec.include))
        logger.debug(f"Scanning objects with prefix:{prefix}")

        matches = re.finditer(r"{\s*\w+\s*}", path_spec.include, re.MULTILINE)
        matches_list = list(matches)
        if matches_list and path_spec.sample_files:
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
                container_name,
                get_container_relative_path(include[:table_index]),
            ):
                try:
                    for f in list_folders(
                        container_name, f"{folder}", self.source_config.azure_config
                    ):
                        logger.info(f"Processing folder: {f}")
                        protocol = ContainerWUCreator.get_protocol(path_spec.include)
                        dir_to_process = self.get_dir_to_process(
                            container_name=container_name,
                            folder=f + "/",
                            path_spec=path_spec,
                            protocol=protocol,
                        )
                        logger.info(f"Getting files from folder: {dir_to_process}")
                        dir_to_process = dir_to_process.rstrip("\\")
                        for obj in container_client.list_blobs(
                            name_starts_with=f"{dir_to_process}",
                            results_per_page=PAGE_SIZE,
                        ):
                            abs_path = self.create_abs_path(obj.name)
                            logger.debug(f"Sampling file: {abs_path}")
                            yield abs_path, obj.name, obj.last_modified, obj.size,
                except Exception as e:
                    # This odd check if being done because boto does not have a proper exception to catch
                    # The exception that appears in stacktrace cannot actually be caught without a lot more work
                    # https://github.com/boto/boto3/issues/1195
                    if "NoSuchBucket" in repr(e):
                        logger.debug(
                            f"Got NoSuchBucket exception for {container_name}", e
                        )
                        self.get_report().report_warning(
                            "Missing bucket", f"No bucket found {container_name}"
                        )
                    else:
                        raise e
        else:
            logger.debug(
                "No template in the pathspec can't do sampling, fallbacking to do full scan"
            )
            path_spec.sample_files = False
            for obj in container_client.list_blobs(
                prefix=f"{prefix}", results_per_page=PAGE_SIZE
            ):
                abs_path = self.create_abs_path(obj.name)
                logger.debug(f"Path: {abs_path}")
                # the following line if using the file_system_client
                # yield abs_path, obj.last_modified, obj.content_length,
                yield abs_path, obj.name, obj.last_modified, obj.size

    def create_abs_path(self, key: str) -> str:
        if self.source_config.azure_config:
            account_name = self.source_config.azure_config.account_name
            container_name = self.source_config.azure_config.container_name
            return (
                f"https://{account_name}.blob.core.windows.net/{container_name}/{key}"
            )
        return ""

    def local_browser(
        self, path_spec: PathSpec
    ) -> Iterable[Tuple[str, str, datetime, int]]:
        prefix = self.get_prefix(path_spec.include)
        if os.path.isfile(prefix):
            logger.debug(f"Scanning single local file: {prefix}")
            file_name = prefix
            yield prefix, file_name, datetime.utcfromtimestamp(
                os.path.getmtime(prefix)
            ), os.path.getsize(prefix)
        else:
            logger.debug(f"Scanning files under local folder: {prefix}")
            for root, dirs, files in os.walk(prefix):
                dirs.sort(key=functools.cmp_to_key(partitioned_folder_comparator))

                for file in sorted(files):
                    # We need to make sure the path is in posix style which is not true on windows
                    full_path = PurePath(
                        os.path.normpath(os.path.join(root, file))
                    ).as_posix()
                    yield full_path, file, datetime.utcfromtimestamp(
                        os.path.getmtime(full_path)
                    ), os.path.getsize(full_path)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        with PerfTimer():
            assert self.source_config.path_specs
            for path_spec in self.source_config.path_specs:
                file_browser = (
                    self.abs_browser(
                        path_spec, self.source_config.number_of_files_to_sample
                    )
                    if self.is_abs_platform()
                    else self.local_browser(path_spec)
                )
                table_dict: Dict[str, TableData] = {}
                for file, name, timestamp, size in file_browser:
                    if not path_spec.allowed(file):
                        continue
                    table_data = self.extract_table_data(
                        path_spec, file, name, timestamp, size
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
                    yield from self.ingest_table(table_data, path_spec)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def is_abs_platform(self):
        return self.source_config.platform == "abs"

    def get_report(self):
        return self.report
