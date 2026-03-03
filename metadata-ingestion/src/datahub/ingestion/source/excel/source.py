import glob
import io
import logging
import os
import re
from datetime import datetime, timezone
from enum import Enum, auto
from io import BytesIO
from pathlib import PurePath
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_boto_utils import get_s3_tags
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    strip_s3_prefix,
)
from datahub.ingestion.source.azure.abs_folder_utils import (
    get_abs_tags,
)
from datahub.ingestion.source.azure.abs_utils import (
    get_container_relative_path,
    strip_abs_prefix,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.excel.config import ExcelSourceConfig
from datahub.ingestion.source.excel.excel_file import ExcelFile, ExcelTable
from datahub.ingestion.source.excel.profiling import ExcelProfiler
from datahub.ingestion.source.excel.report import ExcelSourceReport
from datahub.ingestion.source.excel.util import gen_dataset_name
from datahub.ingestion.source.s3.source import BrowsePath
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import TimeStamp
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    GlobalTagsClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    SchemaMetadataClass as SchemaMetadata,
    StringTypeClass,
)
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

field_type_mapping: Dict[str, Type] = {
    "int8": NumberTypeClass,
    "int16": NumberTypeClass,
    "int32": NumberTypeClass,
    "int64": NumberTypeClass,
    "uint8": NumberTypeClass,
    "uint16": NumberTypeClass,
    "uint32": NumberTypeClass,
    "uint64": NumberTypeClass,
    "Int8": NumberTypeClass,
    "Int16": NumberTypeClass,
    "Int32": NumberTypeClass,
    "Int64": NumberTypeClass,
    "UInt8": NumberTypeClass,
    "UInt16": NumberTypeClass,
    "UInt32": NumberTypeClass,
    "UInt64": NumberTypeClass,
    "intp": NumberTypeClass,
    "uintp": NumberTypeClass,
    "float16": NumberTypeClass,
    "float32": NumberTypeClass,
    "float64": NumberTypeClass,
    "float128": NumberTypeClass,
    "Float32": NumberTypeClass,
    "Float64": NumberTypeClass,
    "complex64": NumberTypeClass,
    "complex128": NumberTypeClass,
    "complex256": NumberTypeClass,
    "bool": BooleanTypeClass,
    "boolean": BooleanTypeClass,
    "object": StringTypeClass,
    "string": StringTypeClass,
    "datetime64": DateTypeClass,
    "datetime64[ns]": DateTypeClass,
    "datetime64[ns, tz]": DateTypeClass,
    "timedelta64": DateTypeClass,
    "timedelta64[ns]": DateTypeClass,
    "period": DateTypeClass,
    "period[D]": DateTypeClass,
    "period[M]": DateTypeClass,
    "period[Y]": DateTypeClass,
    "category": RecordTypeClass,
    "interval": RecordTypeClass,
    "sparse": RecordTypeClass,
    "NA": NullTypeClass,
}


ALLOWED_EXTENSIONS = [".xlsx", ".xlsm", ".xltx", ".xltm"]


class UriType(Enum):
    HTTP = auto()
    HTTPS = auto()
    LOCAL_FILE = auto()
    ABSOLUTE_PATH = auto()
    RELATIVE_PATH = auto()
    S3 = auto()
    S3A = auto()
    ABS = auto()
    UNKNOWN = auto()


@platform_name("Excel")
@config_class(ExcelSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class ExcelSource(StatefulIngestionSourceBase):
    config: ExcelSourceConfig
    report: ExcelSourceReport
    container_WU_creator: ContainerWUCreator
    platform: str = "excel"

    def __init__(self, ctx: PipelineContext, config: ExcelSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report: ExcelSourceReport = ExcelSourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ExcelSource":
        config = ExcelSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    @staticmethod
    def uri_type(uri: str) -> Tuple[UriType, str]:
        if not uri or not isinstance(uri, str):
            return UriType.UNKNOWN, ""

        uri = uri.strip()
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()

        if scheme == "http":
            return UriType.HTTP, uri[7:]
        elif scheme == "https":
            if parsed.netloc and ".blob.core.windows.net" in parsed.netloc:
                return UriType.ABS, uri[8:]
            else:
                return UriType.HTTPS, uri[8:]
        elif scheme == "file":
            if uri.startswith("file:///"):
                return UriType.LOCAL_FILE, uri[7:]

        if scheme == "s3":
            return UriType.S3, uri[5:]
        elif scheme == "s3a":
            return UriType.S3A, uri[6:]

        if scheme:
            return UriType.UNKNOWN, uri[len(scheme) + 3 :]

        if os.path.isabs(uri):
            return UriType.ABSOLUTE_PATH, uri
        else:
            return UriType.RELATIVE_PATH, uri

    @staticmethod
    def is_excel_file(path: str) -> bool:
        _, ext = os.path.splitext(path)
        return ext.lower() in ALLOWED_EXTENSIONS

    @staticmethod
    def local_browser(path_spec: str) -> Iterable[BrowsePath]:
        matching_paths = glob.glob(path_spec, recursive=True)
        matching_files = [path for path in matching_paths if os.path.isfile(path)]

        for file in sorted(matching_files):
            full_path = PurePath(os.path.normpath(file)).as_posix()
            yield BrowsePath(
                file=full_path,
                timestamp=datetime.fromtimestamp(
                    os.path.getmtime(full_path), timezone.utc
                ),
                size=os.path.getsize(full_path),
                partitions=[],
            )

    def get_local_file(self, file_path: str) -> Union[BytesIO, None]:
        try:
            with open(file_path, "rb") as f:
                bytes_io = io.BytesIO(f.read())
                bytes_io.seek(0)
                return bytes_io
        except Exception as e:
            self.report.report_file_dropped(file_path)
            self.report.warning(
                message="Error reading local Excel file",
                context=f"Path={file_path}",
                exc=e,
            )
            return None

    @staticmethod
    def get_prefix(relative_path: str) -> str:
        index = re.search(r"[*|{]", relative_path)
        if index:
            return relative_path[: index.start()]
        else:
            return relative_path

    @staticmethod
    def create_s3_path(bucket_name: str, key: str) -> str:
        return f"s3://{bucket_name}/{key}"

    def create_abs_path(self, key: str) -> str:
        if self.config.azure_config:
            account_name = self.config.azure_config.account_name
            container_name = self.config.azure_config.container_name
            return (
                f"https://{account_name}.blob.core.windows.net/{container_name}/{key}"
            )
        return ""

    @staticmethod
    def strip_file_prefix(path: str) -> str:
        if path.startswith("/"):
            return path[1:]
        else:
            return path

    def s3_browser(self, path_spec: str) -> Iterable[BrowsePath]:
        if self.config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.config.aws_config.get_s3_resource(self.config.verify_ssl)
        bucket_name = get_bucket_name(path_spec)
        logger.debug(f"Scanning bucket: {bucket_name}")
        bucket = s3.Bucket(bucket_name)
        prefix = self.get_prefix(get_bucket_relative_path(path_spec))
        logger.debug(f"Scanning objects with prefix:{prefix}")

        for obj in bucket.objects.filter(Prefix=prefix).page_size(1000):
            s3_path = self.create_s3_path(obj.bucket_name, obj.key)
            logger.debug(f"Path: {s3_path}")

            yield BrowsePath(
                file=s3_path,
                timestamp=obj.last_modified,
                size=obj.size,
                partitions=[],
                content_type=None,
            )

    def get_s3_file(self, path_spec: str) -> Union[BytesIO, None]:
        if self.config.aws_config is None:
            raise ValueError("aws_config not set. Cannot browse s3")
        s3 = self.config.aws_config.get_s3_resource(self.config.verify_ssl)
        bucket_name = get_bucket_name(path_spec)
        key = get_bucket_relative_path(path_spec)
        logger.debug(f"Getting file: {key} from bucket: {bucket_name}")
        try:
            obj = s3.Object(bucket_name, key)
            file_content = obj.get()["Body"].read()
            binary_stream = io.BytesIO(file_content)
            binary_stream.seek(0)
            return binary_stream
        except Exception as e:
            self.report.report_file_dropped(path_spec)
            self.report.warning(
                message="Error reading Excel file from S3",
                context=f"Path={path_spec}",
                exc=e,
            )
            return None

    def process_s3_tags(
        self, path_spec: str, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        bucket_name = get_bucket_name(path_spec)
        key = get_bucket_relative_path(path_spec)

        s3_tags = get_s3_tags(
            bucket_name,
            key,
            dataset_urn,
            self.config.aws_config,
            self.ctx,
            self.config.use_s3_bucket_tags,
            self.config.use_s3_object_tags,
            self.config.verify_ssl,
        )

        if s3_tags:
            yield from self.process_global_tags(s3_tags, dataset_urn)

    def abs_browser(self, path_spec: str) -> Iterable[BrowsePath]:
        if self.config.azure_config is None:
            raise ValueError("azure_config not set. Cannot browse Azure Blob Storage")
        abs_blob_service_client = self.config.azure_config.get_blob_service_client()
        container_client = abs_blob_service_client.get_container_client(
            self.config.azure_config.container_name
        )

        container_name = self.config.azure_config.container_name
        logger.debug(f"Scanning container: {container_name}")

        prefix = self.get_prefix(get_container_relative_path(path_spec))
        logger.debug(f"Scanning objects with prefix: {prefix}")

        for obj in container_client.list_blobs(
            name_starts_with=f"{prefix}", results_per_page=1000
        ):
            abs_path = self.create_abs_path(obj.name)
            logger.debug(f"Path: {abs_path}")

            yield BrowsePath(
                file=abs_path,
                timestamp=obj.last_modified,
                size=obj.size,
                partitions=[],
                content_type=None,
            )

    def get_abs_file(self, path_spec: str) -> Union[BytesIO, None]:
        if self.config.azure_config is None:
            raise ValueError("azure_config not set. Cannot browse Azure Blob Storage")
        abs_blob_service_client = self.config.azure_config.get_blob_service_client()
        container_client = abs_blob_service_client.get_container_client(
            self.config.azure_config.container_name
        )

        container_name = self.config.azure_config.container_name
        blob_path = get_container_relative_path(path_spec)
        logger.debug(f"Getting file: {blob_path} from container: {container_name}")

        try:
            blob_client = container_client.get_blob_client(blob_path)
            download_stream = blob_client.download_blob()
            file_content = download_stream.readall()
            binary_stream = io.BytesIO(file_content)
            binary_stream.seek(0)
            return binary_stream
        except Exception as e:
            self.report.report_file_dropped(path_spec)
            self.report.warning(
                message="Error reading Excel file from Azure Blob Storage",
                context=f"Path={path_spec}",
                exc=e,
            )
            return None

    def process_abs_tags(
        self, path_spec: str, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        if (
            self.config.azure_config
            and self.config.azure_config.container_name is not None
        ):
            container_name = self.config.azure_config.container_name
            blob_path = get_container_relative_path(path_spec)

            abs_tags = get_abs_tags(
                container_name,
                blob_path,
                dataset_urn,
                self.config.azure_config,
                self.ctx,
                self.config.use_abs_blob_tags,
            )

            if abs_tags:
                yield from self.process_global_tags(abs_tags, dataset_urn)

    @staticmethod
    def get_field_type(field_type: str) -> SchemaFieldDataType:
        type_class = field_type_mapping.get(field_type, NullTypeClass)
        return SchemaFieldDataType(type=type_class())

    def construct_schema_field(self, f_name: str, f_type: str) -> SchemaField:
        logger.debug(f"Field: {f_name} Type: {f_type}")
        return SchemaField(
            fieldPath=f_name,
            nativeDataType=f_type,
            type=self.get_field_type(f_type),
            description=None,
            nullable=False,
            recursive=False,
        )

    def construct_schema_metadata(
        self,
        name: str,
        dataset: ExcelTable,
    ) -> SchemaMetadata:
        canonical_schema: List[SchemaField] = []

        # Get data types for each column
        data_types = dataset.df.dtypes.to_dict()

        # Convert numpy types to string representation for better readability
        data_types = {col: str(dtype) for col, dtype in data_types.items()}

        for f_name, f_type in data_types.items():
            canonical_schema.append(self.construct_schema_field(f_name, f_type))

        return SchemaMetadata(
            schemaName=name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=canonical_schema,
        )

    @staticmethod
    def get_dataset_attributes(metadata: Dict[str, Any]) -> dict:
        result = {}
        for key, value in metadata.items():
            result[key] = str(value)
        return result

    @staticmethod
    def process_global_tags(
        global_tags: GlobalTagsClass, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspect=global_tags,
            changeType=ChangeTypeClass.UPSERT,
        ).as_workunit()

    def process_dataset(
        self,
        relative_path: str,
        full_path: str,
        filename: str,
        table: ExcelTable,
        source_type: UriType,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_worksheet_processed()
        dataset_name = gen_dataset_name(
            relative_path, table.sheet_name, self.config.convert_urns_to_lowercase
        )
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        attributes = self.get_dataset_attributes(table.metadata)
        created: Optional[datetime] = table.metadata.get("created")
        modified: Optional[datetime] = table.metadata.get("modified")
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            customProperties=attributes,
            created=(
                TimeStamp(time=int(created.timestamp() * 1000)) if created else None
            ),
            lastModified=(
                TimeStamp(time=int(modified.timestamp() * 1000)) if modified else None
            ),
        )

        schema_metadata = self.construct_schema_metadata(
            name=dataset_name,
            dataset=table,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        yield from self.container_WU_creator.create_container_hierarchy(
            relative_path, dataset_urn
        )

        if source_type == UriType.S3 and (
            self.config.use_s3_bucket_tags or self.config.use_s3_object_tags
        ):
            yield from self.process_s3_tags(full_path, dataset_urn)
        elif source_type == UriType.ABS and self.config.use_abs_blob_tags:
            yield from self.process_abs_tags(full_path, dataset_urn)

        if self.config.is_profiling_enabled():
            profiler = ExcelProfiler(
                self.config,
                self.report,
                table.df,
                filename,
                table.sheet_name,
                dataset_urn,
                relative_path,
            )
            yield from profiler.get_workunits()

    def process_file(
        self,
        file_content: BytesIO,
        relative_path: str,
        full_path: str,
        filename: str,
        source_type: UriType,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_file_processed()
        xls = ExcelFile(filename, file_content, self.report)
        result = xls.load_workbook()

        if result:
            for table in xls.get_tables(active_only=self.config.active_sheet_only):
                self.report.report_worksheet_scanned()
                dataset_name = gen_dataset_name(
                    relative_path,
                    table.sheet_name,
                    self.config.convert_urns_to_lowercase,
                )
                if not self.config.worksheet_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                yield from self.process_dataset(
                    relative_path, full_path, filename, table, source_type
                )

    def check_file_is_valid(self, filename: str) -> bool:
        self.report.report_file_scanned()
        if not self.config.path_pattern.allowed(filename):
            self.report.report_dropped(filename)
            return False
        elif not self.is_excel_file(filename):
            logger.debug(f"File is not an Excel workbook: {filename}")
            return False
        return True

    def retrieve_file_data(
        self, uri_type: UriType, path: str, path_spec: str
    ) -> Iterator[Tuple[BytesIO, str, str, str]]:
        if (
            uri_type == UriType.LOCAL_FILE
            or uri_type == UriType.ABSOLUTE_PATH
            or uri_type == UriType.RELATIVE_PATH
        ):
            logger.debug(f"Searching local path: {path}")
            for browse_path in self.local_browser(path):
                if self.check_file_is_valid(browse_path.file):
                    basename = os.path.basename(browse_path.file)
                    file_path = self.strip_file_prefix(browse_path.file)
                    filename = os.path.splitext(basename)[0]

                    logger.debug(f"Processing {filename}")
                    with self.report.local_file_get_timer:
                        file_data = self.get_local_file(browse_path.file)

                    if file_data is not None:
                        yield file_data, file_path, browse_path.file, filename

        elif uri_type == UriType.S3:
            logger.debug(f"Searching S3 path: {path}")
            for browse_path in self.s3_browser(path_spec):
                if self.check_file_is_valid(browse_path.file):
                    uri_path = strip_s3_prefix(browse_path.file)
                    basename = os.path.basename(uri_path)
                    filename = os.path.splitext(basename)[0]

                    logger.debug(f"Processing {browse_path.file}")
                    with self.report.s3_file_get_timer:
                        file_data = self.get_s3_file(browse_path.file)

                    if file_data is not None:
                        yield file_data, uri_path, browse_path.file, filename

        elif uri_type == UriType.ABS:
            logger.debug(f"Searching Azure Blob Storage path: {path}")
            for browse_path in self.abs_browser(path_spec):
                if self.check_file_is_valid(browse_path.file):
                    uri_path = strip_abs_prefix(browse_path.file)
                    basename = os.path.basename(uri_path)
                    filename = os.path.splitext(basename)[0]

                    logger.debug(f"Processing {browse_path.file}")
                    with self.report.abs_file_get_timer:
                        file_data = self.get_abs_file(browse_path.file)

                    if file_data is not None:
                        yield file_data, uri_path, browse_path.file, filename

        else:
            self.report.report_file_dropped(path_spec)
            self.report.warning(
                message="Unsupported URI Type",
                context=f"Type={uri_type.name},URI={path_spec}",
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

        with PerfTimer() as timer:
            for path_spec in self.config.path_list:
                logger.debug(f"Processing path: {path_spec}")
                uri_type, path = self.uri_type(path_spec)
                logger.debug(f"URI Type: {uri_type} Path: {path}")

                for (
                    file_data,
                    relative_path,
                    full_path,
                    filename,
                ) in self.retrieve_file_data(uri_type, path, path_spec):
                    yield from self.process_file(
                        file_data, relative_path, full_path, filename, uri_type
                    )

        time_taken = timer.elapsed_seconds()
        logger.info(f"Finished ingestion in {time_taken:.3f} seconds")

    def get_report(self):
        return self.report
