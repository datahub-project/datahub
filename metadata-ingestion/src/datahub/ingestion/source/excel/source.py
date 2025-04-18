import glob
import io
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Any, Dict, Iterable, List, Optional, Type

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
)
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
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
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
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
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


class HDF5ContainerKey(ContainerKey):
    path: str


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
        config = ExcelSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    @staticmethod
    def local_browser(path_spec: str) -> Iterable[BrowsePath]:
        # Use glob to find all paths matching the pattern
        matching_paths = glob.glob(path_spec, recursive=True)

        # Filter to include only files (not directories)
        matching_files = [path for path in matching_paths if os.path.isfile(path)]

        for file in sorted(matching_files):
            # We need to make sure the path is in posix style that is not true on windows
            full_path = PurePath(os.path.normpath(file)).as_posix()
            yield BrowsePath(
                file=full_path,
                timestamp=datetime.fromtimestamp(
                    os.path.getmtime(full_path), timezone.utc
                ),
                size=os.path.getsize(full_path),
                partitions=[],
            )

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

            content_type = None
            if self.config.use_s3_content_type:
                content_type = s3.Object(obj.bucket_name, obj.key).content_type

            yield BrowsePath(
                file=s3_path,
                timestamp=obj.last_modified,
                size=obj.size,
                partitions=[],
                content_type=content_type,
            )

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

    def process_dataset(
        self, path: str, filename: str, table: ExcelTable
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = gen_dataset_name(
            filename, table.sheet_name, self.config.convert_urns_to_lowercase
        )
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        attributes = self.get_dataset_attributes(table.metadata)
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            customProperties=attributes,
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
            path, dataset_urn
        )

        if self.config.is_profiling_enabled():
            profiler = ExcelProfiler(
                self.config,
                self.report,
                table.df,
                filename,
                table.sheet_name,
                dataset_urn,
                path,
            )
            yield from profiler.get_workunits()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

        with PerfTimer() as timer:
            for path_spec in self.config.path_list:
                for browse_path in self.local_browser(path_spec):
                    if not self.config.path_pattern.allowed(browse_path.file):
                        self.report.report_dropped(browse_path.file)
                        continue
                    basename = os.path.basename(browse_path.file)
                    path = os.path.dirname(browse_path.file)
                    filename = os.path.splitext(basename)[0]
                    logger.debug(f"Processing {filename}")
                    with open(browse_path.file, "rb") as f:
                        file_content = f.read()
                    bytes_io = io.BytesIO(file_content)
                    xls = ExcelFile(bytes_io)
                    for table in xls.get_tables():
                        yield from self.process_dataset(path, filename, table)

            time_taken = timer.elapsed_seconds()

            logger.info(f"Finished ingestion; took {time_taken:.3f} seconds")

    def get_report(self):
        return self.report
