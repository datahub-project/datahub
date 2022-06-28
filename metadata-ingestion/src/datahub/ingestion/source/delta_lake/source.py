import logging
import os
from typing import Callable, Iterable, List

from deltalake import DeltaTable

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext, WorkUnit
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
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_key_prefix,
    get_s3_tags,
    list_folders_path,
    strip_s3_prefix,
)
from datahub.ingestion.source.data_lake.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.delta_lake_utils import (
    get_file_count,
    read_delta_table,
)
from datahub.ingestion.source.delta_lake.report import DeltaLakeSourceReport
from datahub.ingestion.source.schema_inference.csv_tsv import tableschema_type_map
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    NullTypeClass,
    OtherSchemaClass,
)
from datahub.telemetry import telemetry

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

config_options_to_report = [
    "platform",
]


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

    def __init__(self, config: DeltaLakeSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = DeltaLakeSourceReport()
        # self.profiling_times_taken = []
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }
        config_report = config_report

        telemetry.telemetry_instance.ping(
            "delta_lake_config",
            config_report,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = DeltaLakeSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_fields(self, delta_table: DeltaTable) -> List[SchemaField]:

        fields: List[SchemaField] = []

        for raw_field in delta_table.schema().fields:
            field = SchemaField(
                fieldPath=raw_field.name,
                type=SchemaFieldDataType(
                    tableschema_type_map.get(raw_field.type.type, NullTypeClass)()
                ),
                nativeDataType=raw_field.type.type,
                recursive=False,
                nullable=raw_field.nullable,
                description=str(raw_field.metadata),
                isPartitioningKey=True
                if raw_field.name in delta_table.metadata().partition_columns
                else False,
            )
            fields.append(field)
        fields = sorted(fields, key=lambda f: f.fieldPath)

        return fields

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
                strip_s3_prefix(path) if self.source_config.is_s3() else path.strip("/")
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
            aspects=[],
        )

        customProperties = {
            "number_of_files": str(get_file_count(delta_table)),
            "partition_columns": str(delta_table.metadata().partition_columns),
            "table_creation_time": str(delta_table.metadata().created_time),
            "id": str(delta_table.metadata().id),
            "version": str(delta_table.version()),
            "location": self.source_config.get_complete_path(),
        }
        customProperties.update(delta_table.history()[-1])
        customProperties["version_creation_time"] = customProperties["timestamp"]
        del customProperties["timestamp"]
        for key in customProperties.keys():
            customProperties[key] = str(customProperties[key])

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
            self.source_config.is_s3()
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
        wu = MetadataWorkUnit(id=delta_table.metadata().id, mce=mce)
        self.report.report_workunit(wu)
        yield wu

        container_wus = self.container_WU_creator.create_container_hierarchy(
            browse_path, self.source_config.is_s3(), dataset_urn
        )
        for wu in container_wus:
            self.report.report_workunit(wu)
            yield wu

    def process_folder(
        self, path: str, get_folders: Callable[[str], Iterable[str]]
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Processing folder: {path}")
        delta_table = read_delta_table(path, self.source_config)
        if delta_table:
            logger.debug(f"Delta table found at: {path}")
            for wu in self.ingest_table(delta_table, path):
                yield wu
        else:
            for folder in get_folders(path):
                yield from self.process_folder(path + "/" + folder, get_folders)

    def s3_get_folders(self, path: str) -> Iterable[str]:
        if self.source_config.s3 is not None:
            yield from list_folders_path(path, self.source_config.s3.aws_config)

    def local_get_folders(self, path: str) -> Iterable[str]:
        if not os.path.isdir(path):
            raise Exception(
                f"{path} does not exist. Please check base_path configuration."
            )
        for _, folders, _ in os.walk(path):
            for folder in folders:
                yield folder
            break
        return

    def get_workunits(self) -> Iterable[WorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        get_folders = (
            self.s3_get_folders
            if self.source_config.is_s3()
            else self.local_get_folders
        )
        for wu in self.process_folder(
            self.source_config.get_complete_path(), get_folders
        ):
            yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass
