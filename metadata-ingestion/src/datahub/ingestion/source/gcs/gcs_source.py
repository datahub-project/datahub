import logging
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import unquote

from pandas import DataFrame
from pydantic import Field, SecretStr, validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
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
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common.config import PathSpecsConfigMixin
from datahub.ingestion.source.data_lake_common.data_lake_utils import PLATFORM_GCS
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec, is_gcs_uri
from datahub.ingestion.source.gcs.gcs_utils import (
    get_gcs_bucket_name,
    get_gcs_bucket_relative_path,
)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.s3.source import S3Source, TableData
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)

logger: logging.Logger = logging.getLogger(__name__)


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


class GCSSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, PathSpecsConfigMixin
):
    credential: HMACKey = Field(
        description="Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys)",
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    number_of_files_to_sample: int = Field(
        default=100,
        description="Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
    )

    profiling: Optional[DataLakeProfilerConfig] = Field(
        default=DataLakeProfilerConfig(), description="Data profiling configuration"
    )

    spark_driver_memory: str = Field(
        default="4g", description="Max amount of memory to grant Spark."
    )

    spark_config: Dict[str, Any] = Field(
        description="Spark configuration properties",
        default={},
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("path_specs", always=True)
    def check_path_specs_and_infer_platform(
        cls, path_specs: List[PathSpec], values: Dict
    ) -> List[PathSpec]:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")

        # Check that all path specs have the gs:// prefix.
        if any([not is_gcs_uri(path_spec.include) for path_spec in path_specs]):
            raise ValueError("All path_spec.include should start with gs://")

        return path_specs

    def is_profiling_enabled(self) -> bool:
        return self.profiling is not None and self.profiling.enabled


class GCSSourceReport(DataLakeSourceReport):
    pass


@platform_name("Google Cloud Storage", id=PLATFORM_GCS)
@config_class(GCSSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Enabled via configuration")
class GCSSource(StatefulIngestionSourceBase):
    def __init__(self, config: GCSSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = GCSSourceReport()
        self.platform: str = PLATFORM_GCS
        self.s3_source = self.create_equivalent_s3_source(ctx)

    @classmethod
    def create(cls, config_dict, ctx):
        config = GCSSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def create_equivalent_s3_config(self):
        s3_path_specs = self.create_equivalent_s3_path_specs()

        s3_config = DataLakeSourceConfig(
            path_specs=s3_path_specs,
            aws_config=AwsConnectionConfig(
                aws_endpoint_url="https://storage.googleapis.com",
                aws_access_key_id=self.config.credential.hmac_access_id,
                aws_secret_access_key=self.config.credential.hmac_access_secret.get_secret_value(),
                aws_region="auto",
            ),
            env=self.config.env,
            max_rows=self.config.max_rows,
            number_of_files_to_sample=self.config.number_of_files_to_sample,
            profiling=self.config.profiling,
            spark_driver_memory=self.config.spark_driver_memory,
            spark_config=self.config.spark_config,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
        )
        return s3_config

    def create_equivalent_s3_path_specs(self):
        s3_path_specs = []
        for path_spec in self.config.path_specs:
            s3_path_specs.append(
                PathSpec(
                    include=path_spec.include.replace("gs://", "s3://"),
                    exclude=(
                        [exc.replace("gs://", "s3://") for exc in path_spec.exclude]
                        if path_spec.exclude
                        else None
                    ),
                    file_types=path_spec.file_types,
                    default_extension=path_spec.default_extension,
                    table_name=path_spec.table_name,
                    enable_compression=path_spec.enable_compression,
                    sample_files=path_spec.sample_files,
                )
            )

        return s3_path_specs

    def create_equivalent_s3_source(self, ctx: PipelineContext) -> S3Source:
        config = self.create_equivalent_s3_config()
        return self.s3_source_overrides(S3Source(config, PipelineContext(ctx.run_id)))

    def s3_source_overrides(self, source: S3Source) -> S3Source:
        source.source_config.platform = PLATFORM_GCS

        source.is_s3_platform = lambda: True  # type: ignore
        source.create_s3_path = lambda bucket_name, key: unquote(  # type: ignore
            f"s3://{bucket_name}/{key}"
        )

        if self.config.is_profiling_enabled():
            original_read_file_spark = source.read_file_spark

            from types import MethodType

            def read_file_spark_with_gcs(
                self_source: S3Source, file: str, ext: str
            ) -> Optional[DataFrame]:
                # Convert s3:// path back to gs:// for Spark
                if file.startswith("s3://"):
                    file = f"gs://{file[5:]}"
                return original_read_file_spark(file, ext)

            source.read_file_spark = MethodType(read_file_spark_with_gcs, source)  # type: ignore

        def get_external_url_override(table_data: TableData) -> Optional[str]:
            bucket_name = get_gcs_bucket_name(table_data.table_path)
            key_prefix = get_gcs_bucket_relative_path(table_data.table_path)
            return f"https://console.cloud.google.com/storage/browser/{bucket_name}/{key_prefix}"

        source.get_external_url = get_external_url_override  # type: ignore
        return source

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        return self.s3_source.get_workunits_internal()

    def get_report(self):
        return self.report
