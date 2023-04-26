import logging
from typing import Dict, Iterable, List
from urllib.parse import unquote

from pydantic import Field, SecretStr, validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common.config import PathSpecsConfigMixin
from datahub.ingestion.source.data_lake_common.data_lake_utils import PLATFORM_GCS
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec, is_gcs_uri
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.source import S3Source
from datahub.utilities.source_helpers import auto_status_aspect, auto_workunit_reporter

logger: logging.Logger = logging.getLogger(__name__)


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


class GCSSourceConfig(DatasetSourceConfigMixin, PathSpecsConfigMixin):
    credential: HMACKey = Field(
        description="Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys)",
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

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


class GCSSourceReport(SourceReport):
    pass


@platform_name("Google Cloud Storage", id=PLATFORM_GCS)
@config_class(GCSSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Not supported", supported=False)
class GCSSource(Source):
    """
    This connector extracting datasets located on Google Cloud Storage. Supported file types are as follows:

    - CSV
    - TSV
    - JSON
    - Parquet
    - Apache Avro

    Schemas for Parquet and Avro files are extracted as provided.

    Schemas for schemaless formats (CSV, TSV, JSON) are inferred. For CSV and TSV files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
    JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.


    This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
    and uses DataHub S3 Data Lake integration source under the hood.

    ### Prerequisites
    1. Create a service account with "Storage Object Viewer" Role - https://cloud.google.com/iam/docs/service-accounts-create
    2. Make sure you meet following requirements to generate HMAC key - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin
    3. Create an HMAC key for service account created above - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create .
    """

    def __init__(self, config: GCSSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = GCSSourceReport()
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
        )
        return s3_config

    def create_equivalent_s3_path_specs(self):
        s3_path_specs = []
        for path_spec in self.config.path_specs:
            s3_path_specs.append(
                PathSpec(
                    include=path_spec.include.replace("gs://", "s3://"),
                    exclude=[exc.replace("gs://", "s3://") for exc in path_spec.exclude]
                    if path_spec.exclude
                    else None,
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
        return self.s3_source_overrides(S3Source(config, ctx))

    def s3_source_overrides(self, source: S3Source) -> S3Source:

        source.source_config.platform = PLATFORM_GCS

        source.is_s3_platform = lambda: True  # type: ignore
        source.create_s3_path = lambda bucket_name, key: unquote(f"s3://{bucket_name}/{key}")  # type: ignore

        return source

    def get_workunits(self) -> Iterable[WorkUnit]:
        yield from auto_workunit_reporter(
            self.report, auto_status_aspect(self.s3_source.get_workunits())
        )

    def get_report(self):
        return self.report
