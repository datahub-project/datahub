import json
import logging
import os
from enum import Enum
from typing import Dict, Iterable, List, Optional, Union

from google.auth import external_account
from google.auth.transport.requests import Request
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
from datahub.ingestion.source.data_lake_common.object_store import (
    create_object_store_adapter,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec, is_gcs_uri
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.s3.source import S3Source
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)

logger: logging.Logger = logging.getLogger(__name__)


class GCSAuthType(str, Enum):
    HMAC = "hmac"
    WORKLOAD_IDENTITY_FEDERATION = "workload_identity_federation"


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


class GCSSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, PathSpecsConfigMixin
):
    auth_type: GCSAuthType = Field(
        default=GCSAuthType.HMAC,
        description="Authentication type to use. Defaults to HMAC keys. Set to 'workload_identity_federation' to use Workload Identity Federation.",
    )

    credential: Optional[HMACKey] = Field(
        default=None,
        description="Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys). Required when auth_type is 'hmac'.",
    )

    gcp_wif_configuration: Optional[str] = Field(
        default=None,
        description="Path to the Google Cloud Workload Identity Federation configuration JSON file. Required when auth_type is 'workload_identity_federation' and gcp_wif_configuration_json is not provided.",
    )

    gcp_wif_configuration_json: Optional[Union[str, Dict]] = Field(
        default=None,
        description="Google Cloud Workload Identity Federation configuration as JSON string or dict. Alternative to gcp_wif_configuration file path. Required when auth_type is 'workload_identity_federation' and gcp_wif_configuration is not provided.",
    )

    gcp_wif_configuration_json_string: Optional[str] = Field(
        default=None,
        description="Google Cloud Workload Identity Federation configuration as a JSON string (contents of the configuration file). Useful for copying configuration from files into secrets. Alternative to gcp_wif_configuration file path. Required when auth_type is 'workload_identity_federation' and other gcp_wif_configuration options are not provided.",
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    number_of_files_to_sample: int = Field(
        default=100,
        description="Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("credential", always=True)
    def validate_credential(cls, v, values):
        auth_type = values.get("auth_type", GCSAuthType.HMAC)
        if auth_type == GCSAuthType.HMAC and v is None:
            raise ValueError("credential is required when auth_type is 'hmac'")
        return v

    @validator("gcp_wif_configuration_json_string", always=True)
    def validate_gcp_wif_configuration_options(cls, v, values):
        auth_type = values.get("auth_type", GCSAuthType.HMAC)
        gcp_wif_configuration = values.get("gcp_wif_configuration")
        gcp_wif_configuration_json = values.get("gcp_wif_configuration_json")

        if auth_type == GCSAuthType.WORKLOAD_IDENTITY_FEDERATION:
            wif_options = [gcp_wif_configuration, gcp_wif_configuration_json, v]
            provided_options = [opt for opt in wif_options if opt is not None]

            if len(provided_options) == 0:
                raise ValueError(
                    "One of gcp_wif_configuration (file path), gcp_wif_configuration_json (JSON content), "
                    "or gcp_wif_configuration_json_string (JSON string) is required when auth_type is 'workload_identity_federation'"
                )
            elif len(provided_options) > 1:
                raise ValueError(
                    "Cannot specify multiple WIF configuration options. Use only one of: "
                    "gcp_wif_configuration, gcp_wif_configuration_json, or gcp_wif_configuration_json_string."
                )

        # Validate JSON format for both JSON options
        if gcp_wif_configuration_json:
            if isinstance(gcp_wif_configuration_json, str):
                try:
                    json.loads(gcp_wif_configuration_json)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"gcp_wif_configuration_json must be valid JSON: {e}"
                    ) from e
            elif not isinstance(gcp_wif_configuration_json, dict):
                raise ValueError(
                    "gcp_wif_configuration_json must be either a JSON string or a dictionary"
                )

        if v:
            try:
                json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"gcp_wif_configuration_json_string must be valid JSON: {e}"
                ) from e

        return v

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


class GCSSourceReport(DataLakeSourceReport):
    pass


@platform_name("Google Cloud Storage", id=PLATFORM_GCS)
@config_class(GCSSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Not supported", supported=False)
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

    def _setup_wif_credentials(self) -> None:
        """Set up Workload Identity Federation credentials using Google Auth library."""

        # Parse the WIF configuration
        wif_config_dict = None

        if self.config.gcp_wif_configuration:
            # Read from file
            try:
                with open(self.config.gcp_wif_configuration, "r") as f:
                    wif_config_dict = json.load(f)
                logger.info(
                    "Using Workload Identity Federation configuration from file: %s",
                    self.config.gcp_wif_configuration,
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to read WIF configuration file {self.config.gcp_wif_configuration}: {e}"
                ) from e
        elif self.config.gcp_wif_configuration_json:
            # Use the JSON content directly
            if isinstance(self.config.gcp_wif_configuration_json, dict):
                wif_config_dict = self.config.gcp_wif_configuration_json
            else:
                # It's a JSON string (validated in the validator)
                wif_config_dict = json.loads(self.config.gcp_wif_configuration_json)
            logger.info(
                "Using Workload Identity Federation configuration from JSON content"
            )
        elif self.config.gcp_wif_configuration_json_string:
            # Parse the JSON string (validated in the validator)
            wif_config_dict = json.loads(self.config.gcp_wif_configuration_json_string)
            logger.info(
                "Using Workload Identity Federation configuration from JSON string"
            )

        if wif_config_dict is None:
            raise ValueError("No valid WIF configuration provided")

        # Create credentials using Google Auth library
        try:
            credentials = external_account.Credentials.from_info(wif_config_dict)
            credentials.refresh(Request())

            # Set the credentials in the environment for the GCS client libraries
            # The Google Cloud client libraries will automatically use these credentials
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
                "workload_identity_federation"
            )

            logger.info("Successfully set up Workload Identity Federation credentials")

        except Exception as e:
            raise ValueError(
                f"Failed to setup Workload Identity Federation credentials: {e}"
            ) from e

    def create_equivalent_s3_config(self):
        s3_path_specs = self.create_equivalent_s3_path_specs()

        if self.config.auth_type == GCSAuthType.HMAC:
            if not self.config.credential:
                raise ValueError(
                    "HMAC credentials are required when auth_type is 'hmac'"
                )

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
            )
        else:  # workload_identity_federation
            wif_options = [
                self.config.gcp_wif_configuration,
                self.config.gcp_wif_configuration_json,
                self.config.gcp_wif_configuration_json_string,
            ]
            if not any(wif_options):
                raise ValueError(
                    "One of gcp_wif_configuration, gcp_wif_configuration_json, or gcp_wif_configuration_json_string is required when auth_type is 'workload_identity_federation'"
                )

            # For workload identity federation, we don't use HMAC credentials
            # The authentication will be handled by the Google Cloud client libraries
            # using the GOOGLE_APPLICATION_CREDENTIALS environment variable
            self._setup_wif_credentials()

            s3_config = DataLakeSourceConfig(
                path_specs=s3_path_specs,
                aws_config=AwsConnectionConfig(
                    aws_endpoint_url="https://storage.googleapis.com",
                    aws_region="auto",
                ),
                env=self.config.env,
                max_rows=self.config.max_rows,
                number_of_files_to_sample=self.config.number_of_files_to_sample,
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
        s3_source = S3Source(config, PipelineContext(ctx.run_id))
        return self.s3_source_overrides(s3_source)

    def s3_source_overrides(self, source: S3Source) -> S3Source:
        """
        Override S3Source methods with GCS-specific implementations using the adapter pattern.

        This method customizes the S3Source instance to behave like a GCS source by
        applying the GCS-specific adapter that replaces the necessary functionality.

        Args:
            source: The S3Source instance to customize

        Returns:
            The modified S3Source instance with GCS behavior
        """
        # Create a GCS adapter with project ID and region from our config
        adapter = create_object_store_adapter(
            "gcs",
        )

        # Apply all customizations to the source
        return adapter.apply_customizations(source)

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

    def close(self) -> None:
        """Clean up resources when the source is closed."""
        super().close()

    def __del__(self):
        """Destructor to ensure cleanup even if close() is not called explicitly."""
        # No cleanup needed since we use Google Auth library directly
        pass
