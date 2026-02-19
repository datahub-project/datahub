import json
import logging
import os
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from google.auth import load_credentials_from_file
from google.auth.transport.requests import Request
from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
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
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
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

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client, S3ServiceResource

logger: logging.Logger = logging.getLogger(__name__)

GCS_ENDPOINT_URL = "https://storage.googleapis.com"

# S3 API operations used by the S3 source when browsing/reading GCS
_GCS_OAUTH_S3_OPERATIONS = (
    "ListBuckets",
    "ListObjectsV2",
    "ListObjects",
    "GetObject",
    "HeadObject",
    "GetObjectTagging",
    "GetBucketTagging",
)


def _register_gcs_oauth_before_send(
    client: Any,
    credentials: Any,
    project_id: Optional[str],
) -> None:
    """
    Register a before-send handler on the boto3 S3 client to inject
    Authorization: Bearer <token> for GCS (Workload Identity Federation).
    boto3 uses SigV4 by default; GCS XML API accepts Bearer tokens instead.
    """

    def inject_bearer(request: Any, **kwargs: Any) -> None:
        import time

        need_refresh = not getattr(credentials, "token", None)
        if not need_refresh and getattr(credentials, "expiry", None) is not None:
            need_refresh = credentials.expiry.timestamp() < time.time()
        if need_refresh:
            credentials.refresh(Request())
        request.headers["Authorization"] = f"Bearer {credentials.token}"
        if project_id:
            request.headers["x-goog-project-id"] = project_id

    for op in _GCS_OAUTH_S3_OPERATIONS:
        client.meta.events.register(f"before-send.s3.{op}", inject_bearer)


class GCSOAuthAwsConnectionConfig(AwsConnectionConfig):
    """
    AwsConnectionConfig-compatible wrapper for GCS with Workload Identity Federation.
    Uses dummy AWS-style keys so boto3 creates a session; before-send handlers
    replace the Authorization header with Bearer <token> from WIF credentials.
    Only used when auth_type is workload_identity_federation.
    """

    def get_s3_client(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3Client":
        client = super().get_s3_client(verify_ssl=verify_ssl)
        creds = getattr(self, "_gcs_oauth_credentials", None)
        project_id = getattr(self, "_gcs_oauth_project_id", None)
        if creds is not None:
            _register_gcs_oauth_before_send(client, creds, project_id)
        return client

    def get_s3_resource(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3ServiceResource":
        resource = super().get_s3_resource(verify_ssl=verify_ssl)
        creds = getattr(self, "_gcs_oauth_credentials", None)
        project_id = getattr(self, "_gcs_oauth_project_id", None)
        if creds is not None:
            _register_gcs_oauth_before_send(resource.meta.client, creds, project_id)
        return resource


class GCSAuthType(str, Enum):
    HMAC = "hmac"
    WORKLOAD_IDENTITY_FEDERATION = "workload_identity_federation"


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


class GCSSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    PathSpecsConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
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

    @model_validator(mode="before")
    @classmethod
    def validate_credential(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(values, dict):
            auth_type = values.get("auth_type", GCSAuthType.HMAC)
            credential = values.get("credential")
            if auth_type == GCSAuthType.HMAC and credential is None:
                raise ValueError("credential is required when auth_type is 'hmac'")
        return values

    @model_validator(mode="before")
    @classmethod
    def validate_gcp_wif_configuration_options(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        if isinstance(values, dict):
            auth_type = values.get("auth_type", GCSAuthType.HMAC)
            gcp_wif_configuration = values.get("gcp_wif_configuration")
            gcp_wif_configuration_json = values.get("gcp_wif_configuration_json")
            gcp_wif_configuration_json_string = values.get(
                "gcp_wif_configuration_json_string"
            )

            if auth_type == GCSAuthType.WORKLOAD_IDENTITY_FEDERATION:
                wif_options = [
                    gcp_wif_configuration,
                    gcp_wif_configuration_json,
                    gcp_wif_configuration_json_string,
                ]
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

            if gcp_wif_configuration_json_string:
                try:
                    json.loads(gcp_wif_configuration_json_string)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"gcp_wif_configuration_json_string must be valid JSON: {e}"
                    ) from e

        return values

    @field_validator("path_specs", mode="after")
    @classmethod
    def check_path_specs_and_infer_platform(
        cls, path_specs: List[PathSpec]
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
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.GCS_BUCKET,
        SourceCapabilityModifier.FOLDER,
    ],
)
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
        config = GCSSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _setup_wif_credentials(self) -> None:
        """Set up Workload Identity Federation credentials using Google Auth library."""
        import tempfile

        # Convert all formats to a file path (load_credentials_from_file works reliably)
        wif_config_file = None

        if self.config.gcp_wif_configuration:
            wif_config_file = self.config.gcp_wif_configuration
            logger.info(
                "Using Workload Identity Federation configuration from file: %s",
                wif_config_file,
            )
        elif self.config.gcp_wif_configuration_json:
            # Write dict/string to temp file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                if isinstance(self.config.gcp_wif_configuration_json, dict):
                    json.dump(self.config.gcp_wif_configuration_json, f)
                else:
                    f.write(self.config.gcp_wif_configuration_json)
                wif_config_file = f.name
            logger.info(
                "Using Workload Identity Federation configuration from JSON content"
            )
        elif self.config.gcp_wif_configuration_json_string:
            # Write string to temp file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                f.write(self.config.gcp_wif_configuration_json_string)
                wif_config_file = f.name
            logger.info(
                "Using Workload Identity Federation configuration from JSON string"
            )
        else:
            raise ValueError("No valid WIF configuration provided")

        # Load credentials from file (this method works correctly)
        try:
            credentials, project_id = load_credentials_from_file(wif_config_file)
            # Impersonation (WIF → SA) requires scopes; otherwise IAM returns 400 "Scope required."
            credentials = credentials.with_scopes(
                ["https://www.googleapis.com/auth/cloud-platform"]
            )

            # Try to refresh credentials to validate they work
            # If refresh fails, log a warning but continue - the GCS client libraries
            # will handle the refresh when they actually need to make API calls
            try:
                credentials.refresh(Request())
                logger.debug("Successfully refreshed WIF credentials")
            except Exception as refresh_error:
                logger.warning(
                    "Failed to refresh WIF credentials during setup (this may be expected): %s",
                    refresh_error,
                )
                logger.info(
                    "Credentials will be refreshed automatically when GCS client libraries need them"
                )

            # Set environment variable for GCS client libraries
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = wif_config_file

            # For boto3 to work with GCS, we need to provide credentials
            # Since WIF provides OAuth2 credentials (not HMAC keys), we need to
            # get an access token and configure boto3 to use it
            # Store the credentials object so we can use it later if needed
            # Note: boto3 doesn't natively support OAuth2 tokens, so this is a limitation
            # The S3 source will need to be modified to handle WIF credentials properly
            self._wif_credentials = credentials
            self._wif_project_id = project_id

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

            # For workload identity federation, we don't use HMAC credentials.
            # Use a GCS OAuth config that injects Bearer token into boto3 requests.
            self._setup_wif_credentials()

            aws_config = GCSOAuthAwsConnectionConfig(
                aws_endpoint_url=GCS_ENDPOINT_URL,
                aws_region="auto",
                aws_access_key_id="gcs-oauth",
                aws_secret_access_key="not-used",
            )
            object.__setattr__(
                aws_config, "_gcs_oauth_credentials", self._wif_credentials
            )
            object.__setattr__(
                aws_config, "_gcs_oauth_project_id", self._wif_project_id
            )

            s3_config = DataLakeSourceConfig(
                path_specs=s3_path_specs,
                aws_config=aws_config,
                env=self.config.env,
                max_rows=self.config.max_rows,
                number_of_files_to_sample=self.config.number_of_files_to_sample,
            )

        return s3_config

    def create_equivalent_s3_path_specs(self):
        s3_path_specs = []
        for path_spec in self.config.path_specs:
            # PathSpec modifies the passed-in include to add /** to the end if
            # autodetecting partitions. Remove that, otherwise creating a new
            # PathSpec will complain.
            # TODO: this should be handled inside PathSpec, which probably shouldn't
            # modify its input.
            include = path_spec.include
            if include.endswith("{table}/**") and not path_spec.allow_double_stars:
                include = include.removesuffix("**")

            s3_path_specs.append(
                PathSpec(
                    include=include.replace("gs://", "s3://"),
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
                    allow_double_stars=path_spec.allow_double_stars,
                    autodetect_partitions=path_spec.autodetect_partitions,
                    include_hidden_folders=path_spec.include_hidden_folders,
                    tables_filter_pattern=path_spec.tables_filter_pattern,
                    traversal_method=path_spec.traversal_method,
                )
            )

        return s3_path_specs

    def create_equivalent_s3_source(self, ctx: PipelineContext) -> S3Source:
        config = self.create_equivalent_s3_config()
        # Create a new context for S3 source without graph to avoid duplicate checkpointer registration
        s3_ctx = PipelineContext(run_id=ctx.run_id, pipeline_name=ctx.pipeline_name)
        s3_source = S3Source(config, s3_ctx)
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
