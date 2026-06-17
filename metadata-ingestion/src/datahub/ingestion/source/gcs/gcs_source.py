import logging
import time
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Union

import google.auth
import google.auth.exceptions
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from pydantic import Field, PrivateAttr, SecretStr, field_validator, model_validator

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
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.common.gcp_wif_config import (
    GCPWIFConfig,
    load_wif_credentials,
)
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
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.utilities.str_enum import StrEnum

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client, S3ServiceResource

logger: logging.Logger = logging.getLogger(__name__)

GCS_ENDPOINT_URL = "https://storage.googleapis.com"

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
    credentials: Credentials,
    project_id: Optional[str],
) -> None:
    # boto3 uses SigV4 by default; GCS XML API accepts Bearer tokens instead.

    def inject_bearer(request: Any, **kwargs: Any) -> None:
        need_refresh = not getattr(credentials, "token", None)
        expiry = getattr(credentials, "expiry", None)
        if not need_refresh and expiry is not None:
            need_refresh = expiry.timestamp() < time.time()
        if need_refresh:
            credentials.refresh(Request())
        request.headers["Authorization"] = f"Bearer {credentials.token}"
        if project_id:
            request.headers["x-goog-project-id"] = project_id

    for op in _GCS_OAUTH_S3_OPERATIONS:
        client.meta.events.register(f"before-send.s3.{op}", inject_bearer)


class GCSOAuthAwsConnectionConfig(AwsConnectionConfig):
    # Shared by both OAuth-based GCS auth types: workload_identity_federation (WIF)
    # and workload_identity (Application Default Credentials). Uses dummy AWS-style
    # keys so boto3 creates a session; before-send handlers then replace the
    # Authorization header with Bearer <token> from the GCP credentials.

    _gcs_oauth_credentials: Optional[Credentials] = PrivateAttr(default=None)
    _gcs_oauth_project_id: Optional[str] = PrivateAttr(default=None)

    def _apply_oauth(self, boto3_client: Any) -> None:
        creds = self._gcs_oauth_credentials
        # Explicit raise (not assert) so the fail-fast guarantee survives `python -O`,
        # which strips asserts. Missing creds would otherwise produce opaque 403s.
        if creds is None:
            raise RuntimeError(
                "_gcs_oauth_credentials must be set before calling get_s3_client/get_s3_resource"
            )
        _register_gcs_oauth_before_send(boto3_client, creds, self._gcs_oauth_project_id)

    def get_s3_client(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3Client":
        client = super().get_s3_client(verify_ssl=verify_ssl)
        self._apply_oauth(client)
        return client

    def get_s3_resource(
        self, verify_ssl: Optional[Union[bool, str]] = None
    ) -> "S3ServiceResource":
        resource = super().get_s3_resource(verify_ssl=verify_ssl)
        self._apply_oauth(resource.meta.client)
        return resource


class GCSAuthType(StrEnum):
    HMAC = "hmac"
    WORKLOAD_IDENTITY_FEDERATION = "workload_identity_federation"
    WORKLOAD_IDENTITY = "workload_identity"


class HMACKey(ConfigModel):
    hmac_access_id: str = Field(description="Access ID")
    hmac_access_secret: SecretStr = Field(description="Secret")


class GCSSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    GCPWIFConfig,
    PathSpecsConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    auth_type: GCSAuthType = Field(
        default=GCSAuthType.HMAC,
        description=(
            "Authentication type to use. Defaults to 'hmac'. "
            "Set to 'workload_identity_federation' to authenticate via a WIF configuration file (for external workloads). "
            "Set to 'workload_identity' to use Application Default Credentials — the recommended option when running inside GKE with Workload Identity enabled."
        ),
    )

    credential: Optional[HMACKey] = Field(
        default=None,
        description="Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys). Required when auth_type is 'hmac'.",
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
    def validate_credential(cls, values: Any) -> Any:
        if isinstance(values, dict):
            auth_type = values.get("auth_type", GCSAuthType.HMAC)
            credential = values.get("credential")
            if auth_type == GCSAuthType.HMAC and credential is None:
                raise ValueError("credential is required when auth_type is 'hmac'")
            if auth_type != GCSAuthType.HMAC and credential is not None:
                raise ValueError(
                    f"credential (HMAC key) must not be set when auth_type is '{auth_type}'. "
                    "HMAC credentials are only used with auth_type 'hmac'."
                )
        return values

    @model_validator(mode="before")
    @classmethod
    def validate_gcp_wif_configuration_options(cls, values: Any) -> Any:
        if isinstance(values, dict):
            auth_type = values.get("auth_type", GCSAuthType.HMAC)
            wif_options = [
                values.get("gcp_wif_configuration"),
                values.get("gcp_wif_configuration_json"),
                values.get("gcp_wif_configuration_json_string"),
            ]
            if auth_type == GCSAuthType.WORKLOAD_IDENTITY_FEDERATION:
                if not any(opt is not None for opt in wif_options):
                    raise ValueError(
                        "One of gcp_wif_configuration (file path), gcp_wif_configuration_json (JSON content), "
                        "or gcp_wif_configuration_json_string (JSON string) is required when auth_type is 'workload_identity_federation'"
                    )
            elif auth_type == GCSAuthType.WORKLOAD_IDENTITY:
                if any(opt is not None for opt in wif_options):
                    raise ValueError(
                        "gcp_wif_configuration options must not be set when auth_type is 'workload_identity'. "
                        "Credentials are sourced automatically from Application Default Credentials (e.g. GKE Workload Identity)."
                    )
        return values

    @field_validator("path_specs", mode="after")
    @classmethod
    def check_path_specs_and_infer_platform(
        cls, path_specs: List[PathSpec]
    ) -> List[PathSpec]:
        if len(path_specs) == 0:
            raise ValueError("path_specs must not be empty")

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
        self.report: GCSSourceReport = GCSSourceReport()
        self.platform: str = PLATFORM_GCS
        self._gcp_credentials: Optional[Credentials] = None
        self._gcp_project_id: Optional[str] = None
        self.s3_source = self.create_equivalent_s3_source(ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GCSSource":
        config = GCSSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _setup_wif_credentials(self) -> None:
        self._gcp_credentials, self._gcp_project_id = load_wif_credentials(self.config)

    def _setup_adc_credentials(self) -> None:
        try:
            self._gcp_credentials, self._gcp_project_id = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            logger.info(
                "Loaded Application Default Credentials (project: %s)",
                self._gcp_project_id,
            )
            if self._gcp_project_id is None:
                logger.warning(
                    "Application Default Credentials did not return a project ID. "
                    "If GCS requests fail with permission errors, set the "
                    "GCLOUD_PROJECT or GOOGLE_CLOUD_PROJECT environment variable."
                )
        except google.auth.exceptions.DefaultCredentialsError as e:
            raise ValueError(
                "Failed to load Application Default Credentials for GCS workload_identity auth. "
                "Ensure GKE Workload Identity is enabled and the pod service account is bound. "
                f"Details: {e}"
            ) from e
        except google.auth.exceptions.GoogleAuthError as e:
            raise ValueError(
                "Unexpected Google Auth error while loading Application Default Credentials "
                f"for GCS workload_identity auth: {e}"
            ) from e

    def _build_gcs_oauth_aws_config(self) -> GCSOAuthAwsConnectionConfig:
        aws_config = GCSOAuthAwsConnectionConfig(
            aws_endpoint_url=GCS_ENDPOINT_URL,
            aws_region="auto",
            aws_access_key_id="gcs-oauth",
            aws_secret_access_key="not-used",
        )
        aws_config._gcs_oauth_credentials = self._gcp_credentials
        aws_config._gcs_oauth_project_id = self._gcp_project_id
        return aws_config

    def _build_s3_config(
        self, s3_path_specs: List[PathSpec], aws_config: AwsConnectionConfig
    ) -> DataLakeSourceConfig:
        return DataLakeSourceConfig(
            path_specs=s3_path_specs,
            aws_config=aws_config,
            env=self.config.env,
            convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
            max_rows=self.config.max_rows,
            number_of_files_to_sample=self.config.number_of_files_to_sample,
            platform=PLATFORM_GCS,
            platform_instance=self.config.platform_instance,
        )

    def create_equivalent_s3_config(self) -> DataLakeSourceConfig:
        s3_path_specs = self.create_equivalent_s3_path_specs()

        if self.config.auth_type == GCSAuthType.HMAC:
            if not self.config.credential:
                raise ValueError(
                    "HMAC credentials are required when auth_type is 'hmac'"
                )
            aws_config: AwsConnectionConfig = AwsConnectionConfig(
                aws_endpoint_url=GCS_ENDPOINT_URL,
                aws_access_key_id=self.config.credential.hmac_access_id,
                aws_secret_access_key=self.config.credential.hmac_access_secret.get_secret_value(),
                aws_region="auto",
            )
        elif self.config.auth_type == GCSAuthType.WORKLOAD_IDENTITY_FEDERATION:
            self._setup_wif_credentials()
            aws_config = self._build_gcs_oauth_aws_config()
        elif self.config.auth_type == GCSAuthType.WORKLOAD_IDENTITY:
            self._setup_adc_credentials()
            aws_config = self._build_gcs_oauth_aws_config()
        else:
            raise ValueError(f"Unsupported auth_type: {self.config.auth_type!r}")

        return self._build_s3_config(s3_path_specs, aws_config)

    def create_equivalent_s3_path_specs(self) -> List[PathSpec]:
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
        adapter = create_object_store_adapter("gcs")
        return adapter.apply_customizations(source)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        return self.s3_source.get_workunits_internal()

    def get_report(self) -> GCSSourceReport:
        return self.report

    def close(self) -> None:
        self.s3_source.close()
        super().close()
