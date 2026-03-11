import logging
from functools import cached_property
from typing import Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from typing_extensions import Literal

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.azure.azure_auth import (
    AzureAuthenticationMethod,
    AzureCredentialConfig,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


AZURE_FILESYSTEM_SCHEMES = ("abfs", "abfss")
AZURE_CONTAINER_SCHEMES = ("az", "adl")
AZURE_URI_SCHEMES = (*AZURE_FILESYSTEM_SCHEMES, *AZURE_CONTAINER_SCHEMES)
AZURE_HTTP_HOST_SUFFIXES = (".blob.core.windows.net", ".dfs.core.windows.net")
AZURE_SUPPORTED_FORMATS_HINT = "abfss://, abfs://, az://, adl://, and Azure HTTPS"


def is_azure_http_netloc(netloc: str) -> bool:
    lowered = netloc.lower()
    return any(lowered.endswith(suffix) for suffix in AZURE_HTTP_HOST_SUFFIXES)


def is_azure_path(path: str) -> bool:
    parsed = urlparse(path or "")
    scheme = parsed.scheme.lower()
    return scheme in AZURE_URI_SCHEMES or (
        scheme in {"http", "https"} and is_azure_http_netloc(parsed.netloc)
    )


class S3(ConfigModel):
    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None, description="AWS configuration"
    )

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        False, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        False,
        description="# Whether or not to create tags in datahub from the s3 object",
    )


class AzureBlob(ConfigModel):
    account_name: Optional[str] = Field(
        default=None,
        description="Azure storage account name. Required for `az://` and `adl://` style paths.",
    )
    account_key: Optional[SecretStr] = Field(
        default=None,
        description="Azure storage account key.",
    )
    sas_token: Optional[SecretStr] = Field(
        default=None,
        description="Azure shared access signature (SAS) token.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description="Azure service principal client id.",
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description="Azure service principal client secret.",
    )
    tenant_id: Optional[str] = Field(
        default=None,
        description="Azure service principal tenant id.",
    )
    credential: Optional[AzureCredentialConfig] = Field(
        default=None,
        description=(
            "Unified Azure credential configuration. If provided, takes precedence "
            "over static keys."
        ),
    )

    @model_validator(mode="after")
    def validate_service_principal_fields(self) -> "AzureBlob":
        has_any_sp_field = any([self.client_id, self.client_secret, self.tenant_id])
        has_all_sp_fields = all([self.client_id, self.client_secret, self.tenant_id])
        if has_any_sp_field and not has_all_sp_fields:
            raise ValueError(
                "Service principal auth requires `client_id`, `client_secret`, and `tenant_id`."
            )
        return self

    def build_storage_options(
        self, account_name: Optional[str], container_name: Optional[str]
    ) -> dict[str, str]:
        opts: dict[str, str] = {}

        resolved_account = account_name or self.account_name
        if resolved_account:
            opts["account_name"] = resolved_account
        if container_name:
            opts["container_name"] = container_name

        if self.credential is not None:
            if (
                self.credential.authentication_method
                == AzureAuthenticationMethod.SERVICE_PRINCIPAL
            ):
                if self.credential.client_id:
                    opts["client_id"] = self.credential.client_id
                if self.credential.client_secret:
                    opts["client_secret"] = (
                        self.credential.client_secret.get_secret_value()
                    )
                if self.credential.tenant_id:
                    opts["authority_id"] = self.credential.tenant_id
            elif self.credential.authentication_method == AzureAuthenticationMethod.CLI:
                opts["use_azure_cli"] = "true"
            else:
                token = self.credential.get_credential().get_token(
                    "https://storage.azure.com/.default"
                )
                opts["token"] = token.token
            return opts

        if self.account_key is not None:
            opts["access_key"] = self.account_key.get_secret_value()
        elif self.sas_token is not None:
            opts["sas_key"] = self.sas_token.get_secret_value()
        elif self.client_id and self.client_secret and self.tenant_id:
            opts["client_id"] = self.client_id
            opts["client_secret"] = self.client_secret.get_secret_value()
            opts["authority_id"] = self.tenant_id

        return opts


class DeltaLakeSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    base_path: str = Field(
        description="Path to table (s3 or local file system). If path is not a delta table path "
        "then all subfolders will be scanned to detect and ingest delta tables."
    )
    relative_path: Optional[str] = Field(
        default=None,
        description="If set, delta-tables will be searched at location "
        "'<base_path>/<relative_path>' and URNs will be created using "
        "relative_path only.",
    )
    platform: Literal["delta-lake"] = Field(
        default="delta-lake",
        description="The platform that this source connects to",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to filter in ingestion.",
    )
    version_history_lookback: Optional[int] = Field(
        default=1,
        description="Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested.",
    )

    require_files: Optional[bool] = Field(
        default=True,
        description="Whether DeltaTable should track files. "
        "Consider setting this to `False` for large delta tables, "
        "resulting in significant memory reduction for ingestion process."
        "When set to `False`, number_of_files in delta table can not be reported.",
    )

    s3: Optional[S3] = Field(None)
    azure: Optional[AzureBlob] = Field(
        default=None,
        description="Azure configuration for `abfss://`, `abfs://`, `az://`, `adl://`, and Azure HTTPS paths.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful Ingestion Config with stale metadata removal",
    )

    @cached_property
    def is_s3(self):
        return is_s3_uri(self.base_path or "")

    @cached_property
    def is_azure(self) -> bool:
        return is_azure_path(self.base_path or "")

    @cached_property
    def complete_path(self):
        complete_path = self.base_path
        if self.relative_path is not None:
            complete_path = (
                f"{complete_path.rstrip('/')}/{self.relative_path.lstrip('/')}"
            )

        return complete_path

    @field_validator("version_history_lookback", mode="after")
    @classmethod
    def negative_version_history_implies_no_limit(
        cls, v: Optional[int]
    ) -> Optional[int]:
        if v and v < 0:
            return None
        return v
