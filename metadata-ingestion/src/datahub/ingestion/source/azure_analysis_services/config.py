from typing import Dict, List, Literal, Optional, Union

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformDetail,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.common.m_query.config import (
    AthenaPlatformOverride,
    BigQueryExternalQueryPlatformDetail,
    DataBricksPlatformDetail,
    OraclePlatformDetail,
    reject_case_insensitive_duplicate_server_keys,
    validate_athena_platform_overrides,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.str_enum import StrEnum


class AasAuthType(StrEnum):
    SERVICE_PRINCIPAL = "service_principal"
    DEVICE_CODE = "device_code"
    INTERACTIVE = "interactive"
    USERNAME_PASSWORD = "username_password"


class IngestHiddenMetricsConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description=(
            "Ingest measures marked hidden, and measures on a hidden table, as "
            "metrics. Hidden measures are skipped otherwise. They still appear as "
            "schema fields on their table."
        ),
    )
    tag_hidden_metrics: bool = Field(
        default=False,
        description=(
            "Tag metrics created from hidden measures. Ignored when ``enabled`` "
            "is false. The tag name is ``hidden_metric_tag``."
        ),
    )
    hidden_metric_tag: str = Field(
        default="hidden_metric",
        description="Tag applied to hidden metrics when ``tag_hidden_metrics`` is enabled.",
    )

    @model_validator(mode="after")
    def _validate_tagging(self) -> "IngestHiddenMetricsConfig":
        if not self.enabled or not self.tag_hidden_metrics:
            return self
        if not self.hidden_metric_tag.strip():
            raise ValueError(
                "ingest_hidden_metrics.tag_hidden_metrics requires "
                "ingest_hidden_metrics.hidden_metric_tag to be a non-empty tag name"
            )
        return self


class AzureAnalysisServicesConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    server: str = Field(
        description=(
            "Analysis Services connection string. Either an Azure AS endpoint "
            "``asazure://<region>.asazure.windows.net/<server>`` or a Power BI "
            "Premium XMLA endpoint ``powerbi://api.powerbi.com/v1.0/myorg/<workspace>``."
        ),
    )
    platform: HiddenFromDocs[Literal["azure-analysis-services", "powerbi"]] = Field(
        default=constants.PLATFORM_AAS
    )
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description=(
            "Lowercase model and table dataset URNs. Set this when those URNs must "
            "match another connector that lowercases names, including Power BI "
            "lineage (``convert_lineage_urns_to_lowercase`` defaults to true). This "
            "value is part of each dataset's URN identity. Changing it after "
            "ingestion re-keys every dataset; with stateful ingestion the "
            "old-cased URNs are soft-deleted. Pick one value before the first run "
            "and leave it unchanged."
        ),
    )
    include_workspace_name_in_dataset_urn: bool = Field(
        default=False,
        description=(
            "Only applies when ``platform`` is aligned to ``powerbi`` (a Power BI "
            "Premium XMLA endpoint). Prefixes table dataset URNs with the workspace "
            "name so they stitch to a Power BI ingestion that also sets "
            "``include_workspace_name_in_dataset_urn``. The workspace GUID variant "
            "(Power BI's ``workspace_id_as_urn_part``) is not derivable from the XMLA "
            "endpoint, so only the workspace name is used."
        ),
    )

    auth_type: AasAuthType = Field(
        default=AasAuthType.SERVICE_PRINCIPAL,
        description=(
            "Authentication mode: 'service_principal' (client id/secret), "
            "'device_code', 'interactive', or 'username_password'."
        ),
    )
    tenant_id: Optional[str] = Field(
        default=None, description="Azure AD tenant (directory) id."
    )
    client_id: Optional[str] = Field(
        default=None, description="Azure AD application (client) id."
    )
    client_secret: Optional[SecretStr] = Field(
        default=None, description="Client secret for the service principal."
    )
    username: Optional[str] = Field(
        default=None, description="Username for the username_password auth mode."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="Password for the username_password auth mode."
    )

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates when calling the XMLA endpoint.",
    )
    request_timeout: int = Field(
        default=60, gt=0, description="Per-request timeout in seconds for XMLA calls."
    )

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases (models/catalogs) to include.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to include.",
    )

    # Feature toggles.
    extract_lineage: bool = Field(
        default=True,
        description="Extract upstream lineage from partition M/Power Query and native SQL.",
    )
    extract_column_level_lineage: bool = Field(
        default=True,
        description="Extract column-level lineage (upstream CLL and intra-model DAX dependencies).",
    )
    extract_model_definition: bool = Field(
        default=True,
        description="Attach the full TMSL model definition to the semantic model.",
    )
    extract_roles: bool = Field(
        default=True, description="Extract security roles as custom properties."
    )
    ingest_hidden_metrics: IngestHiddenMetricsConfig = Field(
        default_factory=IngestHiddenMetricsConfig,
        description=(
            "Whether to ingest hidden measures as metrics, and optionally tag them. "
            "See ``IngestHiddenMetricsConfig``."
        ),
    )

    # Shared M-Query engine surface (MQueryLineageConfig protocol)
    server_to_platform_instance: Dict[
        str, Union[OraclePlatformDetail, DataBricksPlatformDetail, PlatformDetail]
    ] = Field(
        default_factory=dict,
        description=(
            "Map an upstream server named in a partition's M/Power Query to its "
            "DataHub platform instance / env so lineage stitches to the native "
            "connector's URNs."
        ),
    )
    native_query_parsing: bool = Field(
        default=True,
        description="Parse ``Value.NativeQuery`` native SQL found inside M expressions.",
    )
    enable_advance_lineage_sql_construct: bool = Field(
        default=True,
        description="Use the advanced SQL-construct path when resolving native-query lineage.",
    )
    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description="Lowercase upstream dataset URNs to match connectors that lowercase names.",
    )
    m_query_parse_timeout: int = Field(
        default=30,
        gt=0,
        description="Timeout in seconds for parsing a single M/Power Query expression.",
    )
    dsn_to_platform_name: HiddenFromDocs[Dict[str, str]] = Field(default_factory=dict)
    dsn_to_database_schema: HiddenFromDocs[Dict[str, str]] = Field(default_factory=dict)
    athena_table_platform_override: HiddenFromDocs[List[AthenaPlatformOverride]] = (
        Field(default_factory=list)
    )
    bigquery_external_query_connection_to_platform: Dict[
        str, BigQueryExternalQueryPlatformDetail
    ] = Field(
        default_factory=dict,
        description=(
            "Map a BigQuery ``EXTERNAL_QUERY`` connection id "
            "(``project.region.connection``) to the external source it federates to, "
            "so lineage points at the real upstream table. Needs "
            "``native_query_parsing`` and ``enable_advance_lineage_sql_construct``."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion / stale-entity removal configuration.",
    )

    @field_validator("server")
    @classmethod
    def _validate_server(cls, value: str) -> str:
        # fullmatch (not match) so a trailing path/query suffix is rejected rather
        # than silently accepting the valid prefix and passing a malformed URL on.
        if constants.ASAZURE_ENDPOINT_RE.fullmatch(
            value
        ) or constants.POWERBI_ENDPOINT_RE.fullmatch(value):
            # The scheme is matched case-insensitively but XmlaClient rewrites the
            # literal ``powerbi://`` prefix; normalize it so a mixed-case scheme
            # still produces a valid https URL.
            scheme, sep, rest = value.partition("://")
            return f"{scheme.lower()}{sep}{rest}"
        raise ValueError(
            "server must be an 'asazure://<region>.asazure.windows.net/<server>' or "
            "'powerbi://api.powerbi.com/v1.0/myorg/<workspace>' endpoint; "
            f"got: {value}"
        )

    @field_validator("server_to_platform_instance", mode="after")
    @classmethod
    def _reject_case_insensitive_duplicate_server_keys(cls, value: Dict) -> Dict:
        return reject_case_insensitive_duplicate_server_keys(value)

    @model_validator(mode="after")
    def _validate_athena_table_platform_override(
        self,
    ) -> "AzureAnalysisServicesConfig":
        validate_athena_platform_overrides(self.athena_table_platform_override)
        return self

    @model_validator(mode="after")
    def _validate_platform_endpoint(self) -> "AzureAnalysisServicesConfig":
        # platform=powerbi only makes sense against a Power BI Premium XMLA
        # endpoint: the URNs are built to merge with the Power BI connector, and
        # XmlaClient rewrites the powerbi:// prefix. Pairing it with an asazure
        # endpoint would silently emit entities that stitch to nothing.
        if self.platform == constants.PLATFORM_POWERBI and not (
            constants.POWERBI_ENDPOINT_RE.fullmatch(self.server)
        ):
            raise ValueError(
                "platform='powerbi' requires a Power BI Premium XMLA endpoint "
                "('powerbi://api.powerbi.com/v1.0/myorg/<workspace>'); "
                f"got server: {self.server}"
            )
        return self

    @model_validator(mode="after")
    def _validate_auth(self) -> "AzureAnalysisServicesConfig":
        if self.auth_type == AasAuthType.SERVICE_PRINCIPAL:
            missing = [
                name
                for name, value in (
                    ("tenant_id", self.tenant_id),
                    ("client_id", self.client_id),
                    ("client_secret", self.client_secret),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"auth_type=service_principal requires: {', '.join(missing)}"
                )
        elif self.auth_type == AasAuthType.USERNAME_PASSWORD:
            missing = [
                name
                for name, value in (
                    ("client_id", self.client_id),
                    ("username", self.username),
                    ("password", self.password),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"auth_type=username_password requires: {', '.join(missing)}"
                )
        elif self.auth_type in (AasAuthType.DEVICE_CODE, AasAuthType.INTERACTIVE) and (
            not self.client_id
        ):
            raise ValueError(f"auth_type={self.auth_type} requires: client_id")
        return self
