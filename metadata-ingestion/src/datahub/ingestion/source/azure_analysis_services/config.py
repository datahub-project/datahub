from typing import Dict, List, Optional, Union

from pydantic import Field, SecretStr, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformDetail,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.common.m_query.config import (
    DataBricksPlatformDetail,
    OraclePlatformDetail,
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


class AthenaPlatformOverride(ConfigModel):
    # Structural match for the shared engine's AthenaTableOverride protocol,
    # used only when an M/Power Query partition reaches an Athena federated
    # source and lineage should point at the real upstream platform.
    database: str = Field(min_length=1)
    table: str = Field(min_length=1)
    platform: str = Field(min_length=1)
    dsn: Optional[str] = Field(default=None)


class AzureAnalysisServicesConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    server: str = Field(
        description=(
            "Analysis Services connection string. Either an Azure AS endpoint "
            "``asazure://<region>.asazure.windows.net/<server>`` or a Power BI "
            "Premium XMLA endpoint ``powerbi://api.powerbi.com/v1.0/myorg/<workspace>``."
        ),
    )
    platform: HiddenFromDocs[str] = Field(default=constants.PLATFORM_AAS)

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
        default=60, description="Per-request timeout in seconds for XMLA calls."
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
        description="Attach the full TMSL model definition to the model-level cube dataset.",
    )
    extract_roles: bool = Field(
        default=True, description="Extract security roles as custom properties."
    )

    # --- Shared M-Query engine surface (MQueryLineageConfig protocol) --------
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
        description="Timeout in seconds for parsing a single M/Power Query expression.",
    )
    dsn_to_platform_name: HiddenFromDocs[Dict[str, str]] = Field(default_factory=dict)
    dsn_to_database_schema: HiddenFromDocs[Dict[str, str]] = Field(default_factory=dict)
    athena_table_platform_override: HiddenFromDocs[List[AthenaPlatformOverride]] = (
        Field(default_factory=list)
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion / stale-entity removal configuration.",
    )

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
