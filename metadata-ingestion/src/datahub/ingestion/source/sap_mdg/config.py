from typing import Dict, List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class MdgTargetPlatform(ConfigModel):
    # Describes the downstream DataHub platform that an MDG logical system maps
    # to, so cross-platform lineage can point at the correct dataset urns.
    platform: str = Field(
        description="DataHub platform id of the downstream system MDG replicates to "
        "(e.g. `hana`).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of the downstream system, if it is ingested "
        "with one.",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment (fabric) of the downstream system. Defaults to the "
        "source's `env` when unset.",
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description="Lowercase the downstream dataset name when building its urn. "
        "Most SAP targets are case-insensitive, so this is enabled by default.",
    )


class DrfConfig(ConfigModel):
    # Cross-platform lineage derived from the SAP Data Replication Framework. The
    # replication model (tables DRFC_APPL / DRFC_APPL_SYS) links a governed data
    # model to the target business systems it is replicated to. These tables have
    # no standard OData service, so they are read through a customer-exposed
    # generic table-reader service (e.g. an RFC_READ_TABLE-backed SEGW service)
    # that returns each table as an OData entity set of JSON rows keyed by column.
    enabled: bool = Field(
        default=False,
        description="Read the DRF replication model to emit lineage from MDG entity "
        "sets to the datasets they are replicated to in downstream systems.",
    )
    table_read_service: Optional[str] = Field(
        default=None,
        description="Path (relative to `base_url`) of a generic table-reader OData "
        "service that exposes the DRF customizing tables as entity sets.",
    )
    model_entity_set: str = Field(
        default="DRFC_APPL",
        description="Entity set name of the DRFC_APPL (replication model) table in the "
        "table-reader service.",
    )
    system_entity_set: str = Field(
        default="DRFC_APPL_SYS",
        description="Entity set name of the DRFC_APPL_SYS (model \u2192 business system) "
        "table in the table-reader service.",
    )
    service_to_data_model: Dict[str, str] = Field(
        default_factory=dict,
        description="Maps an ingested OData service id (the last path segment, e.g. "
        "`ZMDG_BP_SRV`) to the MDG data model (`USMD_MODEL`, e.g. `BP`) whose "
        "replication model determines that service's lineage targets.",
    )
    emit_column_lineage: bool = Field(
        default=False,
        description="Also emit column-level lineage. The downstream schema is read "
        "from DataHub and lineage is emitted only for field names present in both the "
        "MDG entity and the downstream dataset (safe by construction). Requires a "
        "DataHub sink/graph to be available.",
    )
    emit_logical_parent: bool = Field(
        default=False,
        description="Also model the MDG entity as the logical parent of each replicated "
        "downstream dataset (a `PhysicalInstanceOf` relationship), in addition to the "
        "COPY lineage. This reflects MDG's role as the canonical master-data model that "
        "is physically instantiated in each target system. Column-level physical-instance "
        "links are emitted for fields matched by name (case-insensitive) against the "
        "downstream schema read from DataHub. Viewing logical models in the UI requires "
        "the `LOGICAL_MODELS_ENABLED` server flag.",
    )


class SapMdgSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    base_url: str = Field(
        description="Base URL of the SAP Gateway host serving the MDG OData services, "
        "e.g. `https://sap-gw.example.com:44300`.",
    )
    services: List[str] = Field(
        description="OData service paths to ingest, relative to `base_url` "
        "(e.g. `/sap/opu/odata/sap/ZMDG_BP_SRV`). Each service's `$metadata` "
        "document is parsed into datasets.",
    )
    username: Optional[SecretStr] = Field(
        default=None, description="Username for HTTP basic authentication."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="Password for HTTP basic authentication."
    )
    token: Optional[SecretStr] = Field(
        default=None,
        description="Bearer token used instead of basic auth (e.g. an OAuth2 access token).",
    )
    sap_client: Optional[str] = Field(
        default=None,
        description="SAP client (`sap-client`) query parameter appended to every request.",
    )
    client_certificate_path: Optional[str] = Field(
        default=None,
        description="Path to a PEM client certificate for X.509 (mutual TLS) authentication.",
    )
    client_key_path: Optional[str] = Field(
        default=None,
        description="Path to the PEM private key matching `client_certificate_path`.",
    )
    ca_certificate_path: Optional[str] = Field(
        default=None,
        description="Path to a CA bundle used to verify the server's TLS certificate.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify the server's TLS certificate. Prefer "
        "`ca_certificate_path` for private CAs over disabling verification.",
    )
    timeout: int = Field(default=30, description="Per-request timeout in seconds.")
    entity_set_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter OData entity sets (by name) for ingestion.",
    )
    emit_entity_types_without_sets: bool = Field(
        default=False,
        description="Also emit entity types that are not exposed through any entity set. "
        "Disabled by default to keep the catalog aligned with queryable collections.",
    )
    include_foreign_keys: bool = Field(
        default=True,
        description="Emit foreign-key constraints derived from OData navigation "
        "properties that carry referential constraints.",
    )
    logical_system_to_platform: Dict[str, MdgTargetPlatform] = Field(
        default_factory=dict,
        description="Maps an MDG logical-system / business-system code to the "
        "downstream DataHub platform it corresponds to. Used to resolve "
        "cross-platform lineage targets (platform, instance, env, casing). A code "
        "not listed here falls back to a small set of well-known SAP platforms.",
    )
    drf: DrfConfig = Field(
        default_factory=DrfConfig,
        description="Data Replication Framework lineage settings (cross-platform "
        "lineage from MDG to the systems it replicates master data to).",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config for stale entity removal.",
    )

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
        return value.rstrip("/")

    @field_validator("services")
    @classmethod
    def _require_at_least_one_service(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("At least one OData service path must be configured.")
        return value

    @model_validator(mode="after")
    def _require_drf_service_when_enabled(self) -> "SapMdgSourceConfig":
        if self.drf.enabled and self.drf.table_read_service is None:
            raise ValueError(
                "drf.table_read_service is required when drf.enabled is true."
            )
        return self

    @model_validator(mode="after")
    def _require_credentials(self) -> "SapMdgSourceConfig":
        has_basic = self.username is not None and self.password is not None
        has_token = self.token is not None
        has_certificate = self.client_certificate_path is not None
        if not (has_basic or has_token or has_certificate):
            raise ValueError(
                "Provide credentials via username/password, token, or a client certificate."
            )
        return self
