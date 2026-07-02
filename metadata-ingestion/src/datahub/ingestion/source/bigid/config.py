from typing import Dict, List, Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.str_enum import StrEnum


class DomainMode(StrEnum):
    """How BigID domain/sub_domain values are mapped into DataHub."""

    NONE = "none"
    AUTO_NAMESPACED = "auto_namespaced"
    CONFIG_MAP = "config_map"


class OwnerType(StrEnum):
    """How BigID owner strings are interpreted when building owner URNs."""

    USER = "user"
    GROUP = "group"
    NONE = "none"


class ConnectionPlatformConfig(ConfigModel):
    """Per-connection platform override for a single BigID data source."""

    platform: str = Field(
        description="DataHub platform name (e.g. 'snowflake', 'mysql')."
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment override for this connection (e.g. 'PROD', 'DEV'). "
        "Falls back to top-level env if not set.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance identifier for this connection.",
    )


class BigIDSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    bigid_url: str = Field(
        description="Base URL of the BigID instance (e.g. 'https://bigid.example.com')."
    )

    @field_validator("bigid_url", mode="before")
    @classmethod
    def _validate_bigid_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"bigid_url must use http or https, got {parsed.scheme!r}. "
                "Example: 'https://bigid.example.com'"
            )
        if not parsed.netloc:
            raise ValueError("bigid_url must include a hostname.")
        return v

    user_token: Optional[SecretStr] = Field(
        default=None,
        description="Recommended auth. Long-lived BigID user token, generated under "
        "Administration → Access Management → System Users (Save the user after generating so "
        "the token activates). Exchanged for a short-lived session token at startup and "
        "auto-refreshed on expiry, so it is safe for scheduled ingestion. Provide the raw "
        "token — no 'Bearer' prefix. Provide either this or access_token; if both are set, "
        "user_token takes precedence because it can auto-refresh.",
    )
    access_token: Optional[SecretStr] = Field(
        default=None,
        description="Short-lived BigID session token, used directly without the startup "
        "exchange and NOT auto-refreshed — a run that outlives it fails. Intended for one-off "
        "runs or SSO-only tenants where a service-account user_token cannot be created; prefer "
        "user_token for scheduled ingestion. Provide either this or user_token; if both are "
        "set, user_token is used and this value is ignored.",
    )
    timeout: int = Field(default=60, description="HTTP request timeout in seconds.")
    max_retries: int = Field(
        default=3, description="Maximum number of retries for transient errors."
    )

    datasource_platform_mapping: Dict[str, ConnectionPlatformConfig] = Field(
        default_factory=dict,
        description="Map BigID connection name → platform config. "
        "Auto-detected from ds-connections API if omitted; explicit entries override.",
    )
    connection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns matched against the BigID connection (data "
        "source) name. Use this to scope ingestion to a subset of connections in large BigID "
        "deployments that expose hundreds of data sources. Catalog objects whose source "
        "connection is denied are skipped entirely.",
    )
    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns matched against the BigID catalog object's "
        "fully-qualified name. Complements connection_pattern with finer, dataset-level "
        "scoping for large deployments where a single connection exposes many objects. "
        "Objects whose fully-qualified name is denied are skipped.",
    )
    create_datasets: bool = Field(
        default=False,
        description="If True, emit DatasetProperties + SchemaMetadata for datasets not yet in DataHub. "
        "Default False (pure enrichment mode — never emits structural aspects).",
    )

    minimum_confidence_threshold: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Filter column classification findings below this confidence level. "
        "Accepts 0.0–1.0 (not a rank string). BigID ranks map to: "
        "HIGH = 0.75, MEDIUM = 0.50, LOW = 0.25 (unknown ranks = 0.0).",
    )

    confidence_level_tag: bool = Field(
        default=False,
        description="Emit urn:li:tag:bigid.confidence:{LEVEL} alongside each GlossaryTerm on a column. "
        "Lossy (can't tie level to a specific term when multiple exist), but visible in DataHub UI.",
    )

    item_types: List[str] = Field(
        default_factory=lambda: [
            "Business Term",
            "Personal Data Category",
            "Personal Data Item",
            "Purpose Of Processing",
            "Document",
        ],
        description="Allow-list of BigID item types to sync. "
        "OOTB Personal Data Items are always included regardless of this filter.",
    )
    domain_mode: DomainMode = Field(
        default=DomainMode.NONE,
        description="Domain handling mode. "
        "'none': store raw domain/sub_domain in customProperties only. "
        "'auto_namespaced': auto-create GUID-based urn:li:domain entities "
        "(one per BigID domain/sub-domain, keyed deterministically by name). "
        "'config_map': map BigID domain values to existing DataHub domain URNs.",
    )
    domain_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Used when domain_mode='config_map'. "
        "Maps BigID domain string → DataHub domain URN.",
    )
    owner_type: OwnerType = Field(
        default=OwnerType.USER,
        description="How to interpret BigID owner strings. "
        "'user' → urn:li:corpuser:{owner}. 'group' → urn:li:corpGroup:{owner}. "
        "'none' → stored in customProperties only.",
    )

    sync_tags: bool = Field(
        default=True, description="Emit BigID tags as DataHub Tag entities."
    )
    tag_application_types: List[str] = Field(
        default_factory=lambda: ["sensitivityClassification", "risk", "userDefined"],
        description="BigID applicationType values to sync as tags.",
    )
    risk_score_structured_property_urn: str = Field(
        default="urn:li:structuredProperty:bigid.riskScore",
        description="URN of the StructuredProperty used for riskScore values.",
    )
    sync_unlinked_classifiers: bool = Field(
        default=True,
        description="Emit GlossaryTerms for classifier findings that have no Business Glossary "
        "linkage in BigID. Terms are auto-generated on demand (only when a column finding "
        "references the classifier) and placed under the same 'bigid' root GlossaryNode. "
        "Term URNs are deterministic GUIDs keyed on the classifier identity.",
    )
    sync_idsor: bool = Field(
        default=True,
        description="Emit GlossaryTerms for IDSoR (Identity Source of Record) attribute findings "
        "from BigID's correlation engine. IDSoR findings are separate from classifier findings "
        "and only appear when a Correlation Set is configured and enabled in the scan profile. "
        "When the attribute links to an existing Business Glossary term (via glossaryId), that "
        "term is reused. Otherwise an auto-generated term is created under a dedicated "
        "'bigid.idsor' GlossaryNode. Term URNs are deterministic GUIDs keyed on the attribute identity.",
    )
    sync_unstructured_enrichment: bool = Field(
        default=False,
        description=(
            "Emit dataset-level GlossaryTerms and DatasetProfile for unstructured and email "
            "sources (SharePoint, Google Drive, O365, Kafka, AI models, etc.) using the "
            "attribute_details field returned by BigID's catalog API. Only applies to objects "
            "where BigID has classification findings (attribute_details non-empty). Controlled "
            "by the same sync_unlinked_classifiers and sync_idsor flags as structured enrichment."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @model_validator(mode="after")
    def _require_some_token(self) -> "BigIDSourceConfig":
        if not self.access_token and not self.user_token:
            raise ValueError("Either user_token or access_token must be provided.")
        return self
