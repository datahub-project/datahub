"""Configuration classes for the BigID DataHub connector."""

from __future__ import annotations

from typing import Literal, Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
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

# ---------------------------------------------------------------------------
# Platform type mapping — BigID ds-connection `type` → DataHub platform name
# Source: `type` field values from the GET /api/v1/ds-connections response; verified against BigID 6.x
# ---------------------------------------------------------------------------

BIGID_TYPE_TO_PLATFORM: dict[str, str] = {
    # Relational databases
    "rdb-mysql": "mysql",
    "rdb-postgresql": "postgres",
    "rdb-mssql": "mssql",
    "rdb-oracle": "oracle",
    "rdb-db2": "db2",
    "rdb-redshift": "redshift",
    "rdb-hive": "hive",
    "rdb-teradata": "teradata",
    # Cloud warehouses / lakehouses
    "snowflake": "snowflake",
    "gcp-big-query": "bigquery",
    "databricks-v2": "databricks",
    # Object / file storage
    "s3-v2": "s3",
    # SaaS / other structured sources
    "salesforce": "salesforce",
    "kafka": "kafka",
    # Collaboration / document stores (unstructured; platform name used for URN only)
    "sharepoint-online-v2": "sharepoint",
    "confluence-v2": "confluence",
    # Partner integrations
    "mongodb": "mongodb",
    "azure-sql": "mssql",
    "sap-hana": "saphana",
    "adls-v2": "adls-gen2",
    # Intentionally unmapped (no standard DataHub platform name):
    #   smb_v2, gdrive-v2, onedrive-v2, o365-outlook-v2, sap-successfactors-v2
    #   amazon-sagemaker, azure-openai, openai, hugging-face, atlas-vectorsearch
}

# Platforms where identifiers should be lowercased for canonical URN form
LOWERCASE_PLATFORMS = {"snowflake", "bigquery", "redshift"}


class ConnectionPlatformConfig(ConfigModel):
    """Per-connection platform override for a single BigID data source."""

    platform: str = Field(description="DataHub platform name (e.g. 'snowflake', 'mysql').")
    env: Optional[str] = Field(
        default=None,
        description="Environment override for this connection (e.g. 'PROD', 'DEV'). "
        "Falls back to top-level env if not set.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="DataHub platform instance identifier for this connection.",
    )


class BigIDSourceConfig(StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin):
    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    bigid_url: str = Field(description="Base URL of the BigID instance (e.g. 'https://bigid.example.com').")

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
        description="Long-lived BigID user token. Exchanged for a short-lived access token at startup.",
    )
    access_token: Optional[SecretStr] = Field(
        default=None,
        description="Short-lived BigID access token. Used directly; primarily for testing. "
        "Provide either this or user_token.",
    )
    timeout: int = Field(default=60, description="HTTP request timeout in seconds.")
    max_retries: int = Field(default=3, description="Maximum number of retries for transient errors.")

    # ------------------------------------------------------------------
    # Platform / URN resolution
    # ------------------------------------------------------------------
    datasource_platform_mapping: dict[str, ConnectionPlatformConfig] = Field(
        default_factory=dict,
        description="Map BigID connection name → platform config. "
        "Auto-detected from ds-connections API if omitted; explicit entries override.",
    )

    # ------------------------------------------------------------------
    # Dataset creation
    # ------------------------------------------------------------------
    create_datasets: bool = Field(
        default=False,
        description="If True, emit DatasetProperties + SchemaMetadata for datasets not yet in DataHub. "
        "Default False (pure enrichment mode — never emits structural aspects).",
    )

    # ------------------------------------------------------------------
    # Classification findings
    # ------------------------------------------------------------------
    minimum_confidence_threshold: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Filter column classification findings below this confidence level. "
        "Accepts 0.0–1.0 (not a rank string). "
        "HIGH ≥ 0.75, MEDIUM ≥ 0.50, LOW ≥ 0.0.",
    )

    confidence_level_tag: bool = Field(
        default=False,
        description="Emit urn:li:tag:bigid.confidence:{LEVEL} alongside each GlossaryTerm on a column. "
        "Lossy (can't tie level to a specific term when multiple exist), but visible in DataHub UI.",
    )

    # ------------------------------------------------------------------
    # Business Glossary
    # ------------------------------------------------------------------
    item_types: list[str] = Field(
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
    domain_mode: Literal["none", "auto_namespaced", "config_map"] = Field(
        default="none",
        description="Domain handling mode. "
        "'none': store raw domain/sub_domain in customProperties only. "
        "'auto_namespaced': auto-create urn:li:domain:bigid.{slug} entities. "
        "'config_map': map BigID domain values to existing DataHub domain URNs.",
    )
    domain_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Used when domain_mode='config_map'. "
        "Maps BigID domain string → DataHub domain URN.",
    )
    owner_type: Literal["user", "group", "none"] = Field(
        default="user",
        description="How to interpret BigID owner strings. "
        "'user' → urn:li:corpuser:{owner}. 'group' → urn:li:corpGroup:{owner}. "
        "'none' → stored in customProperties only.",
    )

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------
    sync_tags: bool = Field(default=True, description="Emit BigID tags as DataHub Tag entities.")
    tag_application_types: list[str] = Field(
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
        "URN pattern: urn:li:glossaryTerm:bigid.classifier.<slug>.",
    )
    sync_idsor: bool = Field(
        default=True,
        description="Emit GlossaryTerms for IDSoR (Identity Source of Record) attribute findings "
        "from BigID's correlation engine. IDSoR findings are separate from classifier findings "
        "and only appear when a Correlation Set is configured and enabled in the scan profile. "
        "When the attribute links to an existing Business Glossary term (via glossaryId), that "
        "term is reused. Otherwise an auto-generated term is created under a dedicated "
        "'bigid.idsor' GlossaryNode. URN pattern: urn:li:glossaryTerm:bigid.idsor.<slug>.",
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
    def _require_some_token(self) -> BigIDSourceConfig:
        if not self.access_token and not self.user_token:
            raise ValueError("Either user_token or access_token must be provided.")
        return self
