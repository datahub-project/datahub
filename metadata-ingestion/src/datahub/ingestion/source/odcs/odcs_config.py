from enum import auto
from typing import Dict, List, Optional, Union

from pydantic import Field, field_validator, model_validator

from datahub.configuration._config_enum import ConfigEnum
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.git import GitInfo
from datahub.configuration.source_common import EnvConfigMixin
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mce_builder import ALL_ENV_TYPES, DEFAULT_ENV
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.common.gcs_connection_config import GCSConnectionConfig
from datahub.ingestion.source.common.http_connection_config import HTTPConnectionConfig
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

# Display name and platform id for the logical-model platform ODCS contracts are
# materialized under. ODCS v3 describes a producer-published dataset spec, so it
# is modeled as a logical `dataset` on its own platform rather than a
# bilateral `dataContract`.
ODCS_PLATFORM = "odcs"

# ODCS `servers[].type` (required by the spec) -> DataHub data platform id.
# Physical binding is derived from the contract itself via this map;
# `server_overrides` refines env / platform_instance / platform per server.
# Server types without an entry (kafka, s3, api, ...) leave the schema entry
# unbound — the logical dataset and its assertions are unaffected.
SERVER_TYPE_TO_PLATFORM: Dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "sqlserver": "mssql",
    "trino": "trino",
    "oracle": "oracle",
}

# Platforms whose DataHub connectors lowercase dataset names by default; the
# composed physical name follows suit when `lowercase_physical_urns` is on.
LOWERCASE_BY_DEFAULT_PLATFORMS = frozenset(("snowflake",))


class ServerMapping(ConfigModel):
    """Per-server overrides for physical dataset binding.

    The physical platform is normally derived from the spec-required
    `servers[].type`; an override entry refines that derivation for a named
    `servers[].server` — most commonly to set `env` or `platform_instance`,
    or to force a different platform id.
    """

    server: str = Field(
        description="The value of `servers[].server` in the ODCS contract. "
        "If `match_any` is True, this is treated as a wildcard match for any server."
    )
    platform: Optional[str] = Field(
        default=None,
        description="Optional DataHub data platform identifier (e.g. `postgres`, `snowflake`). "
        "When unset, the platform is derived from the server's `type`.",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment / fabric type for the produced physical dataset URNs (e.g. PROD, DEV).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional DataHub platform instance, used when the same platform is "
        "deployed multiple times (e.g. multiple Snowflake accounts).",
    )
    match_any: bool = Field(
        default=False,
        description="If True, this mapping matches any `servers[].server` not matched by a more specific mapping. "
        "Use a single catch-all mapping to apply one override to all contracts.",
    )

    @field_validator("env")
    @classmethod
    def env_must_be_one_of(cls, v: str) -> str:
        if v.upper() not in ALL_ENV_TYPES:
            raise ValueError(f"env must be one of {ALL_ENV_TYPES}, found {v}")
        return v.upper()


class SchemaAssertionCompatibility(ConfigEnum):
    """Compatibility mode for the emitted DATA_SCHEMA assertion.

    Member names are the DataHub `SchemaAssertionCompatibilityClass` constants,
    so this enum is the single source of truth for the valid set; the mapper
    passes the member value straight onto the assertion aspect.
    """

    EXACT_MATCH = auto()
    SUPERSET = auto()
    SUBSET = auto()


class ODCSSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], EnvConfigMixin
):
    """Config for the ODCS ingestion source.

    Reads Open Data Contract Standard (ODCS) v3.x YAML files from a path (file,
    directory, or glob) and materializes each `schema[]` entry as a **logical
    dataset** on the `odcs` platform: dataset properties, canonical schema
    metadata, ownership, tags, quality + schema-compliance assertions, and a
    link to the source document. When a physical dataset can be resolved from
    the contract's typed `servers[]` (refined via `server_overrides` or
    `physical_urn_overrides`), the source also emits a `logicalParent` link
    from the physical dataset to the logical one.
    """

    path: Union[str, List[str]] = Field(
        description="Location of ODCS YAML files: a local file, directory, or glob pattern; an "
        "`s3://` / `gs://` object-store URI (file or glob); or an `http(s)://` URL to a single "
        "file. May also be a list mixing any of the above. When `git_info` is set, non-URI "
        "entries are interpreted relative to the repository checkout.",
    )
    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS connection details for reading ODCS files from `s3://` URIs in `path`. "
        "Required when any `path` entry is an S3 URI.",
    )
    gcs_connection: Optional[GCSConnectionConfig] = Field(
        default=None,
        description="GCS connection (HMAC credentials via the S3-compatible API) for reading ODCS "
        "files from `gs://` URIs in `path`. Required when any `path` entry is a GCS URI. See "
        "https://cloud.google.com/storage/docs/authentication/hmackeys",
    )
    git_info: Optional[GitInfo] = Field(
        default=None,
        description="Git repository to shallow-clone and scan for ODCS files, authenticated with "
        "an SSH deploy key. When set, each non-URI `path` entry is resolved relative to the "
        "repository checkout (e.g. `path: contracts/` or `path: '**/*.odcs.yaml'`).",
    )
    http_connection: Optional[HTTPConnectionConfig] = Field(
        default=None,
        description="Authentication and TLS options for reading ODCS files from `http(s)://` URLs "
        "in `path`. Supports a bearer token or HTTP basic auth, and a `verify_ssl` toggle. "
        "Omit for public URLs that need no authentication.",
    )
    server_overrides: List[ServerMapping] = Field(
        default_factory=list,
        description="Optional per-server overrides for physical binding. The physical platform "
        "is derived from the contract's spec-required `servers[].type`; an override refines "
        "env / platform_instance / platform for a named `servers[].server` value. Binding only "
        "affects the `logicalParent` link — assertions always attach to the logical dataset.",
    )
    # Deliberately NOT named `convert_urns_to_lowercase`: the pipeline-level
    # AutoLowercaseUrnsProcessor duck-types on that attribute name and would
    # blanket-lowercase every dataset URN in the stream — including logical
    # `odcs` URNs (losing the contract's casing) and logicalParent targets
    # AFTER they were existence-verified with their original casing.
    lowercase_physical_urns: bool = Field(
        default=True,
        description="Lowercase composed physical dataset names for platforms whose DataHub "
        "connectors lowercase URNs by default (currently snowflake). Set False if your "
        "snowflake ingestion runs with convert_urns_to_lowercase disabled. Logical `odcs` "
        "dataset URNs always preserve the contract's casing.",
    )
    verify_physical_urns_exist: bool = Field(
        default=True,
        description="When a DataHub graph is available (datahub-rest sink), verify each derived "
        "physical dataset URN exists before emitting its `logicalParent` link; URNs not found "
        "are left unbound with a warning instead of creating stub datasets. With no graph "
        "(file sink), emission proceeds without verification. Set False to emit links "
        "optimistically for tables that do not exist yet.",
    )
    tag_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix prepended to every tag emitted from ODCS `tags` and "
        "property-level `tags` (e.g. `odcs.`). Useful for distinguishing ODCS-sourced tags.",
    )
    strip_owner_email_domain: bool = Field(
        default=False,
        description="ODCS team usernames may be emails. When true, the domain is stripped "
        "so `alice@acme.com` maps to corpuser `alice` — use this when your identity source "
        "(Okta / Azure AD / …) ingests users by bare username. Mutually exclusive with "
        "`owner_email_domain`.",
    )
    owner_email_domain: Optional[str] = Field(
        default=None,
        description="When set, bare (non-email) team usernames get `@<domain>` appended, so "
        "`alice` maps to corpuser `alice@acme.com` — use this when your identity source "
        "ingests users by email. Mutually exclusive with `strip_owner_email_domain`.",
    )
    strict_validation: bool = Field(
        default=False,
        description=(
            "If True, ODCS files failing JSON-Schema validation are skipped (logged as warnings). "
            "If False (default), schema violations are reported as warnings; the contract is still "
            "ingested. Default False matches real-world ODCS files that use deprecated forms (e.g. "
            "top-level `quality[]`); set True to fail-fast on schema violations."
        ),
    )
    replicate_contract_metadata: bool = Field(
        default=True,
        description=(
            "If True (default), contract-level Ownership and GlobalTags are written to the logical "
            "dataset on every ingest run. Set False to skip emitting contract-level Ownership and "
            "GlobalTags so manual UI edits to those aspects survive subsequent ingest runs (the "
            "contract is then a one-time enricher rather than a source of truth)."
        ),
    )
    odcs_versions: List[str] = Field(
        default_factory=lambda: ["3.0.0", "3.0.1", "3.0.2", "3.1.0"],
        description="List of supported ODCS `apiVersion` values. Contracts with `apiVersion` "
        "outside this list are skipped with a warning.",
    )
    emit_assertions: bool = Field(
        default=True,
        description="Whether to emit Assertion entities derived from the ODCS `quality[]` rules. "
        "Assertions target the logical `odcs` dataset and are emitted whether or not a "
        "physical binding resolves.",
    )
    emit_schema_assertion: bool = Field(
        default=True,
        description="Whether to emit one DATA_SCHEMA assertion per `schema[]` entry, pinning the "
        "contract's declared schema on the logical dataset so schema drift is evaluable as a "
        "contract violation.",
    )
    schema_assertion_compatibility: SchemaAssertionCompatibility = Field(
        default=SchemaAssertionCompatibility.SUPERSET,
        description="Compatibility mode for the DATA_SCHEMA assertion: `SUPERSET` (an instance "
        "must contain at least the contract's fields; extras allowed), `EXACT_MATCH`, or "
        "`SUBSET`.",
    )

    @field_validator("owner_email_domain")
    @classmethod
    def owner_email_domain_without_at(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lstrip("@")
        return v or None

    @model_validator(mode="after")
    def object_store_connection_required_for_uris(self) -> "ODCSSourceConfig":
        raw_paths = self.path if isinstance(self.path, list) else [self.path]
        if any(is_s3_uri(p) for p in raw_paths) and self.aws_connection is None:
            raise ValueError(
                "aws_connection is required because path contains one or more s3:// URIs."
            )
        if any(is_gcs_uri(p) for p in raw_paths) and self.gcs_connection is None:
            raise ValueError(
                "gcs_connection is required because path contains one or more gs:// URIs."
            )
        return self

    @model_validator(mode="after")
    def owner_normalization_knobs_are_exclusive(self) -> "ODCSSourceConfig":
        if self.strip_owner_email_domain and self.owner_email_domain:
            raise ValueError(
                "strip_owner_email_domain and owner_email_domain are mutually "
                "exclusive: one strips domains from emails, the other appends a "
                "domain to bare usernames."
            )
        return self

    emit_logical_parent: bool = Field(
        default=True,
        description="Whether to emit a `logicalParent` link from each resolved physical dataset to "
        "its logical ODCS dataset (the `PhysicalInstanceOf` relationship). Disable to keep ODCS "
        "from writing any aspect onto physical datasets.",
    )
    physical_urn_overrides: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Map of ODCS contract `id` to a map of `schema[]` entry NAME to an explicit "
        "physical DataHub Dataset URN, bypassing server-based derivation for those entries. An "
        "empty-string URN deliberately leaves the named entry unbound; schema entries absent "
        "from the map fall back to server-based derivation. Keys that match no schema entry "
        "produce a warning (typo guard).",
    )
    logical_dataset_name_template: str = Field(
        default="{contract_id}.{schema_name}",
        description="Template for the logical `odcs` dataset name (the URN name segment). "
        "Available placeholders: `{contract_id}`, `{schema_name}`, `{contract_version}`.",
    )
    dataset_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex allow/deny patterns applied to the logical `odcs` dataset name "
        "(as composed by `logical_dataset_name_template`, e.g. `{contract_id}.{schema_name}`). "
        "Schema entries whose logical name does not match are skipped along with their "
        "assertions and `logicalParent` link. Matched case-insensitively by default.",
    )
    file_extensions: List[str] = Field(
        default_factory=lambda: [".yaml", ".yml", ".odcs.yaml", ".odcs.yml"],
        description="File extensions considered ODCS YAML files when scanning a directory.",
    )
    max_input_file_bytes: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum size of an ODCS YAML file to load. Files exceeding this are skipped "
        "with a warning. Defaults to 5 MB. This guards the YAML parser from unbounded inputs.",
    )
    follow_symlinks: bool = Field(
        default=False,
        description="If true, follow symlinks when discovering ODCS files. Off by default to "
        "prevent disclosure via symlink escape in shared/multi-tenant directories. When enabled, "
        "the source still requires that resolved targets stay within the configured root.",
    )

    # Removed when ODCS pivoted from emitting `dataContract` entities to logical
    # datasets: the raw contract was embedded in DataContractProperties.rawContract,
    # which no longer exists, and dataset URN overrides became physical_urn_overrides
    # (a list per contract to match the schema[] fan-out).
    _removed_raw_contract_size_limit = pydantic_removed_field(
        "raw_contract_size_limit_bytes", "June", 2026
    )
    _removed_dataset_urn_overrides = pydantic_removed_field(
        "dataset_urn_overrides", "June", 2026
    )
