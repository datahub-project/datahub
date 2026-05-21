"""Configuration classes for ThoughtSpot DataHub connector."""

from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import Field, SecretStr, field_validator

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


def _non_empty_str(value: str, field: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field} must be a non-empty, non-whitespace string")
    return value


def _non_empty_secret(value: SecretStr, field: str) -> SecretStr:
    if not value.get_secret_value() or not value.get_secret_value().strip():
        raise ValueError(f"{field} must be a non-empty, non-whitespace secret")
    return value


class TrustedAuth(ConfigModel):
    """Trusted-authentication mode (recommended for production).

    The connector mints a short-lived bearer token per ingestion run via
    ``auth_token_full`` using the ``secret_key`` as the long-lived credential.
    The ``username`` identifies the user to impersonate.
    """

    type: Literal["trusted"] = "trusted"
    username: str = Field(description="ThoughtSpot user to impersonate.")
    secret_key: SecretStr = Field(
        description=(
            "Secret key generated in ThoughtSpot under "
            "Develop > Customizations > Security Settings > Enable Trusted "
            "Authentication. Supports ``${THOUGHTSPOT_SECRET_KEY}``."
        )
    )

    @field_validator("username", mode="after")
    @classmethod
    def _username_nonempty(cls, v: str) -> str:
        return _non_empty_str(v, "username")

    @field_validator("secret_key", mode="after")
    @classmethod
    def _secret_key_nonempty(cls, v: SecretStr) -> SecretStr:
        return _non_empty_secret(v, "secret_key")


class PasswordAuth(ConfigModel):
    """Basic-authentication mode.

    The connector mints a short-lived bearer token per ingestion run via
    ``auth_token_full`` using the user's password. Prefer ``TrustedAuth``
    for production because password rotation policies tend to break
    scheduled ingestion.
    """

    type: Literal["password"] = "password"
    username: str = Field(description="ThoughtSpot username.")
    password: SecretStr = Field(description="Password for the ThoughtSpot user.")

    @field_validator("username", mode="after")
    @classmethod
    def _username_nonempty(cls, v: str) -> str:
        return _non_empty_str(v, "username")

    @field_validator("password", mode="after")
    @classmethod
    def _password_nonempty(cls, v: SecretStr) -> SecretStr:
        return _non_empty_secret(v, "password")


# Discriminated union: pydantic selects the concrete model via the ``type``
# tag (matches the recipe convention used by ``source.type``, ``sink.type``,
# and ``state_provider.type``), so downstream code can dispatch with
# ``isinstance`` without re-deriving "which mode am I in" from optional-field
# truthiness.
AuthConfig = Annotated[Union[TrustedAuth, PasswordAuth], Field(discriminator="type")]


class ThoughtSpotConnectionConfig(ConfigModel):
    """Connection configuration for ThoughtSpot REST API v2.0."""

    base_url: str = Field(
        description=(
            "ThoughtSpot instance URL (e.g., 'https://my-company.thoughtspot.cloud'). "
            "Do not include '/api/rest/2.0' - it will be added automatically."
        )
    )

    auth: AuthConfig = Field(
        description=(
            "Authentication block. Use ``type: trusted`` with ``username`` + "
            "``secret_key`` (recommended for production) or ``type: password`` "
            "with ``username`` + ``password``. Both modes mint short-lived "
            "bearer tokens per ingestion run."
        )
    )

    timeout_seconds: int = Field(
        default=30,
        gt=0,
        description=(
            "Request timeout in seconds. "
            "Increase if you experience timeout errors with large metadata responses."
        ),
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        description=(
            "Maximum number of retry attempts for failed API requests. "
            "Uses exponential backoff between retries."
        ),
    )

    tml_export_batch_size: int = Field(
        default=100,
        gt=0,
        description=(
            "Maximum number of object IDs sent in a single TML export "
            "request. ThoughtSpot's TML export bundles full YAML edocs per "
            "object; at ~1000+ liveboards in one POST the response can "
            "exceed server limits and time out. The connector chunks IDs "
            "into batches of this size; per-batch failures emit a "
            "structured warning, never a silent drop. "
            "Default 100 halves the TML round-trips vs the previous 50 "
            "at 10K+ scale (saves several minutes of wall-clock) and "
            "still leaves a 10x safety margin below the documented limit. "
            "Lower to 50 (or less) if your TS deployment is "
            "memory-constrained or you've observed batch timeouts."
        ),
    )

    metadata_fetch_batch_size: int = Field(
        default=100,
        gt=0,
        description=(
            "Maximum number of object IDs sent in a single "
            "``metadata/search`` detail fetch (used to backfill column-"
            "level schema for upstream tables). Chunked for the same "
            "reason as ``tml_export_batch_size`` — avoiding per-request "
            "body-size limits on large tenants."
        ),
    )

    org_identifier: Optional[str] = Field(
        default=None,
        description=(
            "Optional ThoughtSpot org identifier — numeric org id "
            '(e.g. ``"615000845"``) or org name (e.g. ``"datahub"``). '
            "Forwarded to every metadata API call so the ingestion is "
            "scoped to a single org. Leave unset to use the principal's "
            "primary org (the default). Single-tenant deployments can "
            "ignore this field. If the principal is not a member of the "
            "specified org, API calls will return 403 and the run will "
            "fail with a permission error — this is intentional, since "
            "silently falling back to the primary org would emit data "
            "into the wrong namespace."
        ),
    )

    @field_validator("base_url", mode="after")
    @classmethod
    def normalize_base_url(cls, v: str) -> str:
        """Remove trailing slashes and API path from base URL."""
        v = v.rstrip("/")
        if v.endswith("/api/rest/2.0"):
            v = v[: -len("/api/rest/2.0")]
        return v

    @field_validator("org_identifier", mode="before")
    @classmethod
    def normalize_org_identifier(cls, v: Optional[str]) -> Optional[str]:
        """Collapse empty / whitespace-only org_identifier to ``None``.

        Operators routinely default this via env var (``${THOUGHTSPOT_ORG}``)
        and end up with an empty string when the var is unset. Treating
        ``""`` as "no org configured" matches operator intent and keeps the
        downstream truthiness checks (``if self.config.org_identifier:``)
        unambiguous.
        """
        if v is None:
            return None
        stripped = v.strip()
        return stripped or None


class ExternalConnectionConfig(ConfigModel):
    """Per-TS-connection cross-platform lineage settings.

    All fields are optional — a connection only needs an entry under
    ``external_connections`` if at least one override is needed.
    Connections absent from the map use the defaults defined here
    (no platform_instance, connector-level ``env``, column names
    lowercased).
    """

    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "DataHub ``platform_instance`` to apply to the upstream URN "
            "for this TS connection's federated tables. Default: no "
            "platform_instance prefix on emitted URNs."
        ),
    )

    env: Optional[str] = Field(
        default=None,
        description=(
            "DataHub ``env`` (PROD / STAGING / DEV / ...) override for "
            "this TS connection's federated tables. Default: inherits "
            "the connector-level ``env``."
        ),
    )

    preserve_column_case: bool = Field(
        default=False,
        description=(
            "Preserve column-name case in emitted cross-platform "
            "fine-grained lineage URNs instead of lowercasing. Default "
            "``false`` because most DataHub source connectors lowercase "
            "column URNs (Databricks, Snowflake, Postgres, MySQL, "
            "Redshift, Hive). Set to ``true`` for BigQuery, SQL Server, "
            "or Databricks tenants using case-quoted identifiers — "
            "anywhere the upstream connector preserves case."
        ),
    )

    # Inherits ``extra="forbid"`` from ``ConfigModel`` plus its
    # ``ignored_types=(cached_property, ...)`` and ``json_schema_extra``.
    # Don't override ``model_config`` here — doing so would replace the
    # inherited settings instead of extending them.


class ThoughtSpotConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for ThoughtSpot metadata extraction."""

    # Connection configuration
    connection: ThoughtSpotConnectionConfig = Field(
        description="ThoughtSpot connection settings"
    )

    # Entity filtering patterns
    workspace_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter workspaces by name. "
            "Example: {'allow': ['^Production.*'], 'deny': ['^Test.*']}"
        ),
    )

    liveboard_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter Liveboards (dashboards) by name. "
            "Example: {'allow': ['.*'], 'deny': ['^Draft.*']}"
        ),
    )

    answer_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter Answers (saved queries) by name. "
            "Example: {'allow': ['.*'], 'deny': ['^Temp.*']}"
        ),
    )

    worksheet_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter Worksheets (logical datasets) by name. "
            "Example: {'allow': ['.*'], 'deny': ['^Legacy.*']}"
        ),
    )

    liveboard_tag_filter: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of TS tag names to filter Liveboards on the "
            "server side. When set, only Liveboards tagged with at least "
            "one of these tags are returned by ``metadata_search``. More "
            "efficient than client-side name filtering for tenants where "
            "the ingestion principal can see far more Liveboards than "
            'the desired subset. Example: ``["Production", "Curated"]``. '
            "Leave unset to ingest all Liveboards (subject to "
            "``liveboard_pattern``)."
        ),
    )

    answer_tag_filter: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of TS tag names to filter Answers on the "
            "server side. Same shape as ``liveboard_tag_filter``. Leave "
            "unset to ingest all Answers (subject to ``answer_pattern``)."
        ),
    )

    # Feature toggles
    include_ownership: bool = Field(
        default=True,
        description=(
            "Extract ownership information from ThoughtSpot. "
            "Owners are mapped to DataHub CorpUser entities."
        ),
    )

    include_usage_stats: bool = Field(
        default=True,
        description=(
            "Emit per-entity view counts as ``DashboardUsageStatistics`` / "
            "``ChartUsageStatistics`` aspects. View counts are the global "
            "cumulative counter returned by ``metadata_search`` when "
            "``include_stats=True`` is sent — the same number shown in the "
            "TS UI's 'Views' column. No additional API round-trip is "
            "performed: the data is piggybacked on the same paginated "
            "``metadata_search`` calls that build the entities. Enabled by "
            "default since the cost is ~50 bytes of wire bytes per entity. "
            "Set to ``false`` to skip emitting the usage aspects entirely."
        ),
    )

    external_connections: Dict[str, ExternalConnectionConfig] = Field(
        default_factory=dict,
        description=(
            "Per-TS-connection cross-platform lineage settings. Keyed "
            "by TS connection GUID (stable across renames, preferred) "
            "or display name (operator-friendly). GUID matches take "
            "precedence. Each value is an ``ExternalConnectionConfig`` "
            "block holding the overrides for that connection — "
            "``platform_instance``, ``env``, and ``preserve_column_case``. "
            "Connections absent from the map use the connector defaults."
        ),
        examples=[
            {
                "conn-guid-abc": {
                    "platform_instance": "prod-dbx",
                    "env": "PROD",
                },
                "Prod BigQuery": {
                    "platform_instance": "prod-bq",
                    "preserve_column_case": True,
                },
            }
        ],
    )

    include_external_lineage: bool = Field(
        default=True,
        description=(
            "Emit upstream lineage from TS Logical Tables to external "
            "sources (Databricks, Snowflake, BigQuery, etc.). Disable to "
            "keep dataset lineage scoped to ThoughtSpot only."
        ),
    )

    # Re-declare the inherited ``stateful_ingestion`` field with the
    # ``StatefulStaleMetadataRemovalConfig`` shape so recipes can set
    # ``remove_stale_metadata`` / ``fail_safe_threshold``. Parameterizing the
    # base class itself (``StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig]``)
    # works at runtime but trips mypy on ``StatefulIngestionSourceBase.__init__``
    # due to generic invariance — Tableau / Looker / Unity use this
    # re-declaration pattern instead.
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion config with stale-entity removal support.",
    )
