import re
from typing import TYPE_CHECKING, Dict, Literal, Optional

import pydantic
from pydantic import Field

if TYPE_CHECKING:
    from mypy_boto3_quicksight import QuickSightClient
    from mypy_boto3_sts import STSClient

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class ExternalDataSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    """Per-data-source overrides used when stitching QuickSight Datasets to
    their upstream warehouse/database tables.

    Keyed in :class:`QuickSightSourceConfig.external_data_sources` by the stable
    QuickSight ``DataSourceId`` (UUID) — preferred because it survives renames in
    the QuickSight UI — with the data source display name accepted as a fallback.
    UUID matches take precedence over name matches.
    """

    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether to lower-case identifiers when constructing upstream "
        "Dataset URNs. Must match the `convert_urns_to_lowercase` setting used by the "
        "corresponding upstream connector recipe (e.g. Snowflake / BigQuery typically "
        "preserve case).",
    )
    default_database: Optional[str] = Field(
        default=None,
        description="Default database/catalog name used as the SQL-parser fallback "
        "when a CustomSql definition references unqualified tables.",
    )
    default_schema: Optional[str] = Field(
        default=None,
        description="Default schema name used as the SQL-parser fallback when a "
        "CustomSql definition references unqualified tables.",
    )


class QuickSightSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    AwsConnectionConfig,
):
    """Configuration for the Amazon QuickSight ingestion source.

    Reuses :class:`AwsConnectionConfig` for AWS authentication (explicit keys,
    named profile, IAM role assumption, or instance-role auto-detection).
    QuickSight is regional, so a single ingestion run targets one ``aws_region``.
    """

    aws_account_id: Optional[str] = Field(
        default=None,
        description="AWS account ID that owns the QuickSight assets. Auto-detected "
        "via `sts:GetCallerIdentity` when not provided.",
    )

    # --- Lineage & enrichment toggles ---
    extract_lineage: bool = Field(
        default=True,
        description="Whether to extract upstream lineage from QuickSight Datasets to "
        "their backing warehouse/database tables.",
    )
    extract_column_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage for CustomSql datasets "
        "via sqlglot. Requires `extract_lineage` to be enabled.",
    )
    extract_ownership: bool = Field(
        default=True,
        description="Whether to extract ownership from QuickSight resource permissions.",
    )
    strip_user_email_domain: bool = Field(
        default=False,
        description="When extracting ownership, strip the email domain from user "
        "identities so the CorpUser URN uses the bare username (e.g. "
        "`jane@acme.com` -> `jane`). Mirrors Informatica's `strip_user_email_domain` "
        "and Looker's `strip_user_ids_from_email`. For IAM/SSO-federated principals "
        "the QuickSight identity is the role-session name (typically the email), "
        "which is used regardless of this flag.",
    )
    extract_tags: bool = Field(
        default=True,
        description="Whether to extract AWS resource tags on QuickSight assets.",
    )

    # --- Cost / cardinality knobs ---
    extract_dashboard_definitions: bool = Field(
        default=True,
        description="Whether to fetch full dashboard definitions (sheets/visuals). "
        "These payloads are large; disabling this skips Chart entity emission.",
    )
    extract_analysis_definitions: bool = Field(
        default=True,
        description="Whether to fetch full analysis definitions (sheets/visuals). "
        "These payloads are large.",
    )
    extract_users_and_groups: bool = Field(
        default=False,
        description="Whether to extract QuickSight users and groups (opt-in; often "
        "noisy and requires additional permissions).",
    )

    # --- Container hierarchy ---
    add_shared_folders_container: bool = Field(
        default=False,
        description="When enabled, a synthetic `Shared folders` container is emitted "
        "as the root of the folder hierarchy and every top-level QuickSight folder is "
        "nested beneath it — mirroring the `Shared folders` section in the QuickSight "
        "left-nav. (QuickSight's `Shared folders` is a UI category for SHARED-type "
        "folders, not a real folder, so it is only synthesized when this is on.) Off "
        "by default since we only ingest shared folders, making the level a constant "
        "prefix. Loose assets that belong to no folder are unaffected.",
    )
    add_namespace_container: bool = Field(
        default=False,
        description="When enabled, QuickSight namespaces are emitted as containers "
        "and appear as a level in the browse hierarchy. Most accounts have only the "
        "single built-in `default` namespace, so this is off by default (assets sit "
        "directly under the platform / platform_instance, or under their folder). "
        "Enable it for Enterprise accounts that use multiple namespaces. Mirrors "
        "Tableau's `add_site_container`. Account separation is handled via "
        "`platform_instance`, not a container — matching the Glue / Redshift / "
        "PowerBI / Informatica convention.",
    )

    # --- Filtering ---
    namespace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight namespaces (only relevant "
        "when `add_namespace_container` is enabled).",
    )
    folder_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight folders.",
    )
    dashboard_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight dashboards by name.",
    )
    analysis_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight analyses by name.",
    )
    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight datasets by name.",
    )
    data_source_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter QuickSight data sources by name.",
    )
    tag_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter assets by their AWS resource tags "
        "(formatted as `key=value`).",
    )

    # --- Cross-platform lineage stitching ---
    external_data_sources: Dict[str, ExternalDataSourceConfig] = Field(
        default_factory=dict,
        description="Per-data-source overrides for cross-platform lineage stitching, "
        "keyed by QuickSight `DataSourceId` (UUID, preferred) or display name "
        "(fallback). Provides the upstream platform_instance, env, URN casing, and "
        "SQL-parser default database/schema.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration (enables stale entity removal).",
    )

    # Override AwsConnectionConfig's default ("standard"): QuickSight enforces
    # low per-API TPS limits, so adaptive retries (client-side rate limiting with
    # backoff) markedly reduce ThrottlingException failures on larger accounts.
    aws_retry_mode: Literal["legacy", "standard", "adaptive"] = Field(
        default="adaptive",
        description="Retry mode for failed AWS requests. Defaults to `adaptive` for "
        "QuickSight, whose per-API TPS throttling benefits from client-side rate "
        "limiting. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs.",
    )

    def get_quicksight_client(self) -> "QuickSightClient":
        # Retries (adaptive by default, see aws_retry_mode) are applied via
        # _aws_config() to absorb QuickSight's per-API TPS throttling.
        return self.get_session().client("quicksight", config=self._aws_config())

    def get_sts_client(self) -> "STSClient":
        return self.get_session().client("sts", config=self._aws_config())

    @pydantic.field_validator("aws_account_id")
    @classmethod
    def validate_aws_account_id(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{12}", v):
            raise ValueError("aws_account_id must be a 12-digit AWS account ID")
        return v

    @pydantic.model_validator(mode="after")
    def validate_region_present(self) -> "QuickSightSourceConfig":
        # QuickSight is a regional service; every API call requires a region.
        if not self.aws_region:
            raise ValueError("aws_region is required for the QuickSight source")
        return self
