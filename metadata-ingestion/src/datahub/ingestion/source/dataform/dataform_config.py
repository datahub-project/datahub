import logging
from typing import Dict, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class DataformCloudConfig(ConfigModel):
    """
    Configuration for connecting to Google Cloud Dataform.
    """

    project_id: str = Field(
        description="The Google Cloud project ID where Dataform is hosted.",
    )

    region: str = Field(
        default="us-central1",
        description="The Google Cloud region where Dataform is deployed.",
    )

    repository_id: str = Field(
        description="The Dataform repository ID to extract metadata from.",
    )

    workspace_id: Optional[str] = Field(
        default=None,
        description="The Dataform workspace ID. If not specified, the default workspace will be used.",
    )

    compilation_result_id: Optional[str] = Field(
        default=None,
        description="The Dataform compilation result ID. If not specified, the latest compilation result will be used.",
    )

    credential: Optional[GCPCredential] = Field(
        default=None,
        description="GCP credential configuration. If not provided, uses default GCP authentication.",
    )

    # Deprecated fields - keeping for backward compatibility
    service_account_key_file: Optional[str] = Field(
        default=None,
        description="[Deprecated] Use 'credential' instead. Path to the service account key file for authentication with Google Cloud.",
    )

    credentials_json: Optional[str] = Field(
        default=None,
        description="[Deprecated] Use 'credential' instead. JSON string of the service account credentials for authentication with Google Cloud.",
    )


class DataformCoreConfig(ConfigModel):
    """
    Configuration for connecting to Dataform Core (local/on-premise).
    """

    project_path: str = Field(
        description="Path to the Dataform project directory containing dataform.json and workflow definitions.",
    )

    compilation_results_path: Optional[str] = Field(
        default=None,
        description="Path to the compiled results JSON file. If not specified, will attempt to compile the project.",
    )

    target_name: Optional[str] = Field(
        default=None,
        description="The target name to use for compilation. If not specified, the default target will be used.",
    )


class DataformEntitiesEnabled(ConfigModel):
    """Controls which Dataform entities are going to be emitted by this source"""

    tables: bool = Field(
        default=True,
        description="Emit metadata for Dataform tables (materialized views and tables).",
    )

    views: bool = Field(
        default=True,
        description="Emit metadata for Dataform views.",
    )

    assertions: bool = Field(
        default=True,
        description="Emit metadata for Dataform assertions (data quality tests).",
    )

    operations: bool = Field(
        default=True,
        description="Emit metadata for Dataform operations (custom SQL operations).",
    )

    declarations: bool = Field(
        default=True,
        description="Emit metadata for Dataform declarations (external data sources).",
    )


class DataformSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    IncrementalLineageConfigMixin,
):
    """
    Configuration for the Dataform metadata source.
    """

    # Connection configuration - either cloud or core, but not both
    cloud_config: Optional[DataformCloudConfig] = Field(
        default=None,
        description="Configuration for Google Cloud Dataform. Use this for cloud-hosted Dataform projects.",
    )

    core_config: Optional[DataformCoreConfig] = Field(
        default=None,
        description="Configuration for Dataform Core. Use this for local or on-premise Dataform projects.",
    )

    # Target platform configuration
    target_platform: str = Field(
        description="The platform that Dataform is deploying to (e.g., bigquery, postgres, redshift, snowflake).",
    )

    target_platform_instance: Optional[str] = Field(
        default=None,
        description="The platform instance for the target platform. Use this if you have multiple instances of the same platform.",
    )

    # Entity filtering
    entities_enabled: DataformEntitiesEnabled = Field(
        default=DataformEntitiesEnabled(),
        description="Controls for enabling/disabling metadata emission for different Dataform entity types.",
    )

    # Filtering patterns
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Dataform table names to filter in ingestion.",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schema names to filter in ingestion.",
    )

    tag_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tags to filter in ingestion.",
    )

    # Advanced lineage and schema configuration
    include_column_lineage: bool = Field(
        default=True,
        description="When enabled, column-level lineage will be extracted from Dataform definitions. "
        "Requires infer_dataform_schemas to be enabled.",
    )

    infer_dataform_schemas: bool = Field(
        default=True,
        description="When enabled, schemas will be inferred from Dataform node definitions.",
    )

    # Override default value to True for incremental lineage
    incremental_lineage: bool = Field(
        default=True,
        description="When enabled, emits incremental/patch lineage for non-Dataform entities. "
        "When disabled, re-states lineage on each run. This requires enabling 'incremental_lineage' "
        "in the counterpart warehouse ingestion (e.g., BigQuery, Redshift, etc).",
    )

    include_compiled_code: bool = Field(
        default=True,
        description="When enabled, includes the compiled code in the emitted metadata.",
    )

    include_database_name: bool = Field(
        default=True,
        description="Whether to add database name to the table URN. "
        "Set to False to skip it for engines like AWS Athena where it's not required.",
    )

    # SQL parsing preferences
    prefer_sql_parser_lineage: bool = Field(
        default=False,
        description="Normally we use Dataform's metadata to generate table lineage. "
        "When enabled, we prefer results from the SQL parser when generating lineage instead. "
        "This can be useful when Dataform models reference tables directly.",
    )

    parse_table_names_from_sql: bool = Field(
        default=False,
        description="Whether to parse table names from SQL queries when lineage is not explicitly defined.",
    )

    # Custom properties and tags
    tag_prefix: str = Field(
        default="dataform:",
        description="Prefix added to tags during ingestion.",
    )

    custom_properties: Dict[str, str] = Field(
        default={},
        description="Custom properties to add to all entities.",
    )

    # Owner extraction
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to extract owner information from Dataform metadata. "
        "The pattern should contain a named group 'owner' to capture the owner name.",
    )

    # Test configuration
    test_warnings_are_errors: bool = Field(
        default=False,
        description="When enabled, Dataform test warnings will be treated as failures.",
    )

    # Git integration
    git_repository_url: Optional[str] = Field(
        default=None,
        description="URL of the Git repository containing the Dataform project for linking to source code.",
    )

    git_branch: Optional[str] = Field(
        default="main",
        description="Git branch to link to in the repository.",
    )

    # Sibling relationships
    dataform_is_primary_sibling: bool = Field(
        default=True,
        description="Controls sibling relationship primary designation between Dataform entities and target platform entities. "
        "When True (default), Dataform entities are primary and target platform entities are secondary. "
        "When False, target platform entities are primary and Dataform entities are secondary. "
        "Uses aspect patches for precise control. Requires DataHub server 1.3.0+.",
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )

    @model_validator(mode="after")
    def validate_config(self) -> "DataformSourceConfig":
        """Validate that exactly one of cloud_config or core_config is provided."""
        if not self.cloud_config and not self.core_config:
            raise ValueError("Either 'cloud_config' or 'core_config' must be provided")

        if self.cloud_config and self.core_config:
            raise ValueError(
                "Only one of 'cloud_config' or 'core_config' can be provided, not both"
            )

        # Validate cloud config authentication
        if self.cloud_config:
            # Check if any authentication method is provided
            has_new_auth = self.cloud_config.credential is not None
            has_legacy_auth = (
                self.cloud_config.service_account_key_file is not None
                or self.cloud_config.credentials_json is not None
            )

            # If no authentication is provided, we'll use default GCP authentication
            # This is useful when running on GCP infrastructure with default service accounts
            if not has_new_auth and not has_legacy_auth:
                logger = logging.getLogger(__name__)
                logger.info(
                    "No explicit authentication configured for Dataform Cloud. "
                    "Will attempt to use default GCP authentication (e.g., from environment, metadata server, etc.)"
                )

        # Validate column lineage configuration
        if self.include_column_lineage and not self.infer_dataform_schemas:
            raise ValueError(
                "include_column_lineage requires infer_dataform_schemas to be enabled."
            )

        return self

    def is_cloud_mode(self) -> bool:
        """Returns True if configured for Google Cloud Dataform."""
        return self.cloud_config is not None

    def is_core_mode(self) -> bool:
        """Returns True if configured for Dataform Core."""
        return self.core_config is not None
