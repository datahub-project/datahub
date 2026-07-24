from typing import Any, Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
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

DEFAULT_JDBC_DRIVER_VERSION = "4.50.10"
DEFAULT_BSON_VERSION = "4.11.1"


class InformixSourceConfig(
    PlatformInstanceConfigMixin, StatefulIngestionConfigBase, EnvConfigMixin
):
    host_port: str = Field(
        default="localhost:9088", description="Informix host and port."
    )
    server: str = Field(description="Informix server name (INFORMIXSERVER).")
    database: str = Field(description="Informix database to ingest from.")
    username: Optional[str] = Field(default=None, description="Login user.")
    password: Optional[TransparentSecretStr] = Field(
        default=None, description="Login password."
    )
    extra_props: str = Field(
        default="",
        description="Extra JDBC properties appended to the connection URL, "
        "e.g. 'DB_LOCALE=en_US.utf8;CLIENT_LOCALE=en_US.utf8'.",
    )

    # driver provisioning (see plan Task 2)
    driver_jar_paths: Optional[List[str]] = Field(
        default=None,
        description="Explicit paths to the Informix JDBC jar and org.mongodb bson "
        "jar. If set, no download is attempted (use for air-gapped environments).",
    )
    accept_ibm_jdbc_license: bool = Field(
        default=False,
        description="Set true to allow auto-downloading the proprietary IBM "
        "Informix JDBC driver from Maven Central under the IBM Informix JDBC "
        "Software License Agreement. Ignored when driver_jar_paths is set.",
    )
    jdbc_driver_version: str = Field(
        default=DEFAULT_JDBC_DRIVER_VERSION,
        description="Pinned com.ibm.informix:jdbc version to download.",
    )
    bson_version: str = Field(
        default=DEFAULT_BSON_VERSION,
        description="Pinned org.mongodb:bson version to download.",
    )
    driver_cache_dir: Optional[str] = Field(
        default=None,
        description="Directory to cache downloaded jars. Defaults to "
        "~/.datahub/jars/informix.",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for owners/schemas to filter in ingestion. "
        "Specify regex to only match the owner (schema) name.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex "
        "to match the entire table name in database.owner.table format.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: defaults "
        "to table_pattern if not specified. Specify regex to match the entire view "
        "name in database.owner.view format.",
    )
    include_tables: bool = Field(
        default=True, description="Whether tables should be ingested."
    )
    include_views: bool = Field(
        default=True, description="Whether views should be ingested."
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default_factory=dict,
        description="Attach domains to databases, schemas or tables during ingestion "
        "using regex patterns. Domain key can be a guid like "
        "*urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "
        '"Marketing". If you provide strings, then datahub will attempt to resolve '
        "this name to a guid, and will error out if this fails. There can be multiple "
        "domain keys specified.",
    )
    include_row_counts: bool = Field(
        default=True,
        description="Emit approximate row counts from systables.nrows.",
    )
    include_foreign_keys: bool = Field(
        default=True,
        description="Extract foreign-key relationships from sysconstraints/sysreferences.",
    )
    include_view_lineage: bool = Field(
        default=True,
        description="Extract table- and column-level lineage for views by parsing their SQL definitions.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion / stale-entity removal config."
    )

    @model_validator(mode="before")
    @classmethod
    def view_pattern_is_table_pattern_unless_specified(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Mirrors datahub.ingestion.source.sql.sql_config.SQLFilterConfig so view
        # filtering matches every other SQL source (can't import it directly: that
        # module pulls in sqlalchemy, which this dialect-less connector avoids).
        view_pattern = values.get("view_pattern")
        table_pattern = values.get("table_pattern")
        if table_pattern and not view_pattern:
            values["view_pattern"] = table_pattern
        return values
