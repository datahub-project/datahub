from typing import List, Optional

from pydantic import Field

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
    server: str = Field(
        description="Informix server name (INFORMIXSERVER)."
    )
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

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases (owners layer is schema).",
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for owners/schemas to filter.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion / stale-entity removal config."
    )
