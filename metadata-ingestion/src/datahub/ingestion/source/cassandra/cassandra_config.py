from typing import Optional

from pydantic import Field, root_validator

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

# - Referencing https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useQuerySystem.html#Table3.ColumnsinSystem_SchemaTables-Cassandra3.0 - #
# this keyspace contains details about the cassandra cluster's keyspaces, tables, and columns
SYSTEM_SCHEMA_KEYSPACE_NAME = "system_schema"

# Reference:
# https://docs.datastax.com/en/astra-db-serverless/databases/python-driver.html
# https://docs.datastax.com/en/astra-db-serverless/databases/python-driver.html#production-configuration


class CassandraCloudConfig(ConfigModel):
    """
    Configuration for connecting to DataStax Astra DB in the cloud.
    """

    token: str = Field(
        default="",
        description="The Astra DB application token used for authentication.",
    )

    secure_connect_bundle: str = Field(
        default="",
        description="File path to the Secure Connect Bundle (.zip) used for a secure connection to DataStax Astra DB.",
    )

    connect_timeout: int = Field(
        default=60,
        description="Timeout in seconds for establishing new connections to Cassandra.",
    )

    request_timeout: int = Field(
        default=60, description="Timeout in seconds for individual Cassandra requests."
    )


class CassandraSourceConfig(
    PlatformInstanceConfigMixin, StatefulIngestionConfigBase, EnvConfigMixin
):
    """
    Configuration for connecting to a Cassandra or DataStax Astra DB source.
    """

    contact_point: str = Field(
        default="localhost",
        description="Domain or IP address of the Cassandra instance (excluding port).",
    )

    port: int = Field(
        default=9042, description="Port number to connect to the Cassandra instance."
    )

    username: Optional[str] = Field(
        default=None,
        description=f"Username credential with read access to the {SYSTEM_SCHEMA_KEYSPACE_NAME} keyspace.",
    )

    password: Optional[str] = Field(
        default=None,
        description="Password credential associated with the specified username.",
    )

    cloud_config: Optional[CassandraCloudConfig] = Field(
        default=None,
        description="Configuration for cloud-based Cassandra, such as DataStax Astra DB.",
    )

    keyspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter keyspaces for ingestion.",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter keyspaces.tables for ingestion.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )

    @root_validator(pre=True)
    def validate_cloud_config(cls, values):
        cloud_config = values.get("cloud_config")
        if cloud_config:
            # Check if both secure_connect_bundle and token are present in cloud_config
            if not cloud_config.get("secure_connect_bundle"):
                raise ValueError(
                    "When using cloud_config, secure_connect_bundle must be provided."
                )
            if not cloud_config.get("token"):
                raise ValueError("When using cloud_config, token must be provided.")
        return values

    @property
    def cloud(self) -> bool:
        """
        Returns True if cloud_config is present.
        """
        return (
            self.cloud_config is not None
            and bool(self.cloud_config.secure_connect_bundle)
            and bool(self.cloud_config.token)
        )
