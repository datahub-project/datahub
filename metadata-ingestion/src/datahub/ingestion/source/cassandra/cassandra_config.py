import datetime
from typing import List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled

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
        description="The Astra DB application token used for authentication.",
    )

    secure_connect_bundle: str = Field(
        description="File path to the Secure Connect Bundle (.zip) used for a secure connection to DataStax Astra DB.",
    )

    connect_timeout: int = Field(
        default=60,
        description="Timeout in seconds for establishing new connections to Cassandra.",
    )

    request_timeout: int = Field(
        default=60, description="Timeout in seconds for individual Cassandra requests."
    )


class ProfileConfig(GEProfilingConfig):

    row_count: bool = True
    column_count: bool = True

    # Below Configs inherited from GEProfilingConfig
    # but not used in Dremio so we hide them from docs.
    include_field_null_count: bool = Field(
        default=False,
        hidden_from_docs=True,
        description="Null count has some issue in cassandra.",
    )
    partition_profiling_enabled: bool = Field(default=True, hidden_from_docs=True)
    profile_table_row_count_estimate_only: bool = Field(
        default=False, hidden_from_docs=True
    )
    query_combiner_enabled: bool = Field(default=True, hidden_from_docs=True)
    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = Field(
        default=None, hidden_from_docs=True
    )
    profile_if_updated_since_days: Optional[pydantic.PositiveFloat] = Field(
        default=None, hidden_from_docs=True
    )
    profile_table_size_limit: Optional[int] = Field(
        default=5,
        description="Profile tables only if their size is less then specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `snowflake` and `BigQuery`",
        hidden_from_docs=True,
    )

    profile_table_row_limit: Optional[int] = Field(
        default=5000000,
        hidden_from_docs=True,
        description="Profile tables only if their row count is less then specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `snowflake` and `BigQuery`",
    )

    partition_datetime: Optional[datetime.datetime] = Field(
        default=None,
        hidden_from_docs=True,
        description="If specified, profile only the partition which matches this datetime. "
        "If not specified, profile the latest partition. Only Bigquery supports this.",
    )
    use_sampling: bool = Field(
        default=True,
        hidden_from_docs=True,
        description="Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. "
        "If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
    )

    sample_size: int = Field(
        default=10000,
        hidden_from_docs=True,
        description="Number of rows to be sampled from table for column level profiling."
        "Applicable only if `use_sampling` is set to True.",
    )
    profile_external_tables: bool = Field(
        default=False,
        hidden_from_docs=True,
        description="Whether to profile external tables. Only Snowflake and Redshift supports this.",
    )

    tags_to_ignore_sampling: Optional[List[str]] = pydantic.Field(
        default=None,
        hidden_from_docs=True,
        description=(
            "Fixed list of tags to ignore sampling."
            " If not specified, tables will be sampled based on `use_sampling`."
        ),
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

    datastax_astra_cloud_config: Optional[CassandraCloudConfig] = Field(
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

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )

    profiling: ProfileConfig = Field(
        default=ProfileConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @property
    def cloud(self) -> bool:
        """
        Returns True if datastax_astra_cloud_config is present.
        """
        return (
            self.datastax_astra_cloud_config is not None
            and bool(self.datastax_astra_cloud_config.secure_connect_bundle)
            and bool(self.datastax_astra_cloud_config.token)
        )
