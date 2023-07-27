import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Sequence, cast

from pydantic import Field, SecretStr, root_validator, validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source_config.sql.snowflake import (
    BaseSnowflakeConfig,
    SnowflakeConfig,
)
from datahub.ingestion.source_config.usage.snowflake_usage import SnowflakeUsageConfig

logger = logging.Logger(__name__)

# FIVETRAN creates temporary tables in schema named FIVETRAN_xxx_STAGING.
# Ref - https://support.fivetran.com/hc/en-us/articles/1500003507122-Why-Is-There-an-Empty-Schema-Named-Fivetran-staging-in-the-Destination-
#
# DBT incremental models create temporary tables ending with __dbt_tmp
# Ref - https://discourse.getdbt.com/t/handling-bigquery-incremental-dbt-tmp-tables/7540
DEFAULT_TABLES_DENY_LIST = [
    r".*\.FIVETRAN_.*_STAGING\..*",  # fivetran
    r".*__DBT_TMP$",  # dbt
    rf".*\.SEGMENT_{UUID_REGEX}",  # segment
    rf".*\.STAGING_.*_{UUID_REGEX}",  # stitch
]


class TagOption(str, Enum):
    with_lineage = "with_lineage"
    without_lineage = "without_lineage"
    skip = "skip"


@dataclass(frozen=True)
class SnowflakeDatabaseDataHubId:
    platform_instance: str
    database_name: str


class SnowflakeV2Config(
    SnowflakeConfig,
    SnowflakeUsageConfig,
    StatefulUsageConfigMixin,
    StatefulProfilingConfigMixin,
    ClassificationSourceConfigMixin,
):
    convert_urns_to_lowercase: bool = Field(
        default=True,
    )

    include_usage_stats: bool = Field(
        default=True,
        description="If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
    )

    include_technical_schema: bool = Field(
        default=True,
        description="If enabled, populates the snowflake technical schema and descriptions.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above.",
    )

    include_view_column_lineage: bool = Field(
        default=False,
        description="Populates view->view and table->view column lineage.",
    )

    _check_role_grants_removed = pydantic_removed_field("check_role_grants")
    _provision_role_removed = pydantic_removed_field("provision_role")

    extract_tags: TagOption = Field(
        default=TagOption.skip,
        description="""Optional. Allowed values are `without_lineage`, `with_lineage`, and `skip` (default). `without_lineage` only extracts tags that have been applied directly to the given entity. `with_lineage` extracts both directly applied and propagated tags, but will be significantly slower. See the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/object-tagging.html#tag-lineage) for information about tag lineage/propagation. """,
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate Snowsight url for Snowflake Objects",
    )

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
    )

    use_legacy_lineage_method: bool = Field(
        default=False,
        description=(
            "Whether to use the legacy lineage computation method. "
            "By default, uses new optimised lineage extraction method that requires less ingestion process memory. "
            "Table-to-view and view-to-view column-level lineage are not supported with the legacy method."
        ),
    )

    validate_upstreams_against_patterns: bool = Field(
        default=True,
        description="Whether to validate upstream snowflake tables against allow-deny patterns",
    )

    tag_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for tags to include in ingestion. Only used if `extract_tags` is enabled.",
    )

    # This is required since access_history table does not capture whether the table was temporary table.
    temporary_tables_pattern: List[str] = Field(
        default=DEFAULT_TABLES_DENY_LIST,
        description="[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools. Not used if `use_legacy_lineage_method=True`",
    )

    rename_upstreams_deny_pattern_to_temporary_table_pattern = pydantic_renamed_field(
        "upstreams_deny_pattern", "temporary_tables_pattern"
    )

    inbound_shares_map: Optional[Dict[str, SnowflakeDatabaseDataHubId]] = Field(
        default=None,
        description="Required if the current account has any database created from inbound snowflake share."
        " If specified, connector creates lineage and siblings relationship between current account's database tables and original database tables from which snowflake share was created."
        " Map of database name -> (platform instance of snowflake account containing original database, original database name).",
    )

    outbound_shares_map: Optional[Dict[str, List[SnowflakeDatabaseDataHubId]]] = Field(
        default=None,
        description="Required if the current account has created any outbound snowflake shares and there is at least one consumer account in which database is created from such share."
        " If specified, connector creates siblings relationship between current account's database tables and all database tables created in consumer accounts from the share including current account's database."
        " Map of database name X -> list of (platform instance of snowflake consumer account who've created database from share, name of database created from share) for all shares created from database name X.",
    )

    @validator("include_column_lineage")
    def validate_include_column_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_column_lineage to be set."
            )
        return v

    @root_validator(pre=False)
    def validate_unsupported_configs(cls, values: Dict) -> Dict:
        value = values.get("include_read_operational_stats")
        if value is not None and value:
            raise ValueError(
                "include_read_operational_stats is not supported. Set `include_read_operational_stats` to False.",
            )

        match_fully_qualified_names = values.get("match_fully_qualified_names")

        schema_pattern: Optional[AllowDenyPattern] = values.get("schema_pattern")

        if (
            schema_pattern is not None
            and schema_pattern != AllowDenyPattern.allow_all()
            and match_fully_qualified_names is not None
            and not match_fully_qualified_names
        ):
            logger.warning(
                "Please update `schema_pattern` to match against fully qualified schema name `<catalog_name>.<schema_name>` and set config `match_fully_qualified_names : True`."
                "Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. "
                "The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
            )

        # Always exclude reporting metadata for INFORMATION_SCHEMA schema
        if schema_pattern is not None and schema_pattern:
            logger.debug("Adding deny for INFORMATION_SCHEMA to schema_pattern.")
            cast(AllowDenyPattern, schema_pattern).deny.append(r".*INFORMATION_SCHEMA$")

        include_technical_schema = values.get("include_technical_schema")
        include_profiles = (
            values.get("profiling") is not None and values["profiling"].enabled
        )
        delete_detection_enabled = (
            values.get("stateful_ingestion") is not None
            and values["stateful_ingestion"].enabled
            and values["stateful_ingestion"].remove_stale_metadata
        )

        # TODO: Allow lineage extraction and profiling irrespective of basic schema extraction,
        # as it seems possible with some refactor
        if not include_technical_schema and any(
            [include_profiles, delete_detection_enabled]
        ):
            raise ValueError(
                "Cannot perform Deletion Detection or Profiling without extracting snowflake technical schema. Set `include_technical_schema` to True or disable Deletion Detection and Profiling."
            )

        return values

    def get_sql_alchemy_url(
        self,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        return BaseSnowflakeConfig.get_sql_alchemy_url(
            self, database=database, username=username, password=password, role=role
        )

    @property
    def parse_view_ddl(self) -> bool:
        return self.include_view_column_lineage

    @root_validator(pre=False)
    def validator_inbound_outbound_shares_map(cls, values: Dict) -> Dict:
        inbound_shares_map: Dict[str, SnowflakeDatabaseDataHubId] = (
            values.get("inbound_shares_map") or {}
        )
        outbound_shares_map: Dict[str, List[SnowflakeDatabaseDataHubId]] = (
            values.get("outbound_shares_map") or {}
        )

        # Check: same database from current instance as inbound and outbound
        common_keys = [key for key in inbound_shares_map if key in outbound_shares_map]

        assert (
            len(common_keys) == 0
        ), "Same database can not be present in both `inbound_shares_map` and `outbound_shares_map`."

        current_platform_instance = values.get("platform_instance")

        # Check: current platform_instance present as inbound and outbound
        if current_platform_instance and any(
            [
                db.platform_instance == current_platform_instance
                for db in inbound_shares_map.values()
            ]
        ):
            raise ValueError(
                "Current `platform_instance` can not be present as any database in `inbound_shares_map`."
                "Self-sharing not supported in Snowflake. Please check your configuration."
            )

        if current_platform_instance and any(
            [
                db.platform_instance == current_platform_instance
                for dbs in outbound_shares_map.values()
                for db in dbs
            ]
        ):
            raise ValueError(
                "Current `platform_instance` can not be present as any database in `outbound_shares_map`."
                "Self-sharing not supported in Snowflake. Please check your configuration."
            )

        # Check: platform_instance should be present
        if (
            inbound_shares_map or outbound_shares_map
        ) and not current_platform_instance:
            logger.warn(
                "Did you forget to set `platform_instance` for current ingestion ?"
                "It is advisable to use `platform_instance` when ingesting from multiple snowflake accounts."
            )

        # Check: same database from some platform instance as inbound and outbound
        other_platform_instance_databases: Sequence[SnowflakeDatabaseDataHubId] = [
            db for db in set(inbound_shares_map.values())
        ] + [db for dbs in outbound_shares_map.values() for db in dbs]

        for other_instance_db in other_platform_instance_databases:
            assert (
                other_platform_instance_databases.count(other_instance_db) == 1
            ), "A database can exist only once either in `inbound_shares_map` or in `outbound_shares_map`."

        return values
