import logging
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, cast

from pydantic import Field, SecretStr, root_validator, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source_config.sql.snowflake import (
    BaseSnowflakeConfig,
    SnowflakeConfig,
)
from datahub.ingestion.source_config.usage.snowflake_usage import SnowflakeUsageConfig
from datahub.utilities.global_warning_util import add_global_warning

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
class DatabaseId:
    database: str = Field(
        description="Database created from share in consumer account."
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of consumer snowflake account.",
    )


class SnowflakeShareConfig(ConfigModel):
    database: str = Field(description="Database from which share is created.")
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for snowflake account in which share is created.",
    )

    consumers: Set[DatabaseId] = Field(
        description="List of databases created in consumer accounts."
    )

    @property
    def source_database(self) -> DatabaseId:
        return DatabaseId(self.database, self.platform_instance)


class SnowflakeV2Config(
    SnowflakeConfig,
    SnowflakeUsageConfig,
    StatefulLineageConfigMixin,
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
        default=True,
        description="Populates view->view and table->view column lineage using DataHub's sql parser.",
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

    _use_legacy_lineage_method_removed = pydantic_removed_field(
        "use_legacy_lineage_method"
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
        description="[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to "
        "match the entire table name in database.schema.table format. Defaults are to set in such a way "
        "to ignore the temporary staging tables created by known ETL tools.",
    )

    rename_upstreams_deny_pattern_to_temporary_table_pattern = pydantic_renamed_field(
        "upstreams_deny_pattern", "temporary_tables_pattern"
    )

    shares: Optional[Dict[str, SnowflakeShareConfig]] = Field(
        default=None,
        description="Required if current account owns or consumes snowflake share."
        "If specified, connector creates lineage and siblings relationship between current account's database tables "
        "and consumer/producer account's database tables."
        " Map of share name -> details of share.",
    )

    email_as_user_identifier: bool = Field(
        default=True,
        description="Format user urns as an email, if the snowflake user's email is set. If `email_domain` is "
        "provided, generates email addresses for snowflake users with unset emails, based on their "
        "username.",
    )

    @validator("convert_urns_to_lowercase")
    def validate_convert_urns_to_lowercase(cls, v):
        if not v:
            add_global_warning(
                "Please use `convert_urns_to_lowercase: True`, otherwise lineage to other sources may not work correctly."
            )

        return v

    @validator("include_column_lineage")
    def validate_include_column_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_column_lineage to be set."
            )
        return v

    @root_validator(pre=False, skip_on_failure=True)
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

    @validator("shares")
    def validate_shares(
        cls, shares: Optional[Dict[str, SnowflakeShareConfig]], values: Dict
    ) -> Optional[Dict[str, SnowflakeShareConfig]]:
        current_platform_instance = values.get("platform_instance")

        if shares:
            # Check: platform_instance should be present
            if current_platform_instance is None:
                logger.info(
                    "It is advisable to use `platform_instance` when ingesting from multiple snowflake accounts, if they contain databases with same name. "
                    "Setting `platform_instance` allows distinguishing such databases without conflict and correctly ingest their metadata."
                )

            databases_included_in_share: List[DatabaseId] = []
            databases_created_from_share: List[DatabaseId] = []

            for share_details in shares.values():
                shared_db = DatabaseId(
                    share_details.database, share_details.platform_instance
                )
                if current_platform_instance:
                    assert all(
                        consumer.platform_instance != share_details.platform_instance
                        for consumer in share_details.consumers
                    ), "Share's platform_instance can not be same as consumer's platform instance. Self-sharing not supported in Snowflake."

                databases_included_in_share.append(shared_db)
                databases_created_from_share.extend(share_details.consumers)

            for db_from_share in databases_created_from_share:
                assert (
                    db_from_share not in databases_included_in_share
                ), "Database included in a share can not be present as consumer in any share."
                assert (
                    databases_created_from_share.count(db_from_share) == 1
                ), "Same database can not be present as consumer in more than one share."

        return shares

    def outbounds(self) -> Dict[str, Set[DatabaseId]]:
        """
        Returns mapping of
            database included in current account's outbound share -> all databases created from this share in other accounts
        """
        outbounds: Dict[str, Set[DatabaseId]] = defaultdict(set)
        if self.shares:
            for share_name, share_details in self.shares.items():
                if share_details.platform_instance == self.platform_instance:
                    logger.debug(
                        f"database {share_details.database} is included in outbound share(s) {share_name}."
                    )
                    outbounds[share_details.database].update(share_details.consumers)
        return outbounds

    def inbounds(self) -> Dict[str, DatabaseId]:
        """
        Returns mapping of
            database created from an current account's inbound share -> other-account database from which this share was created
        """
        inbounds: Dict[str, DatabaseId] = {}
        if self.shares:
            for share_name, share_details in self.shares.items():
                for consumer in share_details.consumers:
                    if consumer.platform_instance == self.platform_instance:
                        logger.debug(
                            f"database {consumer.database} is created from inbound share {share_name}."
                        )
                        inbounds[consumer.database] = share_details.source_database
                        if self.platform_instance:
                            break
                        # If not using platform_instance, any one of consumer databases
                        # can be the database from this instance. so we include all relevant
                        # databases in inbounds.
                else:
                    logger.info(
                        f"Skipping Share {share_name}, as it does not include current platform instance {self.platform_instance}",
                    )
        return inbounds
