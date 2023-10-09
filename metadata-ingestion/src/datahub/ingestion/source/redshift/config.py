import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import root_validator
from pydantic.fields import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.sql.postgres import BasePostgresConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

logger = logging.Logger(__name__)


# The lineage modes are documented in the Redshift source's docstring.
class LineageMode(Enum):
    SQL_BASED = "sql_based"
    STL_SCAN_BASED = "stl_scan_based"
    MIXED = "mixed"


class S3LineageProviderConfig(ConfigModel):
    """
    Any source that produces s3 lineage from/to Datasets should inherit this class.
    """

    path_specs: List[PathSpec] = Field(
        default=[],
        description="List of PathSpec. See below the details about PathSpec",
    )

    strip_urls: bool = Field(
        default=True,
        description="Strip filename from s3 url. It only applies if path_specs are not specified.",
    )


class S3DatasetLineageProviderConfigBase(ConfigModel):
    """
    Any source that produces s3 lineage from/to Datasets should inherit this class.
    This is needeed to group all lineage related configs under `s3_lineage_config` config property.
    """

    s3_lineage_config: S3LineageProviderConfig = Field(
        default=S3LineageProviderConfig(),
        description="Common config for S3 lineage generation",
    )


class RedshiftUsageConfig(BaseUsageConfig, StatefulUsageConfigMixin):
    email_domain: Optional[str] = Field(
        default=None,
        description="Email domain of your organisation so users can be displayed on UI appropriately.",
    )


class RedshiftConfig(
    BasePostgresConfig,
    DatasetLineageProviderConfigBase,
    S3DatasetLineageProviderConfigBase,
    RedshiftUsageConfig,
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
):
    database: str = Field(default="dev", description="database")

    # Although Amazon Redshift is compatible with Postgres's wire format,
    # we actually want to use the sqlalchemy-redshift package and dialect
    # because it has better caching behavior. In particular, it queries
    # the full table, column, and constraint information in a single larger
    # query, and then simply pulls out the relevant information as needed.
    # Because of this behavior, it uses dramatically fewer round trips for
    # large Redshift warehouses. As an example, see this query for the columns:
    # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/60b4db04c1d26071c291aeea52f1dcb5dd8b0eb0/sqlalchemy_redshift/dialect.py#L745.
    scheme = Field(
        default="redshift+psycopg2",
        description="",
        hidden_from_schema=True,
    )

    _database_alias_deprecation = pydantic_field_deprecated(
        "database_alias",
        message="database_alias is deprecated. Use platform_instance instead.",
    )

    default_schema: str = Field(
        default="public",
        description="The default schema to use if the sql parser fails to parse the schema with `sql_based` lineage collector",
    )

    include_table_lineage: Optional[bool] = Field(
        default=True, description="Whether table lineage should be ingested."
    )
    include_copy_lineage: Optional[bool] = Field(
        default=True,
        description="Whether lineage should be collected from copy commands",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistic. email_domain config parameter needs to be set if enabled",
    )

    include_unload_lineage: Optional[bool] = Field(
        default=True,
        description="Whether lineage should be collected from unload commands",
    )

    capture_lineage_query_parser_failures: Optional[bool] = Field(
        hide_from_schema=True,
        default=False,
        description="Whether to capture lineage query parser errors with dataset properties for debugging",
    )

    table_lineage_mode: Optional[LineageMode] = Field(
        default=LineageMode.STL_SCAN_BASED,
        description="Which table lineage collector mode to use. Available modes are: [stl_scan_based, sql_based, mixed]",
    )
    extra_client_options: Dict[str, Any] = {}

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<database>.<schema>`.",
    )

    @root_validator(pre=True)
    def check_email_is_set_on_usage(cls, values):
        if values.get("include_usage_statistics"):
            assert (
                "email_domain" in values and values["email_domain"]
            ), "email_domain needs to be set if usage is enabled"
        return values

    @root_validator()
    def check_database_or_database_alias_set(cls, values):
        assert values.get("database") or values.get(
            "database_alias"
        ), "either database or database_alias must be set"
        return values

    @root_validator(pre=False)
    def backward_compatibility_configs_set(cls, values: Dict) -> Dict:
        match_fully_qualified_names = values.get("match_fully_qualified_names")

        schema_pattern: Optional[AllowDenyPattern] = values.get("schema_pattern")

        if (
            schema_pattern is not None
            and schema_pattern != AllowDenyPattern.allow_all()
            and match_fully_qualified_names is not None
            and not match_fully_qualified_names
        ):
            logger.warning(
                "Please update `schema_pattern` to match against fully qualified schema name `<database_name>.<schema_name>` and set config `match_fully_qualified_names : True`."
                "Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. "
                "The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
            )
        return values
