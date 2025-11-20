import logging
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import model_validator
from pydantic.fields import Field

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.configuration.source_common import DatasetLineageProviderConfigBase
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
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

    ignore_non_path_spec_path: bool = Field(
        default=False,
        description="Ignore paths that are not match in path_specs. It only applies if path_specs are specified.",
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
    BasicSQLAlchemyConfig,
    DatasetLineageProviderConfigBase,
    S3DatasetLineageProviderConfigBase,
    IncrementalLineageConfigMixin,
    RedshiftUsageConfig,
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    ClassificationSourceConfigMixin,
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
    scheme: HiddenFromDocs[str] = Field(
        default="redshift+redshift_connector",
        description="",
    )

    _database_alias_removed = pydantic_removed_field("database_alias")
    _use_lineage_v2_removed = pydantic_removed_field("use_lineage_v2")
    _rename_lineage_v2_generate_queries_to_lineage_generate_queries = (
        pydantic_renamed_field(
            "lineage_v2_generate_queries", "lineage_generate_queries"
        )
    )

    default_schema: str = Field(
        default="public",
        description="The default schema to use if the sql parser fails to parse the schema with `sql_based` lineage collector",
    )

    is_serverless: bool = Field(
        default=False,
        description="Whether target Redshift instance is serverless (alternative is provisioned cluster)",
    )

    lineage_generate_queries: bool = Field(
        default=True,
        description="Whether to generate queries entities for the SQL-based lineage collector.",
    )

    include_table_lineage: bool = Field(
        default=True, description="Whether table lineage should be ingested."
    )
    include_copy_lineage: bool = Field(
        default=True,
        description="Whether lineage should be collected from copy commands",
    )
    include_share_lineage: bool = Field(
        default=True,
        description="Whether lineage should be collected from datashares",
    )

    include_usage_statistics: bool = Field(
        default=False,
        description="Generate usage statistic. email_domain config parameter needs to be set if enabled",
    )

    include_unload_lineage: bool = Field(
        default=True,
        description="Whether lineage should be collected from unload commands",
    )

    include_table_rename_lineage: bool = Field(
        default=True,
        description="Whether we should follow `alter table ... rename to` statements when computing lineage. ",
    )
    table_lineage_mode: LineageMode = Field(
        default=LineageMode.MIXED,
        description="Which table lineage collector mode to use. Available modes are: [stl_scan_based, sql_based, mixed]",
    )
    extra_client_options: Dict[str, Any] = {}

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `schema_pattern` is matched against fully qualified schema name `<database>.<schema>`.",
    )

    extract_column_level_lineage: bool = Field(
        default=True,
        description="Whether to extract column level lineage. This config works with rest-sink only.",
    )

    # TODO - use DatasetPropertiesConfigMixin instead
    patch_custom_properties: bool = Field(
        default=True,
        description="Whether to patch custom properties on existing datasets rather than replace.",
    )

    resolve_temp_table_in_lineage: bool = Field(
        default=True,
        description="Whether to resolve temp table appear in lineage to upstream permanent tables.",
    )

    skip_external_tables: bool = Field(
        default=False,
        description="Whether to skip EXTERNAL tables.",
    )

    @model_validator(mode="before")
    @classmethod
    def check_email_is_set_on_usage(cls, values):
        if values.get("include_usage_statistics"):
            assert "email_domain" in values and values["email_domain"], (
                "email_domain needs to be set if usage is enabled"
            )
        return values

    @model_validator(mode="after")
    def check_database_is_set(self) -> "RedshiftConfig":
        assert self.database, "database must be set"
        return self

    @model_validator(mode="after")
    def backward_compatibility_configs_set(self) -> "RedshiftConfig":
        if (
            self.schema_pattern is not None
            and self.schema_pattern != AllowDenyPattern.allow_all()
            and self.match_fully_qualified_names is not None
            and not self.match_fully_qualified_names
        ):
            logger.warning(
                "Please update `schema_pattern` to match against fully qualified schema name `<database_name>.<schema_name>` and set config `match_fully_qualified_names : True`."
                "Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. "
                "The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
            )
        return self

    @model_validator(mode="before")
    @classmethod
    def connection_config_compatibility_set(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = deepcopy(values)

        if (
            ("options" in values and "connect_args" in values["options"])
            and "extra_client_options" in values
            and len(values["extra_client_options"]) > 0
        ):
            raise ValueError(
                "Cannot set both `connect_args` and `extra_client_options` in the config. Please use `extra_client_options` only."
            )

        if "options" in values and "connect_args" in values["options"]:
            values["extra_client_options"] = values["options"]["connect_args"]

        if values.get("extra_client_options"):
            if values.get("options"):
                values["options"]["connect_args"] = values["extra_client_options"]
            else:
                values["options"] = {"connect_args": values["extra_client_options"]}
        return values
