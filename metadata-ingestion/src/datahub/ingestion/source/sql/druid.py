# This import verifies that the dependencies are available.
from typing import Any, Dict, Optional

import pydruid  # noqa: F401
from pydantic.fields import Field
from pydruid.db.sqlalchemy import DruidDialect
from sqlalchemy.exc import ResourceClosedError

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.decorators import (
    IngestionSourceCategory,
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    source_category,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig

get_table_names_source = DruidDialect.get_table_names


def get_table_names(self, connection, schema=None, **kwargs):
    try:
        return get_table_names_source(self, connection, schema=schema, **kwargs)
    # Druid throws ResourceClosedError when there is no table in the schema
    except ResourceClosedError:
        return []


DruidDialect.get_table_names = get_table_names


class DruidConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: HiddenFromDocs[str] = "druid"
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["^(lookup|sysgit|view).*"]),
        description="regex patterns for schemas to filter in ingestion.",
    )

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        base_url = super().get_sql_alchemy_url(uri_opts=uri_opts, database=database)
        return f"{base_url}/druid/v2/sql/"

    """
    The pydruid library already formats the table name correctly, so we do not
    need to use the schema name when constructing the URN. Without this override,
    every URN would incorrectly start with "druid.

    For more information, see https://druid.apache.org/docs/latest/querying/sql.html#schemata-table
    """

    def get_identifier(self, schema: str, table: str) -> str:
        return f"{table}"


@source_category(IngestionSourceCategory.DATABASE)
@platform_name("Druid")
@config_class(DruidConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class DruidSource(SQLAlchemySource):
    """
    Source that extracts metadata from Apache Druid via SQLAlchemy.

    Implementation notes:
    - Uses pydruid SQLAlchemy dialect for database connectivity
    - Overrides get_identifier to skip schema name in URNs (Druid table names are already fully qualified)
    - Patches DruidDialect.get_table_names to handle ResourceClosedError for empty schemas
    - Default schema pattern denies internal databases (lookup, sysgit, view)
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "druid")

    @classmethod
    def create(cls, config_dict, ctx):
        config = DruidConfig.model_validate(config_dict)
        return cls(config, ctx)
