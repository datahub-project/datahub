# This import verifies that the dependencies are available.
import pydruid  # noqa: F401
from pydantic.fields import Field
from pydruid.db.sqlalchemy import DruidDialect
from sqlalchemy.exc import ResourceClosedError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
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
    scheme: str = "druid"
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["^(lookup|sysgit|view).*"]),
        description="regex patterns for schemas to filter in ingestion.",
    )

    def get_sql_alchemy_url(self):
        return f"{super().get_sql_alchemy_url()}/druid/v2/sql/"

    """
    The pydruid library already formats the table name correctly, so we do not
    need to use the schema name when constructing the URN. Without this override,
    every URN would incorrectly start with "druid.

    For more information, see https://druid.apache.org/docs/latest/querying/sql.html#schemata-table
    """

    def get_identifier(self, schema: str, table: str) -> str:
        return (
            f"{self.platform_instance}.{table}"
            if self.platform_instance
            else f"{table}"
        )


@platform_name("Druid")
@config_class(DruidConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class DruidSource(SQLAlchemySource):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling.

    **Note**: It is important to explicitly define the deny schema pattern for internal Druid databases (lookup & sys) if adding a schema pattern. Otherwise, the crawler may crash before processing relevant databases. This deny pattern is defined by default but is overriden by user-submitted configurations.
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "druid")

    @classmethod
    def create(cls, config_dict, ctx):
        config = DruidConfig.parse_obj(config_dict)
        return cls(config, ctx)
