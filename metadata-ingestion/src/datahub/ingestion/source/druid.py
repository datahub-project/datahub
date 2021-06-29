# This import verifies that the dependencies are available.
import pydruid  # noqa: F401

from datahub.configuration.common import AllowDenyPattern

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class DruidConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "druid"
    schema_pattern: AllowDenyPattern = AllowDenyPattern(deny=["^(lookup|sys).*"])

    def get_sql_alchemy_url(self):
        return f"{super().get_sql_alchemy_url(self)}/druid/v2/sql/"

    """
    The pydruid library already formats the table name correctly, so we do not
    need to use the schema name when constructing the URN. Without this override,
    every URN would incorrectly start with "druid.

    For more information, see https://druid.apache.org/docs/latest/querying/sql.html#schemata-table
    """

    def get_identifier(self, schema: str, table: str) -> str:
        return f"{table}"


class DruidSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "druid")

    @classmethod
    def create(cls, config_dict, ctx):
        config = DruidConfig.parse_obj(config_dict)
        return cls(config, ctx)
