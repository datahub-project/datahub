# This import verifies that the dependencies are available.

import pymysql  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import util
from sqlalchemy.dialects.mysql import BIT, base
from sqlalchemy.dialects.mysql.enumerated import SET
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import (
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.metadata._schema_classes import BytesTypeClass

SET.__repr__ = util.generic_repr  # type:ignore

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
POINT = make_sqlalchemy_type("POINT")
LINESTRING = make_sqlalchemy_type("LINESTRING")
POLYGON = make_sqlalchemy_type("POLYGON")
DECIMAL128 = make_sqlalchemy_type("DECIMAL128")

register_custom_type(GEOMETRY)
register_custom_type(POINT)
register_custom_type(LINESTRING)
register_custom_type(POLYGON)
register_custom_type(DECIMAL128)
register_custom_type(BIT, BytesTypeClass)

base.ischema_names["geometry"] = GEOMETRY
base.ischema_names["point"] = POINT
base.ischema_names["linestring"] = LINESTRING
base.ischema_names["polygon"] = POLYGON
base.ischema_names["decimal128"] = DECIMAL128


class MySQLConnectionConfig(SQLAlchemyConnectionConfig):
    # defaults
    host_port: str = Field(default="localhost:3306", description="MySQL host URL.")
    scheme: str = "mysql+pymysql"


class MySQLConfig(MySQLConnectionConfig, TwoTierSQLAlchemyConfig):
    def get_identifier(self, *, schema: str, table: str) -> str:
        return f"{schema}.{table}"


@platform_name("MySQL")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class MySQLSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    Metadata for databases, schemas, and tables
    Column types and schema associated with each table
    Table, row, and column statistics via optional SQL profiling
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def add_profile_metadata(self, inspector: Inspector) -> None:
        if not self.config.is_profiling_enabled():
            return
        with inspector.engine.connect() as conn:
            for row in conn.execute(
                "SELECT table_schema, table_name, data_length from information_schema.tables"
            ):
                self.profile_metadata_info.dataset_name_to_storage_bytes[
                    f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
                ] = row.DATA_LENGTH
