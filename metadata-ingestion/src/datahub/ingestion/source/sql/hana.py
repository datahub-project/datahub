# These imports verify that the dependencies are available.
import hdbcli  # noqa: F401
from sqlalchemy.dialects.hana import base  # noqa: F401

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)


class HanaConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:39041"
    scheme = "hana+hdbcli"


class HanaSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "hana"

    @classmethod
    def create(cls, config_dict, ctx):
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)
