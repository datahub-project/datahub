# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class HiveConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "hive"


class HiveSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "hive")

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveConfig.parse_obj(config_dict)
        return cls(config, ctx)
