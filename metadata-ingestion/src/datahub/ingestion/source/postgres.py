from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource

try:
    # GeoAlchemy adds support for PostGIS extensions in SQLAlchemy. In order to
    # activate it, we must import it so that it can hook into SQLAlchemy. While
    # we don't use the Geometry type that we import, we do care about the side
    # effects of the import. For more details, see here:
    # https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html#reflecting-tables.
    from geoalchemy2 import Geometry  # noqa
except ImportError:
    pass


class PostgresConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "postgresql+psycopg2"


class PostgresSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "postgresql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.parse_obj(config_dict)
        return cls(config, ctx)
