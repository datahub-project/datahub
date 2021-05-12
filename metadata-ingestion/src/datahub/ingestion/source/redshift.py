from datahub.ingestion.source.postgres import PostgresSource
from datahub.ingestion.source.sql_common import SQLAlchemySource


class RedshiftSource(PostgresSource):
    def __init__(self, config, ctx):
        SQLAlchemySource.__init__(self, config, ctx, "redshift")
