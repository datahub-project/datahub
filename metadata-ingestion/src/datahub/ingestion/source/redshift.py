# These imports verifies that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy_redshift  # noqa: F401

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.postgres import PostgresConfig
from datahub.ingestion.source.sql_common import SQLAlchemySource

# TRICKY: it's necessary to import the Postgres source because
# that module has some side effects that we care about here.


class RedshiftConfig(PostgresConfig):
    # Although Amazon Redshift is compatible with Postgres's wire format,
    # we actually want to use the sqlalchemy-redshift package and dialect
    # because it has better caching behavior. In particular, it queries
    # the full table, column, and constraint information in a single larger
    # query, and then simply pulls out the relevant information as needed.
    # Because of this behavior, it uses dramatically fewer round trips for
    # large Redshift warehouses. As an example, see this query for the columns:
    # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/60b4db04c1d26071c291aeea52f1dcb5dd8b0eb0/sqlalchemy_redshift/dialect.py#L745.
    scheme = "redshift+psycopg2"


class RedshiftSource(SQLAlchemySource):
    def __init__(self, config: RedshiftConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "redshift")

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftConfig.parse_obj(config_dict)
        return cls(config, ctx)
