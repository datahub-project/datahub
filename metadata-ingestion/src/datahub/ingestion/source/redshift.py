from typing import Dict

# These imports verify that the dependencies are available.
import psycopg2  # noqa: F401
import sqlalchemy_redshift  # noqa: F401
from sqlalchemy.engine import reflection
from sqlalchemy_redshift.dialect import RedshiftDialect, RelationKey

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


# reflection.cache uses eval and other magic to partially rewrite the function.
# mypy can't handle it, so we ignore it for now.
@reflection.cache  # type: ignore
def _get_all_table_comments(self, connection, **kw):
    COMMENT_SQL = """
        SELECT n.nspname as schema,
               c.relname as table_name,
               pgd.description as table_comment
        FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_description pgd ON pgd.objsubid = 0 AND pgd.objoid = c.oid
        WHERE c.relkind in ('r', 'v', 'm', 'f', 'p')
          AND pgd.description IS NOT NULL
        ORDER BY "schema", "table_name";
    """

    all_table_comments: Dict[RelationKey, str] = {}

    result = connection.execute(COMMENT_SQL)
    for table in result:
        key = RelationKey(table.table_name, table.schema, connection)
        all_table_comments[key] = table.table_comment

    return all_table_comments


@reflection.cache  # type: ignore
def get_table_comment(self, connection, table_name, schema=None, **kw):
    all_table_comments = self._get_all_table_comments(connection, **kw)
    key = RelationKey(table_name, schema, connection)
    if key not in all_table_comments.keys():
        key = key.unquoted()
    return {"text": all_table_comments.get(key)}


# This monkey-patching enables us to batch fetch the table descriptions, rather than
# fetching them one at a time.
RedshiftDialect._get_all_table_comments = _get_all_table_comments
RedshiftDialect.get_table_comment = get_table_comment


class RedshiftSource(SQLAlchemySource):
    def __init__(self, config: RedshiftConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "redshift")

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftConfig.parse_obj(config_dict)
        return cls(config, ctx)
