from textwrap import dedent
from typing import Optional

from pydantic.fields import Field
from pyhive.sqlalchemy_presto import PrestoDialect
from sqlalchemy import exc, sql
from sqlalchemy.engine import reflection

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.trino import (
    TrinoConfig,
    TrinoSource,
    _get_columns,
    get_table_comment,
    get_table_names,
)


# On Presto the information_schema.views does not return views but the tables table returns
@reflection.cache  # type: ignore
def get_view_names(self, connection, schema: str = None, **kw):  # type: ignore
    schema = schema or self._get_default_schema_name(connection)
    if schema is None:
        raise exc.NoSuchTableError("schema is required")
    query = dedent(
        """
        SELECT "table_name"
        FROM "information_schema"."tables"
        WHERE "table_schema" = :schema and "table_type" = 'VIEW'
    """
    ).strip()
    res = connection.execute(sql.text(query), schema=schema)
    return [row.table_name for row in res]


# The pyhive presto driver doesn't return view definitions, so we have to query it
@reflection.cache  # type: ignore
def get_view_definition(self, connection, view_name, schema=None, **kw):
    schema = schema or self._get_default_schema_name(connection)
    if schema is None:
        raise exc.NoSuchTableError("schema is required")

    if view_name is None:
        raise exc.NoSuchTableError("view_name is required")

    query = dedent(
        f"""
        SHOW CREATE VIEW "{schema}"."{view_name}"
    """
    ).strip()
    res = connection.execute(sql.text(query))
    return next(res)[0]


def _get_full_table(  # type: ignore
    self, table_name: str, schema: Optional[str] = None, quote: bool = True
) -> str:
    table_part = (
        self.identifier_preparer.quote_identifier(table_name) if quote else table_name
    )
    if schema:
        schema_part = (
            self.identifier_preparer.quote_identifier(schema) if quote else schema
        )
        return f"{schema_part}.{table_part}"

    return table_part


PrestoDialect.get_table_names = get_table_names
PrestoDialect.get_view_names = get_view_names
PrestoDialect.get_view_definition = get_view_definition
PrestoDialect.get_table_comment = get_table_comment
PrestoDialect.get_columns = _get_columns
PrestoDialect._get_full_table = _get_full_table


class PrestoConfig(TrinoConfig):
    # defaults
    scheme: str = Field(default="presto", description="", hidden_from_docs=True)


@platform_name("Presto", doc_order=1)
@config_class(PrestoConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class PrestoSource(TrinoSource):
    """

    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types and schema associated with each table
    - Table, row, and column statistics via optional SQL profiling

    """

    config: PrestoConfig

    def __init__(self, config: PrestoConfig, ctx: PipelineContext):
        super().__init__(config, ctx, platform="presto")
        # We have to override platform as this source inherits from the Trino source

    @classmethod
    def create(cls, config_dict, ctx):
        config = PrestoConfig.parse_obj(config_dict)
        return cls(config, ctx)
