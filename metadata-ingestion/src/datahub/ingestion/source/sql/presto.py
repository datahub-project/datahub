import functools
from textwrap import dedent
from typing import Any, Dict, List, Optional, cast

from pydantic.fields import Field
from sqlalchemy import exc, sql
from sqlalchemy.dialects import registry
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedTableComment

from datahub.configuration.common import HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.presto_dialect import PrestoDialect
from datahub.ingestion.source.sql.trino import (
    TrinoConfig,
    TrinoSource,
    _get_columns as trino_get_columns,
    get_table_comment as trino_get_table_comment,
    get_table_names as trino_get_table_names,
)


# On Presto the information_schema.views does not return views but the tables table returns
@reflection.cache
def _get_view_names(
    self: Any, connection: Connection, schema: Optional[str] = None, **kw: Any
) -> List[str]:
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
    res = connection.execute(sql.text(query), {"schema": schema})
    return [row.table_name for row in res]


# The base Presto dialect doesn't return view definitions, so we have to query it
@reflection.cache
def _get_view_definition(
    self: Any,
    connection: Connection,
    view_name: str,
    schema: Optional[str] = None,
    **kw: Any,
) -> str:
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


def _full_table(
    self: Any, table_name: str, schema: Optional[str] = None, quote: bool = True
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


class PrestoSourceDialect(PrestoDialect):
    """PrestoDialect with the information_schema based reflection the Presto source
    shares with Trino."""

    # SQLAlchemy reads this from each dialect class's own __dict__.
    supports_statement_cache = True

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        return trino_get_table_names(self, connection, schema, **kw)

    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        return _get_view_names(self, connection, schema, **kw)

    def get_view_definition(
        self,
        connection: Connection,
        view_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> str:
        return _get_view_definition(self, connection, view_name, schema, **kw)

    def get_table_comment(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> ReflectedTableComment:
        # The Trino helpers build the reflection dicts, plus "properties"/"full_type".
        return cast(
            ReflectedTableComment,
            trino_get_table_comment(self, connection, table_name, schema, **kw),
        )

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[ReflectedColumn]:
        return cast(
            List[ReflectedColumn],
            trino_get_columns(self, connection, table_name, schema, **kw),
        )

    def _get_full_table(
        self, table_name: str, schema: Optional[str] = None, quote: bool = True
    ) -> str:
        return _full_table(self, table_name, schema, quote)


# Once the Presto source is loaded, presto:// URLs resolve to its dialect; this
# overrides the base registration made by presto_dialect.
registry.register("presto", __name__, "PrestoSourceDialect")
registry.register("presto.prestodb", __name__, "PrestoSourceDialect")


class PrestoConfig(TrinoConfig):
    # defaults
    scheme: HiddenFromDocs[str] = Field(default="presto")


@platform_name("Presto", doc_order=1)
@config_class(PrestoConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.PROBE,
    "Check what a recipe would ingest before running it, with `datahub recipe probe`",
)
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
        config = PrestoConfig.model_validate(config_dict)
        return cls(config, ctx)


# Unfortunately, the Presto dialect provide catalog_name as a column
# therefore we we need some workaround to not fail.
# This is a workaround to not fail which casuses to only get the table comment as property which is still better than to fail.
@functools.lru_cache
def gen_catalog_connector_dict(engine: Engine) -> Dict[str, str]:
    query = dedent(
        """
        SELECT *
        FROM "system"."metadata"."catalogs"
        """
    ).strip()
    with engine.connect() as conn:
        res = conn.execute(sql.text(query))
        return {row.catalog_name: "" for row in res}
