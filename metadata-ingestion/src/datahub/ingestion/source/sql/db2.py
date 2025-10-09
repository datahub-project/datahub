import logging
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

import ibm_db_sa
import pydantic
import sqlglot
from sqlalchemy.engine.reflection import Inspector
from sqlglot.dialects.dialect import NormalizationStrategy

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure

logger = logging.getLogger(__name__)


class CustomDb2SqlGlotDialect(sqlglot.Dialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE


class CustomDb2SqlAlchemyDialect(ibm_db_sa.dialect):
    # ibm_db_sa result column names have inconsistent casing
    # see: https://github.com/ibmdb/python-ibmdbsa/issues/173
    requires_name_normalize = False

    def initialize(self, connection):
        # ibm_db_sa unconditionally lowercases names, making it impossible
        # to distinguish tables with case-sensitive names (and thus impossible
        # to get further metadata on them).
        # see:
        # - https://github.com/ibmdb/python-ibmdbsa/issues/153
        # - https://github.com/ibmdb/python-ibmdbsa/issues/170
        super().initialize(connection)
        self._reflector.normalize_name = lambda s: s
        self._reflector.denormalize_name = lambda s: s

    def get_schema_names(self, connection, **kwargs) -> Iterable[str]:
        for s in super().get_schema_names(connection, **kwargs):
            # get_schema_names() can return schema names with extra space on the end
            # see https://github.com/ibmdb/python-ibmdbsa/issues/172
            yield s.rstrip()

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        # get_table_comment returns nothing for views
        # see: https://github.com/ibmdb/python-ibmdbsa/issues/171
        comment = super().get_table_comment(
            connection, table_name, schema=schema, **kwargs
        )
        if comment and comment.get("text"):
            return comment

        result = connection.execute(
            """
            select REMARKS
            from SYSCAT.TABLES
            where TABSCHEMA = ?
            and TABNAME = ?
        """,
            (schema, table_name),
        )
        return {"text": result.scalar()}


class Db2Config(BasicSQLAlchemyConfig):
    database: str = pydantic.Field(description="The Db2 database to ingest from.")

    include_stored_procedures: bool = pydantic.Field(
        default=True,
        description="Ingest stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in schema.procedure_name format.",
    )

    # Override defaults
    host_port: str = pydantic.Field(default="localhost:50000")
    scheme: HiddenFromDocs[str] = pydantic.Field(default="db2+ibm_db")


def _quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


@platform_name("IBM Db2", id="db2")
@config_class(Db2Config)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class Db2Source(SQLAlchemySource):
    def __init__(self, config: Db2Config, ctx: PipelineContext):
        super().__init__(config, ctx, "db2")
        # register custom SQLGlot dialect
        if not sqlglot.Dialect.get("db2"):
            sqlglot.Dialect.classes["db2"] = CustomDb2SqlGlotDialect

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "Db2Source":
        config = Db2Config.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        for inspector in super().get_inspectors():
            # use our custom SQLAlchemy dialect for connections (for the custom SQL
            # we run) and inspectors (for everything else).
            inspector.dialect = inspector.bind.dialect = CustomDb2SqlAlchemyDialect()
            inspector.dialect.initialize(inspector.bind)
            yield inspector

    def get_db_name(self, _inspector: Inspector) -> str:
        # database names are case-insensitive, so normalize them to uppercase
        # to match everything else.
        return self.config.database.upper()

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        # database.schema.object
        return f"{self.get_db_name(inspector)}.{schema}.{entity}"

    def get_view_default_db_schema(
        self, _dataset_name: str, inspector: Inspector, schema: str, view: str
    ) -> Tuple[Optional[str], str]:
        # Db2 views look up unqualified names in the schema from the session
        # when the view was created. Not the schema that the view itself lives in!
        result = inspector.bind.execute(
            """
            select QUALIFIER
            from SYSCAT.VIEWS
            where VIEWSCHEMA = ?
            and VIEWNAME = ?
        """,
            (schema, view),
        )
        # the schema name must be quoted so that case-sensitive names make it through
        # to the sqlglot lineage parser without being normalized.
        schema_name = _quote_identifier(result.scalar().rstrip())
        db_name = self.get_db_name(inspector)
        return db_name, schema_name

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, _db_name: str
    ) -> Iterable[BaseProcedure]:
        result = inspector.bind.execute(
            """
            select
                ROUTINENAME,
                LANGUAGE,
                CREATE_TIME,
                ALTER_TIME,
                QUALIFIER,
                TEXT,
                REMARKS
            from SYSCAT.ROUTINES
            where ROUTINESCHEMA = ?
            and ROUTINETYPE = 'P'
        """,
            (schema,),
        )
        for row in result:
            yield BaseProcedure(
                name=row["ROUTINENAME"],
                # language can have trailing spaces
                language=row["LANGUAGE"].rstrip(),
                procedure_definition=row["TEXT"],
                comment=row["REMARKS"],
                created=row["CREATE_TIME"],
                last_altered=row["ALTER_TIME"],
                # similar to views, stored procedures look up unqualified names in the
                # schema from the session when they were created, not the schema of the
                # proc itself. also quote the schema name so case-sensitive names make
                # it through sqlglot without being normalized.
                default_schema=_quote_identifier(row["QUALIFIER"].rstrip()),
                argument_signature=None,
                return_type=None,
                extra_properties={},
            )
