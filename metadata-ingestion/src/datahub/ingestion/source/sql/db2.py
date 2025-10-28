import logging
import re
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

import ibm_db_sa
import pydantic
import sqlalchemy
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


def patch_dialect(dialect):
    # NOTE: why is this manual patching, and not creating a new custom dialect class?
    # The ibm_db_sa package has multiple possible dialects based on the scheme — e.g.
    # DB2Dialect_ibm_db, DB2Dialect_pyodbc, AS400Dialect, AS400Dialect_pyodbc, etc.
    # We want to make sure this works no matter which dialect class gets used.
    if not isinstance(dialect, ibm_db_sa.base.DB2Dialect):
        raise TypeError(
            f"unable to patch dialect of type {type(dialect)} which does not descend from ibm_db_sa.base.DB2Dialect"
        )

    # ibm_db_sa result column names have inconsistent casing
    # https://github.com/ibmdb/python-ibmdbsa/issues/173
    dialect.requires_name_normalize = False

    # TODO: remove this once ibm_db_sa v??? is released
    # Fix AS400 stuff
    print(f"{dialect=} {type(dialect)=} {dialect._reflector_cls} {dialect._reflector}")
    if not isinstance(dialect._reflector, dialect._reflector_cls):
        logger.debug(
            f"Resetting reflector from {type(dialect._reflector)} to {dialect._reflector_cls}"
        )
        dialect._reflector = dialect._reflector_cls(dialect)

    # ibm_db_sa unconditionally lowercases names, making it impossible
    # to distinguish tables with case-sensitive names (and thus impossible
    # to get further metadata on them).
    # https://github.com/ibmdb/python-ibmdbsa/issues/153
    # https://github.com/ibmdb/python-ibmdbsa/issues/170
    dialect._reflector.normalize_name = lambda s: s
    dialect._reflector.denormalize_name = lambda s: s

    # TODO: remove this once ibm_db_sa v0.4.3 is released
    # get_schema_names() can return schema names with extra space on the end
    # https://github.com/ibmdb/python-ibmdbsa/issues/172
    original_schema_names = dialect.get_schema_names

    def get_schema_names(connection, **kwargs):
        for s in original_schema_names(connection, **kwargs):
            yield s.rstrip()

    dialect.get_schema_names = get_schema_names

    # TODO: remove this once ibm_db_sa v0.4.3 is released
    # get_table_comment returns nothing for views: https://github.com/ibmdb/python-ibmdbsa/issues/171
    # get_table_comment doesn't work on z/OS or i/AS400: https://github.com/ibmdb/python-ibmdbsa/issues/174
    def get_table_comment(connection, table_name, schema=None, **kwargs):
        return {
            "text": _db2_get_table_comment(
                sqlalchemy.inspect(connection), schema, table_name
            )
        }

    dialect.get_table_comment = get_table_comment


class Db2Config(BasicSQLAlchemyConfig):
    database: Optional[str] = pydantic.Field(
        default=None, description="The Db2 database to ingest from."
    )

    include_stored_procedures: bool = pydantic.Field(
        default=True,
        description="Ingest stored procedures.",
    )

    procedure_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in schema.procedure_name format.",
    )

    uri_args: Dict[str, str] = pydantic.Field(
        default={},
        description="Arguments to add to the URL when connecting.",
    )

    # Override defaults
    host_port: str = pydantic.Field(default="localhost:50000")
    scheme: HiddenFromDocs[str] = pydantic.Field(default="db2")

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> str:
        # include config.uri_args in SQLAlchemy URL
        return super().get_sql_alchemy_url(
            {**self.uri_args, **(uri_opts or {})}, database
        )


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
            # use our custom SQLAlchemy dialect
            patch_dialect(inspector.dialect)
            yield inspector

    def get_db_name(self, inspector: Inspector) -> str:
        # database names are case-insensitive, so normalize them to uppercase
        # to match everything else.
        return super().get_db_name(inspector).upper()

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        # database.schema.object
        return f"{self.get_db_name(inspector)}.{schema}.{entity}"

    def get_view_default_db_schema(
        self, _dataset_name: str, inspector: Inspector, schema: str, view: str
    ) -> Tuple[Optional[str], Optional[str]]:
        # Db2 views look up unqualified names in the schema from the session
        # when the view was created. Not the schema that the view itself lives in!
        schema_name = _db2_get_view_qualifier_quoted(inspector, schema, view)
        db_name = self.get_db_name(inspector)
        return db_name, schema_name

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, _db_name: str
    ) -> Iterable[BaseProcedure]:
        for row in _db2_get_procedures(inspector, schema):
            if row["QUALIFIER"]:
                # similar to views, stored procedures look up unqualified names in the
                # schema from the session when they were created, not the schema of the
                # proc itself. also quote the schema name so case-sensitive names make
                # it through sqlglot without being normalized.
                default_schema = _quote_identifier(row["QUALIFIER"].rstrip())
            else:
                default_schema = None

            yield BaseProcedure(
                name=row["ROUTINENAME"],
                # language can have trailing spaces
                language=row["LANGUAGE"].rstrip() if row["LANGUAGE"] else "",
                procedure_definition=row["TEXT"],
                comment=row["REMARKS"],
                created=row["CREATE_TIME"],
                last_altered=row["ALTER_TIME"],
                default_schema=default_schema,
                argument_signature=None,
                return_type=None,
                extra_properties={},
            )


def _db2_get_table_comment(
    inspector: Inspector, schema: str, table_name: str
) -> Optional[str]:
    if inspector.has_table("TABLES", schema="SYSCAT"):
        # Db2 LUW
        query = """
            select REMARKS
            from SYSCAT.TABLES
            where TABSCHEMA = ?
            and TABNAME = ?
        """

    elif inspector.has_table("SYSTABLES", schema="SYSIBM"):
        # Db2 z/OS
        query = """
            select REMARKS
            from SYSIBM.SYSTABLES
            where CREATOR = ?
            and NAME = ?
        """

    elif inspector.has_table("SYSTABLES", schema="QSYS2"):
        # Db2 for i/AS400
        query = """
            select LONG_COMMENT
            from QSYS2.SYSTABLES
            where TABLE_SCHEMA = ?
            and TABLE_NAME = ?
        """

    else:
        raise NotImplementedError(
            "Couldn't find SYSCAT.TABLES, SYSIBM.SYSTABLES, or QSYS2.SYSTABLES"
        )

    return inspector.bind.execute(
        query,
        (schema, table_name),
    ).scalar()


def _db2_get_view_qualifier_quoted(
    inspector: Inspector, schema: str, view: str
) -> Optional[str]:
    if inspector.has_table("VIEWS", schema="SYSCAT"):
        # Db2 LUW
        result = inspector.bind.execute(
            """
            select QUALIFIER
            from SYSCAT.VIEWS
            where VIEWSCHEMA = ?
            and VIEWNAME = ?
        """,
            (
                schema,
                view,
            ),
        ).scalar()
        if not result:
            return None

        # the schema name must be quoted so that case-sensitive names make it through
        # to the sqlglot lineage parser without being normalized.
        return _quote_identifier(result.rstrip())

    elif inspector.has_table("SYSVIEWS", schema="SYSIBM"):
        # Db2 z/OS
        result = inspector.bind.execute(
            """
                select PATHSCHEMAS
                from SYSIBM.SYSVIEWS
                where CREATOR = ?
                and NAME = ?
            """,
            (
                schema,
                view,
            ),
        ).scalar()
        if not result:
            return None

        # format is like: "SYSIBM","SYSPROC","SMITH","SESSION_USER"
        # split, ignoring commas inside double quotes
        logger.debug(f"Got PATHSCHEMAS={repr(result)} view {repr(schema)}.{repr(view)}")
        pathschemas = re.findall(r'([^",]+|"(?:[^"]|"")*")(?:,\s*|$)', result.strip())
        if len(pathschemas) > 1:
            raise NotImplementedError(f"len(PATHSCHEMAS) > 1: {repr(pathschemas)}")
        # already quoted, don't have to call _quote_identifier.
        return pathschemas[0]

    elif inspector.has_table("SYSVIEWS", schema="QSYS2"):
        # Db2 i/AS400
        # doesn't have this concept.
        return None

    else:
        raise NotImplementedError(
            "Couldn't find SYSCAT.VIEWS, SYSIBM.SYSVIEWS, or QSYS2.SYSVIEWS"
        )


def _db2_get_procedures(
    inspector: Inspector, schema: str
) -> Iterable[sqlalchemy.engine.Row]:
    if inspector.has_table("ROUTINES", schema="SYSCAT"):
        # Db2 LUW
        yield from inspector.bind.execute(
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

    elif inspector.has_table("SYSROUTINES", schema="SYSIBM"):
        # Db2 z/OS
        yield from inspector.bind.execute(
            """
            select
                NAME as ROUTINENAME,
                LANGUAGE,
                CREATEDTS as CREATE_TIME,
                ALTEREDTS as ALTER_TIME,
                NULL as QUALIFIER,
                TEXT,
                REMARKS
            from SYSIBM.SYSROUTINES
            where SCHEMA = ?
            and ROUTINETYPE = 'P'
        """,
            (schema,),
        )

    elif inspector.has_table("SYSROUTINES", schema="QSYS2"):
        # Db2 i/AS400
        yield from inspector.bind.execute(
            """
            select
                ROUTINE_NAME as ROUTINENAME,
                ROUTINE_BODY as LANGUAGE,
                ROUTINE_CREATED as CREATE_TIME,
                LAST_ALTERED as ALTER_TIME,
                SQL_PATH as QUALIFIER,
                ROUTINE_DEFINITION as TEXT,
                LONG_COMMENT as REMARKS
            from QSYS2.SYSROUTINES
            where ROUTINE_SCHEMA = ?
            and ROUTINE_TYPE = 'PROCEDURE'
        """,
            (schema,),
        )

    else:
        raise NotImplementedError(
            "Couldn't find SYSCAT.ROUTINES, SYSIBM.SYSROUTINES, or QSYS2.SYSROUTINES"
        )
