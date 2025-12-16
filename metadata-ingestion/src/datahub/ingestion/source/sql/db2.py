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
import sqlalchemy
import sqlglot
from sqlalchemy.engine.reflection import Inspector
from sqlglot.dialects.dialect import NormalizationStrategy

from datahub.configuration.common import AllowDenyPattern
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


def patch_dialect(dialect: sqlalchemy.engine.interfaces.Dialect) -> None:
    # NOTE: why is this manual patching an instance, and not just creating a new custom dialect class?
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

    # ibm_db_sa unconditionally lowercases names, making it impossible
    # to distinguish tables with case-sensitive names (and thus impossible
    # to get further metadata on them).
    # https://github.com/ibmdb/python-ibmdbsa/issues/153
    # https://github.com/ibmdb/python-ibmdbsa/issues/170
    dialect._reflector.normalize_name = lambda name: name
    dialect._reflector.denormalize_name = lambda name: name


class Db2Config(BasicSQLAlchemyConfig):
    database: str = pydantic.Field(
        description="The Db2 database to ingest from.",
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
    host_port: str = pydantic.Field(
        default="localhost:50000", description="Db2 host URL."
    )
    scheme: str = pydantic.Field(
        default="db2",
        description="ibm_db_sa scheme to use (db2, db2+pyodbc, db2+pyodbc400).",
    )

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> str:
        # include config.uri_args in SQLAlchemy URL
        return super().get_sql_alchemy_url(
            {**self.uri_args, **(uri_opts or {})}, database
        )


def _quote_identifier(value: str) -> str:
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
        # register custom SQLGlot dialect if one doesn't exist. at the time of writing,
        # SQLGlot doesn't have a built-in Db2 dialect, but this is forward-compatible
        # in case it ever is added.
        if not sqlglot.Dialect.get("db2"):
            sqlglot.Dialect.classes["db2"] = CustomDb2SqlGlotDialect

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
        # Db2 views on LUW and z/OS look up unqualified names in the schema from the session
        # when the view was created. Not the schema that the view itself lives in!
        schema_name = _db2_get_view_qualifier_quoted(inspector, schema, view)
        db_name = self.get_db_name(inspector)
        return db_name, schema_name

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, _db_name: str
    ) -> Iterable[BaseProcedure]:
        for row in _db2_get_procedures(inspector, schema):
            if row["QUALIFIER"]:
                # similar to views, stored procedures can look up unqualified names in the
                # schema from the session when they were created, not the schema of the
                # proc itself. also quote the schema name so case-sensitive names make
                # it through sqlglot without being normalized.
                default_schema = _quote_identifier(row["QUALIFIER"].rstrip())
            else:
                default_schema = None

            yield BaseProcedure(
                name=row["ROUTINENAME"],
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

        logger.debug(f"Got PATHSCHEMAS={repr(result)} view {repr(schema)}.{repr(view)}")
        pathschemas = _split_zos_pathschemas(result.strip())
        if len(pathschemas) > 1:
            raise NotImplementedError(f"len(PATHSCHEMAS) > 1: {repr(pathschemas)}")

        # the schema name must be quoted so that case-sensitive names make it through
        # to the sqlglot lineage parser without being normalized.
        return _quote_identifier(pathschemas[0])

    elif inspector.has_table("SYSVIEWS", schema="QSYS2"):
        # Db2 i/AS400 doesn't have this concept.
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


def _split_zos_pathschemas(text: str) -> list[str]:
    # format is schema names in double quotes separated by commas
    # e.g.: "SYSIBM","SYSPROC","SMITH","SESSION_USER"

    schema_names = []
    p = 0
    while p < len(text):
        if text[p] != '"':
            raise ValueError(
                f"Expected schema name to start with double-quote: {repr(text)}"
            )
        p += 1  # consume starting double quote

        schema_name = ""
        while True:
            if text[p : p + 2] == '""':
                schema_name += '"'
                p += 2  # consume escaped double quote
            elif text[p] == '"':
                break
            else:
                schema_name += text[p]
                p += 1  # consume schema name character inside quotes
        schema_names.append(schema_name)

        if text[p] != '"':
            raise ValueError(
                f"Expected schema name to end with double-quote: {repr(text)}"
            )
        p += 1  # consume ending double quote

        if p < len(text):
            if text[p] != ",":
                raise ValueError(
                    f"Expected schema names to be separated by commas: {repr(text)}"
                )
            p += 1  # consume comma

    if p != len(text):  # must consume entire string
        raise ValueError(f"Failed to parse entire PATHSCHEMAS string: {repr(text)}")

    return schema_names
