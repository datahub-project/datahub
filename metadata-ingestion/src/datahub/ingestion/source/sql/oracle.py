import datetime
import logging
import platform
import re
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, NoReturn, Optional, Tuple, Union, cast
from unittest.mock import patch

import oracledb
import sqlalchemy.engine
from pydantic import Field, ValidationInfo, field_validator, model_validator
from sqlalchemy import event, sql
from sqlalchemy.dialects.oracle.base import ischema_names
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import FLOAT, INTEGER, TIMESTAMP

import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    get_schema_metadata,
    make_sqlalchemy_type,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
)
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

# Oracle uses SQL aggregator for usage and lineage like SQL Server
from datahub.metadata.schema_classes import (
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class DataDictionaryMode(StrEnum):
    """Oracle data dictionary access modes."""

    ALL = "ALL"
    DBA = "DBA"


# Oracle system schemas to exclude from ingestion
# These contain internal Oracle objects that should not be ingested
ORACLE_SYSTEM_SCHEMAS = (
    "SYS",
    "SYSTEM",
    "DBSNMP",
    "WMSYS",
    "CTXSYS",
    "XDB",
    "MDSYS",
    "ORDSYS",
    "OUTLN",
    "ORDDATA",
)

# Format system schemas for SQL IN clause
_SYSTEM_SCHEMAS_SQL = ", ".join(f"'{schema}'" for schema in ORACLE_SYSTEM_SCHEMAS)

# SQL Query Constants
# Note: System schemas are explicitly excluded to prevent ingesting internal Oracle objects,
# even if they match the user's schema_pattern. This is a defense-in-depth measure.
PROCEDURES_QUERY = (
    """
    SELECT
        o.object_name as name,
        o.object_type as type,
        o.created,
        o.last_ddl_time,
        o.status
    FROM {tables_prefix}_OBJECTS o
    WHERE o.owner = :schema
        AND o.object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')
        AND o.status = 'VALID'
        AND o.owner NOT IN ("""
    + _SYSTEM_SCHEMAS_SQL
    + """)
    ORDER BY o.last_ddl_time
"""
)

PROCEDURE_SOURCE_QUERY = """
    SELECT text
    FROM {tables_prefix}_SOURCE
    WHERE owner = :schema
        AND name = :procedure_name
        AND type = :object_type
    ORDER BY line
"""

PROCEDURE_ARGUMENTS_QUERY = """
    SELECT
        argument_name,
        data_type,
        in_out,
        position
    FROM {tables_prefix}_ARGUMENTS
    WHERE owner = :schema
        AND object_name = :procedure_name
        AND argument_name IS NOT NULL
    ORDER BY position
"""

PROCEDURE_UPSTREAM_DEPENDENCIES_QUERY = """
    SELECT DISTINCT 
        referenced_owner,
        referenced_name,
        referenced_type
    FROM {tables_prefix}_DEPENDENCIES
    WHERE owner = :schema
        AND name = :procedure_name
        AND type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')
        AND referenced_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'SYNONYM')
"""

PROCEDURE_DOWNSTREAM_DEPENDENCIES_QUERY = """
    SELECT DISTINCT 
        owner,
        name,
        type
    FROM {tables_prefix}_DEPENDENCIES
    WHERE referenced_owner = :schema
        AND referenced_name = :procedure_name
        AND referenced_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')
        AND type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'PROCEDURE', 'FUNCTION', 'PACKAGE')
"""

MATERIALIZED_VIEWS_QUERY = (
    """
    SELECT mview_name, last_refresh_date 
    FROM {tables_prefix}_MVIEWS 
    WHERE owner = :owner
        AND owner NOT IN ("""
    + _SYSTEM_SCHEMAS_SQL
    + """)
    ORDER BY COALESCE(last_refresh_date, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
"""
)

MATERIALIZED_VIEW_DEFINITION_QUERY = """
    SELECT query FROM {tables_prefix}_MVIEWS WHERE mview_name=:mview_name
"""

# DBA-specific queries for OracleInspectorObjectWrapper
DBA_VIEWS_QUERY = """
    SELECT view_name FROM dba_views WHERE owner = :owner
"""

DBA_MVIEWS_QUERY = (
    """
    SELECT mview_name, last_refresh_date 
    FROM dba_mviews 
    WHERE owner = :owner
        AND owner NOT IN ("""
    + _SYSTEM_SCHEMAS_SQL
    + """)
    ORDER BY COALESCE(last_refresh_date, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
"""
)

DBA_MVIEW_DEFINITION_QUERY = """
    SELECT query FROM dba_mviews WHERE mview_name=:mview_name
"""

PROFILE_CANDIDATES_QUERY = """
    SELECT
        t.OWNER,
        t.TABLE_NAME,
        t.NUM_ROWS,
        t.LAST_ANALYZED,
        COALESCE(t.NUM_ROWS * t.AVG_ROW_LEN, 0) / (1024 * 1024 * 1024) AS SIZE_GB
    FROM {tables_table_name} t
    WHERE t.OWNER = :owner
    AND (t.NUM_ROWS < :table_row_limit OR t.NUM_ROWS IS NULL)
    AND COALESCE(t.NUM_ROWS * t.AVG_ROW_LEN, 0) / (1024 * 1024 * 1024) < :table_size_limit
"""


def _setup_oracle_compatibility() -> None:
    """
    Set up Oracle compatibility for SQLAlchemy.

    SQLAlchemy's Oracle dialect expects cx_Oracle, but we use oracledb (the newer replacement).
    This function ensures compatibility by making oracledb appear as cx_Oracle.
    """
    sys.modules["cx_Oracle"] = oracledb
    oracledb.version = "8.3.0"  # type: ignore[assignment]
    oracledb.__version__ = "8.3.0"  # type: ignore[assignment]


# Set up compatibility immediately when module is imported
_setup_oracle_compatibility()

extra_oracle_types = {
    make_sqlalchemy_type("SDO_GEOMETRY"),
    make_sqlalchemy_type("SDO_POINT_TYPE"),
    make_sqlalchemy_type("SDO_ELEM_INFO_ARRAY"),
    make_sqlalchemy_type("SDO_ORDINATE_ARRAY"),
}
assert ischema_names


def _raise_err(exc: Exception) -> NoReturn:
    raise exc


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    """Add CLOB and BLOB support to Oracle connection."""

    if defaultType == oracledb.CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    elif defaultType == oracledb.BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    cursor.outputtypehandler = output_type_handler


class OracleConfig(BasicSQLAlchemyConfig, BaseUsageConfig):
    # TODO: Change scheme to oracle+oracledb when sqlalchemy>=2 is supported
    scheme: str = Field(
        default="oracle", description="Will be set automatically to default value."
    )
    service_name: Optional[str] = Field(
        default=None,
        description="Oracle service name. If using, omit `database`.",
    )
    database: Optional[str] = Field(
        default=None,
        description="If using, omit `service_name`.",
    )
    add_database_name_to_urn: Optional[bool] = Field(
        default=False,
        description="Add oracle database name to urn, default urn is schema.table",
    )
    # custom
    data_dictionary_mode: DataDictionaryMode = Field(
        default=DataDictionaryMode.ALL,
        description="The data dictionary views mode, to extract information about schema objects "
        "('ALL' and 'DBA' views are supported). (https://docs.oracle.com/cd/E11882_01/nav/catalog_views.htm)",
    )
    # oracledb settings to enable thick mode and client library location
    enable_thick_mode: Optional[bool] = Field(
        default=False,
        description="Connection defaults to thin mode. Set to True to enable thick mode.",
    )
    thick_mode_lib_dir: Optional[str] = Field(
        default=None,
        description="If using thick mode on Windows or Mac, set thick_mode_lib_dir to the oracle client libraries path. "
        "On Linux, this value is ignored, as ldconfig or LD_LIBRARY_PATH will define the location.",
    )
    # Stored procedures configuration
    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures, functions, and packages. Requires access to DBA_PROCEDURES or ALL_PROCEDURES.",
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion. "
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. "
        "e.g. to match all procedures starting with customer in HR schema, use the regex 'ORCL.HR.CUSTOMER.*'",
    )
    include_materialized_views: bool = Field(
        default=True,
        description="Include materialized views in ingestion. Requires access to DBA_MVIEWS or ALL_MVIEWS. "
        "If permission errors occur, you can disable this feature or grant the required permissions.",
    )

    include_usage_stats: bool = Field(
        default=False,
        description="Generate usage statistics via SQL aggregator. Requires observed queries to be processed.",
    )

    include_operational_stats: bool = Field(
        default=False,
        description="Generate operation statistics from audit trail data (CREATE, INSERT, UPDATE, DELETE operations).",
    )

    @field_validator("service_name", mode="after")
    @classmethod
    def check_service_name(
        cls, v: Optional[str], info: ValidationInfo
    ) -> Optional[str]:
        if info.data.get("database") and v:
            raise ValueError(
                "specify one of 'database' and 'service_name', but not both"
            )
        return v

    @field_validator("data_dictionary_mode", mode="before")
    @classmethod
    def check_data_dictionary_mode(cls, value):
        if isinstance(value, str) and value not in ("ALL", "DBA"):
            raise ValueError("Specify one of data dictionary views mode: 'ALL', 'DBA'.")
        return value

    @model_validator(mode="after")
    def check_thick_mode_lib_dir(self):
        if (
            self.thick_mode_lib_dir is None
            and self.enable_thick_mode
            and (platform.system() == "Darwin" or platform.system() == "Windows")
        ):
            raise ValueError(
                "Specify 'thick_mode_lib_dir' on Mac/Windows when enable_thick_mode is true"
            )
        return self

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        url = super().get_sql_alchemy_url(uri_opts=uri_opts, database=database)

        if self.service_name:
            assert not self.database
            url = f"{url}/?service_name={self.service_name}"

        return url

    def get_identifier(self, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.add_database_name_to_urn:
            if self.database:
                return f"{self.database}.{regular}"
            return regular
        else:
            return regular


class OracleInspectorObjectWrapper:
    """
    Inspector class wrapper, which queries DBA_TABLES instead of ALL_TABLES
    """

    def __init__(self, inspector_instance: Inspector):
        self._inspector_instance = inspector_instance
        self.log = logging.getLogger(__name__)
        # tables that we don't want to ingest into the DataHub
        self.exclude_tablespaces: Tuple[str, str] = ("SYSTEM", "SYSAUX")

    def get_db_name(self) -> str:
        db_name = None
        try:
            db_name = self._inspector_instance.bind.execute(
                sql.text("select sys_context('USERENV','DB_NAME') from dual")
            ).scalar()
            return str(db_name)
        except sqlalchemy.exc.DatabaseError as e:
            self.report.failure(
                title="Error fetching database name using sys_context.",
                message="database_fetch_error",
                context=db_name,
                exc=e,
            )
            return ""

    def get_schema_names(self) -> List[str]:
        cursor = self._inspector_instance.bind.execute(
            sql.text("SELECT username FROM dba_users")
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid schema name: {row[0]}"))
            for row in cursor
        ]

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        sql_str = "SELECT table_name FROM dba_tables WHERE "
        if self.exclude_tablespaces:
            tablespace_str = ", ".join([f"'{ts}'" for ts in self.exclude_tablespaces])
            sql_str += (
                f"nvl(tablespace_name, 'no tablespace') NOT IN ({tablespace_str}) AND "
            )

        sql_str += "OWNER = :owner AND IOT_NAME IS NULL "

        cursor = self._inspector_instance.bind.execute(sql.text(sql_str), owner=schema)

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_view_names(self, schema: Optional[str] = None) -> List[str]:
        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text(DBA_VIEWS_QUERY),
            dict(owner=self._inspector_instance.dialect.denormalize_name(schema)),
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_materialized_view_names(self, schema: Optional[str] = None) -> List[str]:
        """Get materialized view names for the given schema."""
        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text(DBA_MVIEWS_QUERY),
            dict(owner=self._inspector_instance.dialect.denormalize_name(schema)),
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid materialized view name: {row[0]}"))
            for row in cursor
        ]

    def get_materialized_view_definition(
        self, mview_name: str, schema: Optional[str] = None
    ) -> Union[str, None]:
        """Get materialized view definition."""
        denormalized_mview_name = self._inspector_instance.dialect.denormalize_name(
            mview_name
        )
        if not denormalized_mview_name:
            logger.warning(
                f"Could not denormalize materialized view name: {mview_name}"
            )
            return None

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        params = {"mview_name": denormalized_mview_name}
        text = DBA_MVIEW_DEFINITION_QUERY.strip()

        if schema is not None:
            params["owner"] = schema
            text += "\nAND owner = :owner"

        rp = self._inspector_instance.bind.execute(sql.text(text), params).scalar()

        return rp

    def get_columns(
        self, table_name: str, schema: Optional[str] = None, dblink: str = ""
    ) -> List[dict]:
        denormalized_table_name = self._inspector_instance.dialect.denormalize_name(
            table_name
        )
        assert denormalized_table_name

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        columns = []
        if not (
            self._inspector_instance.dialect.server_version_info
            and self._inspector_instance.dialect.server_version_info < (9,)
        ):
            # _supports_char_length --> not self._is_oracle_8
            char_length_col = "char_length"
        else:
            char_length_col = "data_length"

        if (
            self._inspector_instance.dialect.server_version_info
            and self._inspector_instance.dialect.server_version_info >= (12,)
        ):
            identity_cols = """\
                col.default_on_null,
                (
                    SELECT id.generation_type || ',' || id.IDENTITY_OPTIONS
                    FROM DBA_TAB_IDENTITY_COLS{dblink} id
                    WHERE col.table_name = id.table_name
                    AND col.column_name = id.column_name
                    AND col.owner = id.owner
                ) AS identity_options""".format(dblink=dblink)
        else:
            identity_cols = "NULL as default_on_null, NULL as identity_options"

        params = {"table_name": denormalized_table_name}

        text = """
            SELECT
                col.column_name,
                col.data_type,
                col.%(char_length_col)s,
                col.data_precision,
                col.data_scale,
                col.nullable,
                col.data_default,
                com.comments,
                col.virtual_column,
                %(identity_cols)s
            FROM dba_tab_cols%(dblink)s col
            LEFT JOIN dba_col_comments%(dblink)s com
            ON col.table_name = com.table_name
            AND col.column_name = com.column_name
            AND col.owner = com.owner
            WHERE col.table_name = CAST(:table_name AS VARCHAR2(128))
            AND col.hidden_column = 'NO'
        """
        if schema is not None:
            params["owner"] = schema
            text += " AND col.owner = :owner "
        text += " ORDER BY col.column_id"
        text = text % {
            "dblink": dblink,
            "char_length_col": char_length_col,
            "identity_cols": identity_cols,
        }

        c = self._inspector_instance.bind.execute(sql.text(text), params)

        for row in c:
            colname = self._inspector_instance.dialect.normalize_name(row[0])
            orig_colname = row[0]
            coltype = row[1]
            length = row[2]
            precision = row[3]
            scale = row[4]
            nullable = row[5] == "Y"
            default = row[6]
            comment = row[7]
            generated = row[8]
            default_on_nul = row[9]
            identity_options = row[10]

            if coltype == "NUMBER":
                if precision is None and scale == 0:
                    coltype = INTEGER()
                else:
                    coltype = ischema_names.get(coltype)(precision, scale)
            elif coltype == "FLOAT":
                # TODO: support "precision" here as "binary_precision"
                coltype = FLOAT()
            elif coltype in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
                coltype = ischema_names.get(coltype)(length)
            elif "WITH TIME ZONE" in coltype:
                coltype = TIMESTAMP(timezone=True)
            else:
                coltype = re.sub(r"\(\d+\)", "", coltype)
                try:
                    coltype = ischema_names[coltype]()
                except KeyError:
                    logger.info(
                        f"Unrecognized column datatype {coltype} of column {colname}"
                    )
                    coltype = sqltypes.NULLTYPE

            if generated == "YES":
                computed = dict(sqltext=default)
                default = None
            else:
                computed = None

            if identity_options is not None:
                identity = self._inspector_instance.dialect._parse_identity_options(  # type: ignore
                    identity_options, default_on_nul
                )
                default = None
            else:
                identity = None

            cdict = {
                "name": colname,
                "type": coltype,
                "nullable": nullable,
                "default": default,
                "autoincrement": "auto",
                "comment": comment,
            }
            if orig_colname.lower() == orig_colname:
                cdict["quote"] = True
            if computed is not None:
                cdict["computed"] = computed
            if identity is not None:
                cdict["identity"] = identity

            columns.append(cdict)
        return columns

    def get_table_comment(self, table_name: str, schema: Optional[str] = None) -> Dict:
        denormalized_table_name = self._inspector_instance.dialect.denormalize_name(
            table_name
        )
        assert denormalized_table_name

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        COMMENT_SQL = """
            SELECT comments
            FROM dba_tab_comments
            WHERE table_name = :table_name
            AND owner = :schema_name
        """

        c = self._inspector_instance.bind.execute(
            sql.text(COMMENT_SQL),
            dict(table_name=denormalized_table_name, schema_name=schema),
        )

        return {"text": c.scalar()}

    def _get_constraint_data(
        self, table_name: str, schema: Optional[str] = None, dblink: str = ""
    ) -> List[sqlalchemy.engine.Row]:
        params = {"table_name": table_name}

        text = (
            "SELECT"
            "\nac.constraint_name,"
            "\nac.constraint_type,"
            "\nacc.column_name AS local_column,"
            "\nNULL AS remote_table,"
            "\nNULL AS remote_column,"
            "\nNULL AS remote_owner,"
            "\nacc.position AS loc_pos,"
            "\nNULL AS rem_pos,"
            "\nac.search_condition,"
            "\nac.delete_rule"
            "\nFROM dba_constraints ac"
            "\nJOIN dba_cons_columns acc"
            "\nON ac.owner = acc.owner"
            "\nAND ac.constraint_name = acc.constraint_name"
            "\nAND ac.table_name = acc.table_name"
            "\nWHERE ac.table_name = :table_name"
            "\nAND ac.constraint_type IN ('P', 'U', 'C')"
        )

        if schema is not None:
            params["owner"] = schema
            text += "\nAND ac.owner = :owner"

        # Splitting into queries with UNION ALL for execution efficiency
        text += (
            "\nUNION ALL"
            "\nSELECT"
            "\nac.constraint_name,"
            "\nac.constraint_type,"
            "\nacc.column_name AS local_column,"
            "\nac.table_name AS remote_table,"
            "\nrcc.column_name AS remote_column,"
            "\nac.r_owner AS remote_owner,"
            "\nacc.position AS loc_pos,"
            "\nrcc.position AS rem_pos,"
            "\nac.search_condition,"
            "\nac.delete_rule"
            "\nFROM dba_constraints ac"
            "\nJOIN dba_cons_columns acc"
            "\nON ac.owner = acc.owner"
            "\nAND ac.constraint_name = acc.constraint_name"
            "\nAND ac.table_name = acc.table_name"
            "\nLEFT JOIN dba_cons_columns rcc"
            "\nON ac.r_owner = rcc.owner"
            "\nAND ac.r_constraint_name = rcc.constraint_name"
            "\nAND acc.position = rcc.position"
            "\nWHERE ac.table_name = :table_name"
            "\nAND ac.constraint_type = 'R'"
        )

        if schema is not None:
            text += "\nAND ac.owner = :owner"

        text += "\nORDER BY constraint_name, loc_pos"

        rp = self._inspector_instance.bind.execute(sql.text(text), params)
        return rp.fetchall()

    def get_pk_constraint(
        self, table_name: str, schema: Optional[str] = None, dblink: str = ""
    ) -> Dict:
        # Denormalize table name for querying Oracle data dictionary
        denormalized_table_name = self._inspector_instance.dialect.denormalize_name(
            table_name
        )
        assert denormalized_table_name

        # Denormalize schema name for querying Oracle data dictionary
        denormalized_schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if denormalized_schema is None:
            denormalized_schema = self._inspector_instance.dialect.default_schema_name

        pkeys = []
        constraint_name = None

        try:
            for row in self._get_constraint_data(
                denormalized_table_name, denormalized_schema, dblink
            ):
                if row[1] == "P":  # constraint_type is 'P' for primary key
                    if constraint_name is None:
                        constraint_name = (
                            self._inspector_instance.dialect.normalize_name(row[0])
                        )
                    col_name = self._inspector_instance.dialect.normalize_name(
                        row[2]
                    )  # local_column
                    pkeys.append(col_name)
        except Exception as e:
            self.report.warning(
                title="Failed to Process Primary Keys",
                message=(
                    f"Unable to process primary key constraints for {schema}.{table_name}. "
                    "Ensure SELECT access on DBA_CONSTRAINTS and DBA_CONS_COLUMNS.",
                ),
                context=f"{schema}.{table_name}",
                exc=e,
            )
            # Return empty constraint if we can't process it
            return {"constrained_columns": [], "name": None}

        return {"constrained_columns": pkeys, "name": constraint_name}

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None, dblink: str = ""
    ) -> List:
        denormalized_table_name = self._inspector_instance.dialect.denormalize_name(
            table_name
        )
        assert denormalized_table_name

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        requested_schema = schema  # to check later on

        constraint_data = self._get_constraint_data(
            denormalized_table_name, schema, dblink
        )

        def fkey_rec():
            return {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }

        fkeys = defaultdict(fkey_rec)  # type: defaultdict

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple(
                [self._inspector_instance.dialect.normalize_name(x) for x in row[2:6]]
            )

            cons_name = self._inspector_instance.dialect.normalize_name(cons_name)

            if cons_type == "R":
                if remote_table is None:
                    logger.warning(
                        "Got 'None' querying 'table_name' from "
                        f"dba_cons_columns{dblink} - does the user have "
                        "proper rights to the table?"
                    )
                    self.report.warning(
                        title="Missing Table Permissions",
                        message=(
                            f"Unable to query table_name from dba_cons_columns{dblink}. "
                            "This usually indicates insufficient permissions on the target table. "
                            f"Foreign key relationships will not be detected for {schema}.{table_name}. "
                            "Please ensure the user has SELECT privileges on dba_cons_columns."
                        ),
                        context=f"{schema}.{table_name}",
                    )

                rec = fkeys[cons_name]
                rec["name"] = cons_name
                local_cols, remote_cols = (
                    rec["constrained_columns"],
                    rec["referred_columns"],
                )

                if not rec["referred_table"]:
                    rec["referred_table"] = remote_table
                    if (
                        requested_schema is not None
                        or self._inspector_instance.dialect.denormalize_name(
                            remote_owner
                        )
                        != schema
                    ):
                        rec["referred_schema"] = remote_owner

                    if row[9] != "NO ACTION":
                        rec["options"]["ondelete"] = row[9]

                local_cols.append(local_column)
                remote_cols.append(remote_column)

        return list(fkeys.values())

    def get_view_definition(
        self, view_name: str, schema: Optional[str] = None
    ) -> Union[str, None]:
        denormalized_view_name = self._inspector_instance.dialect.denormalize_name(
            view_name
        )
        assert denormalized_view_name

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        params = {"view_name": denormalized_view_name}
        text = "SELECT text FROM dba_views WHERE view_name=:view_name"

        if schema is not None:
            params["owner"] = schema
            text += "\nAND owner = :owner"

        rp = self._inspector_instance.bind.execute(sql.text(text), params).scalar()

        return rp

    def __getattr__(self, item: str) -> Any:
        if item in self.__dict__:
            return getattr(self, item)
        return getattr(self._inspector_instance, item)


# Oracle usage and lineage are handled by the SQL aggregator
# when parsing stored procedures and materialized views, similar to SQL Server


@platform_name("Oracle")
@config_class(OracleConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default to get lineage for stored procedures via `include_lineage` and for views via `include_view_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.STORED_PROCEDURE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default to get lineage for stored procedures via `include_lineage` and for views via `include_view_column_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.STORED_PROCEDURE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default via SQL aggregator when processing observed queries",
)
class OracleSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    - Stored procedures, functions, and packages with dependency tracking
    - Materialized views with proper lineage
    - Lineage, usage statistics, and operations via SQL aggregator (similar to BigQuery/Snowflake/Teradata)

    Lineage is automatically generated when parsing:
    - Stored procedure definitions (via SQL aggregator)
    - Materialized view definitions (via SQL aggregator)
    - View definitions (via SQL aggregator)

    Usage statistics and operations are generated from observed queries and audit trail data
    processed by the SQL aggregator. This provides comprehensive lineage, usage, and
    operational tracking from the same SQL parsing infrastructure.

    Using the Oracle source requires that you've also installed the correct drivers; see the [oracledb docs](https://python-oracledb.readthedocs.io/). The easiest approach is to use the thin mode (default) which requires no additional Oracle client installation.
    """

    config: OracleConfig

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "oracle")

        # if connecting to oracle with enable_thick_mode, it must be initialized before calling
        # create_engine, which is called in get_inspectors()
        # https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html#enabling-python-oracledb-thick-mode
        if self.config.enable_thick_mode:
            if platform.system() == "Darwin" or platform.system() == "Windows":
                # windows and mac os require lib_dir to be set explicitly
                oracledb.init_oracle_client(lib_dir=self.config.thick_mode_lib_dir)
            else:
                # linux requires configurating the library path with ldconfig or LD_LIBRARY_PATH
                oracledb.init_oracle_client()

        # Override SQL aggregator to enable usage and operations like BigQuery/Snowflake/Teradata
        if self.config.include_usage_stats or self.config.include_operational_stats:
            self.aggregator = SqlParsingAggregator(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
                generate_lineage=self.include_lineage,
                generate_usage_statistics=self.config.include_usage_stats,
                generate_operations=self.config.include_operational_stats,
                usage_config=self.config if self.config.include_usage_stats else None,
                eager_graph_load=False,
            )
            self.report.sql_aggregator = self.aggregator.report

    # Oracle inherits standard workunit generation from SQLAlchemySource
    # Usage and lineage are handled automatically by the SQL aggregator

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.model_validate(config_dict)
        return cls(config, ctx)

    @classmethod
    def test_connection(cls, config_dict: dict) -> TestConnectionReport:
        """Test Oracle connection."""
        import os

        # Force thin mode in test environments to avoid Oracle Client issues
        os.environ["ORACLE_CLIENT_LIBRARY_DIR"] = ""
        os.environ["TNS_ADMIN"] = ""

        # Ensure oracledb stays in thin mode
        if hasattr(oracledb, "defaults"):
            oracledb.defaults.config_dir = ""

        return super().test_connection(config_dict)

    def get_db_name(self, inspector: Inspector) -> str:
        """
        This overwrites the default implementation, which only tries to read
        database name from Connection URL, which does not work when using
        service instead of database.
        In that case, it tries to retrieve the database name by sending a query to the DB.

        Note: This is used as a fallback if database is not specified in the config.
        """

        # call default implementation first
        db_name = super().get_db_name(inspector)

        if db_name == "" and isinstance(inspector, OracleInspectorObjectWrapper):
            db_name = inspector.get_db_name()

        return db_name

    def get_inspectors(self) -> Iterable[Inspector]:
        for inspector in super().get_inspectors():
            event.listen(
                inspector.engine, "before_cursor_execute", before_cursor_execute
            )
            logger.info(
                f'Data dictionary mode is: "{self.config.data_dictionary_mode}".'
            )

            # SQLAlchemy inspector uses ALL_* tables; OracleInspectorObjectWrapper uses DBA_* tables
            if self.config.data_dictionary_mode != DataDictionaryMode.ALL:
                yield cast(Inspector, OracleInspectorObjectWrapper(inspector))
            else:
                # To silent the mypy lint error
                yield cast(Inspector, inspector)

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        """
        Override the get_db_schema method to ensure proper schema name extraction.
        This method is used during view lineage extraction to determine the default schema
        for unqualified table names in view definitions.
        """
        try:
            # Try to get the schema from the dataset identifier
            parts = dataset_identifier.split(".")

            # Handle the identifier format differently based on add_database_name_to_urn flag
            if self.config.add_database_name_to_urn:
                if len(parts) >= 3:
                    # Format is: database.schema.view when add_database_name_to_urn=True
                    db_name = parts[-3]
                    schema_name = parts[-2]
                    return db_name, schema_name
                elif len(parts) >= 2:
                    # Handle the case where database might be missing even with flag enabled
                    # If we have a database in the config, use that
                    db_name = str(self.config.database)
                    schema_name = parts[-2]
                    return db_name, schema_name
            else:
                # Format is: schema.view when add_database_name_to_urn=False
                if len(parts) >= 2:
                    # When add_database_name_to_urn is False, don't include database in the result
                    db_name = None
                    schema_name = parts[-2]
                    return db_name, schema_name
        except Exception as e:
            logger.warning(
                f"Error extracting schema from identifier {dataset_identifier}: {e}"
            )

        # Fall back to parent implementation if our approach fails
        db_name, schema_name = super().get_db_schema(dataset_identifier)
        return db_name, schema_name

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_schema_level_workunits(inspector, schema, database)

        # Process materialized views if enabled
        if self.config.include_materialized_views:
            try:
                yield from self.loop_materialized_views(inspector, schema, self.config)
            except Exception as e:
                self.report.warning(
                    title="Failed to Ingest Materialized Views",
                    message="Missing permissions to access MVIEWS. Grant SELECT on this view or set 'include_materialized_views: false' to disable.",
                    context=f"schema={schema}, table={self.config.data_dictionary_mode}_MVIEWS",
                    exc=e,
                )

    def _validate_tables_prefix(self, tables_prefix: str) -> None:
        """Validate tables_prefix to prevent SQL injection."""
        if tables_prefix not in (
            DataDictionaryMode.ALL.value,
            DataDictionaryMode.DBA.value,
        ):
            raise ValueError(f"Invalid tables_prefix: {tables_prefix}")

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """
        Get stored procedures, functions, and packages for a specific schema.
        """
        base_procedures = []
        tables_prefix = self.config.data_dictionary_mode.value

        # Oracle stores schema names in uppercase, so normalize the schema name
        normalized_schema = inspector.dialect.denormalize_name(schema) or schema.upper()

        with inspector.engine.connect() as conn:
            try:
                self._validate_tables_prefix(tables_prefix)
                procedures_query = PROCEDURES_QUERY.format(tables_prefix=tables_prefix)
                procedures = conn.execute(
                    sql.text(procedures_query), dict(schema=normalized_schema)
                )

                for row in procedures:
                    source_code = self._get_procedure_source_code(
                        conn=conn,
                        schema=normalized_schema,
                        procedure_name=row.name,
                        object_type=row.type,
                        tables_prefix=tables_prefix,
                    )

                    arguments = self._get_procedure_arguments(
                        conn=conn,
                        schema=normalized_schema,
                        procedure_name=row.name,
                        tables_prefix=tables_prefix,
                    )

                    dependencies = self._get_procedure_dependencies(
                        conn=conn,
                        schema=normalized_schema,
                        procedure_name=row.name,
                        tables_prefix=tables_prefix,
                    )

                    extra_props = {"object_type": row.type, "status": row.status}

                    # Add dependency information if available (flatten to strings)
                    if dependencies:
                        if "upstream" in dependencies:
                            extra_props["upstream_dependencies"] = ", ".join(
                                dependencies["upstream"]
                            )
                        if "downstream" in dependencies:
                            extra_props["downstream_dependencies"] = ", ".join(
                                dependencies["downstream"]
                            )

                    base_procedures.append(
                        BaseProcedure(
                            name=row.name,
                            language="SQL",
                            argument_signature=arguments,
                            return_type=None,
                            procedure_definition=source_code,
                            created=row.created,
                            last_altered=row.last_ddl_time,
                            comment=None,
                            extra_properties=extra_props,
                        )
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to get stored procedures for schema {schema}: {e}"
                )

                self.report.warning(
                    title="Failed to Ingest Stored Procedures",
                    message="Missing permissions to access OBJECTS/SOURCE/ARGUMENTS/DEPENDENCIES. Grant SELECT on these views or set 'include_stored_procedures: false' to disable.",
                    context=f"schema={schema}, table={self.config.data_dictionary_mode}_OBJECTS/SOURCE/ARGUMENTS/DEPENDENCIES",
                    exc=e,
                )

        return base_procedures

    def _get_procedure_source_code(
        self,
        conn: sqlalchemy.engine.Connection,
        schema: str,
        procedure_name: str,
        object_type: str,
        tables_prefix: str,
    ) -> Optional[str]:
        """Get procedure source code from ALL_SOURCE or DBA_SOURCE."""
        try:
            self._validate_tables_prefix(tables_prefix)
            source_query = PROCEDURE_SOURCE_QUERY.format(tables_prefix=tables_prefix)

            source_data = conn.execute(
                sql.text(source_query),
                dict(
                    schema=schema,
                    procedure_name=procedure_name,
                    object_type=object_type,
                ),
            )

            source_lines = []
            for row in source_data:
                source_lines.append(row.text)

            return "".join(source_lines) if source_lines else None

        except Exception as e:
            logger.warning(
                f"Failed to get source code for {object_type} {schema}.{procedure_name}: {e}"
            )
            return None

    def _get_procedure_arguments(
        self,
        conn: sqlalchemy.engine.Connection,
        schema: str,
        procedure_name: str,
        tables_prefix: str,
    ) -> Optional[str]:
        """Get procedure arguments from ALL_ARGUMENTS or DBA_ARGUMENTS."""
        try:
            # Validate tables_prefix to prevent injection
            self._validate_tables_prefix(tables_prefix)
            args_query = PROCEDURE_ARGUMENTS_QUERY.format(tables_prefix=tables_prefix)

            args_data = conn.execute(
                sql.text(args_query), dict(schema=schema, procedure_name=procedure_name)
            )

            arguments = []
            for row in args_data:
                arg_str = f"{row.in_out} {row.argument_name} {row.data_type}"
                arguments.append(arg_str)

            return ", ".join(arguments) if arguments else None

        except Exception as e:
            logger.warning(
                f"Failed to get arguments for procedure {schema}.{procedure_name}: {e}"
            )
            return None

    def _get_procedure_dependencies(
        self,
        conn: sqlalchemy.engine.Connection,
        schema: str,
        procedure_name: str,
        tables_prefix: str,
    ) -> Optional[Dict[str, List[str]]]:
        """Get procedure dependencies from ALL_DEPENDENCIES or DBA_DEPENDENCIES."""
        try:
            # Validate tables_prefix to prevent injection
            self._validate_tables_prefix(tables_prefix)

            upstream_query = PROCEDURE_UPSTREAM_DEPENDENCIES_QUERY.format(
                tables_prefix=tables_prefix
            )
            upstream_data = conn.execute(
                sql.text(upstream_query),
                dict(schema=schema, procedure_name=procedure_name),
            )

            downstream_query = PROCEDURE_DOWNSTREAM_DEPENDENCIES_QUERY.format(
                tables_prefix=tables_prefix
            )
            downstream_data = conn.execute(
                sql.text(downstream_query),
                dict(schema=schema, procedure_name=procedure_name),
            )

            dependencies = {}

            upstream_deps = []
            for row in upstream_data:
                dep_str = f"{row.referenced_owner}.{row.referenced_name} ({row.referenced_type})"
                upstream_deps.append(dep_str)

            if upstream_deps:
                dependencies["upstream"] = upstream_deps

            downstream_deps = []
            for row in downstream_data:
                dep_str = f"{row.owner}.{row.name} ({row.type})"
                downstream_deps.append(dep_str)

            if downstream_deps:
                dependencies["downstream"] = downstream_deps

            return dependencies if dependencies else None

        except Exception as e:
            logger.warning(
                f"Failed to get dependencies for procedure {schema}.{procedure_name}: {e}"
            )
            return None

    def loop_materialized_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: OracleConfig,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        """Loop through materialized views in the schema."""
        if hasattr(inspector, "get_materialized_view_names"):
            mview_names = inspector.get_materialized_view_names(schema=schema)
        else:
            logger.info("Fallback for regular inspector")
            mview_names = self._get_materialized_view_names_fallback(
                inspector=inspector, schema=schema
            )

        for mview in mview_names:
            dataset_name = self.get_identifier(
                schema=schema, entity=mview, inspector=inspector
            )

            if not self.config.view_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            yield from self._process_materialized_view(
                dataset_name=dataset_name,
                inspector=inspector,
                schema=schema,
                mview=mview,
                sql_config=sql_config,
            )

    def _get_materialized_view_names_fallback(
        self, inspector: Inspector, schema: Optional[str] = None
    ) -> List[str]:
        """Fallback method to get materialized view names when using regular inspector."""
        try:
            schema = inspector.dialect.denormalize_name(
                schema or inspector.dialect.default_schema_name
            )
            tables_prefix = self.config.data_dictionary_mode.value

            self._validate_tables_prefix(tables_prefix)
            with inspector.engine.connect() as conn:
                cursor = conn.execute(
                    sql.text(
                        MATERIALIZED_VIEWS_QUERY.format(tables_prefix=tables_prefix)
                    ),
                    dict(owner=schema),
                )

                return [
                    inspector.dialect.normalize_name(row[0])
                    or _raise_err(
                        ValueError(f"Invalid materialized view name: {row[0]}")
                    )
                    for row in cursor
                ]
        except Exception as e:
            logger.warning(
                f"Failed to get materialized view names for schema {schema}: {e}"
            )
            return []

    def _process_materialized_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        mview: str,
        sql_config: OracleConfig,
    ) -> Iterable[Union[MetadataWorkUnit]]:
        """Process materialized view similar to regular view but with materialized flag."""
        try:
            # Get materialized view definition
            if hasattr(inspector, "get_materialized_view_definition"):
                mview_definition = inspector.get_materialized_view_definition(
                    mview_name=mview, schema=schema
                )
            else:
                # Fallback for regular inspector
                mview_definition = self._get_materialized_view_definition_fallback(
                    inspector=inspector, mview_name=mview, schema=schema
                )

            description, properties, location_urn = self.get_table_properties(
                inspector=inspector, schema=schema, table=mview
            )

            # Get columns for the materialized view
            columns = self._get_columns(
                dataset_name=dataset_name,
                inspector=inspector,
                schema=schema,
                table=mview,
            )
            schema_metadata = get_schema_metadata(
                sql_report=self.report,
                dataset_name=dataset_name,
                platform=self.platform,
                columns=columns,
            )

            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            properties["materialized_view_definition"] = mview_definition
            if mview_definition and self.config.include_view_lineage:
                default_db = None
                default_schema = None
                try:
                    default_db, default_schema = self.get_db_schema(dataset_name)
                except ValueError:
                    logger.warning(
                        f"Invalid materialized view identifier: {dataset_name}"
                    )
                self.aggregator.add_view_definition(
                    view_urn=dataset_urn,
                    view_definition=mview_definition,
                    default_db=default_db,
                    default_schema=default_schema,
                )

            db_name = self.get_db_name(inspector=inspector)
            yield from self.add_table_to_schema_container(
                dataset_urn=dataset_urn,
                db_name=db_name,
                schema=schema,
            )

            # Create dataset properties
            dataset_properties = models.DatasetPropertiesClass(
                name=mview,
                description=description,
                customProperties=properties,
            )

            # Create MCP for dataset properties
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=dataset_properties,
            ).as_workunit()

            if schema_metadata:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=schema_metadata,
                ).as_workunit()

            # Add platform instance if configured
            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            # Mark as materialized view subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
            ).as_workunit()

            # Add view properties aspect with materialized=True
            view_properties_aspect = ViewPropertiesClass(
                materialized=True, viewLanguage="SQL", viewLogic=mview_definition
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

            if self.config.domain and self.domain_registry:
                domain_urn = self.domain_registry.get_domain_urn(dataset_urn)
                if domain_urn:
                    yield from auto_workunit(
                        self.gen_domain_aspect(dataset_urn, domain_urn)  # type: ignore[attr-defined]
                    )

        except Exception as e:
            self.report.warning(
                title="Failed to Process Materialized View",
                message="Error processing materialized view. Check permissions on _MVIEWS or set 'include_materialized_views: false'.",
                context=f"view={schema}.{mview}, {self.config.data_dictionary_mode}_MVIEWS",
                exc=e,
            )

    def _get_materialized_view_definition_fallback(
        self, inspector: Inspector, mview_name: str, schema: Optional[str] = None
    ) -> Union[str, None]:
        """Fallback method to get materialized view definition when using regular inspector."""
        try:
            denormalized_mview_name = inspector.dialect.denormalize_name(mview_name)
            schema = inspector.dialect.denormalize_name(
                schema or inspector.dialect.default_schema_name
            )

            tables_prefix = self.config.data_dictionary_mode.value

            params = {"mview_name": denormalized_mview_name}
            text = MATERIALIZED_VIEW_DEFINITION_QUERY.format(
                tables_prefix=tables_prefix
            )

            if schema is not None:
                params["owner"] = schema
                text += "\nAND owner = :owner"

            result = inspector.bind.execute(sql.text(text), params).scalar()
            return result
        except Exception as e:
            logger.warning(
                f"Failed to get materialized view definition for {schema}.{mview_name}: {e}"
            )
            return None

    def get_workunits(self):
        """
        Override get_workunits to patch Oracle dialect for custom types.
        """
        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            return super().get_workunits()

    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime.datetime],
        schema: str,
    ) -> Optional[List[str]]:
        tables_table_name = (
            "ALL_TABLES"
            if self.config.data_dictionary_mode == DataDictionaryMode.ALL
            else "DBA_TABLES"
        )

        # If stats are available , they are used even if they are stale.
        # Assuming that the table would typically grow over time, this will ensure to filter
        # large tables known at stats collection time from profiling candidates.
        # If stats are not available (NULL), such tables are not filtered and are considered
        # as profiling candidates.
        cursor = inspector.bind.execute(
            sql.text(
                PROFILE_CANDIDATES_QUERY.format(tables_table_name=tables_table_name)
            ),
            dict(
                owner=inspector.dialect.denormalize_name(schema),
                table_row_limit=self.config.profiling.profile_table_row_limit,
                table_size_limit=self.config.profiling.profile_table_size_limit,
            ),
        )

        TABLE_NAME_COL_LOC = 1
        return [
            self.get_identifier(
                schema=schema,
                entity=inspector.dialect.normalize_name(row[TABLE_NAME_COL_LOC])
                or _raise_err(
                    ValueError(f"Invalid table name: {row[TABLE_NAME_COL_LOC]}")
                ),
                inspector=inspector,
            )
            for row in cursor
        ]
