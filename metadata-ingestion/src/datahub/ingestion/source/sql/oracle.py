import datetime
import logging
import platform
import re

# This import verifies that the dependencies are available.
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, NoReturn, Optional, Tuple, Union, cast
from unittest.mock import patch

import oracledb
import pydantic
import sqlalchemy.engine
from pydantic.fields import Field
from sqlalchemy import event, sql
from sqlalchemy.dialects.oracle.base import ischema_names
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import FLOAT, INTEGER, TIMESTAMP

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    make_sqlalchemy_type,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
)

logger = logging.getLogger(__name__)

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

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


class OracleConfig(BasicSQLAlchemyConfig):
    # TODO: Change scheme to oracle+oracledb when sqlalchemy>=2 is supported
    scheme: str = Field(
        default="oracle",
        description="Will be set automatically to default value.",
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
    data_dictionary_mode: Optional[str] = Field(
        default="ALL",
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

    @pydantic.validator("service_name")
    def check_service_name(cls, v, values):
        if values.get("database") and v:
            raise ValueError(
                "specify one of 'database' and 'service_name', but not both"
            )
        return v

    @pydantic.validator("data_dictionary_mode")
    def check_data_dictionary_mode(cls, values):
        if values not in ("ALL", "DBA"):
            raise ValueError("Specify one of data dictionary views mode: 'ALL', 'DBA'.")
        return values

    @pydantic.validator("thick_mode_lib_dir", always=True)
    def check_thick_mode_lib_dir(cls, v, values):
        if (
            v is None
            and values.get("enable_thick_mode")
            and (platform.system() == "Darwin" or platform.system() == "Windows")
        ):
            raise ValueError(
                "Specify 'thick_mode_lib_dir' on Mac/Windows when enable_thick_mode is true"
            )
        return v

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
            # Try to retrieve current DB name by executing query
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
            sql.text("SELECT username FROM dba_users ORDER BY username")
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid schema name: {row[0]}"))
            for row in cursor
        ]

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        """
        skip order_by, we are not using order_by
        """
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
            sql.text("SELECT view_name FROM dba_views WHERE owner = :owner"),
            dict(owner=self._inspector_instance.dialect.denormalize_name(schema)),
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

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
            "\nac.r_table_name AS remote_table,"
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
        pkeys = []
        constraint_name = None

        try:
            for row in self._get_constraint_data(table_name, schema, dblink):
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
        # Map method call to wrapper class
        if item in self.__dict__:
            return getattr(self, item)
        # Map method call to original class
        return getattr(self._inspector_instance, item)


@platform_name("Oracle")
@config_class(OracleConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
class OracleSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling

    Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).
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

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.parse_obj(config_dict)
        return cls(config, ctx)

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
            # Sqlalchemy inspector uses ALL_* tables as per oracle dialect implementation.
            # OracleInspectorObjectWrapper provides alternate implementation using DBA_* tables.
            if self.config.data_dictionary_mode != "ALL":
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
            "ALL_TABLES" if self.config.data_dictionary_mode == "ALL" else "DBA_TABLES"
        )

        # If stats are available , they are used even if they are stale.
        # Assuming that the table would typically grow over time, this will ensure to filter
        # large tables known at stats collection time from profiling candidates.
        # If stats are not available (NULL), such tables are not filtered and are considered
        # as profiling candidates.
        cursor = inspector.bind.execute(
            sql.text(
                f"""SELECT
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
