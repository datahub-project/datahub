import logging
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
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig

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
    # defaults
    scheme: str = Field(
        default="oracle+oracledb",
        description="Will be set automatically to default value.",
    )
    service_name: Optional[str] = Field(
        default=None, description="Oracle service name. If using, omit `database`."
    )
    database: Optional[str] = Field(
        default=None, description="If using, omit `service_name`."
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

    def get_sql_alchemy_url(self):
        url = super().get_sql_alchemy_url()
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
        try:
            # Try to retrieve current DB name by executing query
            db_name = self._inspector_instance.bind.execute(
                sql.text("select sys_context('USERENV','DB_NAME') from dual")
            ).scalar()
            return str(db_name)
        except sqlalchemy.exc.DatabaseError as e:
            logger.error("Error fetching DB name: " + str(e))
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
                ) AS identity_options""".format(
                dblink=dblink
            )
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
                    logger.warning(
                        f"Did not recognize type {coltype} of column {colname}"
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
            WHERE table_name = CAST(:table_name AS VARCHAR(128))
            AND owner = CAST(:schema_name AS VARCHAR(128))
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
            "\nac.constraint_name,"  # 0
            "\nac.constraint_type,"  # 1
            "\nloc.column_name AS local_column,"  # 2
            "\nrem.table_name AS remote_table,"  # 3
            "\nrem.column_name AS remote_column,"  # 4
            "\nrem.owner AS remote_owner,"  # 5
            "\nloc.position as loc_pos,"  # 6
            "\nrem.position as rem_pos,"  # 7
            "\nac.search_condition,"  # 8
            "\nac.delete_rule"  # 9
            "\nFROM dba_constraints%(dblink)s ac,"
            "\ndba_cons_columns%(dblink)s loc,"
            "\ndba_cons_columns%(dblink)s rem"
            "\nWHERE ac.table_name = CAST(:table_name AS VARCHAR2(128))"
            "\nAND ac.constraint_type IN ('R','P', 'U', 'C')"
        )

        if schema is not None:
            params["owner"] = schema
            text += "\nAND ac.owner = CAST(:owner AS VARCHAR2(128))"

        text += (
            "\nAND ac.owner = loc.owner"
            "\nAND ac.constraint_name = loc.constraint_name"
            "\nAND ac.r_owner = rem.owner(+)"
            "\nAND ac.r_constraint_name = rem.constraint_name(+)"
            "\nAND (rem.position IS NULL or loc.position=rem.position)"
            "\nORDER BY ac.constraint_name, loc.position"
        )

        text = text % {"dblink": dblink}
        rp = self._inspector_instance.bind.execute(sql.text(text), params)
        constraint_data = rp.fetchall()
        return constraint_data

    def get_pk_constraint(
        self, table_name: str, schema: Optional[str] = None, dblink: str = ""
    ) -> Dict:

        denormalized_table_name = self._inspector_instance.dialect.denormalize_name(
            table_name
        )
        assert denormalized_table_name

        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        pkeys = []
        constraint_name = None
        constraint_data = self._get_constraint_data(
            denormalized_table_name, schema, dblink
        )

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
            if cons_type == "P":
                if constraint_name is None:
                    constraint_name = self._inspector_instance.dialect.normalize_name(
                        cons_name
                    )
                pkeys.append(local_column)

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
            text += " AND owner = :schema"
            params["schema"] = schema

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

    def get_workunits(self):
        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            return super().get_workunits()
