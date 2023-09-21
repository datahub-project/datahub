import logging
from typing import Any, Iterable, List, NoReturn, Optional, Tuple, cast
from unittest.mock import patch
import re

# This import verifies that the dependencies are available.
import cx_Oracle
import pydantic
from pydantic.fields import Field
from sqlalchemy import event, sql
from sqlalchemy.dialects.oracle.base import ischema_names
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.util import warn, defaultdict, py2k
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import INTEGER, FLOAT, TIMESTAMP

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

    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    elif defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    cursor.outputtypehandler = output_type_handler


def class_usage_notification(cls, func):
    def _wrapper(*args, **kwargs):
        logger.info(f"{cls.__name__}.{func.__name__} is in used.")
        return func(*args, **kwargs)

    return _wrapper


def inspector_wraper_usage_notificcation(dec):
    def _decorator(cls):
        for attr in cls.__dict__:
            if not attr.startswith('__') and callable(getattr(cls, attr)):
                setattr(cls, attr, dec(cls, getattr(cls, attr)))
        return cls

    return _decorator


class OracleConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: str = Field(
        default="oracle+cx_oracle",
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
        default='ALL',
        description="The data dictionary views mode, to extract information about schema objects ('All' and 'DBA' views are supported). (https://docs.oracle.com/cd/E11882_01/nav/catalog_views.htm)"
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
        if values not in ('ALL', 'DBA'):
            raise ValueError(
                "Specify one of data dictionary views mode: 'ALL', 'DBA'."
            )
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
            if self.database_alias:
                return f"{self.database_alias}.{regular}"
            if self.database:
                return f"{self.database}.{regular}"
            return regular
        else:
            return regular


@inspector_wraper_usage_notificcation(class_usage_notification)
class OracleInspectorObjectWrapper:
    """
    Inspector class wrapper, which queries DBA_TABLES instead of ALL_TABLES
    """

    def __init__(self, inspector_instance: Inspector):
        self._inspector_instance = inspector_instance
        self.log = logging.getLogger(__name__)
        # tables that we don't want to ingest into the DataHub
        self.exclude_tablespaces: Tuple[str, str] = ("SYSTEM", "SYSAUX")

    def has_table(self, table_name, schema=None):
        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text("SELECT table_name FROM dba_tables "
                     "WHERE table_name = CAST(:name AS VARCHAR2(128)) "
                     "AND owner = CAST(:schema_name AS VARCHAR2(128))"
                     ),
            dict(
                name=self._inspector_instance.dialect.denormalize_name(table_name),
                schema_name=self._inspector_instance.dialect.denormalize_name(schema)
            )
        )

        return cursor.first() is not None

    def has_sequence(self, sequence_name, schema=None):
        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text(
                "SELECT sequence_name FROM dba_sequences "
                "WHERE sequence_name = :name AND "
                "sequence_owner = :schema_name"
            ),
            dict(
                name=self._inspector_instance.dialect.denormalize_name(sequence_name),
                schema_name=self._inspector_instance.dialect.denormalize_name(schema)
            )
        )

        return cursor.first() is not None

    def _resolve_synonym(
            self,
            desired_owner=None,
            desired_synonym=None,
            desired_table=None,
    ):
        """search for a local synonym matching the given desired owner/name.

        if desired_owner is None, attempts to locate a distinct owner.

        returns the actual name, owner, dblink name, and synonym name if
        found.
        """

        q = (
            "SELECT owner, table_owner, table_name, db_link, "
            "synonym_name FROM dba_synonyms WHERE "
        )
        clauses = []
        params = {}
        if desired_synonym:
            clauses.append(
                "synonym_name = CAST(:synonym_name AS VARCHAR2(128))"
            )
            params["synonym_name"] = desired_synonym
        if desired_owner:
            clauses.append("owner = CAST(:desired_owner AS VARCHAR2(128))")
            params["desired_owner"] = desired_owner
        if desired_table:
            clauses.append("table_name = CAST(:tname AS VARCHAR2(128))")
            params["tname"] = desired_table

        q += " AND ".join(clauses)

        result = self._inspector_instance.bind.execution_options(future_result=True).execute(sql.text(q), params)

        if desired_owner:
            row = result.mappings().first()
            if row:
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None
        else:
            rows = result.mappings().all()
            if len(rows) > 1:
                raise AssertionError(
                    "There are multiple tables visible to the schema, you "
                    "must specify owner"
                )
            elif len(rows) == 1:
                row = rows[0]
                return (
                    row["table_name"],
                    row["table_owner"],
                    row["db_link"],
                    row["synonym_name"],
                )
            else:
                return None, None, None, None

    def _prepare_reflection_args(
            self,
            table_name,
            schema=None,
            resolve_synonyms=False,
            dblink="",
            **kw
    ):

        if resolve_synonyms:
            actual_name, owner, dblink, synonym = self._resolve_synonym(
                desired_owner=self._inspector_instance.dialect.denormalize_name(schema),
                desired_synonym=self._inspector_instance.dialect.denormalize_name(table_name)
            )
        else:
            actual_name, owner, dblink, synonym = None, None, None, None
        if not actual_name:
            actual_name = self._inspector_instance.dialect.denormalize_name(table_name)

        if dblink:
            # using user_db_links here since all_db_links appears
            # to have more restricted permissions.
            # https://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_admin005.htm
            # will need to hear from more users if we are doing
            # the right thing here.  See [ticket:2619]
            owner = self._inspector_instance.bind.scalar(
                sql.text("SELECT username FROM user_db_links " "WHERE db_link=:link"),
                dict(link=dblink)
            )

            dblink = "@" + dblink
        elif not owner:
            owner = self._inspector_instance.dialect.denormalize_name(
                schema or self._inspector_instance.dialect.default_schema_name)

        return actual_name, owner, dblink or "", synonym

    def get_schema_names(self) -> List[str]:
        cursor = self._inspector_instance.bind.execute(sql.text("SELECT username FROM dba_users ORDER BY username"))

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid schema name: {row[0]}"))
            for row in cursor
        ]

    def get_table_names(
            self, schema: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[str]:
        """
        skip order_by, we are not using order_by
        """
        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        sql_str = "SELECT table_name FROM dba_tables WHERE "
        if self.exclude_tablespaces:
            tablespace_str = ", ".join([f"'{ts}'" for ts in self.exclude_tablespaces])
            sql_str += (
                f"nvl(tablespace_name, 'no tablespace') NOT IN ({tablespace_str}) AND "
            )

        sql_str += "OWNER = :owner AND IOT_NAME IS NULL "
        logger.debug(f"SQL = {sql_str}")
        cursor = self._inspector_instance.bind.execute(sql.text(sql_str), owner=schema)

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_temp_table_names(self, **kw):

        schema = self._inspector_instance.dialect.denormalize_name(self._inspector_instance.dialect.default_schema_name)

        sql_str = "SELECT table_name FROM dba_tables WHERE "
        if self.exclude_tablespaces:
            sql_str += (
                    "nvl(tablespace_name, 'no tablespace') "
                    "NOT IN (%s) AND "
                    % (", ".join(["'%s'" % ts for ts in self.exclude_tablespaces]))
            )
        sql_str += (
            "OWNER = :owner "
            "AND IOT_NAME IS NULL "
            "AND DURATION IS NOT NULL"
        )

        cursor = self._inspector_instance.bind.execute(sql.text(sql_str), dict(owner=schema))
        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_view_names(self, schema=None, **kw):

        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text("SELECT view_name FROM dba_views WHERE owner = :owner"),
            dict(owner=self._inspector_instance.dialect.denormalize_name(schema))
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_sequence_names(self, schema=None, **kw):

        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        cursor = self._inspector_instance.bind.execute(
            sql.text(
                "SELECT sequence_name FROM dba_sequences "
                "WHERE sequence_owner = :schema_name"
            ),
            dict(schema_name=self._inspector_instance.dialect.denormalize_name(schema))
        )

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def get_table_options(self, table_name, schema=None, **kw):
        options = {}

        resolve_synonyms = kw.get("oracle_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        params = {"table_name": table_name}

        columns = ["table_name"]
        if self._inspector_instance.dialect._supports_table_compression:
            columns.append("compression")
        if self._inspector_instance.dialect._supports_table_compress_for:
            columns.append("compress_for")

        text = (
            "SELECT %(columns)s "
            "FROM DBA_TABLES%(dblink)s "
            "WHERE table_name = CAST(:table_name AS VARCHAR(128))"
        )

        if schema is not None:
            params["owner"] = schema
            text += " AND owner = CAST(:owner AS VARCHAR(128)) "
        text = text % {"dblink": dblink, "columns": ", ".join(columns)}

        result = self._inspector_instance.bind.execute(sql.text(text), params)

        enabled = dict(DISABLED=False, ENABLED=True)

        row = result.first()
        if row:
            if "compression" in row._fields and enabled.get(
                    row.compression, False
            ):
                if "compress_for" in row._fields:
                    options["oracle_compress"] = row.compress_for
                else:
                    options["oracle_compress"] = True

        return options

    def get_columns(self, table_name, schema=None, **kw):
        """

        kw arguments can be:

            oracle_resolve_synonyms

            dblink

        """
        resolve_synonyms = kw.get("oracle_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        columns = []
        if self._inspector_instance.dialect._supports_char_length:
            char_length_col = "char_length"
        else:
            char_length_col = "data_length"

        if self._inspector_instance.dialect.server_version_info >= (12,):
            identity_cols = """\
                col.default_on_null,
                (
                    SELECT id.generation_type || ',' || id.IDENTITY_OPTIONS
                    FROM DBA_TAB_IDENTITY_COLS%(dblink)s id
                    WHERE col.table_name = id.table_name
                    AND col.column_name = id.column_name
                    AND col.owner = id.owner
                ) AS identity_options""" % {
                "dblink": dblink
            }
        else:
            identity_cols = "NULL as default_on_null, NULL as identity_options"

        params = {"table_name": table_name}

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
                    warn(
                        "Did not recognize type '%s' of column '%s'"
                        % (coltype, colname)
                    )
                    coltype = sqltypes.NULLTYPE

            if generated == "YES":
                computed = dict(sqltext=default)
                default = None
            else:
                computed = None

            if identity_options is not None:
                identity = self._inspector_instance.dialect._parse_identity_options(identity_options, default_on_nul)
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

    def get_table_comment(
            self,
            table_name,
            schema=None,
            resolve_synonyms=False,
            dblink="",
            **kw
    ):

        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        schema = self._inspector_instance.dialect.denormalize_name(schema or self.default_schema_name)

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
            dict(table_name=table_name, schema_name=schema)
        )

        return {"text": c.scalar()}

    def get_indexes(
            self,
            table_name,
            schema=None,
            resolve_synonyms=False,
            dblink="",
            **kw
    ):
        info_cache = kw.get("info_cache")
        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        indexes = []

        params = {"table_name": table_name}
        text = (
            "SELECT a.index_name, a.column_name, "
            "\nb.index_type, b.uniqueness, b.compression, b.prefix_length "
            "\nFROM DBA_IND_COLUMNS%(dblink)s a, "
            "\nDBA_INDEXES%(dblink)s b "
            "\nWHERE "
            "\na.index_name = b.index_name "
            "\nAND a.table_owner = b.table_owner "
            "\nAND a.table_name = b.table_name "
            "\nAND a.table_name = CAST(:table_name AS VARCHAR(128))"
        )

        if schema is not None:
            params["schema"] = schema
            text += "AND a.table_owner = :schema "

        text += "ORDER BY a.index_name, a.column_position"

        text = text % {"dblink": dblink}

        q = sql.text(text)
        rp = self._inspector_instance.bind.execute(q, params)
        last_index_name = None
        pk_constraint = self.get_pk_constraint(
            table_name,
            schema,
            resolve_synonyms=resolve_synonyms,
            dblink=dblink,
            info_cache=kw.get("info_cache"),
        )

        uniqueness = dict(NONUNIQUE=False, UNIQUE=True)
        enabled = dict(DISABLED=False, ENABLED=True)

        oracle_sys_col = re.compile(r"SYS_NC\d+\$", re.IGNORECASE)

        index = None
        for rset in rp:
            index_name_normalized = self._inspector_instance.dialect.normalize_name(rset.index_name)

            # skip primary key index.  This is refined as of
            # [ticket:5421].  Note that ALL_INDEXES.GENERATED will by "Y"
            # if the name of this index was generated by Oracle, however
            # if a named primary key constraint was created then this flag
            # is false.
            if (
                    pk_constraint
                    and index_name_normalized == pk_constraint["name"]
            ):
                continue

            if rset.index_name != last_index_name:
                index = dict(
                    name=index_name_normalized,
                    column_names=[],
                    dialect_options={},
                )
                indexes.append(index)
            index["unique"] = uniqueness.get(rset.uniqueness, False)

            if rset.index_type in ("BITMAP", "FUNCTION-BASED BITMAP"):
                index["dialect_options"]["oracle_bitmap"] = True
            if enabled.get(rset.compression, False):
                index["dialect_options"][
                    "oracle_compress"
                ] = rset.prefix_length

            # filter out Oracle SYS_NC names.  could also do an outer join
            # to the all_tab_columns table and check for real col names there.
            if not oracle_sys_col.match(rset.column_name):
                index["column_names"].append(
                    self._inspector_instance.dialect.normalize_name(rset.column_name)
                )
            last_index_name = rset.index_name

        return indexes

    def _get_constraint_data(
            self, table_name, schema=None, dblink="", **kw
    ):

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

    def get_pk_constraint(self, table_name, schema=None, **kw):

        resolve_synonyms = kw.get("oracle_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )
        pkeys = []
        constraint_name = None
        constraint_data = self._get_constraint_data(
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple([self._inspector_instance.dialect.normalize_name(x) for x in row[2:6]])
            if cons_type == "P":
                if constraint_name is None:
                    constraint_name = self._inspector_instance.dialect.normalize_name(cons_name)
                pkeys.append(local_column)

        return {"constrained_columns": pkeys, "name": constraint_name}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """

        kw arguments can be:

            oracle_resolve_synonyms

            dblink

        """

        requested_schema = schema  # to check later on
        resolve_synonyms = kw.get("oracle_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
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

        fkeys = defaultdict(fkey_rec)

        for row in constraint_data:
            (
                cons_name,
                cons_type,
                local_column,
                remote_table,
                remote_column,
                remote_owner,
            ) = row[0:2] + tuple([self._inspector_instance.dialect.normalize_name(x) for x in row[2:6]])

            cons_name = self._inspector_instance.dialect.normalize_name(cons_name)

            if cons_type == "R":
                if remote_table is None:
                    # ticket 363
                    warn(
                        (
                            "Got 'None' querying 'table_name' from "
                            "dba_cons_columns%(dblink)s - does the user have "
                            "proper rights to the table?"
                        )
                        % {"dblink": dblink}
                    )
                    continue

                rec = fkeys[cons_name]
                rec["name"] = cons_name
                local_cols, remote_cols = (
                    rec["constrained_columns"],
                    rec["referred_columns"],
                )

                if not rec["referred_table"]:
                    if resolve_synonyms:
                        (
                            ref_remote_name,
                            ref_remote_owner,
                            ref_dblink,
                            ref_synonym,
                        ) = self._resolve_synonym(
                            connection,
                            desired_owner=self._inspector_instance.dialect.denormalize_name(remote_owner),
                            desired_table=self._inspector_instance.dialect.denormalize_name(remote_table),
                        )
                        if ref_synonym:
                            remote_table = self._inspector_instance.dialect.normalize_name(ref_synonym)
                            remote_owner = self._inspector_instance.dialect.normalize_name(
                                ref_remote_owner
                            )

                    rec["referred_table"] = remote_table

                    if (
                            requested_schema is not None
                            or self._inspector_instance.dialect.denormalize_name(remote_owner) != schema
                    ):
                        rec["referred_schema"] = remote_owner

                    if row[9] != "NO ACTION":
                        rec["options"]["ondelete"] = row[9]

                local_cols.append(local_column)
                remote_cols.append(remote_column)

        return list(fkeys.values())

    def get_view_definition(
            self,
            view_name,
            schema=None,
            resolve_synonyms=False,
            dblink="",
            **kw
    ):

        info_cache = kw.get("info_cache")
        (view_name, schema, dblink, synonym) = self._prepare_reflection_args(
            view_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        params = {"view_name": view_name}
        text = "SELECT text FROM dba_views WHERE view_name=:view_name"

        if schema is not None:
            text += " AND owner = :schema"
            params["schema"] = schema

        rp = self._inspector_instance.bind.execute(sql.text(text), params).scalar()
        if rp:
            if py2k:
                rp = rp.decode(self.encoding)
            return rp
        else:
            return None

    def get_check_constraints(
            self, table_name, schema=None, include_all=False, **kw
    ):

        resolve_synonyms = kw.get("oracle_resolve_synonyms", False)
        dblink = kw.get("dblink", "")
        info_cache = kw.get("info_cache")

        (table_name, schema, dblink, synonym) = self._prepare_reflection_args(
            table_name,
            schema,
            resolve_synonyms,
            dblink,
            info_cache=info_cache,
        )

        constraint_data = self._get_constraint_data(
            table_name,
            schema,
            dblink,
            info_cache=kw.get("info_cache"),
        )

        check_constraints = filter(lambda x: x[1] == "C", constraint_data)

        return [
            {"name": self._inspector_instance.dialect.normalize_name(cons[0]), "sqltext": cons[8]}
            for cons in check_constraints
            if include_all or not re.match(r"..+?. IS NOT NULL$", cons[8])
        ]

    def __getattr__(self, item: str) -> Any:
        # Map method call to wrapper class
        if item in self.__dict__:
            return getattr(self, item)
        # Map method call to original class
        return getattr(self._inspector_instance, item)


@platform_name("Oracle")
@config_class(OracleConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
class OracleSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling

    Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "oracle")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        for inspector in super().get_inspectors():
            event.listen(
                inspector.engine, "before_cursor_execute", before_cursor_execute
            )
            logger.info(f'Data dictionary mode is: "{self.config.data_dictionary_mode}".')
            if self.config.data_dictionary_mode != OracleConfig.__fields__.get("data_dictionary_mode").default:
                yield cast(Inspector, OracleInspectorObjectWrapper(inspector))
            # To silent the mypy lint error
            yield cast(Inspector, inspector)

    def get_workunits(self):
        with patch.dict(
                "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
                {klass.__name__: klass for klass in extra_oracle_types},
                clear=False,
        ):
            return super().get_workunits()
