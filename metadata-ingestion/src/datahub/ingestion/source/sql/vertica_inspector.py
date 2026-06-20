"""Vertica-specific SQLAlchemy inspection helpers.

This module re-creates the Vertica-specific inspection methods that used to live
in the abandoned ``vertica-sqlalchemy-dialect`` package (its
``VerticaInspector(reflection.Inspector)`` subclass). That package pinned
``sqlalchemy<=1.4.44`` and is no longer maintained, which blocked DataHub from
moving to SQLAlchemy 2.0.

The replacement base dialect is ``sqlalchemy-vertica-python`` (module
``sqla_vertica_python``), which works on SQLAlchemy 2.0 but only implements the
standard reflection surface (``get_columns``, ``get_table_names``,
``get_view_names``, ``get_schema_names``, ``get_pk_constraint``,
``get_foreign_keys``). The Vertica-specific concepts -- projections, ML models,
owners, projection/view lineage, and the database/schema property bags -- are no
longer provided by any dialect.

:class:`VerticaInspector` here WRAPS a standard SQLAlchemy ``Inspector`` via
composition. Standard reflection calls are delegated to the wrapped inspector;
the Vertica-specific methods are re-implemented as direct SQL against the
``v_catalog``/``v_monitor`` system tables, lifted (and adapted to SQLAlchemy 2.0
idioms -- ``sqlalchemy.text(...)`` plus ``.mappings()`` for dict-style rows) from
the original dialect's ``base.py``.
"""

import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, INTERVAL
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import String
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    FLOAT,
    INTEGER,
    NUMERIC,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
)
from sqlalchemy.util import warn

logger: logging.Logger = logging.getLogger(__name__)


class UUID(String):
    """The SQL UUID type."""

    __visit_name__ = "UUID"


class TIMESTAMP_WITH_PRECISION(TIMESTAMP):
    """The SQL TIMESTAMP With Precision type.

    Since Vertica supports precision values for timestamp this allows ingestion
    of timestamp fields with precision values.
    PS: THIS DATA IS CURRENTLY UNUSED, IT JUST FIXES INGESTION PROBLEMS.
    """

    __visit_name__ = "TIMESTAMP"

    def __init__(self, timezone: bool = False, precision: Optional[int] = None) -> None:
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


def TIMESTAMP_WITH_TIMEZONE(*args: Any, **kwargs: Any) -> TIMESTAMP_WITH_PRECISION:
    kwargs["timezone"] = True
    return TIMESTAMP_WITH_PRECISION(*args, **kwargs)


def TIME_WITH_TIMEZONE(*args: Any, **kwargs: Any) -> TIME:
    kwargs["timezone"] = True
    return TIME(*args, **kwargs)


# Lifted verbatim from vertica-sqlalchemy-dialect's base.py so reflected column
# data types map to the same SQLAlchemy types as before.
ischema_names: Dict[str, Any] = {
    "INT": INTEGER,
    "INTEGER": INTEGER,
    "INT8": INTEGER,
    "BIGINT": BIGINT,
    "SMALLINT": SMALLINT,
    "TINYINT": SMALLINT,
    "CHAR": CHAR,
    "VARCHAR": VARCHAR,
    "VARCHAR2": VARCHAR,
    "TEXT": VARCHAR,
    "NUMERIC": NUMERIC,
    "DECIMAL": NUMERIC,
    "NUMBER": NUMERIC,
    "MONEY": NUMERIC,
    "FLOAT": FLOAT,
    "FLOAT8": FLOAT,
    "REAL": REAL,
    "DOUBLE": DOUBLE_PRECISION,
    "TIMESTAMP": TIMESTAMP_WITH_PRECISION,
    "TIMESTAMP WITH TIMEZONE": TIMESTAMP(timezone=True),
    "TIMESTAMPTZ": TIMESTAMP_WITH_TIMEZONE,
    "TIME": TIME,
    "TIME WITH TIMEZONE": TIME(timezone=True),
    "TIMETZ": TIME_WITH_TIMEZONE,
    "INTERVAL": INTERVAL,
    "INTERVAL HOUR TO SECOND": INTERVAL,
    "INTERVAL HOUR TO MINUTE": INTERVAL,
    "INTERVAL DAY TO SECOND": INTERVAL,
    "INTERVAL YEAR TO MONTH": INTERVAL,
    "DOUBLE PRECISION": DOUBLE_PRECISION,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "SMALLDATETIME": DATETIME,
    "BINARY": BINARY,
    "VARBINARY": VARBINARY,
    "RAW": BLOB,
    "BYTEA": BYTEA,
    "BOOLEAN": BOOLEAN,
    "LONG VARBINARY": BLOB,
    "LONG VARCHAR": VARCHAR,
    "GEOMETRY": BLOB,
    "GEOGRAPHY": BLOB,
    "UUID": UUID,
}


class VerticaInspector:
    """SQLAlchemy 2.0 compatible replacement for the dialect's VerticaInspector.

    Wraps a standard SQLAlchemy :class:`Inspector` (composition rather than
    subclassing). Standard reflection methods are delegated to the wrapped
    inspector via ``__getattr__``; the Vertica-specific methods below are
    implemented as direct SQL.
    """

    def __init__(self, inspector: Inspector) -> None:
        self._inspector = inspector

    def __getattr__(self, name: str) -> Any:
        # Delegate everything we don't explicitly implement (get_columns,
        # get_view_names, get_schema_names, get_pk_constraint, get_foreign_keys,
        # get_table_names, engine, bind, ...) to the wrapped standard inspector.
        return getattr(self._inspector, name)

    # ------------------------------------------------------------------
    # Column type reconstruction (ported from the dialect's _get_column_info)
    # ------------------------------------------------------------------
    def _get_column_info(
        self,
        name: str,
        data_type: str,
        default: Optional[str],
        is_nullable: bool,
        table_name: str,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        attype: str = re.sub(r"\(.*\)", "", data_type)

        charlen = re.search(r"\(([\d,]+)\)", data_type)
        if charlen:
            charlen = charlen.group(1)  # type: ignore
        args = re.search(r"\((.*)\)", data_type)
        if args and args.group(1):
            args = tuple(re.split(r"\s*,\s*", args.group(1)))  # type: ignore
        else:
            args = ()  # type: ignore
        kwargs: Dict[str, Any] = {}

        if attype == "numeric":
            if charlen:
                prec, scale = charlen.split(",")  # type: ignore
                args = (int(prec), int(scale))  # type: ignore
            else:
                args = ()  # type: ignore
        elif attype == "integer":
            args = ()  # type: ignore
        elif attype in ("timestamptz", "timetz"):
            kwargs["timezone"] = True
            args = ()  # type: ignore
        elif attype == "date":
            args = ()  # type: ignore
        elif charlen:
            args = (int(charlen),)  # type: ignore

        coltype = ischema_names.get(attype.upper(), None)

        if coltype:
            coltype = coltype(*args, **kwargs)
        else:
            warn(f"Did not recognize type '{attype}' of column '{name}'")
            coltype = sqltypes.NULLTYPE

        autoincrement = False
        if default is not None:
            match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
            if match is not None:
                if issubclass(coltype._type_affinity, sqltypes.Integer):
                    autoincrement = True
                sch = schema
                if "." not in match.group(2) and sch is not None:
                    default = (
                        match.group(1)
                        + ('"%s"' % sch)
                        + "."
                        + match.group(2)
                        + match.group(3)
                    )

        return dict(
            name=name,
            type=coltype,
            nullable=is_nullable,
            default=default,
            autoincrement=autoincrement,
            table_name=table_name,
            comment=str(default),
        )

    # ------------------------------------------------------------------
    # Database / schema property bags
    # ------------------------------------------------------------------
    def _get_database_properties(self, database: str, **kw: Any) -> Dict[str, str]:
        with self._inspector.engine.connect() as conn:
            cluster_type = ""
            communal_path = ""
            cluster_type_res = conn.execute(
                sqlalchemy.text(
                    "SELECT CASE COUNT(*) WHEN 0 THEN 'Enterprise' ELSE 'Eon' END "
                    "AS database_mode FROM v_catalog.shards"
                )
            ).mappings()
            for each in cluster_type_res:
                cluster_type = each["database_mode"]
                if cluster_type.lower() == "eon":
                    communal_rows = conn.execute(
                        sqlalchemy.text(
                            "SELECT location_path from storage_locations "
                            "WHERE sharing_type = 'COMMUNAL'"
                        )
                    ).mappings()
                    for crow in communal_rows:
                        communal_path += str(crow["location_path"]) + " | "

            subclusters = " "
            subcluster_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT subclusters.subcluster_name, "
                    "CAST(sum(disk_space_used_mb // 1024) as varchar(10)) as subclustersize "
                    "from subclusters "
                    "inner join disk_storage using (node_name) "
                    "group by subclusters.subcluster_name"
                )
            ).mappings()
            for data in subcluster_rows:
                subclusters += (
                    f"{data['subcluster_name']} -- {data['subclustersize']} GB |  "
                )

            cluster_size = ""
            cluster_size_rows = conn.execute(
                sqlalchemy.text(
                    "select ROUND(SUM(disk_space_used_mb) //1024 ) as cluster_size "
                    "from disk_storage"
                )
            ).mappings()
            for each in cluster_size_rows:
                cluster_size = str(each["cluster_size"]) + " GB"

            return {
                "cluster_type": cluster_type,
                "cluster_size": cluster_size,
                "subcluster": subclusters,
                "communal_storage_path": communal_path,
            }

    def _get_schema_properties(self, schema: str, **kw: Any) -> Dict[str, str]:
        with self._inspector.engine.connect() as conn:
            projection_count: Optional[Any] = None
            projection_count_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(projection_name) as pc "
                    "from v_catalog.projections "
                    "WHERE lower(projection_schema) = :schema"
                ),
                {"schema": schema.lower()},
            ).mappings()
            for each in projection_count_rows:
                projection_count = each["pc"]

            udx_list = ""
            udx_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT function_name FROM USER_FUNCTIONS WHERE schema_name = :schema"
                ),
                {"schema": schema.lower()},
            ).mappings()
            for each in udx_rows:
                udx_list += each["function_name"] + ", "

            user_defined_library = ""
            udl_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT lib_name, description FROM USER_LIBRARIES "
                    "WHERE lower(schema_name) = :schema"
                ),
                {"schema": schema.lower()},
            ).mappings()
            for data in udl_rows:
                user_defined_library += (
                    f"{data['lib_name']} -- {data['description']} |  "
                )

            return {
                "projection_count": str(projection_count),
                "udx_list": str(udx_list),
                "udx_language": str(user_defined_library),
            }

    # ------------------------------------------------------------------
    # Owners
    # ------------------------------------------------------------------
    def get_table_owner(
        self, table: str, schema: Optional[str] = None, **kw: Any
    ) -> Optional[str]:
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT table_name, owner_name FROM v_catalog.tables "
                    "where lower(table_schema) = :schema"
                ),
                {"schema": schema.lower() if schema else schema},
            ).mappings()
            for row in rows:
                if row["table_name"].lower() == table.lower():
                    return row["owner_name"]
        return None

    def get_view_owner(
        self, view: str, schema: Optional[str] = None, **kw: Any
    ) -> Optional[str]:
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT table_name, owner_name FROM v_catalog.views "
                    "where lower(table_schema) = :schema"
                ),
                {"schema": schema.lower() if schema else schema},
            ).mappings()
            for row in rows:
                if row["table_name"].lower() == view.lower():
                    return row["owner_name"]
        return None

    # ------------------------------------------------------------------
    # Projections
    # ------------------------------------------------------------------
    def get_projection_names(
        self, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        with self._inspector.engine.connect() as conn:
            if schema is not None:
                stmt = sqlalchemy.text(
                    "SELECT projection_name from v_catalog.projections "
                    "WHERE lower(projection_schema) = :schema"
                )
                rows = conn.execute(stmt, {"schema": schema.lower()})
            else:
                stmt = sqlalchemy.text(
                    "SELECT projection_name from v_catalog.projections"
                )
                rows = conn.execute(stmt)
            return [row[0] for row in rows]

    def get_projection_columns(
        self, projection: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT projection_column_name, data_type, "
                    "'' as column_default, true as is_nullable, "
                    "lower(projection_name) as projection_name "
                    "FROM PROJECTION_COLUMNS "
                    "where lower(table_schema) = :schema"
                ),
                {"schema": schema.lower() if schema else schema},
            ).mappings()

            columns: List[Dict[str, Any]] = []
            for row in rows:
                table_name = row["projection_name"].lower()
                if table_name != projection.lower():
                    continue
                columns.append(
                    self._get_column_info(
                        row["projection_column_name"],
                        row["data_type"].lower(),
                        row["column_default"],
                        row["is_nullable"],
                        table_name,
                        schema,
                    )
                )
            return columns

    def get_projection_owner(
        self, projection: str, schema: Optional[str] = None, **kw: Any
    ) -> Optional[str]:
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT projection_name as table_name, owner_name "
                    "FROM v_catalog.projections "
                    "WHERE lower(projection_schema) = :schema"
                ),
                {"schema": schema.lower() if schema else schema},
            ).mappings()
            for row in rows:
                if row["table_name"].lower() == projection.lower():
                    return row["owner_name"]
        return None

    def get_projection_comment(
        self, projection: str, schema: Optional[str] = None, **kw: Any
    ) -> Dict[str, Any]:
        projection_properties: Dict[str, str] = {}
        try:
            comment = self._fetch_projection_comments(schema, projection)

            projection_properties["ROS_Count"] = (
                str(comment["ROS_Count"]) if "ROS_Count" in comment else "Not Available"
            )
            projection_properties["Projection_Type"] = (
                str(comment["Projection_Type"])
                if "Projection_Type" in comment
                else "Not Available"
            )
            projection_properties["Is_Segmented"] = (
                str(comment["is_segmented"])
                if "is_segmented" in comment
                else "Not Available"
            )
            projection_properties["Segmentation_key"] = (
                str(comment["Segmentation_key"])
                if "Segmentation_key" in comment
                else "Not Available"
            )
            projection_properties["Projection_size"] = (
                str(comment["projection_size"])
                if "projection_size" in comment
                else "0 KB"
            )
            projection_properties["Partition_Key"] = (
                str(comment["Partition_Key"])
                if "Partition_Key" in comment
                else "Not Available"
            )
            projection_properties["Number_Of_Partitions"] = (
                str(comment["Partition_Size"]) if "Partition_Size" in comment else "0"
            )
            projection_properties["Projection_Cached"] = (
                str(comment["Projection_Cached"])
                if "Projection_Cached" in comment
                else "False"
            )
        except Exception as e:
            logger.warning(
                f"Failed to get projection comment for {schema}.{projection}: {e}"
            )

        return {
            "text": "Vertica physically stores table data in projections, "
            "which are collections of table columns. Projections store data in a "
            "format that optimizes query execution. For more info on projections "
            "and corresponding properties check out the Vertica Docs: "
            "https://www.vertica.com/docs",
            "properties": projection_properties,
        }

    def _fetch_projection_comments(
        self, schema: Optional[str], projection: str
    ) -> Dict[str, Any]:
        """Build the property bag for a single projection.

        Ported from the dialect's ``fetch_projection_comments`` (which built the
        bag for every projection in the schema and then filtered). Here we filter
        per-projection in Python after fetching the schema-wide rows, matching the
        original behaviour exactly.
        """
        target = projection.lower()
        with self._inspector.engine.connect() as conn:
            comment: Dict[str, Any] = {"projection_name": target}

            ros_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT ros_count, LOWER(projection_name) as projection_name "
                    "FROM v_monitor.projection_storage "
                    "WHERE projection_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for row in ros_rows:
                if row["projection_name"] == target:
                    comment["ROS_Count"] = row["ros_count"]

            type_flags = [
                "is_super_projection",
                "is_key_constraint_projection",
                "is_aggregate_projection",
                "has_expressions",
            ]
            ptype_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT DISTINCT is_super_projection, "
                    "is_key_constraint_projection, is_aggregate_projection, "
                    "has_expressions, LOWER(projection_name) as projection_name "
                    "FROM v_catalog.projections "
                    "WHERE projection_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for ptype in ptype_rows:
                if ptype["projection_name"] != target:
                    continue
                for flag in type_flags:
                    if ptype[flag] is True:
                        if "Projection_Type" in comment:
                            comment["Projection_Type"] = (
                                comment["Projection_Type"] + ", " + str(flag)
                            )
                        else:
                            comment["Projection_Type"] = str(flag)

            segmented_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT is_segmented, segment_expression, "
                    "LOWER(projection_name) as projection_name "
                    "FROM v_catalog.projections "
                    "WHERE projection_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for row in segmented_rows:
                if row["projection_name"] == target:
                    comment["is_segmented"] = str(row["is_segmented"])
                    comment["Segmentation_key"] = str(row["segment_expression"])

            partition_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT DISTINCT LOWER(projection_name) as projection_name, "
                    "partition_key FROM v_monitor.partitions "
                    "WHERE table_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for row in partition_rows:
                if row["projection_name"] == target:
                    comment["Partition_Key"] = str(row["partition_key"])

            size_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT used_bytes, LOWER(projection_name) as projection_name "
                    "from v_monitor.projection_storage "
                    "WHERE projection_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            projection_size = 0.0
            matched_size = False
            for row in size_rows:
                if row["projection_name"] == target:
                    matched_size = True
                    projection_size += row["used_bytes"] / 1024
            comment["projection_size"] = (
                str(int(projection_size)) + " KB" if matched_size else "0 KB"
            )

            num_partition_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT LOWER(projection_name) as projection_name, "
                    "count(partition_key) as Partition_Size "
                    "FROM v_monitor.partitions "
                    "WHERE lower(table_schema) = :schema group by 1"
                ),
                {"schema": schema.lower() if schema else schema},
            ).mappings()
            for row in num_partition_rows:
                if row["projection_name"] == target:
                    comment["Partition_Size"] = str(row["Partition_Size"])

            cache_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(*) as cnt, object_name FROM DEPOT_PIN_POLICIES "
                    "WHERE schema_name = :schema GROUP BY object_name"
                ),
                {"schema": schema},
            ).mappings()
            for row in cache_rows:
                # The original dialect set this flag for ANY cached object in the
                # schema (its loop did not match object_name to the projection);
                # preserve that behaviour.
                comment["Projection_Cached"] = row["cnt"] > 0

            return comment

    # ------------------------------------------------------------------
    # ML Models
    # ------------------------------------------------------------------
    def get_models_names(self, schema: Optional[str] = None, **kw: Any) -> List[str]:
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT model_name FROM models "
                    "WHERE lower(schema_name) = :schema ORDER BY model_name"
                ),
                {"schema": schema.lower() if schema else schema},
            )
            return [row[0] for row in rows]

    def get_model_comment(
        self, model_name: str, schema: Optional[str] = None, **kw: Any
    ) -> Dict[str, Any]:
        with self._inspector.engine.connect() as conn:
            used_by = ""
            used_by_rows = conn.execute(
                sqlalchemy.text(
                    "select owner_name from models where model_name = :model"
                ),
                {"model": model_name},
            ).mappings()
            for data in used_by_rows:
                used_by = data["owner_name"]

            attr_name: List[Dict[str, Any]] = []
            attr_rows = conn.execute(
                sqlalchemy.text(
                    "SELECT GET_MODEL_ATTRIBUTE "
                    "( USING PARAMETERS model_name= :model_full )"
                ),
                {"model_full": f"{schema.lower() if schema else schema}.{model_name}"},
            )
            for data in attr_rows:
                attr_name.append(
                    {
                        "attr_name": data[0],
                        "attr_fields": data[1],
                        "#_of_rows": data[2],
                    }
                )

            attributes_details: List[Dict[str, Any]] = []
            for data in attr_name:
                attr_names = data["attr_name"]
                attr_fields = str(data["attr_fields"]).split(",")

                value_final: Dict[str, List[Any]] = {}
                attr_details_dict: Dict[str, Any] = {"attr_name": attr_names}
                detail_rows = conn.execute(
                    sqlalchemy.text(
                        "SELECT GET_MODEL_ATTRIBUTE "
                        "( USING PARAMETERS model_name= :model_full, "
                        "attr_name= :attr_name )"
                    ),
                    {
                        "model_full": (
                            f"{schema.lower() if schema else schema}.{model_name}"
                        ),
                        "attr_name": attr_names,
                    },
                )
                for detail in detail_rows:
                    if len(attr_fields) > 1:
                        for index, each in enumerate(attr_fields):
                            value_final.setdefault(each, [])
                            value_final[each].append(detail[index])
                    else:
                        value_final.setdefault(attr_fields[0], [])
                        value_final[attr_fields[0]].append(detail[0])

                attr_details_dict.update(value_final)
                attributes_details.append(attr_details_dict)

            return {
                "text": "Vertica provides a number of machine learning functions "
                "for performing in-database analysis. These functions perform data "
                "preparation, model training, and predictive tasks. These properties "
                "shows the Model attributes and Specifications in the current schema.",
                "properties": {
                    "used_by": str(used_by),
                    "Model Attributes": str(attr_name),
                    "Model Specifications": str(attributes_details),
                },
            }

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------
    def _populate_view_lineage(
        self, view: str, schema: str, **kw: Any
    ) -> Dict[str, List[Tuple[str, str, str]]]:
        view_lineage_map: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "select table_name, table_schema, reference_table_name, "
                    "reference_table_schema from v_catalog.view_tables "
                    "where table_schema = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for lineage in rows:
                downstream = f"{lineage['table_schema']}.{lineage['table_name']}"
                upstream = (
                    f"{lineage['reference_table_schema']}."
                    f"{lineage['reference_table_name']}"
                )
                view_lineage_map[downstream].append((upstream, "[]", "[]"))
        return view_lineage_map

    def _populate_projection_lineage(
        self, projection: str, schema: str, **kw: Any
    ) -> Dict[str, List[Tuple[str, str, str]]]:
        projection_lineage_map: Dict[str, List[Tuple[str, str, str]]] = defaultdict(
            list
        )
        with self._inspector.engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text(
                    "select basename, schemaname, name from vs_projections "
                    "where schemaname = :schema"
                ),
                {"schema": schema},
            ).mappings()
            for lineage in rows:
                downstream = f"{lineage['schemaname']}.{lineage['name']}"
                upstream = f"{lineage['schemaname']}.{lineage['basename']}"
                projection_lineage_map[downstream].append((upstream, "[]", "[]"))
        return projection_lineage_map
