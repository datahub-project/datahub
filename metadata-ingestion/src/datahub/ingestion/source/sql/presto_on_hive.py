import base64
import json
import logging
import re
import uuid
from collections import namedtuple
from itertools import groupby
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConfig,
    SQLAlchemySource,
    SqlContainerSubTypes,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import DatasetPropertiesClass, ViewPropertiesClass

logger: logging.Logger = logging.getLogger(__name__)

TableKey = namedtuple("TableKey", ["schema", "table"])


class PrestoOnHiveConfig(BasicSQLAlchemyConfig):
    views_where_clause_suffix = ""
    tables_where_clause_suffix = ""
    schemas_where_clause_suffix = ""
    host_port = "localhost:3306"
    scheme = "mysql+pymysql"


class PrestoOnHiveSource(SQLAlchemySource):
    config: PrestoOnHiveConfig

    _TABLES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,
           p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
           c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _TABLES_POSTGRES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
            to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name,
            p."INTEGER_IDX" as col_sort_order, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _VIEWS_SQL_STATEMENT = """
    SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t.TBL_ID desc;
    """

    _VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t."TBL_ID" desc;
    """

    _PRESTO_VIEW_PREFIX = "/* Presto View: "
    _PRESTO_VIEW_SUFFIX = " */"

    _SCHEMAS_SQL_STATEMENT = """
    SELECT d.NAME as `schema`
    FROM DBS d
    WHERE 1
    {where_clause_suffix}
    ORDER BY d.NAME desc;
    """

    _SCHEMAS_POSTGRES_SQL_STATEMENT = """
    SELECT d."NAME" as "schema"
    FROM "DBS" d
    WHERE 1=1
    {where_clause_suffix}
    ORDER BY d."NAME" desc;
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "presto-on-hive")
        self._alchemy_client = SQLAlchemyClient(config)

    def get_db_name(self, inspector: Inspector) -> str:
        if self.config.database_alias:
            return f"{self.config.database_alias}"
        if self.config.database:
            return f"{self.config.database}"
        else:
            return super().get_db_name(inspector)

    @classmethod
    def create(cls, config_dict, ctx):
        config = PrestoOnHiveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:

        if isinstance(self.config, PrestoOnHiveConfig):
            if "postgresql" in self.config.scheme:
                statement = PrestoOnHiveSource._SCHEMAS_POSTGRES_SQL_STATEMENT.format(
                    where_clause_suffix=self.config.schemas_where_clause_suffix
                )
            else:
                statement = PrestoOnHiveSource._SCHEMAS_SQL_STATEMENT.format(
                    where_clause_suffix=self.config.schemas_where_clause_suffix
                )

            iter_res = self._alchemy_client.execute_query(statement)
            for row in iter_res:
                schema = row["schema"]

                schema_container_key = self.gen_schema_key(db_name, schema)
                logger.debug("schema_container_key = {} ".format(schema_container_key))

                database_container_key = self.gen_database_key(database=db_name)

                container_workunits = gen_containers(
                    schema_container_key,
                    schema,
                    [SqlContainerSubTypes.SCHEMA],
                    database_container_key,
                )

                for wu in container_workunits:
                    self.report.report_workunit(wu)
                    yield wu

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:

        if isinstance(sql_config, PrestoOnHiveConfig):
            if "postgresql" in sql_config.scheme:
                statement = PrestoOnHiveSource._TABLES_POSTGRES_SQL_STATEMENT.format(
                    where_clause_suffix=sql_config.tables_where_clause_suffix
                )
            else:
                statement = PrestoOnHiveSource._TABLES_SQL_STATEMENT.format(
                    where_clause_suffix=sql_config.tables_where_clause_suffix
                )

            iter_res = self._alchemy_client.execute_query(statement)

            for key, group in groupby(iter_res, self._get_table_key):
                dataset_name = self.get_identifier(
                    schema=key.schema, entity=key.table, inspector=inspector
                )
                self.report.report_entity_scanned(dataset_name, ent_type="table")

                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                columns = list(group)
                if len(columns) == 0:
                    self.report.report_warning(
                        dataset_name, "missing column information"
                    )

                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[],
                )

                # add table schema fields
                schema_fields = self.get_schema_fields(dataset_name, columns)

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    columns,
                    None,
                    None,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                # add table properties
                properties: Dict[str, str] = {
                    "create_date": columns[-1]["create_date"],
                    "table_type": columns[-1]["table_type"],
                    "table_location": ""
                    if columns[-1]["table_location"] is None
                    else columns[-1]["table_location"],
                }

                par_columns: str = ", ".join(
                    [c["col_name"] for c in columns if c["is_partition_col"]]
                )
                if par_columns != "":
                    properties["partitioned_columns"] = par_columns

                dataset_properties = DatasetPropertiesClass(
                    description=None,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

                db_name = self.get_db_name(inspector)
                schema = key.schema
                yield from self.add_table_to_schema_container(
                    dataset_urn, db_name, schema
                )

                # construct mce
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect

                yield from self._get_domain_wu(
                    dataset_name=dataset_name,
                    entity_urn=dataset_urn,
                    entity_type="dataset",
                    sql_config=sql_config,
                )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:

        if isinstance(sql_config, PrestoOnHiveConfig):
            if "postgresql" in sql_config.scheme:
                statement = PrestoOnHiveSource._VIEWS_POSTGRES_SQL_STATEMENT.format(
                    where_clause_suffix=sql_config.views_where_clause_suffix
                )
            else:
                statement = PrestoOnHiveSource._VIEWS_SQL_STATEMENT.format(
                    where_clause_suffix=sql_config.views_where_clause_suffix
                )
            iter_res = self._alchemy_client.execute_query(statement)
            for row in iter_res:
                dataset_name = self.get_identifier(
                    schema=row["schema"], entity=row["name"], inspector=inspector
                )
                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                columns, view_definition = self._get_column_metadata(
                    row["view_original_text"]
                )

                if len(columns) == 0:
                    self.report.report_warning(
                        dataset_name, "missing column information"
                    )

                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                # add view schema fields
                schema_fields = self.get_schema_fields(dataset_name, columns)

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    columns,
                    canonical_schema=schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                # add view properties
                properties: Dict[str, str] = {
                    "is_view": "True",
                }
                dataset_properties = DatasetPropertiesClass(
                    description=None,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

                # add view properties
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLogic=view_definition,
                    viewLanguage="SQL",
                )
                dataset_snapshot.aspects.append(view_properties)

                db_name = self.get_db_name(inspector)
                schema = row["schema"]
                yield from self.add_table_to_schema_container(
                    dataset_urn, db_name, schema
                )

                # construct mce
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect

                yield from self._get_domain_wu(
                    dataset_name=dataset_name,
                    entity_urn=dataset_urn,
                    entity_type="dataset",
                    sql_config=sql_config,
                )

    def _get_table_key(self, row: Dict[str, Any]) -> TableKey:
        return TableKey(schema=row["schema_name"], table=row["table_name"])

    def _get_column_metadata(self, view_original_text: str) -> Tuple[List[Dict], str]:
        """
        Get Column Metadata from VIEW_ORIGINAL_TEXT from TBLS table for Presto Views.
        Columns are sorted the same way as they appear in Presto Create View SQL.
        :param view_original_text:
        :return:
        """
        # remove encoded Presto View data prefix and suffix
        encoded_view_info = view_original_text.split(
            PrestoOnHiveSource._PRESTO_VIEW_PREFIX, 1
        )[-1].rsplit(PrestoOnHiveSource._PRESTO_VIEW_SUFFIX, 1)[0]

        # view_original_text is b64 encoded:
        decoded_view_info = base64.b64decode(encoded_view_info)
        view_definition = json.loads(decoded_view_info).get("originalSql")

        columns = json.loads(decoded_view_info).get("columns")
        for col in columns:
            col["col_name"], col["col_type"] = col["name"], col["type"]

        return list(columns), view_definition

    def close(self) -> None:
        if self._alchemy_client.connection is not None:
            self._alchemy_client.connection.close()

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        pk_constraints: Optional[Dict[Any, Any]] = None,
    ) -> List[SchemaField]:
        avro_schema = _get_avro_schema_from_native_data_type(
            column["col_type"], column["col_name"]
        )

        fields = schema_util.avro_schema_to_mce_fields(
            json.dumps(avro_schema), default_nullable=True
        )

        return fields


_all_atomic_types = {
    "string": "string",
    "integer": "int",
    "double": "double",
    "double precision": "double",
    "binary": "string",
    "boolean": "boolean",
    "float": "float",
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "bigint": "long",
    "varchar": "string",
    "char": "string",
}

_FIXED_DECIMAL = re.compile(r"(decimal|numeric)(\(\s*(\d+)\s*,\s*(\d+)\s*\))?")
_FIXED_STRING = re.compile(r"(var)?char\(\s*(\d+)\s*\)")
_BRACKETS = {"(": ")", "[": "]", "{": "}", "<": ">"}


def _get_avro_schema_from_native_data_type(
    column_type: str, column_name: str
) -> Dict[str, Any]:
    # Below Record structure represents the dataset level
    # Inner fields represent the complex field (struct/array/map/union)
    return {
        "type": "record",
        "name": "__struct_",
        "fields": [{"name": column_name, "type": _parse_datatype_string(column_type)}],
    }


def _parse_datatype_string(s, **kwargs):
    s = s.strip()
    if s.startswith("array<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        return {
            "type": "array",
            "items": _parse_datatype_string(s[6:-1]),
            "native_data_type": s,
        }
    elif s.startswith("map<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        parts = _ignore_brackets_split(s[4:-1], ",")
        if len(parts) != 2:
            raise ValueError(
                "The map type string format is: 'map<key_type,value_type>', "
                + "but got: %s" % s
            )
        kt = _parse_datatype_string(parts[0])
        vt = _parse_datatype_string(parts[1])
        # keys are assumed to be strings in avro map
        return {
            "type": "map",
            "values": vt,
            "native_data_type": s,
            "key_type": kt,
            "key_native_data_type": parts[0],
        }
    elif s.startswith("uniontype<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        parts = _ignore_brackets_split(s[10:-1], ",")
        t = []
        ustruct_seqn = 0
        for part in parts:
            if part.startswith("struct<"):
                # ustruct_seqn defines sequence number of struct in union
                t.append(_parse_datatype_string(part, ustruct_seqn=ustruct_seqn))
                ustruct_seqn += 1
            else:
                t.append(_parse_datatype_string(part))
        return t
    elif s.startswith("struct<"):
        if s[-1] != ">":
            raise ValueError("'>' should be the last char, but got: %s" % s)
        return _parse_struct_fields_string(s[7:-1], **kwargs)
    elif ":" in s:
        return _parse_struct_fields_string(s, **kwargs)
    else:
        return _parse_basic_datatype_string(s)


def _parse_struct_fields_string(s, **kwargs):
    parts = _ignore_brackets_split(s, ",")
    fields = []
    for part in parts:
        name_and_type = _ignore_brackets_split(part, ":")
        if len(name_and_type) != 2:
            raise ValueError(
                "The struct field string format is: 'field_name:field_type', "
                + "but got: %s" % part
            )
        field_name = name_and_type[0].strip()
        if field_name.startswith("`"):
            if field_name[-1] != "`":
                raise ValueError("'`' should be the last char, but got: %s" % s)
            field_name = field_name[1:-1]
        field_type = _parse_datatype_string(name_and_type[1])
        fields.append({"name": field_name, "type": field_type})

    if kwargs.get("ustruct_seqn") is not None:
        struct_name = "__structn_{}_{}".format(
            kwargs["ustruct_seqn"], str(uuid.uuid4()).replace("-", "")
        )
    else:
        struct_name = "__struct_{}".format(str(uuid.uuid4()).replace("-", ""))
    return {
        "type": "record",
        "name": struct_name,
        "fields": fields,
        "native_data_type": "struct<{}>".format(s),
    }


def _parse_basic_datatype_string(s):
    if s in _all_atomic_types.keys():
        return {
            "type": _all_atomic_types[s],
            "native_data_type": s,
            "_nullable": True,
        }

    elif _FIXED_STRING.match(s):
        return {"type": "string", "native_data_type": s, "_nullable": True}

    elif _FIXED_DECIMAL.match(s):
        m = _FIXED_DECIMAL.match(s)
        if m.group(2) is not None:  # type: ignore
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": int(m.group(3)),  # type: ignore
                "scale": int(m.group(4)),  # type: ignore
                "native_data_type": s,
                "_nullable": True,
            }
        else:
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "native_data_type": s,
                "_nullable": True,
            }
    elif s == "date":
        return {
            "type": "int",
            "logicalType": "date",
            "native_data_type": s,
            "_nullable": True,
        }
    elif s == "timestamp":
        return {
            "type": "int",
            "logicalType": "timestamp-millis",
            "native_data_type": s,
            "_nullable": True,
        }
    else:
        return {"type": "null", "native_data_type": s, "_nullable": True}


def _ignore_brackets_split(s, separator):
    """
    Splits the given string by given separator, but ignore separators inside brackets pairs, e.g.
    given "a,b" and separator ",", it will return ["a", "b"], but given "a<b,c>, d", it will return
    ["a<b,c>", "d"].
    """
    parts = []
    buf = ""
    level = 0
    for c in s:
        if c in _BRACKETS.keys():
            level += 1
            buf += c
        elif c in _BRACKETS.values():
            if level == 0:
                raise ValueError("Brackets are not correctly paired: %s" % s)
            level -= 1
            buf += c
        elif c == separator and level > 0:
            buf += c
        elif c == separator:
            parts.append(buf)
            buf = ""
        else:
            buf += c

    if len(buf) == 0:
        raise ValueError("The %s cannot be the last char: %s" % (separator, s))
    parts.append(buf)
    return parts


class SQLAlchemyClient:
    def __init__(self, config: SQLAlchemyConfig):
        self.config = config
        self.connection = self._get_connection()

    def _get_connection(self) -> Any:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        conn = engine.connect()
        return conn

    def execute_query(self, query: str) -> Iterable:
        """
        Create an iterator to execute sql.
        """
        results = self.connection.execute(text(query))
        return iter(results)
