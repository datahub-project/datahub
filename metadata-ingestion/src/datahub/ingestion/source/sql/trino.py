import json
import sys
import uuid
from textwrap import dedent
from typing import Any, Dict, List

import sqlalchemy
from sqlalchemy.sql.type_api import TypeEngine
from trino.exceptions import TrinoQueryError  # noqa

from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    register_custom_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
)

if sys.version_info >= (3, 7):  # noqa: C901
    # This import verifies that the dependencies are available.
    import sqlalchemy_trino  # noqa: F401
    from sqlalchemy import exc, sql
    from sqlalchemy.engine import reflection
    from sqlalchemy.sql import sqltypes
    from sqlalchemy_trino import datatype, error
    from sqlalchemy_trino.dialect import TrinoDialect

    register_custom_type(datatype.ROW, RecordTypeClass)
    register_custom_type(datatype.MAP, MapTypeClass)
    register_custom_type(datatype.DOUBLE, NumberTypeClass)

    # Read only table names and skip view names, as view names will also be returned
    # from get_view_names
    @reflection.cache  # type: ignore
    def get_table_names(self, connection, schema: str = None, **kw):  # type: ignore
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent(
            """
            SELECT "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" = :schema and "table_type" != 'VIEW'
        """
        ).strip()
        res = connection.execute(sql.text(query), schema=schema)
        return [row.table_name for row in res]

    # Include all table properties instead of only "comment" property
    @reflection.cache  # type: ignore
    def get_table_comment(self, connection, table_name: str, schema: str = None, **kw):  # type: ignore
        try:
            properties_table = self._get_full_table(f"{table_name}$properties", schema)
            query = f"SELECT * FROM {properties_table}"
            row = connection.execute(sql.text(query)).fetchone()

            # Generate properties dictionary.
            properties = {}
            for col_name, col_value in row.items():
                properties[col_name] = col_value

            return {"text": properties.get("comment", None), "properties": properties}
        except TrinoQueryError as e:
            if e.error_name in (
                error.TABLE_NOT_FOUND,
                error.COLUMN_NOT_FOUND,
                error.NOT_FOUND,
                error.NOT_SUPPORTED,
            ):
                return dict(text=None)
            raise

    # Include column comment, original trino datatype as full_type
    @reflection.cache  # type: ignore
    def _get_columns(self, connection, table_name, schema: str = None, **kw):  # type: ignore
        schema = schema or self._get_default_schema_name(connection)
        query = dedent(
            """
            SELECT
                "column_name",
                "data_type",
                "column_default",
                UPPER("is_nullable") AS "is_nullable",
                "comment"
            FROM "information_schema"."columns"
            WHERE "table_schema" = :schema
              AND "table_name" = :table
            ORDER BY "ordinal_position" ASC
        """
        ).strip()
        res = connection.execute(sql.text(query), schema=schema, table=table_name)
        columns = []
        for record in res:
            column = dict(
                name=record.column_name,
                type=datatype.parse_sqltype(record.data_type),
                nullable=record.is_nullable == "YES",
                default=record.column_default,
                comment=record.comment,
            )
            columns.append(column)
        return columns

    TrinoDialect.get_table_names = get_table_names
    TrinoDialect.get_table_comment = get_table_comment
    TrinoDialect._get_columns = _get_columns

    class TrinoConfig(BasicSQLAlchemyConfig):
        # defaults
        scheme = "trino"

        def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
            regular = f"{schema}.{table}"
            if self.database_alias:
                return f"{self.database_alias}.{regular}"
            if self.database:
                return f"{self.database}.{regular}"
            return regular

    class TrinoSource(SQLAlchemySource):
        def __init__(self, config, ctx):
            super().__init__(config, ctx, "trino")

        @classmethod
        def create(cls, config_dict, ctx):
            config = TrinoConfig.parse_obj(config_dict)
            return cls(config, ctx)

        def get_schema_fields_for_column(
            self, dataset_name: str, column: dict, pk_constraints: dict = None
        ) -> List[SchemaField]:

            fields = super().get_schema_fields_for_column(
                dataset_name, column, pk_constraints
            )

            if isinstance(column["type"], (datatype.ROW, sqltypes.ARRAY, datatype.MAP)):
                assert len(fields) == 1
                field = fields[0]
                # Get avro schema for subfields along with parent complex field
                avro_schema = self.get_avro_schema_from_data_type(
                    column["type"], column["name"]
                )

                newfields = schema_util.avro_schema_to_mce_fields(
                    json.dumps(avro_schema), default_nullable=True
                )

                # First field is the parent complex field
                newfields[0].nullable = field.nullable
                newfields[0].description = field.description
                newfields[0].isPartOfKey = field.isPartOfKey
                return newfields

            return fields

        def get_avro_schema_from_data_type(
            self, column_type: TypeEngine, column_name: str
        ) -> Dict[str, Any]:
            # Below Record structure represents the dataset level
            # Inner fields represent the complex field (struct/array/map/union)
            return {
                "type": "record",
                "name": "__struct_",
                "fields": [{"name": column_name, "type": _parse_datatype(column_type)}],
            }

    _all_atomic_types = {
        sqltypes.BOOLEAN: "boolean",
        sqltypes.SMALLINT: "int",
        sqltypes.INTEGER: "int",
        sqltypes.BIGINT: "long",
        sqltypes.REAL: "float",
        datatype.DOUBLE: "double",  # type: ignore
        sqltypes.VARCHAR: "string",
        sqltypes.CHAR: "string",
        sqltypes.JSON: "record",
    }

    def _parse_datatype(s):
        if isinstance(s, sqlalchemy.types.ARRAY):
            return {
                "type": "array",
                "items": _parse_datatype(s.item_type),
                "native_data_type": repr(s),
            }
        elif isinstance(s, datatype.MAP):
            kt = _parse_datatype(s.key_type)
            vt = _parse_datatype(s.value_type)
            # keys are assumed to be strings in avro map
            return {
                "type": "map",
                "values": vt,
                "native_data_type": repr(s),
                "key_type": kt,
                "key_native_data_type": repr(s.key_type),
            }
        elif isinstance(s, datatype.ROW):
            return _parse_struct_fields(s.attr_types)
        else:
            return _parse_basic_datatype(s)

    def _parse_struct_fields(parts):
        fields = []
        for name_and_type in parts:
            field_name = name_and_type[0].strip()
            field_type = _parse_datatype(name_and_type[1])
            fields.append({"name": field_name, "type": field_type})
        return {
            "type": "record",
            "name": "__struct_{}".format(str(uuid.uuid4()).replace("-", "")),
            "fields": fields,
            "native_data_type": "ROW({})".format(parts),
        }

    def _parse_basic_datatype(s):
        for sql_type in _all_atomic_types.keys():
            if isinstance(s, sql_type):
                return {
                    "type": _all_atomic_types[sql_type],
                    "native_data_type": repr(s),
                    "_nullable": True,
                }

        if isinstance(s, sqlalchemy.types.DECIMAL):
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": s.precision,  # type: ignore
                "scale": s.scale,  # type: ignore
                "native_data_type": repr(s),
                "_nullable": True,
            }
        elif isinstance(s, sqlalchemy.types.Date):
            return {
                "type": "int",
                "logicalType": "date",
                "native_data_type": repr(s),
                "_nullable": True,
            }
        elif isinstance(s, (sqlalchemy.types.DATETIME, sqlalchemy.types.TIMESTAMP)):
            return {
                "type": "int",
                "logicalType": "timestamp-millis",
                "native_data_type": repr(s),
                "_nullable": True,
            }

        return {"type": "null", "native_data_type": repr(s)}

else:
    raise ModuleNotFoundError("The trino plugin requires Python 3.7 or newer.")
