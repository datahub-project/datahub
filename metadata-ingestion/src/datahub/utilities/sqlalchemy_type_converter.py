import json
import logging
import traceback
import uuid
from typing import Any, Dict, List, Optional, Type, Union

from sqlalchemy import types
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.visitors import Visitable

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import NullTypeClass, SchemaFieldDataTypeClass

logger = logging.getLogger(__name__)

try:
    # This is used for both BigQuery and Athena.
    from sqlalchemy_bigquery import STRUCT
except ImportError:
    STRUCT = None


class MapType(types.TupleType):
    # Wrapper class around SQLalchemy's TupleType to increase compatibility with DataHub
    pass


class SqlAlchemyColumnToAvroConverter:
    """Helper class that collects some methods to convert SQLalchemy columns to Avro schema."""

    # tuple of complex data types that require a special handling
    _COMPLEX_TYPES = (STRUCT, types.ARRAY, MapType)

    # mapping of primitive SQLalchemy data types to AVRO schema data types
    PRIMITIVE_SQL_ALCHEMY_TYPE_TO_AVRO_TYPE: Dict[Type[types.TypeEngine], str] = {
        types.String: "string",
        types.BINARY: "string",
        types.BOOLEAN: "boolean",
        types.FLOAT: "float",
        types.INTEGER: "int",
        types.BIGINT: "long",
        types.VARCHAR: "string",
        types.CHAR: "string",
    }

    @classmethod
    def get_avro_type(
        cls, column_type: Union[types.TypeEngine, STRUCT, MapType], nullable: bool
    ) -> Dict[str, Any]:
        """Determines the concrete AVRO schema type for a SQLalchemy-typed column"""
        if isinstance(
            column_type, tuple(cls.PRIMITIVE_SQL_ALCHEMY_TYPE_TO_AVRO_TYPE.keys())
        ):
            return {
                "type": cls.PRIMITIVE_SQL_ALCHEMY_TYPE_TO_AVRO_TYPE[type(column_type)],
                "native_data_type": str(column_type),
                "_nullable": nullable,
            }
        if isinstance(column_type, types.DECIMAL):
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": int(column_type.precision),
                "scale": int(column_type.scale),
                "native_data_type": str(column_type),
                "_nullable": nullable,
            }
        if isinstance(column_type, types.DATE):
            return {
                "type": "int",
                "logicalType": "date",
                "native_data_type": str(column_type),
                "_nullable": nullable,
            }
        if isinstance(column_type, types.TIMESTAMP):
            return {
                "type": "long",
                "logicalType": "timestamp-millis",
                "native_data_type": str(column_type),
                "_nullable": nullable,
            }
        if isinstance(column_type, types.ARRAY):
            array_type = column_type.item_type

            return {
                "type": "array",
                "items": cls.get_avro_type(column_type=array_type, nullable=nullable),
                "native_data_type": f"array<{str(column_type.item_type)}>",
            }
        if isinstance(column_type, MapType):
            try:
                key_type = column_type.types[0]
                value_type = column_type.types[1]
                return {
                    "type": "map",
                    "values": cls.get_avro_type(
                        column_type=value_type, nullable=nullable
                    ),
                    "native_data_type": str(column_type),
                    "key_type": cls.get_avro_type(
                        column_type=key_type, nullable=nullable
                    ),
                    "key_native_data_type": str(key_type),
                }
            except Exception as e:
                logger.warning(
                    f"Unable to parse MapType {column_type} the error was: {e}"
                )
                return {
                    "type": "map",
                    "values": {"type": "null", "_nullable": True},
                    "native_data_type": str(column_type),
                    "key_type": {"type": "null", "_nullable": True},
                    "key_native_data_type": "null",
                }
        if STRUCT and isinstance(column_type, STRUCT):
            fields = []
            for field_def in column_type._STRUCT_fields:
                field_name, field_type = field_def
                fields.append(
                    {
                        "name": field_name,
                        "type": cls.get_avro_type(
                            column_type=field_type, nullable=nullable
                        ),
                    }
                )
            struct_name = f"__struct_{str(uuid.uuid4()).replace('-', '')}"
            try:
                return {
                    "type": "record",
                    "name": struct_name,
                    "fields": fields,
                    "native_data_type": str(column_type),
                    "_nullable": nullable,
                }
            except Exception:
                # This is a workaround for the case when the struct name is not string convertable because SqlAlchemt throws an error
                return {
                    "type": "record",
                    "name": struct_name,
                    "fields": fields,
                    "native_data_type": "map",
                    "_nullable": nullable,
                }

        return {
            "type": "null",
            "native_data_type": str(column_type),
            "_nullable": nullable,
        }

    @classmethod
    def get_avro_for_sqlalchemy_column(
        cls,
        column_name: str,
        column_type: types.TypeEngine,
        nullable: bool,
    ) -> Union[object, Dict[str, object]]:
        """Returns the AVRO schema representation of a SQLalchemy column."""
        if isinstance(column_type, cls._COMPLEX_TYPES):
            return {
                "type": "record",
                "name": "__struct_",
                "fields": [
                    {
                        "name": column_name,
                        "type": cls.get_avro_type(
                            column_type=column_type, nullable=nullable
                        ),
                    }
                ],
            }
        return cls.get_avro_type(column_type=column_type, nullable=nullable)


def get_schema_fields_for_sqlalchemy_column(
    column_name: str,
    column_type: types.TypeEngine,
    inspector: Inspector,
    description: Optional[str] = None,
    nullable: Optional[bool] = True,
    is_part_of_key: Optional[bool] = False,
    is_partitioning_key: Optional[bool] = False,
) -> List[SchemaField]:
    """Creates SchemaFields from a given SQLalchemy column.

    This function is analogous to `get_schema_fields_for_hive_column` from datahub.utilities.hive_schema_to_avro.
    The main purpose of implementing it this way, is to make it ready/compatible for second field path generation,
    which allows to explore nested structures within the UI.
    """

    if nullable is None:
        nullable = True

    try:
        # as a first step, the column is converted to AVRO JSON which can then be used by an existing function
        avro_schema_json = (
            SqlAlchemyColumnToAvroConverter.get_avro_for_sqlalchemy_column(
                column_name=column_name,
                column_type=column_type,
                nullable=nullable,
            )
        )
        # retrieve schema field definitions from the above generated AVRO JSON structure
        schema_fields = avro_schema_to_mce_fields(
            avro_schema=json.dumps(avro_schema_json),
            default_nullable=nullable,
            swallow_exceptions=False,
        )
    except Exception as e:
        logger.warning(
            f"Unable to parse column {column_name} and type {column_type} the error was: {e} Traceback: {traceback.format_exc()}"
        )

        # fallback description in case any exception occurred
        schema_fields = [
            SchemaField(
                fieldPath=column_name,
                type=SchemaFieldDataTypeClass(type=NullTypeClass()),
                nativeDataType=get_native_data_type_for_sqlalchemy_type(
                    column_type,
                    inspector,
                ),
            )
        ]

    # for all non-nested data types an additional modification of the `fieldPath` property is required
    if type(column_type) in (
        *SqlAlchemyColumnToAvroConverter.PRIMITIVE_SQL_ALCHEMY_TYPE_TO_AVRO_TYPE.keys(),
        types.TIMESTAMP,
        types.DATE,
        types.DECIMAL,
    ):
        schema_fields[0].fieldPath += f".{column_name}"

    if description:
        schema_fields[0].description = description
    schema_fields[0].isPartOfKey = (
        is_part_of_key if is_part_of_key is not None else False
    )

    schema_fields[0].isPartitioningKey = (
        is_partitioning_key if is_partitioning_key is not None else False
    )

    return schema_fields


def get_native_data_type_for_sqlalchemy_type(
    column_type: types.TypeEngine, inspector: Inspector
) -> str:
    if isinstance(column_type, types.NullType):
        return column_type.__visit_name__

    try:
        return column_type.compile(dialect=inspector.dialect)
    except Exception as e:
        logger.debug(
            f"Unable to compile sqlalchemy type {column_type} the error was: {e}"
        )

        if (
            isinstance(column_type, Visitable)
            and column_type.__visit_name__ is not None
        ):
            return column_type.__visit_name__

        return repr(column_type)
