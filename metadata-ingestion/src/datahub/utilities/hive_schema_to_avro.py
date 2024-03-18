import json
import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Union

from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import NullTypeClass, SchemaFieldDataTypeClass

logger: logging.Logger = logging.getLogger(__name__)


class HiveColumnToAvroConverter:
    _BRACKETS = {"(": ")", "[": "]", "{": "}", "<": ">"}

    _PRIVIMITE_HIVE_TYPE_TO_AVRO_TYPE = {
        "string": "string",
        "int": "int",
        "integer": "int",
        "double": "double",
        "double precision": "double",
        "binary": "string",
        "boolean": "boolean",
        "float": "float",
        "tinyint": "int",
        "smallint": "int",
        "bigint": "long",
        "varchar": "string",
        "char": "string",
    }

    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    _FIXED_DECIMAL = re.compile(r"(decimal|numeric)(\(\s*(\d+)\s*,\s*(\d+)\s*\))?")

    _FIXED_STRING = re.compile(r"(var)?char\(\s*(\d+)\s*\)")

    _STRUCT_TYPE_SEPARATOR = ":"

    @staticmethod
    def _parse_datatype_string(
        s: str, **kwargs: Any
    ) -> Union[object, Dict[str, object]]:
        s = s.strip()
        if s.startswith("array<"):
            if s[-1] != ">":
                raise ValueError("'>' should be the last char, but got: %s" % s)
            return {
                "type": "array",
                "items": HiveColumnToAvroConverter._parse_datatype_string(s[6:-1]),
                "native_data_type": s,
            }
        elif s.startswith("map<"):
            if s[-1] != ">":
                raise ValueError("'>' should be the last char, but got: %s" % s)
            parts = HiveColumnToAvroConverter._ignore_brackets_split(s[4:-1], ",")
            if len(parts) != 2:
                raise ValueError(
                    (
                        "The map type string format is: 'map<key_type,value_type>', "
                        + f"but got: {s}"
                    )
                )

            kt = HiveColumnToAvroConverter._parse_datatype_string(parts[0])
            vt = HiveColumnToAvroConverter._parse_datatype_string(parts[1])
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
            parts = HiveColumnToAvroConverter._ignore_brackets_split(s[10:-1], ",")
            t = []
            ustruct_seqn = 0
            for part in parts:
                if part.startswith("struct<"):
                    # ustruct_seqn defines sequence number of struct in union
                    t.append(
                        HiveColumnToAvroConverter._parse_datatype_string(
                            part, ustruct_seqn=ustruct_seqn
                        )
                    )
                    ustruct_seqn += 1
                else:
                    t.append(HiveColumnToAvroConverter._parse_datatype_string(part))
            return t
        elif s.startswith("struct<"):
            if s[-1] != ">":
                raise ValueError("'>' should be the last char, but got: %s" % s)
            return HiveColumnToAvroConverter._parse_struct_fields_string(
                s[7:-1], **kwargs
            )
        elif ":" in s:
            return HiveColumnToAvroConverter._parse_struct_fields_string(s, **kwargs)
        else:
            return HiveColumnToAvroConverter._parse_basic_datatype_string(s)

    @staticmethod
    def _parse_struct_fields_string(s: str, **kwargs: Any) -> Dict[str, object]:
        parts = HiveColumnToAvroConverter._ignore_brackets_split(s, ",")
        fields: List[Dict] = []
        for part in parts:
            name_and_type = HiveColumnToAvroConverter._ignore_brackets_split(
                part.strip(), HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR
            )
            if len(name_and_type) != 2:
                raise ValueError(
                    (
                        "The struct field string format is: 'field_name:field_type', "
                        + f"but got: {part}"
                    )
                )

            field_name = name_and_type[0].strip()
            if field_name.startswith("`"):
                if field_name[-1] != "`":
                    raise ValueError("'`' should be the last char, but got: %s" % s)
                field_name = field_name[1:-1]
            field_type = HiveColumnToAvroConverter._parse_datatype_string(
                name_and_type[1]
            )

            if not any(field["name"] == field_name for field in fields):
                fields.append({"name": field_name, "type": field_type})

        if kwargs.get("ustruct_seqn") is not None:
            struct_name = f'__structn_{kwargs["ustruct_seqn"]}_{str(uuid.uuid4()).replace("-", "")}'

        else:
            struct_name = f'__struct_{str(uuid.uuid4()).replace("-", "")}'
        return {
            "type": "record",
            "name": struct_name,
            "fields": fields,
            "native_data_type": f"struct<{s}>",
        }

    @staticmethod
    def _parse_basic_datatype_string(s: str) -> Dict[str, object]:
        if s in HiveColumnToAvroConverter._PRIVIMITE_HIVE_TYPE_TO_AVRO_TYPE.keys():
            return {
                "type": HiveColumnToAvroConverter._PRIVIMITE_HIVE_TYPE_TO_AVRO_TYPE[s],
                "native_data_type": s,
                "_nullable": True,
            }

        elif HiveColumnToAvroConverter._FIXED_STRING.match(s):
            m = HiveColumnToAvroConverter._FIXED_STRING.match(s)
            return {"type": "string", "native_data_type": s, "_nullable": True}

        elif HiveColumnToAvroConverter._FIXED_DECIMAL.match(s):
            m = HiveColumnToAvroConverter._FIXED_DECIMAL.match(s)
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

    @staticmethod
    def _ignore_brackets_split(s: str, separator: str) -> List[str]:
        """
        Splits the given string by given separator, but ignore separators inside brackets pairs, e.g.
        given "a,b" and separator ",", it will return ["a", "b"], but given "a<b,c>, d", it will return
        ["a<b,c>", "d"].
        """
        parts = []
        buf = ""
        level = 0
        for c in s:
            if c in HiveColumnToAvroConverter._BRACKETS.keys():
                level += 1
                buf += c
            elif c in HiveColumnToAvroConverter._BRACKETS.values():
                if level == 0:
                    raise ValueError(f"Brackets are not correctly paired: {s}")
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
            raise ValueError(f"The {separator} cannot be the last char: {s}")
        parts.append(buf)
        return parts

    @staticmethod
    def is_primitive_hive_type(hive_type: str) -> bool:
        return not HiveColumnToAvroConverter._COMPLEX_TYPE.match(hive_type)

    @classmethod
    def get_avro_schema_for_hive_column(
        cls, hive_column_name: str, hive_column_type: str
    ) -> Union[object, Dict[str, object]]:
        converter = cls()
        # Below Record structure represents the dataset level
        # Inner fields represent the complex field (struct/array/map/union)
        if converter.is_primitive_hive_type(hive_column_type):
            return converter._parse_datatype_string(hive_column_type)
        else:
            return {
                "type": "record",
                "name": "__struct_",
                "fields": [
                    {
                        "name": hive_column_name,
                        "type": converter._parse_datatype_string(hive_column_type),
                    }
                ],
            }


def get_avro_schema_for_hive_column(
    hive_column_name: str,
    hive_column_type: str,
) -> Union[object, Dict[str, object]]:
    return HiveColumnToAvroConverter.get_avro_schema_for_hive_column(
        hive_column_name, hive_column_type
    )


def get_schema_fields_for_hive_column(
    hive_column_name: str,
    hive_column_type: str,
    description: Optional[str] = None,
    default_nullable: bool = False,
    is_part_of_key: bool = False,
) -> List[SchemaField]:
    try:
        avro_schema_json = get_avro_schema_for_hive_column(
            hive_column_name=hive_column_name, hive_column_type=hive_column_type
        )
        schema_fields = avro_schema_to_mce_fields(
            avro_schema=json.dumps(avro_schema_json),
            default_nullable=default_nullable,
            swallow_exceptions=False,
        )
    except Exception as e:
        logger.warning(
            f"Unable to parse column {hive_column_name} and type {hive_column_type} the error was: {e}"
        )
        schema_fields = [
            SchemaField(
                fieldPath=hive_column_name,
                type=SchemaFieldDataTypeClass(type=NullTypeClass()),
                nativeDataType=hive_column_type,
            )
        ]

    assert schema_fields
    if HiveColumnToAvroConverter.is_primitive_hive_type(hive_column_type):
        # Primitive avro schema does not have any field names. Append it to fieldPath.
        schema_fields[0].fieldPath += f".{hive_column_name}"
    if description:
        schema_fields[0].description = description
    schema_fields[0].isPartOfKey = is_part_of_key
    return schema_fields
