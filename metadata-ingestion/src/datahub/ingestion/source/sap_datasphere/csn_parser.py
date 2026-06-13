"""Parse a SAP Datasphere CSN ``elements`` map into DataHub SchemaFieldClass.

CSN (Core Schema Notation) is SAP's JSON AST for CDS definitions. Each column
appears under ``definitions[name].elements`` as ``{name: {type, length, precision,
scale, @EndUserText.label, ...}}``. We map the type literal to a DataHub
schema-field type, surface the length/precision/scale in the nativeDataType
string, and preserve the human label as the description.

Mirrors edmx_parser.py for the EDMX path, but consumes CSN directly so we can
fetch Local Table schemas (which aren't exposed via the OData $metadata
endpoint that views use).
"""

from typing import Dict, List

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

# CDS type -> (DataHub type class, native-type display root)
_TYPE_MAP = {
    "cds.String": (StringTypeClass, "VARCHAR"),
    "cds.LargeString": (StringTypeClass, "NCLOB"),
    "cds.Binary": (BytesTypeClass, "BINARY"),
    "cds.LargeBinary": (BytesTypeClass, "BLOB"),
    "cds.Boolean": (BooleanTypeClass, "BOOLEAN"),
    "cds.Integer": (NumberTypeClass, "INTEGER"),
    "cds.Integer64": (NumberTypeClass, "BIGINT"),
    "cds.UInt8": (NumberTypeClass, "TINYINT"),
    "cds.Int16": (NumberTypeClass, "SMALLINT"),
    "cds.Int32": (NumberTypeClass, "INTEGER"),
    "cds.Int64": (NumberTypeClass, "BIGINT"),
    "cds.Decimal": (NumberTypeClass, "DECIMAL"),
    "cds.DecimalFloat": (NumberTypeClass, "DECIMAL_FLOAT"),
    "cds.Double": (NumberTypeClass, "DOUBLE"),
    "cds.Date": (DateTypeClass, "DATE"),
    "cds.Time": (TimeTypeClass, "TIME"),
    "cds.DateTime": (TimeTypeClass, "TIMESTAMP"),
    "cds.Timestamp": (TimeTypeClass, "TIMESTAMP"),
    # HANA-flavored aliases
    "cds.hana.TINYINT": (NumberTypeClass, "TINYINT"),
    "cds.hana.SMALLINT": (NumberTypeClass, "SMALLINT"),
    "cds.hana.INTEGER": (NumberTypeClass, "INTEGER"),
    "cds.hana.BIGINT": (NumberTypeClass, "BIGINT"),
    "cds.hana.SMALLDECIMAL": (NumberTypeClass, "SMALLDECIMAL"),
    "cds.hana.REAL": (NumberTypeClass, "REAL"),
    "cds.hana.DOUBLE": (NumberTypeClass, "DOUBLE"),
    "cds.hana.CHAR": (StringTypeClass, "CHAR"),
    "cds.hana.NCHAR": (StringTypeClass, "NCHAR"),
    "cds.hana.VARCHAR": (StringTypeClass, "VARCHAR"),
    "cds.hana.NVARCHAR": (StringTypeClass, "NVARCHAR"),
    "cds.hana.CLOB": (StringTypeClass, "CLOB"),
    "cds.hana.NCLOB": (StringTypeClass, "NCLOB"),
    "cds.hana.BLOB": (BytesTypeClass, "BLOB"),
    "cds.hana.ST_POINT": (StringTypeClass, "ST_POINT"),
    "cds.hana.ST_GEOMETRY": (StringTypeClass, "ST_GEOMETRY"),
}


def _native_type_string(root: str, element: Dict) -> str:
    """Render ``VARCHAR(10)`` / ``DECIMAL(10,2)`` / ``TINYINT`` etc."""
    if "length" in element:
        return f"{root}({element['length']})"
    precision = element.get("precision")
    scale = element.get("scale")
    if precision is not None and scale is not None:
        return f"{root}({precision},{scale})"
    if precision is not None:
        return f"{root}({precision})"
    return root


def parse_csn_elements_to_schema_fields(
    elements: Dict[str, Dict],
) -> List[SchemaFieldClass]:
    """Convert a CSN ``elements`` map to a list of DataHub SchemaFieldClass.

    Preserves dict insertion order so the schema rendered in DataHub matches
    the upstream SAP definition's column order.
    """
    fields: List[SchemaFieldClass] = []
    for col_name, element in elements.items():
        if not isinstance(element, dict):
            continue
        cds_type = element.get("type", "")
        type_ctor, native_root = _TYPE_MAP.get(
            cds_type, (StringTypeClass, cds_type or "UNKNOWN")
        )
        native = _native_type_string(native_root, element)
        description = element.get("@EndUserText.label")
        fields.append(
            SchemaFieldClass(
                fieldPath=col_name,
                type=SchemaFieldDataTypeClass(type=type_ctor()),
                nativeDataType=native,
                description=description,
                nullable=True,
            )
        )
    return fields
