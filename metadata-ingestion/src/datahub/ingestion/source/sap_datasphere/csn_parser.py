from typing import Dict, List

from datahub.ingestion.source.sap_datasphere.constants import (
    CSN_ATTR_LABEL,
    CSN_ATTR_LENGTH,
    CSN_ATTR_PRECISION,
    CSN_ATTR_SCALE,
    CSN_TYPE,
    CSN_TYPE_ASSOCIATION,
    CSN_TYPE_COMPOSITION,
)
from datahub.ingestion.source.sap_datasphere.models import (
    CsnSchemaResult,
    UnknownColumnType,
)
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

# CDS type literal -> (DataHub type class, native-type display root)
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

# Navigation elements — a relationship to another entity, not a scalar column.
# They must never become schema fields (they carry no data type) and must not be
# reported as "unknown CDS types"; the lineage extractor turns their targets into
# upstream edges instead.
_NAVIGATION_TYPES = frozenset({CSN_TYPE_ASSOCIATION, CSN_TYPE_COMPOSITION})


def _native_type_string(root: str, element: Dict) -> str:
    if CSN_ATTR_LENGTH in element:
        return f"{root}({element[CSN_ATTR_LENGTH]})"
    precision = element.get(CSN_ATTR_PRECISION)
    scale = element.get(CSN_ATTR_SCALE)
    if precision is not None and scale is not None:
        return f"{root}({precision},{scale})"
    if precision is not None:
        return f"{root}({precision})"
    return root


def parse_csn_elements_to_schema_fields(
    elements: Dict[str, Dict],
) -> CsnSchemaResult:
    """Convert a CSN ``elements`` map to DataHub schema fields.

    Consumes CSN directly (mirroring the EDMX path) so Local Table and analytic
    model schemas are available even without an OData ``$metadata`` endpoint.
    Returns the fields plus any columns whose CDS type literal is unmapped;
    those still emit as ``StringTypeClass`` with the raw type in
    ``nativeDataType``, but the caller surfaces them in the report rather than
    silently degrading. Dict insertion order is preserved so the rendered schema
    matches the upstream column order.
    """
    fields: List[SchemaFieldClass] = []
    unknown_types: List[UnknownColumnType] = []
    navigation_elements: List[str] = []
    for col_name, element in elements.items():
        if not isinstance(element, dict):
            continue
        cds_type = element.get(CSN_TYPE, "")
        # Associations/compositions are navigations, not columns: skip them from
        # the scalar schema (the lineage extractor consumes their targets) and do
        # not misreport them as unknown scalar types.
        if cds_type in _NAVIGATION_TYPES:
            navigation_elements.append(col_name)
            continue
        # A missing type key is a structural concern, not an unknown type.
        if cds_type and cds_type not in _TYPE_MAP:
            unknown_types.append(UnknownColumnType(type=cds_type, column=col_name))
        type_ctor, native_root = _TYPE_MAP.get(
            cds_type, (StringTypeClass, cds_type or "UNKNOWN")
        )
        native = _native_type_string(native_root, element)
        description = element.get(CSN_ATTR_LABEL)
        fields.append(
            SchemaFieldClass(
                fieldPath=col_name,
                type=SchemaFieldDataTypeClass(type=type_ctor()),
                nativeDataType=native,
                description=description,
                nullable=True,
            )
        )
    return CsnSchemaResult(
        fields=fields,
        unknown_types=unknown_types,
        navigation_elements=navigation_elements,
    )
