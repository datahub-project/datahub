import logging
from typing import Dict, List, Optional, Tuple
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

import defusedxml.ElementTree as ET
from defusedxml.common import DefusedXmlException

from datahub.ingestion.source.sap_common.models import UnknownColumnType
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)

# OData v4 EDMX namespaces, shared by every SAP OData surface (SAP Datasphere
# view exposure and SAC Data Export Service both emit v4 EDMX).
EDMX_NS: Dict[str, str] = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm",
}

EDM_TYPE_MAP: Dict[str, type] = {
    "Edm.String": StringTypeClass,
    "Edm.Int16": NumberTypeClass,
    "Edm.Int32": NumberTypeClass,
    "Edm.Int64": NumberTypeClass,
    "Edm.Byte": NumberTypeClass,
    "Edm.SByte": NumberTypeClass,
    "Edm.Decimal": NumberTypeClass,
    "Edm.Double": NumberTypeClass,
    "Edm.Single": NumberTypeClass,
    "Edm.Boolean": BooleanTypeClass,
    "Edm.Date": DateTypeClass,
    "Edm.DateTimeOffset": DateTypeClass,
    "Edm.TimeOfDay": TimeTypeClass,
    "Edm.Duration": StringTypeClass,
    "Edm.Guid": StringTypeClass,
    "Edm.Binary": BytesTypeClass,
}


def parse_edmx_document(xml_text: str) -> Tuple[Optional[Element], Optional[str]]:
    """Safely parse an EDMX document. Returns (root, None) on success or
    (None, error) on a malformed or hostile payload.

    defusedxml raises DTDForbidden / EntitiesForbidden / ExternalReferenceForbidden
    (all DefusedXmlException subclasses, not ParseError) on a hostile or
    proxy/error-page payload, so both are caught here and surfaced as a structured
    error rather than escaping to a generic handler."""
    try:
        return ET.fromstring(xml_text), None
    except (ET.ParseError, DefusedXmlException) as e:
        return None, f"Malformed or unsafe EDMX XML: {e}"


def edm_decimal_native_type(
    edm_type: str, precision: Optional[str], scale: Optional[str]
) -> str:
    """Default OData native-type rendering: qualify Edm.Decimal with its
    precision/scale, pass everything else through verbatim."""
    if edm_type == "Edm.Decimal" and precision:
        return f"Edm.Decimal({precision},{scale or '0'})"
    return edm_type


def build_schema_field(
    *,
    name: str,
    edm_type: str,
    nullable: bool,
    is_key: bool,
    description: Optional[str],
    native_type: str,
    unknown_sink: List[UnknownColumnType],
) -> SchemaFieldClass:
    """Map one OData property to a SchemaFieldClass, recording unknown Edm types in
    ``unknown_sink`` and falling back to NullType so ingestion never aborts."""
    type_class = EDM_TYPE_MAP.get(edm_type)
    if type_class is None:
        logger.warning(
            "Unknown Edm type %s on field %s, falling back to NullType",
            edm_type,
            name,
        )
        unknown_sink.append(UnknownColumnType(type=edm_type, column=name))
        type_class = NullTypeClass

    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(type=type_class()),
        nativeDataType=native_type,
        nullable=nullable,
        isPartOfKey=is_key,
        description=description,
    )
