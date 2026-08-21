import logging
from typing import List, Optional
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

from datahub.ingestion.source.sap_common.edmx import (
    EDMX_NS,
    build_schema_field,
    edm_decimal_native_type,
    parse_edmx_document,
)
from datahub.ingestion.source.sap_common.models import (
    EdmxParseResult,
    UnknownColumnType,
)
from datahub.metadata.schema_classes import SchemaFieldClass

logger = logging.getLogger(__name__)

# The Data Export Service exposes one EntityType per model surface; "FactData" is
# the model's own fact table (measures + dimension keys). The sibling "MasterData"
# and per-dimension "*Master" entity types describe dimension attributes, not the
# model, so only FactData is turned into the dataset schema.
FACT_DATA_ENTITY = "FactData"

# DES carries the source system's native type (e.g. NVARCHAR, DECIMAL) here rather
# than in the Edm.* type, which is always the OData-normalised type.
TERM_ORIGINAL_DATA_TYPE = "Integration.OriginalDataType"


def _annotation_string(prop: Element, term: str) -> Optional[str]:
    for ann in prop.findall("edm:Annotation", EDMX_NS):
        if ann.get("Term") != term:
            continue
        # The value is either a nested <String> child or a String= attribute.
        string_child = ann.find("edm:String", EDMX_NS)
        if string_child is not None and string_child.text is not None:
            return string_child.text
        return ann.get("String")
    return None


def parse_data_export_metadata(xml_text: str) -> EdmxParseResult:
    """Parse a SAC Data Export Service ``$metadata`` document into the FactData
    schema. Returns an EdmxParseResult carrying an ``error`` (and no payload) when
    the document is malformed or has no FactData entity."""
    root, parse_error = parse_edmx_document(xml_text)
    if parse_error is not None or root is None:
        return EdmxParseResult(
            fields=[],
            field_custom_props={},
            entity_label=None,
            entity_custom_props={},
            error=parse_error or "Malformed or unsafe EDMX XML",
        )

    fact_entity: Optional[Element] = None
    for entity_type in root.findall(".//edm:EntityType", EDMX_NS):
        if entity_type.get("Name") == FACT_DATA_ENTITY:
            fact_entity = entity_type
            break

    if fact_entity is None:
        return EdmxParseResult(
            fields=[],
            field_custom_props={},
            entity_label=None,
            entity_custom_props={},
            error=f"Data Export metadata has no {FACT_DATA_ENTITY} EntityType",
        )

    key_props = {
        ref.get("Name")
        for ref in fact_entity.findall("edm:Key/edm:PropertyRef", EDMX_NS)
    }

    fields: List[SchemaFieldClass] = []
    unknown_edm_types: List[UnknownColumnType] = []
    for prop in fact_entity.findall("edm:Property", EDMX_NS):
        prop_name = prop.get("Name", "")
        edm_type = prop.get("Type", "Edm.String")
        nullable = prop.get("Nullable", "true").lower() != "false"

        native_type = _annotation_string(
            prop, TERM_ORIGINAL_DATA_TYPE
        ) or edm_decimal_native_type(edm_type, prop.get("Precision"), prop.get("Scale"))

        fields.append(
            build_schema_field(
                name=prop_name,
                edm_type=edm_type,
                nullable=nullable,
                is_key=prop_name in key_props,
                description=None,
                native_type=native_type,
                unknown_sink=unknown_edm_types,
            )
        )

    return EdmxParseResult(
        fields=fields,
        field_custom_props={},
        entity_label=None,
        entity_custom_props={},
        unknown_edm_types=unknown_edm_types,
    )
