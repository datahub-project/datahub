import logging
from typing import Dict, List
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

import defusedxml.ElementTree as ET

from datahub.ingestion.source.sap_datasphere.constants import (
    CALENDAR_DATE,
    CALENDAR_MONTH,
    CALENDAR_QUARTER,
    CALENDAR_WEEK,
    CALENDAR_YEAR,
    CALENDAR_YEARMONTH,
    PROP_SAP_CALENDAR_TYPE,
    PROP_SAP_DIMENSION_TYPE,
    PROP_SAP_IS_DIMENSION,
    PROP_SAP_IS_MEASURE,
    PROP_SAP_SEMANTIC,
    PROP_VALUE_TRUE,
    SEMANTIC_CURRENCY,
    SEMANTIC_UNIT,
)
from datahub.ingestion.source.sap_datasphere.models import (
    EdmxParseResult,
    UnknownColumnType,
)
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

_NS = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm",
}

_EDM_TYPE_MAP: Dict[str, type] = {
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

# SAP Common calendar term → sap_calendar_type value
_CALENDAR_TERMS: Dict[str, str] = {
    "Common.IsCalendarDate": CALENDAR_DATE,
    "Common.IsCalendarYear": CALENDAR_YEAR,
    "Common.IsCalendarMonth": CALENDAR_MONTH,
    "Common.IsCalendarWeek": CALENDAR_WEEK,
    "Common.IsCalendarQuarter": CALENDAR_QUARTER,
    "Common.IsCalendarYearMonth": CALENDAR_YEARMONTH,
}


class EdmxParser:
    @staticmethod
    def parse(xml_text: str) -> EdmxParseResult:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            return EdmxParseResult(
                fields=[],
                field_custom_props={},
                entity_label=None,
                entity_custom_props={},
                error=f"Malformed EDMX XML: {e}",
            )

        entity_type = root.find(".//edm:EntityType", _NS)
        if entity_type is None:
            return EdmxParseResult(
                fields=[],
                field_custom_props={},
                entity_label=None,
                entity_custom_props={},
                error="EDMX document has no EntityType element",
            )

        entity_name = entity_type.get("Name", "")
        namespace_elem = root.find(".//edm:Schema", _NS)
        namespace = (
            namespace_elem.get("Namespace", "") if namespace_elem is not None else ""
        )

        key_props = {
            ref.get("Name")
            for ref in entity_type.findall("edm:Key/edm:PropertyRef", _NS)
        }

        # qualified_target → {term_name: value}
        annotations_map: Dict[str, Dict[str, str]] = {}
        for annots_block in root.findall(".//edm:Annotations", _NS):
            if annots_block.get("Qualifier"):
                continue
            target = annots_block.get("Target", "")
            if target not in annotations_map:
                annotations_map[target] = {}
            for ann in annots_block.findall("edm:Annotation", _NS):
                term = ann.get("Term", "")
                value = EdmxParser._annotation_value(ann)
                annotations_map[target][term] = value

        entity_target = f"{namespace}.{entity_name}"
        entity_annots = annotations_map.get(entity_target, {})
        entity_label = entity_annots.get("Common.Label")
        entity_custom_props: Dict[str, str] = {}
        dim_type_raw = entity_annots.get("Analytics.dimensionType", "")
        if dim_type_raw:
            # Two forms in the wild: OData-canonical
            # "Analytics.DimensionType/Time" (drop slash-prefixed namespace) and
            # CDS-flavored "#TIME" (drop hash prefix). Title-case so the value is
            # stable across both forms (live tenant emits "#TIME").
            normalized = dim_type_raw.rsplit("/", 1)[-1].lstrip("#")
            entity_custom_props[PROP_SAP_DIMENSION_TYPE] = normalized.title()
        if entity_annots.get("Analytics.Dimension") is not None:
            entity_custom_props[PROP_SAP_IS_DIMENSION] = PROP_VALUE_TRUE
        if entity_annots.get("Analytics.Measure") is not None:
            entity_custom_props[PROP_SAP_IS_MEASURE] = PROP_VALUE_TRUE

        schema_fields: List[SchemaFieldClass] = []
        field_custom_props: Dict[str, Dict[str, str]] = {}
        unknown_edm_types_local: List[UnknownColumnType] = []

        for prop in entity_type.findall("edm:Property", _NS):
            prop_name = prop.get("Name", "")
            edm_type = prop.get("Type", "Edm.String")
            nullable = prop.get("Nullable", "true").lower() != "false"
            precision = prop.get("Precision")
            scale = prop.get("Scale")

            type_class = _EDM_TYPE_MAP.get(edm_type)
            if type_class is None:
                logger.warning(
                    "Unknown Edm type %s on field %s, falling back to NullType",
                    edm_type,
                    prop_name,
                )
                unknown_edm_types_local.append(UnknownColumnType(edm_type, prop_name))
                type_class = NullTypeClass
            native_type = edm_type
            if edm_type == "Edm.Decimal" and precision:
                scale_str = scale or "0"
                native_type = f"Edm.Decimal({precision},{scale_str})"

            prop_target = f"{entity_target}/{prop_name}"
            prop_annots = annotations_map.get(prop_target, {})
            description = prop_annots.get("Common.Label")

            custom_props: Dict[str, str] = {}
            for term, cal_value in _CALENDAR_TERMS.items():
                if term in prop_annots:
                    custom_props[PROP_SAP_CALENDAR_TYPE] = cal_value
            if "Common.IsCurrency" in prop_annots:
                custom_props[PROP_SAP_SEMANTIC] = SEMANTIC_CURRENCY
            if "Common.IsUnit" in prop_annots:
                custom_props[PROP_SAP_SEMANTIC] = SEMANTIC_UNIT
            # CDS allows @Analytics.Dimension/@Analytics.Measure on properties, not
            # just on the entity; capture the field-level signal here.
            if "Analytics.Dimension" in prop_annots:
                custom_props[PROP_SAP_IS_DIMENSION] = PROP_VALUE_TRUE
            if "Analytics.Measure" in prop_annots:
                custom_props[PROP_SAP_IS_MEASURE] = PROP_VALUE_TRUE

            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=prop_name,
                    type=SchemaFieldDataTypeClass(type=type_class()),
                    nativeDataType=native_type,
                    nullable=nullable,
                    isPartOfKey=prop_name in key_props,
                    description=description,
                )
            )
            if custom_props:
                field_custom_props[prop_name] = custom_props

        return EdmxParseResult(
            fields=schema_fields,
            field_custom_props=field_custom_props,
            entity_label=entity_label,
            entity_custom_props=entity_custom_props,
            unknown_edm_types=unknown_edm_types_local,
        )

    @staticmethod
    def _annotation_value(ann: Element) -> str:
        for attr in ("String", "Bool", "Int", "Float", "Decimal", "EnumMember"):
            val = ann.get(attr)
            if val is not None:
                return val
        for child in ann:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag in ("String", "Bool", "Int", "EnumMember"):
                return child.text or ""
        # Tag annotation: presence is the signal, no value attribute.
        return ""
