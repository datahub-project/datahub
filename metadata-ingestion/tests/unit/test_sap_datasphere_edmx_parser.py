import logging

from datahub.ingestion.source.sap_datasphere.edmx_parser import EdmxParser

_MINIMAL_EDMX = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="DIM_DAY">
        <Key>
          <PropertyRef Name="DATE_SQL"/>
        </Key>
        <Property Name="DATE_SQL" Type="Edm.Date" Nullable="false"/>
        <Property Name="YEAR_VAL" Type="Edm.Int32" Nullable="true"/>
        <Property Name="MONTH_NAME" Type="Edm.String" Nullable="true"/>
        <Property Name="PRICE_USD" Type="Edm.Decimal" Precision="18" Scale="2" Nullable="true"/>
      </EntityType>
      <Annotations Target="ns.DIM_DAY">
        <Annotation Term="Common.Label" String="Day Dimension"/>
        <Annotation Term="Analytics.Dimension"/>
      </Annotations>
      <Annotations Target="ns.DIM_DAY/DATE_SQL">
        <Annotation Term="Common.IsCalendarDate"/>
        <Annotation Term="Common.Label" String="Calendar Date"/>
      </Annotations>
      <Annotations Target="ns.DIM_DAY/YEAR_VAL">
        <Annotation Term="Common.IsCalendarYear"/>
      </Annotations>
      <Annotations Target="ns.DIM_DAY/PRICE_USD">
        <Annotation Term="Common.IsCurrency"/>
      </Annotations>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_parser_extracts_entity_label():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    assert result.entity_label == "Day Dimension"


def test_parser_marks_entity_as_dimension():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    assert result.entity_custom_props.get("sap_is_dimension") == "true"


def test_parser_extracts_fields_with_correct_native_types():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    field_types = {f.fieldPath: f.nativeDataType for f in result.fields}
    assert field_types["DATE_SQL"] == "Edm.Date"
    assert field_types["YEAR_VAL"] == "Edm.Int32"
    assert field_types["MONTH_NAME"] == "Edm.String"
    # Decimal with precision/scale should be formatted
    assert field_types["PRICE_USD"] == "Edm.Decimal(18,2)"


def test_parser_marks_key_field():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    key_fields = {f.fieldPath for f in result.fields if f.isPartOfKey}
    assert key_fields == {"DATE_SQL"}


def test_parser_extracts_field_descriptions_from_common_label():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    date_field = next(f for f in result.fields if f.fieldPath == "DATE_SQL")
    assert date_field.description == "Calendar Date"


def test_parser_tags_calendar_fields():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    assert (
        result.field_custom_props.get("DATE_SQL", {}).get("sap_calendar_type") == "date"
    )
    assert (
        result.field_custom_props.get("YEAR_VAL", {}).get("sap_calendar_type") == "year"
    )


def test_parser_tags_currency_field():
    result = EdmxParser.parse(_MINIMAL_EDMX)
    assert (
        result.field_custom_props.get("PRICE_USD", {}).get("sap_semantic") == "currency"
    )


def test_parser_returns_empty_result_on_malformed_xml():
    result = EdmxParser.parse("<not-edmx>oops")
    assert result.fields == []
    assert result.entity_label is None
    assert result.entity_custom_props == {}
    assert result.error is not None
    assert "Malformed EDMX" in result.error or "XML" in result.error


def test_parser_returns_empty_result_when_no_entity_type():
    xml = """<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices><Schema Namespace="ns"/></edmx:DataServices>
</edmx:Edmx>"""
    result = EdmxParser.parse(xml)
    assert result.fields == []
    assert result.error is not None
    assert "EntityType" in result.error


def test_parser_falls_back_to_nulltype_for_unknown_edm_type(caplog):
    xml = """<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="WEIRD">
        <Property Name="X" Type="Edm.GarbageType"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    with caplog.at_level(logging.WARNING):
        result = EdmxParser.parse(xml)
    assert len(result.fields) == 1
    # Should have logged a warning about the unknown type
    assert any("Edm.GarbageType" in r.message for r in caplog.records)


def test_parser_collects_unknown_edm_types_for_report():
    """An EDMX with an Edm.* type the connector doesn't know about parses to a
    NullType field AND surfaces (edm_type, property_name) tuples in
    ``unknown_edm_types`` so the source can attribute the issue per-asset."""
    xml = """<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="STREAMY">
        <Property Name="BLOB_COL" Type="Edm.Stream"/>
        <Property Name="OK_COL" Type="Edm.String"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    result = EdmxParser.parse(xml)
    # Both fields parsed; the Edm.Stream column falls back to NullType.
    field_paths = {f.fieldPath for f in result.fields}
    assert field_paths == {"BLOB_COL", "OK_COL"}
    blob_field = next(f for f in result.fields if f.fieldPath == "BLOB_COL")
    # NullType is exposed as schemaFieldDataType.type with class name NullTypeClass
    assert type(blob_field.type.type).__name__ == "NullTypeClass"
    # And the unknown_edm_types list MUST contain the (edm_type, name) tuple.
    assert ("Edm.Stream", "BLOB_COL") in result.unknown_edm_types
    # OK_COL is a known String type so should not be flagged.
    assert all(name != "OK_COL" for _, name in result.unknown_edm_types)


def test_parser_empty_unknown_edm_types_on_clean_doc():
    """A clean EDMX document yields an empty ``unknown_edm_types`` list, not None."""
    result = EdmxParser.parse(_MINIMAL_EDMX)
    assert result.unknown_edm_types == []
