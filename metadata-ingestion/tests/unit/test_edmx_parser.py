from pathlib import Path

from datahub.ingestion.source.sap_datasphere.edmx_parser import EdmxParser
from datahub.metadata.schema_classes import (
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sap_datasphere_dimension_day.xml"


def _parse():
    return EdmxParser.parse(FIXTURE.read_text())


def test_parses_column_count():
    result = _parse()
    assert len(result.fields) == 7


def test_date_type_mapped():
    result = _parse()
    date_sql = next(f for f in result.fields if f.fieldPath == "DATE_SQL")
    assert isinstance(date_sql.type.type, DateTypeClass)
    assert date_sql.nullable is False


def test_string_type_mapped():
    result = _parse()
    date_sap = next(f for f in result.fields if f.fieldPath == "DATE_SAP")
    assert isinstance(date_sap.type.type, StringTypeClass)


def test_decimal_precision_in_native_type():
    result = _parse()
    amount = next(f for f in result.fields if f.fieldPath == "AMOUNT")
    assert isinstance(amount.type.type, NumberTypeClass)
    assert "10" in amount.nativeDataType and "2" in amount.nativeDataType


def test_byte_type_mapped_to_number():
    result = _parse()
    day_int = next(f for f in result.fields if f.fieldPath == "DAY_INT")
    assert isinstance(day_int.type.type, NumberTypeClass)


def test_common_label_becomes_description():
    result = _parse()
    date_sql = next(f for f in result.fields if f.fieldPath == "DATE_SQL")
    assert date_sql.description == "Date"


def test_calendar_annotation_in_custom_props():
    result = _parse()
    assert result.field_custom_props["DATE_SQL"].get("sap_calendar_type") == "date"


def test_currency_annotation_in_custom_props():
    result = _parse()
    assert result.field_custom_props["AMOUNT"].get("sap_semantic") == "currency"


def test_entity_label_in_entity_props():
    result = _parse()
    assert result.entity_label == "Time Dimension - Day"


def test_dimension_type_in_entity_props():
    result = _parse()
    assert result.entity_custom_props.get("sap_dimension_type") == "Time"


def test_dimension_type_hash_prefix_normalized():
    """Regression: live SAP tenants emit ``EnumMember="#TIME"`` (CDS short form)
    rather than the OData-canonical ``"Analytics.DimensionType/Time"``. Both
    must normalize to the same human-readable value.
    """
    edmx = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="E">
        <Key><PropertyRef Name="K"/></Key>
        <Property Name="K" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <Annotations Target="NS.E">
        <Annotation Term="Analytics.dimensionType" EnumMember="#TIME"/>
      </Annotations>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    result = EdmxParser.parse(edmx)
    assert result.entity_custom_props.get("sap_dimension_type") == "Time"
