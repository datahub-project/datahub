from datahub.ingestion.source.sac.data_export_metadata import (
    parse_data_export_metadata,
)
from datahub.ingestion.source.sap_common.models import UnknownColumnType
from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

# MasterData is listed first so a correct parser must skip it and pick FactData. dim_a has
# a sibling annotation before the native-type one; attr_form uses the String= attribute
# form and is non-nullable.
_DES_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="sac">
      <EntityType Name="MasterData">
        <Key><PropertyRef Name="master_id"/></Key>
        <Property Name="master_id" Type="Edm.String">
          <Annotation Term="Integration.OriginalDataType"><String>NVARCHAR</String></Annotation>
        </Property>
      </EntityType>
      <EntityType Name="FactData">
        <Key><PropertyRef Name="dim_a"/></Key>
        <Property Name="dim_a" Type="Edm.String" MaxLength="127">
          <Annotation Term="Common.Label"><String>Dimension A</String></Annotation>
          <Annotation Term="Integration.OriginalDataType"><String>NVARCHAR</String></Annotation>
        </Property>
        <Property Name="measure_x" Type="Edm.Decimal" Precision="34" Scale="6" Nullable="true"/>
        <Property Name="attr_form" Type="Edm.String" Nullable="false">
          <Annotation Term="Integration.OriginalDataType" String="VARCHAR"/>
        </Property>
        <Property Name="weird_col" Type="Edm.Fancy"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_parses_fact_data_entity_skipping_master_data():
    result = parse_data_export_metadata(_DES_METADATA)

    assert result.error is None
    assert [f.fieldPath for f in result.fields] == [
        "dim_a",
        "measure_x",
        "attr_form",
        "weird_col",
    ]


def test_native_type_prefers_integration_original_type_over_edm():
    result = parse_data_export_metadata(_DES_METADATA)
    by_path = {f.fieldPath: f for f in result.fields}

    # dim_a's native-type annotation is preceded by a sibling, exercising the skip path.
    dim_a = by_path["dim_a"]
    assert isinstance(dim_a.type.type, StringTypeClass)
    assert dim_a.nativeDataType == "NVARCHAR"
    assert dim_a.isPartOfKey is True

    # No Integration.OriginalDataType annotation, so the Edm decimal rendering is used.
    measure_x = by_path["measure_x"]
    assert isinstance(measure_x.type.type, NumberTypeClass)
    assert measure_x.nativeDataType == "Edm.Decimal(34,6)"
    assert measure_x.isPartOfKey is False


def test_native_type_reads_attribute_serialized_annotation():
    # Integration.OriginalDataType can be a String= attribute instead of a nested child.
    result = parse_data_export_metadata(_DES_METADATA)
    attr_form = next(f for f in result.fields if f.fieldPath == "attr_form")

    assert attr_form.nativeDataType == "VARCHAR"


def test_nullable_attribute_is_honored():
    result = parse_data_export_metadata(_DES_METADATA)
    by_path = {f.fieldPath: f for f in result.fields}

    assert by_path["attr_form"].nullable is False
    assert by_path["measure_x"].nullable is True


def test_unknown_edm_type_falls_back_to_null_and_is_recorded():
    # Parser records the unmapped type; SACSource surfaces it as a warning (see integration).
    result = parse_data_export_metadata(_DES_METADATA)
    weird_col = next(f for f in result.fields if f.fieldPath == "weird_col")

    assert isinstance(weird_col.type.type, NullTypeClass)
    assert result.unknown_edm_types == [
        UnknownColumnType(type="Edm.Fancy", column="weird_col")
    ]


def test_missing_fact_data_returns_structured_error():
    no_fact = _DES_METADATA.replace('Name="FactData"', 'Name="Other"')
    result = parse_data_export_metadata(no_fact)

    assert result.error is not None
    assert "FactData" in result.error
    assert result.fields == []


def test_malformed_document_returns_structured_error():
    result = parse_data_export_metadata("<edmx:Edmx> not closed")

    assert result.error is not None
    assert result.fields == []
