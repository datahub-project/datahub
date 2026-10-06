from typing import Dict, cast

from datahub.ingestion.source.sap_datasphere.csn_parser import (
    parse_csn_elements_to_schema_fields,
)
from datahub.ingestion.source.sap_datasphere.models import UnknownColumnType
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)


def test_parses_string_type():
    elements = {
        "MONTH": {"type": "cds.String", "@EndUserText.label": "Month", "length": 2}
    }
    result = parse_csn_elements_to_schema_fields(elements)
    fields = result.fields
    assert len(fields) == 1
    assert result.unknown_types == []
    f = fields[0]
    assert f.fieldPath == "MONTH"
    assert isinstance(f.type.type, StringTypeClass)
    assert "Month" in (f.description or "")
    assert "2" in f.nativeDataType


def test_parses_hana_tinyint_as_number():
    elements = {"MONTH_INT": {"type": "cds.hana.TINYINT"}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert isinstance(fields[0].type.type, NumberTypeClass)
    assert fields[0].nativeDataType.upper().startswith("TINYINT")


def test_parses_date_type():
    elements = {"DATE_SQL": {"type": "cds.Date"}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert isinstance(fields[0].type.type, DateTypeClass)


def test_parses_unknown_cds_type_as_null_with_warning_path():
    """Unknown cds.* types fall back to NullType (unclassified), preserve the raw
    CDS literal as native, and are returned in unknown_types for reporting."""
    elements = {"WEIRD": {"type": "cds.SomethingNew"}}
    result = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(result.fields[0].type.type, NullTypeClass)
    assert "cds.SomethingNew" in result.fields[0].nativeDataType
    assert result.unknown_types == [
        UnknownColumnType(type="cds.SomethingNew", column="WEIRD")
    ]


def test_missing_type_key_is_tracked_separately_from_unknown():
    """A column with no type key is a structural gap, not an unknown type, so it lands in columns_missing_type not unknown_types."""
    elements = {
        "NO_TYPE": {"@EndUserText.label": "n/a"},
        "EMPTY_TYPE": {"type": ""},
    }
    result = parse_csn_elements_to_schema_fields(elements)
    assert all(isinstance(f.type.type, NullTypeClass) for f in result.fields)
    assert result.unknown_types == []
    assert set(result.columns_missing_type) == {"NO_TYPE", "EMPTY_TYPE"}


def test_association_and_composition_elements_are_skipped_not_columns():
    """Associations/compositions are navigations, not scalar columns, so they're dropped and not reported as unknown CDS types."""
    elements = {
        "COL": {"type": "cds.String"},
        "_ASSOC": {"type": "cds.Association", "target": "OTHER"},
        "_CHILD": {"type": "cds.Composition", "target": "CHILD_ENTITY"},
    }
    result = parse_csn_elements_to_schema_fields(elements)
    assert [f.fieldPath for f in result.fields] == ["COL"]
    assert result.unknown_types == []
    assert set(result.navigation_elements) == {"_ASSOC", "_CHILD"}


def test_non_dict_element_is_skipped_not_fatal():
    """A non-dict element value must be skipped, not raise and abort the whole schema parse."""
    elements: Dict[str, object] = {
        "BAD_STR": "not-an-object",
        "BAD_NONE": None,
        "BAD_LIST": [1, 2, 3],
        "GOOD": {"type": "cds.String"},
    }
    result = parse_csn_elements_to_schema_fields(cast(Dict[str, Dict], elements))
    assert [f.fieldPath for f in result.fields] == ["GOOD"]
    assert result.unknown_types == []
    assert result.columns_missing_type == []


def test_preserves_order():
    elements = {
        "C": {"type": "cds.String"},
        "A": {"type": "cds.String"},
        "B": {"type": "cds.String"},
    }
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert [f.fieldPath for f in fields] == ["C", "A", "B"]


def test_decimal_precision_in_native_type():
    elements = {"AMOUNT": {"type": "cds.Decimal", "precision": 10, "scale": 2}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert isinstance(fields[0].type.type, NumberTypeClass)
    assert "10" in fields[0].nativeDataType and "2" in fields[0].nativeDataType


def test_decimal_precision_only_omits_scale():
    """Precision but no scale exercises the DECIMAL(p) branch, distinct from the precision+scale case above."""
    elements = {"AMOUNT": {"type": "cds.Decimal", "precision": 12}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert fields[0].nativeDataType == "DECIMAL(12)"


def test_missing_type_native_is_unknown():
    """A column with no type falls back to the UNKNOWN native display so it's distinguishable from a real string column."""
    elements = {"NO_TYPE": {"@EndUserText.label": "n/a"}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert fields[0].nativeDataType == "UNKNOWN"


def test_boolean_type():
    elements = {"FLAG": {"type": "cds.Boolean"}}
    fields = parse_csn_elements_to_schema_fields(elements).fields
    assert isinstance(fields[0].type.type, BooleanTypeClass)
