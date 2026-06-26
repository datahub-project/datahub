"""Unit tests for the SAP Datasphere CSN element parser.

The parser converts the ``elements`` map of a CSN entity definition (as
returned by ``/dwaas-core/api/v1/spaces/{space}/localtables/{name}``) into a
list of DataHub ``SchemaFieldClass`` so that Local Table stubs carry column
metadata. With schema fields on both the View side and the Local Table side,
the DataHub UI can render column-level lineage edges.
"""

from datahub.ingestion.source.sap_datasphere.csn_parser import (
    parse_csn_elements_to_schema_fields,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)


def test_parses_string_type():
    elements = {
        "MONTH": {"type": "cds.String", "@EndUserText.label": "Month", "length": 2}
    }
    fields, unknown = parse_csn_elements_to_schema_fields(elements)
    assert len(fields) == 1
    assert unknown == []
    f = fields[0]
    assert f.fieldPath == "MONTH"
    assert isinstance(f.type.type, StringTypeClass)
    assert "Month" in (f.description or "")
    assert "2" in f.nativeDataType  # surfaces the length


def test_parses_hana_tinyint_as_number():
    elements = {"MONTH_INT": {"type": "cds.hana.TINYINT"}}
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, NumberTypeClass)
    assert fields[0].nativeDataType.upper().startswith("TINYINT")


def test_parses_date_type():
    elements = {"DATE_SQL": {"type": "cds.Date"}}
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, DateTypeClass)


def test_parses_unknown_cds_type_as_string_with_warning_path():
    """Unknown cds.foo types fall back to StringTypeClass with the raw native
    type preserved so the human can see what it actually was, and the type is
    returned in the unknown-types list so the caller can report it."""
    elements = {"WEIRD": {"type": "cds.SomethingNew"}}
    fields, unknown = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, StringTypeClass)
    assert "cds.SomethingNew" in fields[0].nativeDataType
    assert unknown == [("WEIRD", "cds.SomethingNew")]


def test_missing_type_key_is_not_reported_as_unknown():
    """A column with no ``type`` key is a structural gap, not an unknown type —
    it should not pollute the unknown-types report list."""
    elements = {"NO_TYPE": {"@EndUserText.label": "n/a"}}
    fields, unknown = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, StringTypeClass)
    assert unknown == []


def test_preserves_order():
    elements = {
        "C": {"type": "cds.String"},
        "A": {"type": "cds.String"},
        "B": {"type": "cds.String"},
    }
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert [f.fieldPath for f in fields] == ["C", "A", "B"]


def test_decimal_precision_in_native_type():
    elements = {"AMOUNT": {"type": "cds.Decimal", "precision": 10, "scale": 2}}
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, NumberTypeClass)
    assert "10" in fields[0].nativeDataType and "2" in fields[0].nativeDataType


def test_nullable_default_true():
    """CSN does not have a ``nullable: false`` flag at the element level by
    default (key columns get inferred elsewhere). Default ``nullable=True``
    is fine for Local Tables since exact nullability isn't critical for
    lineage UI."""
    elements = {"M": {"type": "cds.String"}}
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert fields[0].nullable is True


def test_boolean_type():
    elements = {"FLAG": {"type": "cds.Boolean"}}
    fields, _ = parse_csn_elements_to_schema_fields(elements)
    assert isinstance(fields[0].type.type, BooleanTypeClass)
