import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM
from datahub.ingestion.source.informix.mapping import (
    build_foreign_key_constraints,
    build_jdbc_url,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.models import (
    ExtendedType,
    InformixColumn,
    InformixForeignKey,
)
from datahub.ingestion.source.informix.report import InformixSourceReport
from datahub.metadata.schema_classes import BooleanTypeClass, StringTypeClass


def test_build_jdbc_url():
    cfg = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "host_port": "ifx:9088",
            "username": "u",
            "password": "p",
        }
    )
    url = build_jdbc_url(cfg)
    assert url == (
        "jdbc:informix-sqli://ifx:9088/testdb:INFORMIXSERVER=informix;user=u;password=p"
    )


def test_build_jdbc_url_password_none():
    cfg = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "host_port": "ifx:9088",
            "username": "u",
        }
    )
    url = build_jdbc_url(cfg)
    assert "password=" in url
    assert url == (
        "jdbc:informix-sqli://ifx:9088/testdb:INFORMIXSERVER=informix;user=u;password="
    )


def test_build_jdbc_url_appends_extra_props():
    cfg = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "host_port": "ifx:9088",
            "username": "u",
            "password": "p",
            "extra_props": "DB_LOCALE=en_US.utf8",
        }
    )
    url = build_jdbc_url(cfg)
    assert url.endswith(";DB_LOCALE=en_US.utf8")


def test_make_table_identifier():
    assert make_table_identifier("testdb", "informix", "customers") == (
        "testdb.informix.customers"
    )


def test_columns_to_schema_fields_maps_types_and_nullable():
    cols = [
        InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True),
        InformixColumn(name="name", coltype=13, length=100, colno=2, is_pk=False),
    ]
    report = InformixSourceReport()
    fields = columns_to_schema_fields(cols, report)
    assert [f.fieldPath for f in fields] == ["id", "name"]
    assert fields[0].nullable is False
    assert fields[0].isPartOfKey is True
    assert fields[1].nativeDataType == "VARCHAR(100)"
    assert fields[1].isPartOfKey is False
    assert len(report.warnings) == 0


@pytest.mark.parametrize(
    "coltype,collength,expected",
    [
        # Values measured against Informix 15.0.1. VARCHAR/NVARCHAR pack
        # (reserved_min * 256) + max_size into a signed SMALLINT collength.
        (13, 100, "VARCHAR(100)"),  # VARCHAR(100), no reserved minimum
        (13, 2660, "VARCHAR(100)"),  # VARCHAR(100,10) -> 10 * 256 + 100
        (13, -26936, "VARCHAR(200)"),  # VARCHAR(200,150) wraps negative
        (16, 5200, "NVARCHAR(80)"),  # NVARCHAR(80,20) -> 20 * 256 + 80
        (0, 30, "CHAR(30)"),  # CHAR stores the length verbatim
        (15, 30, "NCHAR(30)"),
        (43, 4000, "LVARCHAR(4000)"),
        (2, 4, "INTEGER"),  # non-length types get no suffix
    ],
)
def test_columns_to_schema_fields_decodes_declared_length(
    coltype: int, collength: int, expected: str
) -> None:
    cols = [
        InformixColumn(
            name="c", coltype=coltype, length=collength, colno=1, is_pk=False
        )
    ]
    fields = columns_to_schema_fields(cols, InformixSourceReport())
    assert fields[0].nativeDataType == expected


def test_columns_to_schema_fields_uses_extended_type_name():
    # LVARCHAR/BOOLEAN/BLOB share base coltypes 40 and 41, so before the
    # sysxtdtypes lookup all three resolved to UNKNOWN + NullType.
    cols = [
        InformixColumn(
            name="body",
            coltype=40,
            length=4000,
            colno=1,
            extended=ExtendedType(name="lvarchar", mode="B"),
        ),
        InformixColumn(
            name="flag",
            coltype=41,
            length=1,
            colno=2,
            extended=ExtendedType(name="boolean", mode="B"),
        ),
    ]
    report = InformixSourceReport()
    fields = columns_to_schema_fields(cols, report)
    assert [f.nativeDataType for f in fields] == ["LVARCHAR(4000)", "BOOLEAN"]
    assert len(report.warnings) == 0


def test_columns_to_schema_fields_resolves_distinct_over_opaque_builtin():
    # coltype's low byte is 41 for a DISTINCT over BOOLEAN and over CLOB alike,
    # so without sysxtdtypes.source both kept NullType despite a resolved name.
    cols = [
        InformixColumn(
            name="published",
            coltype=18473,
            length=1,
            colno=1,
            extended=ExtendedType(name="flag_type", mode="D", source_name="boolean"),
        ),
        InformixColumn(
            name="abstract",
            coltype=2089,
            length=72,
            colno=2,
            extended=ExtendedType(name="doc_text", mode="D", source_name="clob"),
        ),
    ]
    report = InformixSourceReport()
    fields = columns_to_schema_fields(cols, report)
    assert [f.nativeDataType for f in fields] == ["FLAG_TYPE", "DOC_TEXT"]
    assert isinstance(fields[0].type.type, BooleanTypeClass)
    assert isinstance(fields[1].type.type, StringTypeClass)
    assert len(report.warnings) == 0


def test_columns_to_schema_fields_warns_for_opaque_type_but_keeps_its_name():
    # An opaque type has no DataHub equivalent, so it still warns -- but the
    # recovered name is more useful than UNKNOWN(40).
    cols = [
        InformixColumn(
            name="doc",
            coltype=40,
            length=0,
            colno=1,
            extended=ExtendedType(name="json", mode="O"),
        )
    ]
    report = InformixSourceReport()
    fields = columns_to_schema_fields(cols, report)
    assert fields[0].nativeDataType == "JSON"
    assert len(report.warnings) == 1


def test_columns_to_schema_fields_warns_on_unknown_type():
    cols = [InformixColumn(name="weird", coltype=99, length=1, colno=1, is_pk=False)]
    report = InformixSourceReport()
    fields = columns_to_schema_fields(cols, report)
    assert fields[0].nativeDataType.startswith("UNKNOWN")
    assert len(report.warnings) == 1


def test_build_foreign_key_constraints():
    fk = InformixForeignKey(
        name="fk_orders_customer",
        child_columns=["customer_id"],
        parent_table="customers",
        parent_owner="informix",
        parent_columns=["id"],
    )
    child_urn = make_dataset_urn(PLATFORM, "testdb.informix.orders", "PROD")

    constraints = build_foreign_key_constraints([fk], child_urn, "testdb", "PROD", None)

    assert len(constraints) == 1
    constraint = constraints[0]
    assert constraint.name == "fk_orders_customer"
    assert constraint.foreignDataset == make_dataset_urn(
        PLATFORM, "testdb.informix.customers", "PROD"
    )
    assert constraint.sourceFields == [make_schema_field_urn(child_urn, "customer_id")]
    assert constraint.foreignFields == [
        make_schema_field_urn(constraint.foreignDataset, "id")
    ]
