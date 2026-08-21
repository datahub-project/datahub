from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM
from datahub.ingestion.source.informix.mapping import (
    build_foreign_key_constraints,
    build_jdbc_url,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.models import InformixColumn, InformixForeignKey
from datahub.ingestion.source.informix.report import InformixSourceReport


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
