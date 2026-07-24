from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.mapping import (
    build_jdbc_url,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.models import InformixColumn


class _Report:
    def __init__(self):
        self.warnings = []

    def warning(self, title=None, message=None, context=None, **kwargs):
        self.warnings.append((message, context))


def test_build_jdbc_url():
    cfg = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb",
         "host_port": "ifx:9088", "username": "u", "password": "p"}
    )
    url = build_jdbc_url(cfg)
    assert url == (
        "jdbc:informix-sqli://ifx:9088/testdb:"
        "INFORMIXSERVER=informix;user=u;password=p"
    )


def test_make_table_identifier():
    assert make_table_identifier("testdb", "informix", "customers") == (
        "testdb.informix.customers"
    )


def test_columns_to_schema_fields_maps_types_and_pk():
    cols = [
        InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True),
        InformixColumn(name="name", coltype=13, length=100, colno=2, is_pk=False),
    ]
    report = _Report()
    fields = columns_to_schema_fields(cols, report)
    assert [f.fieldPath for f in fields] == ["id", "name"]
    assert fields[0].nullable is False
    assert fields[1].nativeDataType == "VARCHAR"
    assert report.warnings == []


def test_columns_to_schema_fields_warns_on_unknown_type():
    cols = [InformixColumn(name="weird", coltype=99, length=1, colno=1, is_pk=False)]
    report = _Report()
    fields = columns_to_schema_fields(cols, report)
    assert fields[0].nativeDataType.startswith("UNKNOWN")
    assert len(report.warnings) == 1
