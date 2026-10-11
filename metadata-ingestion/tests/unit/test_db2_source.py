import re
import unittest.mock

import pytest

from datahub.ingestion.source.sql.db2 import _db2_get_view_qualifier, _quote_identifier


@pytest.mark.parametrize(
    "input, expected",
    [
        ('"SINGLE_SCHEMA"', "SINGLE_SCHEMA"),
        ('"lowercase_schema"', "lowercase_schema"),
        ('"SCHEMA WITH ""ESCAPED"" QUOTES"', 'SCHEMA WITH "ESCAPED" QUOTES'),
        (
            '"MULTIPLE_SCHEMAS","SCHEMA_TWO"',
            NotImplementedError(
                "len(PATHSCHEMAS) > 1: ['MULTIPLE_SCHEMAS', 'SCHEMA_TWO']"
            ),
        ),
        (
            '"IGNORE_SYSTEM_SCHEMAS","SYSFUN","SYSIBM","SYSIBMADM","SYSPROC"',
            "IGNORE_SYSTEM_SCHEMAS",
        ),
        (
            '"MULTIPLE_SCHEMAS_AND_SYSTEM_SCHEMAS","SCHEMA_TWO","SYSFUN","SYSIBM","SYSIBMADM","SYSPROC"',
            NotImplementedError(
                "len(PATHSCHEMAS) > 1: ['MULTIPLE_SCHEMAS_AND_SYSTEM_SCHEMAS', 'SCHEMA_TWO']"
            ),
        ),
    ],
)
def test_db2_zos_get_view_qualifier(input, expected):
    inspector = unittest.mock.MagicMock()
    inspector.has_table.side_effect = lambda table, schema: (
        schema == "SYSIBM" and table == "SYSVIEWS"
    )
    # SA-2.0: reflection queries now run via
    # `with inspector.engine.connect() as conn: conn.exec_driver_sql(sql, params).scalar()`.
    conn = inspector.engine.connect.return_value.__enter__.return_value
    conn.exec_driver_sql.return_value.scalar.return_value = input

    if isinstance(expected, Exception):
        with pytest.raises(type(expected), match=re.escape(str(expected))):
            _db2_get_view_qualifier(inspector, "myschema", "myview")
    else:
        assert _db2_get_view_qualifier(inspector, "myschema", "myview") == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("UPPERCASE", '"UPPERCASE"'),
        ("lowercase", '"lowercase"'),
        ('with double "quotes"', '"with double ""quotes"""'),
    ],
)
def test_db2_quote_identifier(input, expected):
    assert _quote_identifier(input) == expected
