import base64
import json

import pytest

from datahub.ingestion.source.common.presto_view_decoder import (
    decode_presto_view,
    get_presto_view_columns,
    is_presto_view,
    trino_type_to_hive_type,
)


def _presto_envelope(payload: dict) -> str:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return f"/* Presto View: {encoded} */"


def test_is_presto_view_distinguishes_encoded_from_raw_sql() -> None:
    assert is_presto_view(_presto_envelope({"originalSql": "SELECT 1"}))
    assert not is_presto_view("SELECT * FROM my_db.my_schema.orders")


def test_decode_presto_view_returns_payload() -> None:
    payload = {
        "originalSql": "SELECT id FROM my_db.my_schema.events",
        "columns": [{"name": "id", "type": "integer"}],
    }
    decoded = decode_presto_view(_presto_envelope(payload))
    assert decoded["originalSql"] == payload["originalSql"]
    assert decoded["columns"] == payload["columns"]


@pytest.mark.parametrize("encoding", ["utf-8-sig", "utf-16", "utf-32-le"])
def test_decode_presto_view_accepts_non_utf8_json_encodings(encoding: str) -> None:
    payload = {"originalSql": "SELECT 1", "columns": [{"name": "é", "type": "int"}]}
    encoded = base64.b64encode(json.dumps(payload).encode(encoding)).decode()
    assert decode_presto_view(f"/* Presto View: {encoded} */") == payload


def test_decode_presto_view_raises_on_invalid_base64() -> None:
    # binascii.Error (bad base64) and json.JSONDecodeError both subclass ValueError.
    with pytest.raises(ValueError):
        decode_presto_view("/* Presto View: not!valid!base64 */")


def test_decode_presto_view_raises_when_payload_not_object() -> None:
    # A JSON array (not an object) is not a valid view payload.
    encoded = base64.b64encode(json.dumps([1, 2, 3]).encode()).decode()
    with pytest.raises(ValueError):
        decode_presto_view(f"/* Presto View: {encoded} */")


def test_decode_presto_view_rejects_deeply_nested_payload_before_parsing() -> None:
    # Deep nesting can segfault json.loads on Python 3.12+, so it must be
    # rejected with a ValueError before the JSON is parsed.
    deep = '{"columns": ' + "[" * 10_000 + "]" * 10_000 + "}"
    encoded = base64.b64encode(deep.encode()).decode()
    with pytest.raises(ValueError):
        decode_presto_view(f"/* Presto View: {encoded} */")


def test_decode_presto_view_ignores_brackets_inside_strings() -> None:
    # Brackets in SQL text (e.g. array literals) are not JSON nesting.
    payload = {"originalSql": "SELECT " + "[" * 200 + "1" + "]" * 200}
    assert decode_presto_view(_presto_envelope(payload)) == payload


@pytest.mark.parametrize(
    "trino_type, hive_type",
    [
        ("timestamp(3) with time zone", "timestamp"),
        ("TIMESTAMP(6)", "timestamp"),
        ("time(3) with time zone", "string"),
        ("char(256)", "char(256)"),
        ("integer", "int"),
        ("decimal(10,2)", "decimal(10,2)"),
        ("map(varchar, bigint)", "map<varchar,bigint>"),
        ("map(varchar)", "string"),
        (
            'array(row(a bigint, "b c" timestamp(3) with time zone))',
            "array<struct<a:bigint,b c:timestamp>>",
        ),
        ("real", "float"),
        ("varbinary", "binary"),
        # Anonymous row fields get positional names so the struct is kept.
        ("row(bigint, varchar)", "struct<field0:bigint,field1:varchar>"),
        # ...that never collide with a named sibling.
        ("row(bigint, field0 varchar)", "struct<_field0:bigint,field0:varchar>"),
        # Anonymous fields whose type is several words are not read as `name type`.
        (
            "row(interval day to second, interval year to month)",
            "struct<field0:string,field1:string>",
        ),
        (
            "row(timestamp with time zone, bigint)",
            "struct<field0:timestamp,field1:bigint>",
        ),
        ("row(double precision)", "struct<field0:string>"),
        # Keywords match in any case; quoted field names keep their case, unquoted
        # names are folded to lowercase as Trino does.
        ('row("UserId" bigint, Name VARCHAR)', "struct<UserId:bigint,name:varchar>"),
        ("TIMESTAMP(3) WITH TIME ZONE", "timestamp"),
        ("ROW(INTERVAL DAY TO SECOND, BIGINT)", "struct<field0:string,field1:bigint>"),
        # Commas inside a quoted field name are not separators. Hive cannot parse
        # the resulting name; that column is reported as untyped downstream.
        ('row("a,b" bigint, c varchar)', "struct<a,b:bigint,c:varchar>"),
        # `""` escapes a quote inside a quoted field name.
        ('row("a""b" bigint)', 'struct<a"b:bigint>'),
        ('row("a"",b" bigint, c varchar)', 'struct<a",b:bigint,c:varchar>'),
        # Types Hive has no equivalent for, and malformed input, fall back to string.
        ("interval day to second", "string"),
        ("hyperloglog", "string"),
        ("row(a bigint", "string"),
        # Scalar parameters must be integers.
        ("varchar(x)", "string"),
        ("row(a varchar(x))", "struct<a:string>"),
    ],
)
def test_trino_type_to_hive_type(trino_type: str, hive_type: str) -> None:
    assert trino_type_to_hive_type(trino_type) == hive_type


def test_trino_type_to_hive_type_raises_past_nesting_cap() -> None:
    # Untrusted catalog data must not be able to exhaust the stack, and hitting
    # the cap must be visible to the caller rather than silently become string.
    deeply_nested = "array(" * 500 + "bigint" + ")" * 500
    with pytest.raises(ValueError):
        trino_type_to_hive_type(deeply_nested)


def test_trino_type_to_hive_type_converts_wide_row() -> None:
    wide_row = (
        "row("
        + ", ".join(f"col_{i} timestamp(3) with time zone" for i in range(500))
        + ")"
    )
    hive_type = trino_type_to_hive_type(wide_row)
    assert hive_type.startswith("struct<col_0:timestamp,")
    assert hive_type.endswith(",col_499:timestamp>")


def test_trino_type_to_hive_type_raises_on_oversized_types() -> None:
    with pytest.raises(ValueError):
        trino_type_to_hive_type("bigint" + " " * 2_000_000)


def test_get_presto_view_columns_returns_validated_columns() -> None:
    definition = decode_presto_view(
        _presto_envelope(
            {"columns": [{"name": "id", "type": "bigint", "comment": "ignored"}]}
        )
    )
    assert get_presto_view_columns(definition) == [{"name": "id", "type": "bigint"}]


@pytest.mark.parametrize(
    "columns",
    [
        None,
        [],
        5,
        [{"name": "id"}],
        [{"name": "", "type": "bigint"}],
        [{"name": "id", "type": "bigint"}, "not-a-column"],
    ],
)
def test_get_presto_view_columns_rejects_malformed_columns(columns: object) -> None:
    payload = {} if columns is None else {"columns": columns}
    definition = decode_presto_view(_presto_envelope(payload))
    assert get_presto_view_columns(definition) is None
