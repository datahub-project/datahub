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


def test_decode_presto_view_raises_on_invalid_base64() -> None:
    # binascii.Error (bad base64) and json.JSONDecodeError both subclass ValueError.
    with pytest.raises(ValueError):
        decode_presto_view("/* Presto View: not!valid!base64 */")


def test_decode_presto_view_raises_when_payload_not_object() -> None:
    # A JSON array (not an object) is not a valid view payload.
    encoded = base64.b64encode(json.dumps([1, 2, 3]).encode()).decode()
    with pytest.raises(ValueError):
        decode_presto_view(f"/* Presto View: {encoded} */")


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
        (
            'array(row(a bigint, "b c" timestamp(3) with time zone))',
            "array<struct<a:bigint,b c:timestamp>>",
        ),
        ("real", "float"),
        ("varbinary", "binary"),
        # Anonymous row fields get positional names so the struct is kept.
        ("row(bigint, varchar)", "struct<field0:bigint,field1:varchar>"),
        # Anonymous fields whose type is several words are not read as `name type`.
        (
            "row(interval day to second, interval year to month)",
            "struct<field0:string,field1:string>",
        ),
        (
            "row(timestamp with time zone, bigint)",
            "struct<field0:timestamp,field1:bigint>",
        ),
        # Commas inside a quoted field name are not separators. Hive cannot parse
        # the resulting name; that column is reported as untyped downstream.
        ('row("a,b" bigint, c varchar)', "struct<a,b:bigint,c:varchar>"),
        # Types Hive has no equivalent for, and malformed input, fall back to string.
        ("interval day to second", "string"),
        ("hyperloglog", "string"),
        ("row(a bigint", "string"),
    ],
)
def test_trino_type_to_hive_type(trino_type: str, hive_type: str) -> None:
    assert trino_type_to_hive_type(trino_type) == hive_type


def test_trino_type_to_hive_type_caps_nesting_depth() -> None:
    # Untrusted catalog data must not be able to exhaust the stack.
    # Deeper than the nesting cap but within the length cap, so the depth limit
    # is what applies.
    deeply_nested = "array(" * 500 + "bigint" + ")" * 500
    hive_type = trino_type_to_hive_type(deeply_nested)
    assert hive_type.startswith("array<")
    assert "string" in hive_type


def test_trino_type_to_hive_type_rejects_oversized_types() -> None:
    # Long whitespace runs make the type regexes quadratic; oversized input is
    # mapped straight to string instead of being parsed.
    assert trino_type_to_hive_type("timestamp" + " " * 50_000 + "x") == "string"


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
