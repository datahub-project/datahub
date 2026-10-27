import base64
import json

import pytest

from datahub.ingestion.source.common.presto_view_decoder import (
    decode_presto_view,
    is_presto_view,
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
