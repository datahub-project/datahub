"""Kafka-style pgQueue headers JSON codec."""

from __future__ import annotations

from datahub.pgqueue.headers import headers_from_db, headers_to_json


def test_headers_round_trip_json() -> None:
    original = [("a", b"x"), ("b", b"")]
    js = headers_to_json(original)
    assert js is not None
    restored = headers_from_db(js)
    assert restored == original


def test_headers_empty_serializes_to_null_column() -> None:
    assert headers_to_json(()) is None
    assert headers_from_db(None) == []
