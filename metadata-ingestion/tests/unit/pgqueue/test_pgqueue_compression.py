"""Unit tests for :mod:`datahub.pgqueue.compression` codec helpers."""

from __future__ import annotations

import pytest

from datahub.pgqueue.compression import (
    PgQueuePayloadCompression,
    decode_stored,
    encode_inner,
    from_wire,
)


class TestFromWire:
    def test_zero_returns_none(self) -> None:
        assert from_wire(0) is PgQueuePayloadCompression.NONE

    def test_one_returns_snappy(self) -> None:
        assert from_wire(1) is PgQueuePayloadCompression.SNAPPY

    def test_unknown_code_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            from_wire(99)


class TestRoundTrip:
    _SAMPLE = b'{"urn":"urn:li:dataset:(urn:li:dataPlatform:pg,db.tbl,PROD)"}'

    def test_none_mode_data_unchanged(self) -> None:
        encoded = encode_inner(self._SAMPLE, PgQueuePayloadCompression.NONE)
        assert encoded == self._SAMPLE
        assert decode_stored(encoded, PgQueuePayloadCompression.NONE) == self._SAMPLE

    def test_snappy_mode_round_trip(self) -> None:
        encoded = encode_inner(self._SAMPLE, PgQueuePayloadCompression.SNAPPY)
        assert decode_stored(encoded, PgQueuePayloadCompression.SNAPPY) == self._SAMPLE

    def test_snappy_produces_different_output(self) -> None:
        encoded = encode_inner(self._SAMPLE, PgQueuePayloadCompression.SNAPPY)
        assert encoded != self._SAMPLE

    def test_empty_bytes_round_trip_none(self) -> None:
        encoded = encode_inner(b"", PgQueuePayloadCompression.NONE)
        assert encoded == b""
        assert decode_stored(encoded, PgQueuePayloadCompression.NONE) == b""

    def test_empty_bytes_round_trip_snappy(self) -> None:
        encoded = encode_inner(b"", PgQueuePayloadCompression.SNAPPY)
        assert decode_stored(encoded, PgQueuePayloadCompression.SNAPPY) == b""
