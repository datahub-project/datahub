import struct
from unittest.mock import patch

from datahub.ingestion.source.sql.hive.sasl_client import PureSASLClient


def _client() -> PureSASLClient:
    return PureSASLClient("hms.example.com", service="hive", mechanism="PLAIN")


def test_encode_wraps_and_prepends_length_header() -> None:
    # thrift_sasl sends whatever encode() returns as-is, so with auth-int/auth-conf
    # the payload must be wrapped (not unwrapped) and carry its own length frame.
    client = _client()
    with patch.object(PureSASLClient, "wrap", return_value=b"wrapped") as wrap:
        ok, encoded = client.encode(b"payload")

    wrap.assert_called_once_with(b"payload")
    assert ok
    assert encoded == struct.pack(">I", 7) + b"wrapped"


def test_decode_strips_length_header_and_unwraps() -> None:
    client = _client()
    frame = struct.pack(">I", 7) + b"wrapped"
    with patch.object(PureSASLClient, "unwrap", return_value=b"payload") as unwrap:
        ok, decoded = client.decode(frame)

    unwrap.assert_called_once_with(b"wrapped")
    assert ok
    assert decoded == b"payload"


def test_errors_are_reported_through_get_error() -> None:
    client = _client()
    with patch.object(PureSASLClient, "wrap", side_effect=RuntimeError("no QOP")):
        ok, encoded = client.encode(b"payload")

    assert not ok
    assert encoded is None
    assert client.getError() == "no QOP"
