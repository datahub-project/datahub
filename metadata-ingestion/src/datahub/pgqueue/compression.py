"""Application-layer pgQueue payload codecs (maps to ``message.payload_compression``)."""

from __future__ import annotations

from enum import IntEnum


class PgQueuePayloadCompression(IntEnum):
    NONE = 0
    SNAPPY = 1


def from_wire(code: int) -> PgQueuePayloadCompression:
    if code == 0:
        return PgQueuePayloadCompression.NONE
    if code == 1:
        return PgQueuePayloadCompression.SNAPPY
    raise ValueError(
        f"Unsupported pgQueue payload_compression code: {code} (reserved or not implemented)"
    )


def encode_inner(inner: bytes, mode: PgQueuePayloadCompression) -> bytes:
    if mode == PgQueuePayloadCompression.NONE:
        return inner
    if mode == PgQueuePayloadCompression.SNAPPY:
        from cramjam import snappy

        # Raw block format — matches org.xerial.snappy.Snappy on the Java producer.
        return bytes(snappy.compress_raw(inner))
    raise ValueError(f"Unhandled pgQueue payload compression mode: {mode}")


def decode_stored(stored: bytes, mode: PgQueuePayloadCompression) -> bytes:
    if mode == PgQueuePayloadCompression.NONE:
        return stored
    if mode == PgQueuePayloadCompression.SNAPPY:
        from cramjam import snappy

        return bytes(snappy.decompress_raw(stored))
    raise ValueError(f"Unhandled pgQueue payload compression mode: {mode}")
