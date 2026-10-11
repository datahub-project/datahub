import base64
import gzip

from datahub.api.entities.common.large_string import (
    DEFAULT_THRESHOLD_BYTES,
    make_large_string,
)
from datahub.metadata.schema_classes import CompressionTypeClass


def test_small_text_stored_uncompressed() -> None:
    text = "hello world"
    ls = make_large_string(text)
    assert ls.compression == CompressionTypeClass.NONE
    assert ls.blob == text
    assert ls.uncompressedSize == len(text.encode("utf-8"))


def test_at_threshold_boundary_stored_uncompressed() -> None:
    # len(utf8) == threshold takes the <= branch (NONE).
    threshold = 16
    text = "a" * threshold
    ls = make_large_string(text, threshold=threshold)
    assert ls.compression == CompressionTypeClass.NONE
    assert ls.blob == text
    assert ls.uncompressedSize == threshold


def test_just_over_threshold_is_gzipped_and_roundtrips() -> None:
    threshold = 16
    text = "a" * (threshold + 1)
    ls = make_large_string(text, threshold=threshold)
    assert ls.compression == CompressionTypeClass.GZIP
    # blob is base64(gzip(utf8(text))); decoding it must recover the original.
    recovered = gzip.decompress(base64.b64decode(ls.blob)).decode("utf-8")
    assert recovered == text
    # uncompressedSize is always the original byte length, not the blob length.
    assert ls.uncompressedSize == len(text.encode("utf-8"))
    assert ls.blob != text


def test_uncompressed_size_counts_utf8_bytes_not_chars() -> None:
    # A multibyte character is one str char but multiple UTF-8 bytes.
    text = "é"  # é -> 2 UTF-8 bytes
    ls = make_large_string(text)
    assert ls.uncompressedSize == 2


def test_default_threshold_matches_java_constant() -> None:
    assert DEFAULT_THRESHOLD_BYTES == 262_144
