"""Helpers for producing :class:`LargeStringClass` aspect values.

A ``LargeString`` stores large text (API specs, schemas, docs) in a form that may
be compressed so it fits under DataHub's aspect-size limit. The logical value is
always the decompressed UTF-8 text; the stored ``blob`` is either that raw text
(``compression = NONE``) or ``base64(gzip(utf8(text)))`` (``compression = GZIP``).

This mirrors the Java ``com.linkedin.metadata.utils.LargeStrings`` helper exactly:
the same threshold and compression decision, so the same input text produces the
same stored form on both producers.
"""

import base64
import gzip

from datahub.metadata.schema_classes import CompressionTypeClass, LargeStringClass

# Text at or below this many UTF-8 bytes is stored uncompressed so small contracts
# stay human-readable/greppable. Must match Java LargeStrings.DEFAULT_THRESHOLD_BYTES.
DEFAULT_THRESHOLD_BYTES = 262_144  # 256 KiB


def make_large_string(
    text: str, threshold: int = DEFAULT_THRESHOLD_BYTES
) -> LargeStringClass:
    """Encode ``text`` into a :class:`LargeStringClass`.

    Compresses (GZIP) only when the UTF-8 byte length exceeds ``threshold``;
    otherwise stores the raw text (NONE). ``uncompressedSize`` is always the
    original UTF-8 byte length.
    """
    utf8 = text.encode("utf-8")
    if len(utf8) <= threshold:
        return LargeStringClass(
            blob=text,
            compression=CompressionTypeClass.NONE,
            uncompressedSize=len(utf8),
        )
    return LargeStringClass(
        blob=base64.b64encode(gzip.compress(utf8)).decode("ascii"),
        compression=CompressionTypeClass.GZIP,
        uncompressedSize=len(utf8),
    )
