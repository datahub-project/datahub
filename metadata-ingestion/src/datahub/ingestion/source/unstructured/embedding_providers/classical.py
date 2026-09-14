"""Classical (deterministic, non-neural) embedding provider.

A stateless hashed-feature embedding: no API key, no endpoint, no model
download. Text is ASCII-case-folded and split into words; each word yields a
word feature plus boundary-marked character bigrams and trigrams; every
distinct feature is SHA-256 hashed into a signed bucket of an integer vector,
weighted by ceil(sqrt(term frequency)). The result captures lexical overlap,
not meaning: it is a fallback for deployments that cannot run a neural model.

The GMS query side implements the identical algorithm in Java
(``ClassicalEmbeddingProvider``). Documents are embedded here and queries
there, so the two MUST produce bit-identical vectors for the same text or kNN
recall silently collapses. Every step is therefore defined over Unicode code
points, integer arithmetic and SHA-256 only, with no dependence on Unicode
tables, locale, libm or numpy. Do not "simplify" the explicit loops into
``str.split()`` / ``str.strip()`` / ``\\s`` regexes: those recognize different
separator sets than the Java implementation.

Vectors are NOT L2-normalized (the index's cosine space type handles scale);
components are exact small integers, emitted as float32-representable floats.
"""

import hashlib
import math
import re
import struct
from collections import Counter

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
)

# The model name carries the algorithm version and the vector width, e.g.
# ``hash-v1-2048``. A future v2 must use a new name so stored v1 vectors are
# never queried with v2 vectors.
_MODEL_NAME_PATTERN = re.compile(r"hash-v1-([1-9][0-9]*)")
MAX_DIMENSIONS = 4096

# Longer inputs are rejected, never truncated: truncating on one side only would
# break query/document parity. The cap also bounds every accumulator value well
# below 2**24, so the float32 conversion of each component is exact.
MAX_CODE_POINTS = 16384

# Word separators: ASCII space, TAB, LF, VT, FF, CR. Deliberately not Unicode
# whitespace (NBSP etc. are word characters).
_SEPARATORS = frozenset({0x20, 0x09, 0x0A, 0x0B, 0x0C, 0x0D})

# Out-of-band feature tags; text containing "^" or "$" cannot alias a boundary.
_TAG_FALLBACK = 0x00
_TAG_WORD = 0x01
_TAG_BIGRAM = 0x02
_TAG_TRIGRAM = 0x03
_BOUNDARY_START = 0x01
_BOUNDARY_END = 0x02


def _parse_dimensions(model: str) -> int:
    match = _MODEL_NAME_PATTERN.fullmatch(model)
    if match is None:
        raise ValueError(
            f"Invalid classical embedding model '{model}': expected "
            f"'hash-v1-<dimensions>' (e.g. 'hash-v1-2048')."
        )
    dims = int(match.group(1))
    if dims > MAX_DIMENSIONS:
        raise ValueError(
            f"Invalid classical embedding model '{model}': dimensions must be "
            f"between 1 and {MAX_DIMENSIONS}."
        )
    return dims


def _words(text: str) -> list[list[int]]:
    """Split ``text`` into words as code-point lists (normalization steps)."""
    # len() of a str is its code-point count.
    if len(text) > MAX_CODE_POINTS:
        raise ValueError(
            f"Text has {len(text)} code points; the classical embedding provider "
            f"accepts at most {MAX_CODE_POINTS}."
        )
    words: list[list[int]] = []
    current: list[int] = []
    for char in text:
        cp = ord(char)
        if cp in _SEPARATORS:
            if current:
                words.append(current)
                current = []
            continue
        if 0xD800 <= cp <= 0xDFFF:
            cp = 0xFFFD  # lone surrogate; the Java side maps these identically
        elif 0x41 <= cp <= 0x5A:
            cp += 0x20  # ASCII A-Z -> a-z only; no Unicode case folding
        current.append(cp)
    if current:
        words.append(current)
    return words


def _utf8(code_points: list[int]) -> bytes:
    return "".join(map(chr, code_points)).encode("utf-8")


def _features(words: list[list[int]]) -> Counter[bytes]:
    counts: Counter[bytes] = Counter()
    for word in words:
        n = len(word)
        counts[bytes((_TAG_WORD, _BOUNDARY_START | _BOUNDARY_END)) + _utf8(word)] += 1
        for size, tag in ((2, _TAG_BIGRAM), (3, _TAG_TRIGRAM)):
            for i in range(n - size + 1):
                boundary = (_BOUNDARY_START if i == 0 else 0) | (
                    _BOUNDARY_END if i + size == n else 0
                )
                counts[bytes((tag, boundary)) + _utf8(word[i : i + size])] += 1
    return counts


def _bucket(digest: bytes, dims: int) -> int:
    return int.from_bytes(digest[:4], "big") % dims


def _ceil_sqrt(n: int) -> int:
    """Smallest integer k with k*k >= n, in integer arithmetic (no float sqrt)."""
    root = math.isqrt(n)
    return root if root * root >= n else root + 1


def _accumulate(words: list[list[int]], dims: int) -> list[int]:
    acc = [0] * dims
    if not words:
        acc[0] = 1  # empty-text sentinel: cosine rejects zero-magnitude vectors
        return acc
    for feature, tf in _features(words).items():
        digest = hashlib.sha256(feature).digest()
        # Digest layout (shared with Java): bytes 0-3 big-endian bucket, byte 4 bit 0 sign.
        sign = 1 if (digest[4] & 1) == 0 else -1
        acc[_bucket(digest, dims)] += sign * _ceil_sqrt(tf)
    if not any(acc):
        # Full cancellation: pick a deterministic bucket from the whole text so
        # different texts do not all collapse onto the empty sentinel.
        fallback = bytes((_TAG_FALLBACK,)) + b" ".join(_utf8(w) for w in words)
        acc[_bucket(hashlib.sha256(fallback).digest(), dims)] = 1
    return acc


def _float32(value: int) -> float:
    return struct.unpack(">f", struct.pack(">f", value))[0]


class ClassicalEmbeddingProvider(EmbeddingProvider):
    """Deterministic hashed-feature embeddings; parity twin of the GMS Java provider.

    Known parity caveat: a string holding an adjacent high+low surrogate pair as two
    code units (only reachable via surrogatepass/surrogateescape decoding in Python)
    hashes as two U+FFFD here but as one supplementary code point in Java; JSON
    ingress on both sides cannot produce it.
    """

    def __init__(self, model: str):
        self.dimensions = _parse_dimensions(model)
        self.model_id = f"classical/{model}"

    def embed(self, texts: list[str]) -> EmbeddingResult:
        return EmbeddingResult(embeddings=[self._embed_one(text) for text in texts])

    def _embed_one(self, text: str) -> list[float]:
        return [_float32(v) for v in _accumulate(_words(text), self.dimensions)]
