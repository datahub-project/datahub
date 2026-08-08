"""Performance benchmark for SecretMaskingFilter.mask_text.

Run via the testPerformance gradle task (scans tests/performance with -m 'perf'):

    ../gradlew :metadata-ingestion:testPerformance

Only the typical case (<=20 secrets, <=1 KB text) is gated (must stay under
~1 ms per mask). The other rows document ceilings via print and never gate.
"""

import time

import pytest

from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry

TYPICAL_CASE_MAX_MS = 1.0


def _make_filter(n_secrets: int) -> SecretMaskingFilter:
    reg = SecretRegistry()
    reg.clear()
    for i in range(n_secrets):
        reg.register_secret(f"SECRET_{i}", f"secret_value_{i:04d}_xxxx")
    return SecretMaskingFilter(reg)


def _make_text(size_chars: int, n_secrets: int) -> str:
    body = "x" * max(0, size_chars - 200)
    snippets = []
    for i in range(min(5, n_secrets)):
        snippets.append(f"leak secret_value_{i:04d}_xxxx here")
    return body + " ".join(snippets)


def _measure_ms(f: SecretMaskingFilter, text: str, iterations: int = 50) -> float:
    f.mask_text(text)  # warm up pattern build
    start = time.perf_counter()
    for _ in range(iterations):
        f.mask_text(text)
    end = time.perf_counter()
    return (end - start) / iterations * 1000.0


@pytest.mark.perf
class TestMaskTextPerformance:
    @pytest.mark.parametrize(
        "n_secrets,text_chars",
        [
            (20, 100),  # typical case — gating row
            (20, 5_000),
            (50, 5_000),
            (100, 5_000),
            (200, 5_000),
            (50, 10_000),
            (50, 20_000),
            (100, 10_000),
            (200, 20_000),
        ],
    )
    def test_mask_text_ceiling(self, n_secrets: int, text_chars: int) -> None:
        f = _make_filter(n_secrets)
        text = _make_text(text_chars, n_secrets)
        ms = _measure_ms(f, text)
        print(f"mask_text: secrets={n_secrets} text={text_chars} -> {ms:.3f} ms/mask")
        if n_secrets <= 20 and text_chars <= 1_000:
            assert ms < TYPICAL_CASE_MAX_MS, (
                f"typical case regression: {ms:.3f} ms > {TYPICAL_CASE_MAX_MS} ms"
            )
