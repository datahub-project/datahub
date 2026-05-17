"""Unit tests for the Suite enum and tc→suite mapping."""

from __future__ import annotations

import pytest

from tests.zdu.framework.suite import Suite, suite_for_tc


class TestSuiteEnum:
    def test_lowercase_string_values(self) -> None:
        assert Suite.A.value == "a"
        assert Suite.F.value == "f"

    def test_all_codified_suites_present(self) -> None:
        # Codified suites map to the four production-phase categories:
        # blocking (B), dual-write (D), non-blocking (A), concurrent (F).
        assert {s.value for s in Suite} == {"a", "b", "d", "f"}


class TestSuiteForTc:
    @pytest.mark.parametrize(
        "tc,expected",
        [
            # Suite A — non-blocking: aspect migration (TC-301..323) +
            # sweep invariants (TC-324..331).
            (301, Suite.A),
            (323, Suite.A),
            (324, Suite.A),
            (331, Suite.A),
            # Suite B — blocking phase.
            (101, Suite.B),
            (109, Suite.B),
            # Suite D — dual-write phase.
            (201, Suite.D),
            (206, Suite.D),
            # Suite F — concurrent operations.
            (401, Suite.F),
            (403, Suite.F),
        ],
    )
    def test_known_ranges(self, tc: int, expected: Suite) -> None:
        assert suite_for_tc(tc) == expected

    @pytest.mark.parametrize(
        "tc",
        [
            0,
            -1,
            100,
            110,
            200,
            207,
            300,
            332,
            400,
            404,
            999,
        ],
    )
    def test_unknown_returns_none(self, tc: int) -> None:
        assert suite_for_tc(tc) is None
