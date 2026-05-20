"""Unit tests for the Suite enum and tc→suite mapping."""

from __future__ import annotations

import pytest

from tests.zdu.framework.suite import Suite, suite_for_tc


class TestSuiteEnum:
    def test_lowercase_string_values(self) -> None:
        assert Suite.B.value == "b"

    def test_all_codified_suites_present(self) -> None:
        # All four production-phase suites are codified.
        assert {s.value for s in Suite} == {"b", "d", "n", "c"}


class TestSuiteForTc:
    @pytest.mark.parametrize(
        "tc,expected",
        [
            (101, Suite.B),
            (109, Suite.B),
            (201, Suite.D),
            (206, Suite.D),
            # Suite N — aspect migration (TC-301..323) + sweep invariants (TC-324..329).
            (301, Suite.N),
            (323, Suite.N),
            (324, Suite.N),
            (329, Suite.N),
            (401, Suite.C),
            (403, Suite.C),
        ],
    )
    def test_known_ranges(self, tc: int, expected: Suite) -> None:
        assert suite_for_tc(tc) == expected

    @pytest.mark.parametrize("tc", [0, -1, 100, 110, 200, 207, 300, 332, 400, 404, 999])
    def test_unknown_returns_none(self, tc: int) -> None:
        assert suite_for_tc(tc) is None
