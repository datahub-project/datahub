"""Unit tests for the Suite enum and tc→suite mapping."""

from __future__ import annotations

import pytest

from tests.zdu.framework.suite import Suite, suite_for_tc


class TestSuiteEnum:
    def test_lowercase_string_values(self) -> None:
        assert Suite.A.value == "a"
        assert Suite.H.value == "h"

    def test_all_eight_suites_present(self) -> None:
        assert {s.value for s in Suite} == {"a", "b", "c", "d", "e", "f", "g", "h"}


class TestSuiteForTc:
    @pytest.mark.parametrize(
        "tc,expected",
        [
            (1, Suite.A),
            (23, Suite.A),
            (101, Suite.B),
            (112, Suite.B),
            (201, Suite.C),
            (208, Suite.C),
            (301, Suite.D),
            (309, Suite.D),
            (401, Suite.E),
            (408, Suite.E),
            (501, Suite.F),
            (507, Suite.F),
            (601, Suite.G),
            (604, Suite.G),
            (701, Suite.H),
            (705, Suite.H),
        ],
    )
    def test_known_ranges(self, tc: int, expected: Suite) -> None:
        assert suite_for_tc(tc) == expected

    @pytest.mark.parametrize(
        "tc",
        [
            0,
            -1,
            24,
            100,
            113,
            200,
            209,
            300,
            310,
            400,
            409,
            500,
            508,
            600,
            605,
            700,
            999,
        ],
    )
    def test_unknown_returns_none(self, tc: int) -> None:
        assert suite_for_tc(tc) is None
