import math
import statistics

import pytest
from hypothesis import given, strategies as st

from datahub.ingestion.source.kafka.kafka_profiler_utils import (
    NumericStats,
    calculate_numeric_stats,
    filter_numeric_values,
)


@given(
    st.lists(
        st.floats(
            min_value=-1e100, max_value=1e100, allow_nan=False, allow_infinity=False
        ),
        min_size=1,
    )
)
def test_mean_between_min_max(values):
    stats = calculate_numeric_stats(values)
    if stats.mean is not None and stats.min is not None and stats.max is not None:
        # Use relative tolerance based on magnitude of values
        # For identical values (min==max), use relative tolerance of the value itself
        magnitude = max(abs(stats.min), abs(stats.max), 1.0)
        rel_tol = magnitude * 1e-14  # Relative tolerance for floating-point precision
        assert stats.min - rel_tol <= stats.mean <= stats.max + rel_tol, (
            f"Mean {stats.mean} not between min {stats.min} and max {stats.max}"
        )


@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=2))
def test_median_between_min_max(values):
    stats = calculate_numeric_stats(values)
    if stats.median is not None and stats.min is not None and stats.max is not None:
        assert stats.min <= stats.median <= stats.max, (
            f"Median {stats.median} not between min {stats.min} and max {stats.max}"
        )


@given(st.lists(st.floats()))
def test_never_crashes_on_any_numeric_input(values):
    stats = calculate_numeric_stats(values)
    assert isinstance(stats, NumericStats)


@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1))
def test_stdev_is_non_negative(values):
    stats = calculate_numeric_stats(values)
    if stats.stdev is not None:
        assert stats.stdev >= 0, f"Standard deviation {stats.stdev} is negative"


@given(st.lists(st.just(42.0), min_size=2, max_size=100))
def test_identical_values_have_zero_stdev(values):
    stats = calculate_numeric_stats(values)
    if stats.stdev is not None:
        assert abs(stats.stdev) < 1e-10, (
            f"Standard deviation {stats.stdev} should be ~0 for identical values"
        )


@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=1))
def test_single_value_stats(values):
    if len(values) == 1:
        stats = calculate_numeric_stats(values)
        if stats.min is not None:
            assert stats.min == stats.max == stats.mean == stats.median


@given(
    st.lists(
        st.one_of(
            st.floats(
                min_value=-1e100, max_value=1e100, allow_nan=False, allow_infinity=False
            ),
            st.just(float("nan")),
            st.just(float("inf")),
            st.just(float("-inf")),
        ),
        min_size=1,
    )
)
def test_filter_numeric_values_removes_special_values(values):
    filtered = filter_numeric_values(values, exclude_special=True)
    for v in filtered:
        assert not math.isnan(v), "NaN should be filtered out"
        assert not math.isinf(v), "Inf should be filtered out"


@given(
    st.lists(
        st.floats(
            min_value=-1e100, max_value=1e100, allow_nan=False, allow_infinity=False
        ),
        min_size=2,
        max_size=1000,
    )
)
def test_mean_is_average_of_min_max_for_two_values(values):
    if len(values) == 2:
        stats = calculate_numeric_stats(values)
        if stats.mean is not None and stats.min is not None and stats.max is not None:
            expected_mean = (stats.min + stats.max) / 2
            assert abs(stats.mean - expected_mean) < 1e-10, (
                f"Mean {stats.mean} should equal (min + max) / 2 = {expected_mean}"
            )


@given(st.lists(st.floats(allow_nan=False, allow_infinity=False), min_size=0))
def test_empty_or_all_invalid_returns_none(values):
    if not values:
        stats = calculate_numeric_stats(values)
        assert stats.min is None
        assert stats.max is None
        assert stats.mean is None
        assert stats.median is None
        assert stats.stdev is None


@given(st.lists(st.floats(min_value=-1e10, max_value=1e10), min_size=3, max_size=100))
def test_median_matches_statistics_median(values):
    filtered = [v for v in values if not math.isnan(v) and not math.isinf(v)]
    if len(filtered) >= 3:
        stats = calculate_numeric_stats(filtered)
        expected_median = statistics.median(filtered)
        if stats.median is not None:
            assert stats.median == expected_median, (
                f"Median {stats.median} doesn't match expected {expected_median}"
            )


def test_median_averages_two_middle_values_for_even_length():
    # Regression: sorted[len // 2] would return 3 here; correct median is 2.5.
    stats = calculate_numeric_stats([1.0, 2.0, 3.0, 4.0])
    assert stats.median == 2.5


def test_near_float_max_median_does_not_overflow():
    # (a + b) / 2 would overflow to inf here; the overflow-safe path must not.
    big = 1e308
    stats = calculate_numeric_stats([big, big])
    assert stats.median == big
    assert not math.isinf(stats.median)


def test_near_float_max_mean_stdev_suppressed():
    # Beyond the aggregation threshold, mean/stdev are left unset rather than
    # returning an overflowed inf, but min/max/median stay populated.
    big = 1e308
    stats = calculate_numeric_stats([big, big, big])
    assert stats.min == big
    assert stats.max == big
    assert stats.median == big
    assert stats.mean is None
    assert stats.stdev is None


def test_stdev_is_sample_not_population():
    # Pins the exact sample stdev: for [2, 4] the sample stdev is sqrt(2) ≈ 1.414,
    # whereas the population stdev would be 1.0. Guards against a pstdev regression.
    stats = calculate_numeric_stats([2.0, 4.0])
    assert stats.stdev == pytest.approx(math.sqrt(2.0))
