import math
import statistics
from decimal import Decimal

import pyarrow as pa
import pytest

from datahub.ingestion.source.data_lake_common.profiling.accumulators import (
    MAX_TRACKED_FREQUENCIES,
    ColumnKind,
    TableAccumulator,
    ValueFrequency,
    classify_arrow_type,
    is_numeric_arrow_type,
    is_string_arrow_type,
    is_temporal_arrow_type,
)


@pytest.mark.parametrize(
    "arrow_type,expected_kind",
    [
        (pa.int64(), ColumnKind.NUMERIC),
        (pa.float32(), ColumnKind.NUMERIC),
        (pa.decimal128(10, 2), ColumnKind.NUMERIC),
        (pa.string(), ColumnKind.STRING),
        (pa.large_string(), ColumnKind.STRING),
        (pa.date32(), ColumnKind.TEMPORAL),
        (pa.timestamp("us"), ColumnKind.TEMPORAL),
        (pa.bool_(), ColumnKind.OTHER),
    ],
)
def test_classify_arrow_type(
    arrow_type: pa.DataType, expected_kind: ColumnKind
) -> None:
    assert classify_arrow_type(arrow_type) == expected_kind
    assert is_numeric_arrow_type(arrow_type) is (expected_kind == ColumnKind.NUMERIC)
    assert is_temporal_arrow_type(arrow_type) is (expected_kind == ColumnKind.TEMPORAL)
    assert is_string_arrow_type(arrow_type) is (expected_kind == ColumnKind.STRING)


def test_nested_struct_column_does_not_crash_and_gets_null_count_only() -> None:
    # list<struct<...>> columns (e.g. from nested JSON) can't be compared
    # with `<`/`>`; classify_arrow_type maps them to OTHER, which must skip
    # min/max tracking entirely rather than raising.
    nested_type = pa.list_(pa.struct([("name", pa.string())]))
    values = pa.array([[{"name": "a"}], [{"name": "b"}], None], type=nested_type)
    acc = TableAccumulator(columns=["tags"], column_kinds={"tags": ColumnKind.OTHER})
    acc.add_batch(pa.record_batch({"tags": values}))
    stats = acc.finalize().columns[0]

    assert stats.non_null_count == 2
    assert stats.null_count == 1
    assert stats.min_value is None
    assert stats.max_value is None


def test_numeric_high_cardinality_column_gets_full_stats() -> None:
    # 200 rows, 100 distinct values (each appears twice): pct_unique=0.5, so
    # this lands in the MANY/VERY_MANY cardinality bucket.
    values = list(range(1, 101)) * 2
    acc = TableAccumulator(
        columns=["amount"], column_kinds={"amount": ColumnKind.NUMERIC}
    )
    acc.add_batch(pa.record_batch({"amount": pa.array(values, type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert stats.min_value == 1
    assert stats.max_value == 100
    assert stats.mean == pytest.approx(50.5, rel=1e-2)
    assert stats.stdev is not None
    assert stats.median is not None
    assert stats.quantiles is not None
    assert len(stats.quantiles) == 5
    assert stats.histogram is not None
    assert stats.distinct_value_frequencies is None


def test_numeric_low_cardinality_column_gets_frequencies_not_stats() -> None:
    # 3 distinct values in a 6-row column: VERY_FEW cardinality.
    acc = TableAccumulator(
        columns=["rating"], column_kinds={"rating": ColumnKind.NUMERIC}
    )
    acc.add_batch(
        pa.record_batch({"rating": pa.array([1, 1, 1, 2, 2, 3], type=pa.int64())})
    )
    stats = acc.finalize().columns[0]

    assert stats.min_value is None
    assert stats.max_value is None
    assert stats.mean is None
    assert stats.stdev is None
    assert stats.distinct_value_frequencies == [
        ValueFrequency("1", 3),
        ValueFrequency("2", 2),
        ValueFrequency("3", 1),
    ]


def test_numeric_small_fully_distinct_column_gets_frequencies_not_unique() -> None:
    # Every value distinct and no nulls: matches the old Deequ profiler's
    # Cardinality.UNIQUE check, which keys off null_fraction (not the unique
    # proportion) and so never fires for a column with zero nulls. With 5
    # distinct values this lands in VERY_FEW, not "nothing".
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert stats.unique_count == 5
    assert stats.min_value is None
    assert stats.max_value is None
    assert stats.mean is None
    assert stats.stdev is None
    assert stats.distinct_value_frequencies == [
        ValueFrequency("1", 1),
        ValueFrequency("2", 1),
        ValueFrequency("3", 1),
        ValueFrequency("4", 1),
        ValueFrequency("5", 1),
    ]


def test_all_null_column_yields_cardinality_none() -> None:
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"id": pa.array([None, None], type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert stats.unique_count is None
    assert stats.distinct_value_frequencies is None


def test_string_low_cardinality_gets_frequencies() -> None:
    acc = TableAccumulator(columns=["color"], column_kinds={"color": ColumnKind.STRING})
    acc.add_batch(
        pa.record_batch({"color": pa.array(["red", "blue", "red", "red", "blue"])})
    )
    stats = acc.finalize().columns[0]

    assert stats.min_value is None  # strings never get min/max
    assert stats.distinct_value_frequencies == [
        ValueFrequency("blue", 2),
        ValueFrequency("red", 3),
    ]


def test_string_high_cardinality_gets_nothing_extra() -> None:
    values = [f"user_{i}" for i in range(200)]
    acc = TableAccumulator(columns=["name"], column_kinds={"name": ColumnKind.STRING})
    acc.add_batch(pa.record_batch({"name": pa.array(values)}))
    stats = acc.finalize().columns[0]

    assert stats.min_value is None
    assert stats.distinct_value_frequencies is None


def test_temporal_column_always_gets_min_max() -> None:
    dates = pa.array(range(200), type=pa.date32())
    acc = TableAccumulator(
        columns=["event_date"], column_kinds={"event_date": ColumnKind.TEMPORAL}
    )
    acc.add_batch(pa.record_batch({"event_date": dates}))
    stats = acc.finalize().columns[0]

    assert stats.min_value is not None
    assert stats.max_value is not None
    assert stats.distinct_value_frequencies is None  # high cardinality here


def test_row_at_a_time_aggregation_matches_batch() -> None:
    values = list(range(1, 101)) * 2  # high cardinality, exercises numeric stats path

    batch_acc = TableAccumulator(
        columns=["id"], column_kinds={"id": ColumnKind.NUMERIC}
    )
    batch_acc.add_batch(pa.record_batch({"id": pa.array(values, type=pa.int64())}))

    row_acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    for value in values:
        row_acc.add_row({"id": value})

    batch_stats = batch_acc.finalize().columns[0]
    row_stats = row_acc.finalize().columns[0]

    assert row_stats.non_null_count == batch_stats.non_null_count
    assert row_stats.min_value == batch_stats.min_value
    assert row_stats.max_value == batch_stats.max_value
    assert row_stats.mean is not None
    assert batch_stats.mean is not None
    assert math.isclose(row_stats.mean, batch_stats.mean, rel_tol=1e-9)
    assert row_stats.stdev is not None
    assert batch_stats.stdev is not None
    assert math.isclose(row_stats.stdev, batch_stats.stdev, rel_tol=1e-9)


def test_all_null_column_reports_zero_non_null_and_no_numeric_stats() -> None:
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(
        pa.record_batch({"id": pa.array([None, None, None], type=pa.int64())})
    )
    stats = acc.finalize().columns[0]

    assert stats.non_null_count == 0
    assert stats.null_count == 3
    assert stats.min_value is None
    assert stats.max_value is None
    assert stats.mean is None
    assert stats.stdev is None
    assert stats.median is None
    assert stats.unique_count is None


def test_sample_values_are_bounded_by_sample_size() -> None:
    acc = TableAccumulator(
        columns=["id"], column_kinds={"id": ColumnKind.NUMERIC}, sample_size=3
    )
    acc.add_batch(pa.record_batch({"id": pa.array(range(100), type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert len(stats.sample_values) == 3
    assert all(0 <= int(v) < 100 for v in stats.sample_values)


def test_no_sample_values_when_sample_size_not_set() -> None:
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"id": pa.array([1, 2, 3], type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert stats.sample_values == []


def test_unknown_column_in_batch_is_ignored() -> None:
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(
        pa.record_batch(
            {"id": pa.array([1], type=pa.int64()), "extra": pa.array(["x"])}
        )
    )
    stats = acc.finalize()

    assert stats.column_count == 1
    assert [c.column for c in stats.columns] == ["id"]


def test_add_row_with_none_value_counts_as_null() -> None:
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_row({"id": 5})
    acc.add_row({"id": None})
    stats = acc.finalize().columns[0]

    assert stats.non_null_count == 1
    assert stats.null_count == 1


def test_temporal_low_cardinality_gets_distinct_value_frequencies() -> None:
    from datetime import datetime

    acc = TableAccumulator(columns=["d"], column_kinds={"d": ColumnKind.TEMPORAL})
    for _ in range(10):
        acc.add_row({"d": datetime(2023, 1, 1)})
        acc.add_row({"d": datetime(2023, 1, 2)})
    stats = acc.finalize().columns[0]

    assert stats.min_value == datetime(2023, 1, 1)
    assert stats.max_value == datetime(2023, 1, 2)
    assert stats.distinct_value_frequencies is not None
    assert {vf.value for vf in stats.distinct_value_frequencies} == {
        "2023-01-01 00:00:00",
        "2023-01-02 00:00:00",
    }


def test_multi_batch_variance_merge_with_differing_means() -> None:
    # Two batches whose means are far apart, fed separately. The parallel
    # variance merge must include the between-batch mean-difference correction;
    # without it (naive pooled variance) stdev is badly underestimated. High
    # cardinality so stdev is actually surfaced.
    batch_a = list(range(0, 100))  # mean 49.5
    batch_b = list(range(1000, 1100))  # mean 1049.5
    combined = batch_a + batch_b

    acc = TableAccumulator(columns=["x"], column_kinds={"x": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"x": pa.array(batch_a, type=pa.float64())}))
    acc.add_batch(pa.record_batch({"x": pa.array(batch_b, type=pa.float64())}))
    stats = acc.finalize().columns[0]

    assert stats.mean == pytest.approx(statistics.fmean(combined))
    assert stats.stdev is not None
    # pstdev (population, ddof=0) matches the accumulator's math.sqrt(m2/n).
    assert stats.stdev == pytest.approx(statistics.pstdev(combined), rel=1e-9)
    # Guard against the naive merge, which would land near the ~28.9 within-batch
    # stdev instead of the true ~500.
    assert stats.stdev > 400


def test_decimal_high_cardinality_gets_histogram_and_stats() -> None:
    values = [Decimal(f"{i}.5") for i in range(100)]  # high cardinality -> MANY
    acc = TableAccumulator(columns=["amt"], column_kinds={"amt": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"amt": pa.array(values, type=pa.decimal128(10, 2))}))
    stats = acc.finalize().columns[0]

    assert stats.histogram is not None
    assert len(stats.histogram.boundaries) == len(stats.histogram.counts)
    assert stats.mean == pytest.approx(statistics.fmean(float(v) for v in values))
    assert stats.quantiles is not None


def test_unique_count_estimate_is_within_tolerance_at_scale() -> None:
    n = 5000
    acc = TableAccumulator(columns=["id"], column_kinds={"id": ColumnKind.NUMERIC})
    acc.add_batch(pa.record_batch({"id": pa.array(range(n), type=pa.int64())}))
    stats = acc.finalize().columns[0]

    assert stats.unique_count is not None
    # CPC sketch targets ~2% error; allow generous headroom for a single run.
    assert abs(stats.unique_count - n) / n < 0.05


def test_reservoir_sampling_is_deterministic_across_runs() -> None:
    values = list(range(200))  # far more than the sample size -> reservoir kicks in

    def sample(size: int) -> list:
        acc = TableAccumulator(
            columns=["v"], column_kinds={"v": ColumnKind.NUMERIC}, sample_size=size
        )
        acc.add_batch(pa.record_batch({"v": pa.array(values, type=pa.int64())}))
        return acc.finalize().columns[0].sample_values

    # Seeded RNG -> identical sample every run (matches the old takeSample seed=0).
    assert sample(20) == sample(20)
    assert len(sample(20)) == 20


def test_frequency_table_abandoned_past_boundary() -> None:
    acc = TableAccumulator(columns=["s"], column_kinds={"s": ColumnKind.STRING})
    # Exactly MAX_TRACKED_FREQUENCIES distinct values: still tracked.
    at_limit = [str(i) for i in range(MAX_TRACKED_FREQUENCIES)]
    acc.add_batch(pa.record_batch({"s": pa.array(at_limit)}))
    assert acc._accumulators["s"]._frequencies is not None
    assert len(acc._accumulators["s"]._frequencies) == MAX_TRACKED_FREQUENCIES

    # One more distinct value tips it over the boundary and the table is dropped
    # so it can't grow unbounded.
    acc.add_batch(pa.record_batch({"s": pa.array([str(MAX_TRACKED_FREQUENCIES)])}))
    assert acc._accumulators["s"]._frequencies is None
