import math

import pyarrow as pa
import pytest

from datahub.ingestion.source.data_lake_common.profiling.accumulators import (
    ColumnKind,
    TableAccumulator,
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
    assert stats.distinct_value_frequencies == [("1", 3), ("2", 2), ("3", 1)]


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
        ("1", 1),
        ("2", 1),
        ("3", 1),
        ("4", 1),
        ("5", 1),
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
    assert stats.distinct_value_frequencies == [("blue", 2), ("red", 3)]


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
    assert {v for v, _ in stats.distinct_value_frequencies} == {
        "2023-01-01 00:00:00",
        "2023-01-02 00:00:00",
    }
