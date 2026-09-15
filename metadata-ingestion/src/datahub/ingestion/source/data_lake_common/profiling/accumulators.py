import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
from datasketches import cpc_sketch, kll_floats_sketch

from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)

# lg_k=11 and k=200 are the DataSketches-recommended defaults, trading off
# accuracy (~2% error) against sketch size; both sketches stay O(KB) regardless
# of how many rows are streamed through them.
CPC_SKETCH_LG_K = 11
KLL_SKETCH_K = 200

QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]
HISTOGRAM_BINS = 25

# Matches Cardinality.FEW's upper bound: past this many distinct values, a
# column can no longer be "few-valued" so the exact frequency table is
# abandoned rather than growing unbounded.
MAX_TRACKED_FREQUENCIES = 100

LOW_CARDINALITY = (
    Cardinality.ONE,
    Cardinality.TWO,
    Cardinality.VERY_FEW,
    Cardinality.FEW,
)
HIGH_CARDINALITY = (Cardinality.MANY, Cardinality.VERY_MANY)

ProfileValue = Union[int, float, str, bool, Decimal, date, datetime]


class ColumnKind(Enum):
    NUMERIC = "numeric"
    STRING = "string"
    TEMPORAL = "temporal"
    OTHER = "other"


def is_numeric_arrow_type(arrow_type: pa.DataType) -> bool:
    return (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_decimal(arrow_type)
    )


def is_temporal_arrow_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_date(arrow_type) or pa.types.is_timestamp(arrow_type)


def is_string_arrow_type(arrow_type: pa.DataType) -> bool:
    return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)


def classify_arrow_type(arrow_type: pa.DataType) -> ColumnKind:
    if is_numeric_arrow_type(arrow_type):
        return ColumnKind.NUMERIC
    if is_temporal_arrow_type(arrow_type):
        return ColumnKind.TEMPORAL
    if is_string_arrow_type(arrow_type):
        return ColumnKind.STRING
    return ColumnKind.OTHER


@dataclass(frozen=True)
class Quantile:
    quantile: float
    value: float


@dataclass(frozen=True)
class ValueFrequency:
    value: str
    frequency: int


@dataclass(frozen=True)
class Histogram:
    boundaries: List[str]
    counts: List[float]

    def __post_init__(self) -> None:
        if len(self.boundaries) != len(self.counts):
            raise ValueError(
                f"Histogram boundaries ({len(self.boundaries)}) and counts "
                f"({len(self.counts)}) must be the same length"
            )


@dataclass(frozen=True)
class ColumnStats:
    column: str
    non_null_count: int
    null_count: int
    unique_count: Optional[int]
    min_value: Optional[ProfileValue] = None
    max_value: Optional[ProfileValue] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    stdev: Optional[float] = None
    quantiles: Optional[List[Quantile]] = None
    histogram: Optional[Histogram] = None
    distinct_value_frequencies: Optional[List[ValueFrequency]] = None
    sample_values: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class TableStats:
    row_count: int
    column_count: int
    columns: List[ColumnStats]


class ColumnAccumulator:
    """Single-pass, streaming aggregation for one column.

    count/null_count/min/max/mean/stdev and the exact distinct-value
    frequency table are computed exactly. unique_count, median, quantiles,
    and the continuous histogram are approximate (Apache DataSketches CPC and
    KLL sketches), matching the precision the previous Deequ-based profiler
    provided via ApproxCountDistinct/ApproxQuantile(s)/Histogram.

    Which fields actually get surfaced (in `finalize()`) depends on the
    column's `Cardinality`, mirroring the old Spark/Deequ profiler: min/max
    and numeric stats for high-cardinality numeric/temporal columns, exact
    frequency tables for low-cardinality columns of any type.
    """

    def __init__(
        self, column: str, kind: ColumnKind, sample_size: Optional[int] = None
    ):
        self.column = column
        self.kind = kind
        self.non_null_count = 0
        self.null_count = 0
        # Untyped: values are homogeneous within a single column at runtime,
        # but ProfileValue is a Union, so mypy can't verify cross-type `</`>`.
        self._min: Any = None
        self._max: Any = None
        self._numeric_count = 0
        self._mean = 0.0
        self._m2 = 0.0
        self._distinct = cpc_sketch(CPC_SKETCH_LG_K)
        self._median = (
            kll_floats_sketch(KLL_SKETCH_K) if kind == ColumnKind.NUMERIC else None
        )
        self._sample_size = sample_size
        self._sample: List[ProfileValue] = []
        self._sample_seen = 0
        # Seeded to match the previous profiler's `takeSample(..., seed=0)` determinism.
        self._sample_rng = random.Random(0)
        self._frequencies: Optional[Dict[str, int]] = {}

    def _add_to_sample(self, value: ProfileValue) -> None:
        if not self._sample_size:
            return
        self._sample_seen += 1
        if len(self._sample) < self._sample_size:
            self._sample.append(value)
        else:
            # Algorithm R: keeps a uniform random sample without buffering the column.
            j = self._sample_rng.randint(0, self._sample_seen - 1)
            if j < self._sample_size:
                self._sample[j] = value

    def _track_frequency(self, value: ProfileValue) -> None:
        if self._frequencies is None:
            return
        key = str(value)
        self._frequencies[key] = self._frequencies.get(key, 0) + 1
        if len(self._frequencies) > MAX_TRACKED_FREQUENCIES:
            # Abandon: this column isn't low-cardinality, so it'll never
            # need an exact frequency table, and tracking one unbounded
            # would grow with every new distinct value seen.
            self._frequencies = None

    def _observe_scalar(self, value: ProfileValue) -> None:
        """Feed one scalar into the distinct sketch, reservoir sample, and
        frequency table — the per-value bookkeeping shared by the batch and
        row code paths."""
        self._distinct.update(str(value))
        self._add_to_sample(value)
        self._track_frequency(value)

    def _merge_numeric_batch(
        self, batch_count: int, batch_mean: float, batch_variance: float
    ) -> None:
        # Chan et al.'s parallel variance algorithm: merges a batch's (count,
        # mean, variance) into the running aggregate without ever
        # materializing the full column, and without the cancellation error a
        # naive sum-of-squares would accumulate over many batches.
        # Defensive: callers only invoke this with a positive count.
        if batch_count == 0:  # pragma: no cover
            return
        batch_m2 = batch_variance * batch_count
        if self._numeric_count == 0:
            self._numeric_count, self._mean, self._m2 = (
                batch_count,
                batch_mean,
                batch_m2,
            )
            return
        delta = batch_mean - self._mean
        total = self._numeric_count + batch_count
        self._mean += delta * batch_count / total
        self._m2 += batch_m2 + delta * delta * self._numeric_count * batch_count / total
        self._numeric_count = total

    def _update_min_max(self, batch_min: Any, batch_max: Any) -> None:
        if self._min is None or batch_min < self._min:
            self._min = batch_min
        if self._max is None or batch_max > self._max:
            self._max = batch_max

    def add_batch(self, values: pa.Array) -> None:
        self.null_count += values.null_count
        non_null = values.drop_null()
        self.non_null_count += len(non_null)
        if len(non_null) == 0:
            return

        # cpc_sketch and the frequency table only accept scalar updates, so
        # this loop (and reservoir sampling) is the one part of this method
        # that can't be vectorized.
        for value in non_null.to_pylist():
            self._observe_scalar(value)

        if self.kind != ColumnKind.OTHER:
            # `<`/`>` aren't defined for nested (list/struct/map) or binary
            # Arrow types, so min/max tracking is skipped for them, same as
            # the old profiler (which never surfaced min/max outside
            # numeric/temporal columns and never attempted it for nested ones).
            self._update_min_max(pc.min(non_null).as_py(), pc.max(non_null).as_py())

        if self.kind == ColumnKind.NUMERIC:
            float_values = non_null.cast(pa.float64())
            batch_mean = pc.mean(float_values).as_py()
            batch_variance = pc.variance(float_values, ddof=0).as_py()
            self._merge_numeric_batch(len(float_values), batch_mean, batch_variance)
            assert self._median is not None
            # kll_floats_sketch's vectorized update() only accepts float32 arrays.
            self._median.update(
                float_values.to_numpy(zero_copy_only=False).astype("float32")
            )

    def add_value(self, value: Optional[ProfileValue]) -> None:
        """Feed a single record's value (used by the row-at-a-time Avro reader)."""
        if value is None:
            self.null_count += 1
            return
        self.non_null_count += 1
        self._observe_scalar(value)
        if self.kind != ColumnKind.OTHER:
            self._update_min_max(value, value)
        if self.kind == ColumnKind.NUMERIC:
            if not isinstance(value, (int, float, Decimal)):
                # The column was classified NUMERIC from its Avro schema but a
                # record carried an off-type value. Raise with a real message
                # (the profiler surfaces it as a warning); a bare assert would
                # be message-less and stripped under `python -O`.
                raise ValueError(
                    f"Column {self.column!r} is numeric but got a "
                    f"non-numeric value of type {type(value).__name__}"
                )
            x = float(value)
            self._merge_numeric_batch(1, x, 0.0)
            assert self._median is not None
            self._median.update(x)

    def _frequency_list(self) -> Optional[List[ValueFrequency]]:
        # Only called for low-cardinality columns, which always have frequencies.
        if not self._frequencies:  # pragma: no cover
            return None
        return [
            ValueFrequency(value=value, frequency=frequency)
            for value, frequency in sorted(self._frequencies.items())
        ]

    def _continuous_histogram(self) -> Optional[Histogram]:
        assert self._median is not None
        # Histogram only runs for high-cardinality numerics, where min != max.
        if (
            self._min is None or self._max is None or self._min == self._max
        ):  # pragma: no cover
            return None
        edges = [
            self._min + (self._max - self._min) * i / HISTOGRAM_BINS
            for i in range(1, HISTOGRAM_BINS)
        ]
        proportions = self._median.get_pmf(edges)
        counts = [p * self.non_null_count for p in proportions]
        bounds = [self._min, *edges, self._max]
        labels = [str(bounds[i]) for i in range(len(bounds) - 1)]
        return Histogram(boundaries=labels, counts=counts)

    def finalize(self) -> ColumnStats:
        unique_count = (
            int(round(self._distinct.get_estimate()))
            if self.non_null_count > 0
            else None
        )
        row_count = self.non_null_count + self.null_count
        # The old Spark/Deequ profiler passed null_fraction (not the unique
        # proportion) as convert_to_cardinality's `pct_unique` argument, so a
        # fully-distinct column is deliberately NOT treated as high-uniqueness
        # here: real recipes/dashboards depend on fully-distinct numeric/string
        # columns still getting full stats or frequency tables, not nothing.
        # (Cardinality.UNIQUE needs pct_unique == 1.0, i.e. null_fraction == 1.0,
        # which means non_null_count == 0 -> unique_count is None ->
        # convert_to_cardinality returns NONE at its first guard. So with
        # null_fraction as the input the UNIQUE branch is never actually hit.)
        null_fraction = self.null_count / row_count if row_count > 0 else None
        cardinality = convert_to_cardinality(unique_count, null_fraction)

        min_value = max_value = None
        mean = stdev = median = None
        quantiles = None
        histogram = None
        distinct_value_frequencies = None

        if self.kind == ColumnKind.NUMERIC:
            if cardinality in LOW_CARDINALITY:
                distinct_value_frequencies = self._frequency_list()
            elif cardinality in HIGH_CARDINALITY:
                min_value, max_value = self._min, self._max
                if self._numeric_count > 0:
                    mean = self._mean
                    stdev = math.sqrt(self._m2 / self._numeric_count)
                    assert self._median is not None
                    median = float(self._median.get_quantile(0.5))
                    quantiles = [
                        Quantile(quantile=q, value=float(self._median.get_quantile(q)))
                        for q in QUANTILES
                    ]
                    histogram = self._continuous_histogram()
        elif self.kind == ColumnKind.STRING:
            if cardinality in LOW_CARDINALITY:
                distinct_value_frequencies = self._frequency_list()
        elif self.kind == ColumnKind.TEMPORAL:
            min_value, max_value = self._min, self._max
            if cardinality in LOW_CARDINALITY:
                distinct_value_frequencies = self._frequency_list()

        return ColumnStats(
            column=self.column,
            non_null_count=self.non_null_count,
            null_count=self.null_count,
            unique_count=unique_count,
            min_value=min_value,
            max_value=max_value,
            mean=mean,
            median=median,
            stdev=stdev,
            quantiles=quantiles,
            histogram=histogram,
            distinct_value_frequencies=distinct_value_frequencies,
            sample_values=sorted(str(v) for v in self._sample),
        )


class TableAccumulator:
    def __init__(
        self,
        columns: List[str],
        column_kinds: Optional[Dict[str, ColumnKind]] = None,
        sample_size: Optional[int] = None,
    ):
        kinds = column_kinds or {}
        self.row_count = 0
        self._accumulators = {
            column: ColumnAccumulator(
                column, kinds.get(column, ColumnKind.OTHER), sample_size
            )
            for column in columns
        }

    def add_batch(self, batch: pa.RecordBatch) -> None:
        self.row_count += batch.num_rows
        # Access columns positionally: a name lookup (batch.column(name)) raises
        # if the file has duplicate column names (e.g. several empty-named
        # trailing columns from stray delimiters), which would otherwise abort
        # the whole table's profile.
        for index, column in enumerate(batch.schema.names):
            accumulator = self._accumulators.get(column)
            if accumulator is not None:
                accumulator.add_batch(batch.column(index))

    def add_row(self, row: Dict[str, Any]) -> None:
        self.row_count += 1
        for column, value in row.items():
            accumulator = self._accumulators.get(column)
            if accumulator is not None:
                accumulator.add_value(value)

    def finalize(self) -> TableStats:
        return TableStats(
            row_count=self.row_count,
            column_count=len(self._accumulators),
            columns=[acc.finalize() for acc in self._accumulators.values()],
        )
