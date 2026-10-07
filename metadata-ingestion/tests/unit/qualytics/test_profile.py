"""Tests for the Qualytics profile -> DataHub profile mapping.

The unit conversions are where this goes wrong quietly: Qualytics reports completeness
as a ratio while DataHub wants an absolute null count, and DataHub types every summary
statistic as a string. A mistake there produces a profile that renders plausibly and
is simply wrong.
"""

from typing import Any

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.models import ContainerProfile, FieldProfile
from datahub.ingestion.source.qualytics.profile import (
    MAX_DISTINCT_VALUE_FREQUENCIES,
    ProfileMapper,
)
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.ingestion.source.qualytics.timeutil import parse_timestamp_millis
from datahub.metadata.schema_classes import DatasetProfileClass

URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,SALES.PUBLIC.ORDERS,PROD)"


def _profile(wu: MetadataWorkUnit) -> DatasetProfileClass:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, DatasetProfileClass)
    return wu.metadata.aspect


def _mapper() -> tuple[ProfileMapper, QualyticsSourceReport]:
    report = QualyticsSourceReport()
    return ProfileMapper(report), report


def _container_profile(**overrides: Any) -> ContainerProfile:
    return ContainerProfile.model_validate(
        {
            "id": 1,
            "created": "2026-09-09T12:00:00Z",
            "operation_id": 7,
            "records_count": 1000,
            "records_processed": 1000,
            "result": "success",
            **overrides,
        }
    )


def _field_profile(**overrides: Any) -> FieldProfile:
    return FieldProfile.model_validate(
        {
            "id": 1,
            "name": "amount",
            "created": "2026-09-09T12:00:00Z",
            "field_type": "Fractional",
            **overrides,
        }
    )


# --- unit conversions --------------------------------------------------------------


def test_completeness_ratio_becomes_null_proportion_and_absolute_null_count() -> None:
    # Qualytics: completeness 0.95 means 95% populated. DataHub wants the inverse as a
    # proportion, and an absolute count that only makes sense against the row count.
    mapper, _ = _mapper()

    fp = mapper.field_profile(_field_profile(completeness=0.95), row_count=1000)

    assert fp.nullProportion == pytest.approx(0.05)
    assert fp.nullCount == 50


def test_null_count_is_omitted_when_the_row_count_is_unknown() -> None:
    # A null *count* without a row count would have to be invented.
    mapper, _ = _mapper()

    fp = mapper.field_profile(_field_profile(completeness=0.95), row_count=None)

    assert fp.nullProportion == pytest.approx(0.05)
    assert fp.nullCount is None


def test_full_completeness_yields_zero_nulls_not_a_missing_value() -> None:
    mapper, _ = _mapper()

    fp = mapper.field_profile(_field_profile(completeness=1.0), row_count=500)

    assert fp.nullProportion == 0.0
    assert fp.nullCount == 0


def test_absent_completeness_leaves_both_null_statistics_unset() -> None:
    mapper, _ = _mapper()

    fp = mapper.field_profile(_field_profile(), row_count=500)

    assert fp.nullProportion is None
    assert fp.nullCount is None


def test_unique_proportion_is_derived_and_clamped_to_one() -> None:
    # approximate_distinct_values is approximate, so on a small table it can exceed
    # the row count. A uniqueProportion above 1 is nonsense in the UI.
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(approximate_distinct_values=1200), row_count=1000
    )

    assert fp.uniqueCount == 1200
    assert fp.uniqueProportion == 1.0


def test_unique_proportion_is_skipped_when_the_row_count_is_zero() -> None:
    # Guards a division by zero on an empty table.
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(approximate_distinct_values=0), row_count=0
    )

    assert fp.uniqueProportion is None


def test_statistics_are_rendered_as_strings_without_spurious_decimals() -> None:
    # DataHub types these as strings. A min of 42 shown as "42.0" reads like a
    # precision artefact of ours rather than the source value.
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(min=42.0, max=1000.5, mean=173.25, median=150.0, std_dev=88.125),
        row_count=1000,
    )

    assert fp.min == "42"
    assert fp.max == "1000.5"
    assert fp.mean == "173.25"
    assert fp.median == "150"
    assert fp.stdev == "88.125"


def test_quartiles_become_a_quantile_series() -> None:
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(q1=10.0, median=50.0, q3=90.0), row_count=100
    )

    assert fp.quantiles is not None
    assert [(q.quantile, q.value) for q in fp.quantiles] == [
        ("0.25", "10"),
        ("0.5", "50"),
        ("0.75", "90"),
    ]


def test_partial_quartiles_emit_only_what_is_present() -> None:
    mapper, _ = _mapper()

    fp = mapper.field_profile(_field_profile(q1=10.0), row_count=100)

    assert fp.quantiles is not None
    assert [q.quantile for q in fp.quantiles] == ["0.25"]


# --- histograms --------------------------------------------------------------------


def test_histogram_buckets_become_distinct_value_frequencies() -> None:
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(
            histogram_buckets=[
                {"value": "US", "count": 700, "ratio": 0.7},
                {"value": "CA", "count": 300, "ratio": 0.3},
            ]
        ),
        row_count=1000,
    )

    assert fp.distinctValueFrequencies is not None
    assert [(v.value, v.frequency) for v in fp.distinctValueFrequencies] == [
        ("US", 700),
        ("CA", 300),
    ]


def test_an_oversized_histogram_keeps_the_most_frequent_buckets_and_is_reported() -> (
    None
):
    # High-cardinality string columns produce a long tail. Truncating arbitrarily
    # would drop the values a user actually cares about; the count in the report is
    # what tells them the distribution shown is partial.
    mapper, report = _mapper()
    buckets = [
        {"value": f"v{i}", "count": i, "ratio": 0.0}
        for i in range(MAX_DISTINCT_VALUE_FREQUENCIES + 50)
    ]

    fp = mapper.field_profile(
        _field_profile(histogram_buckets=buckets), row_count=10_000
    )

    assert fp.distinctValueFrequencies is not None
    assert len(fp.distinctValueFrequencies) == MAX_DISTINCT_VALUE_FREQUENCIES
    assert (
        fp.distinctValueFrequencies[0].frequency == MAX_DISTINCT_VALUE_FREQUENCIES + 49
    )
    assert report.profile_histograms_truncated == 1


def test_a_histogram_at_the_limit_is_left_intact_and_unreported() -> None:
    mapper, report = _mapper()
    buckets = [
        {"value": f"v{i}", "count": 1, "ratio": 0.0}
        for i in range(MAX_DISTINCT_VALUE_FREQUENCIES)
    ]

    fp = mapper.field_profile(_field_profile(histogram_buckets=buckets), row_count=100)

    assert fp.distinctValueFrequencies is not None
    assert len(fp.distinctValueFrequencies) == MAX_DISTINCT_VALUE_FREQUENCIES
    assert report.profile_histograms_truncated == 0


# --- timestamps --------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-09T12:00:00Z",
        "2026-09-09T12:00:00+00:00",
        "2026-09-09T12:00:00",  # naive: treated as UTC
    ],
)
def test_timestamps_parse_across_the_forms_qualytics_emits(value: str) -> None:
    # fromisoformat only accepts a trailing Z from 3.11; we support 3.10. A naive
    # value must be read as UTC, not local time, or every profile shifts by the
    # ingesting machine's offset.
    assert parse_timestamp_millis(value) == 1788955200000


def test_an_unparseable_timestamp_skips_the_profile_with_a_warning() -> None:
    # Defaulting to "now" would date a months-old profile to this run and corrupt the
    # trend line -- worse than having no profile.
    mapper, report = _mapper()

    assert (
        list(
            mapper.workunits(
                URN, _container_profile(created="not a date"), [_field_profile()]
            )
        )
        == []
    )
    assert report.profiles_emitted == 0
    assert any("dataset profile" in str(w).lower() for w in report.warnings)


# --- the dataset profile -----------------------------------------------------------


def test_dataset_profile_carries_row_count_column_count_and_field_profiles() -> None:
    mapper, report = _mapper()
    fields = [_field_profile(name="amount"), _field_profile(id=2, name="country")]

    [wu] = mapper.workunits(URN, _container_profile(records_count=1000), fields)

    aspect = _profile(wu)
    assert aspect.rowCount == 1000
    assert aspect.columnCount == 2
    assert aspect.timestampMillis == 1788955200000
    assert [fp.fieldPath for fp in aspect.fieldProfiles or []] == ["amount", "country"]
    assert report.profiles_emitted == 1


def test_a_container_with_no_field_profiles_still_emits_row_count() -> None:
    # An unprofiled-columns table is still worth a row count on the Stats tab.
    mapper, _ = _mapper()

    [wu] = mapper.workunits(URN, _container_profile(records_count=42), [])

    assert _profile(wu).rowCount == 42
    assert _profile(wu).columnCount is None
    assert _profile(wu).fieldProfiles is None


def test_an_infinite_distinct_count_is_dropped_rather_than_crashing() -> None:
    # JSON permits 1e400, which parses to inf, and int(inf) raises -- which used to
    # abort every remaining datastore from inside one field profile.
    mapper, _ = _mapper()

    fp = mapper.field_profile(
        _field_profile(approximate_distinct_values=1e400), row_count=10
    )

    assert fp.uniqueCount is None
    assert fp.uniqueProportion is None
