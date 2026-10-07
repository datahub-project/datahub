"""Map Qualytics profiles onto DataHub dataset and field profiles.

Emitted as plain MCPs rather than through SDK V2's ``Dataset``. Two reasons, both
worth stating because "new connectors must use SDK V2" is a standards blocker and this
looks like an exception:

1. ``datasetProfile`` is a timeseries aspect. SDK V2's ``Dataset`` does not model it --
   its settable surface is schema, properties, tags, terms, owners and friends -- so
   there is no SDK V2 way to emit one.
2. These attach to datasets **another source owns**. Emitting a
   single additive aspect is exactly right; constructing a ``Dataset`` would also
   write ``dataPlatformInstance`` onto an entity the warehouse source is responsible
   for.

Note what is deliberately *not* here: ``schemaMetadata``. The warehouse source owns
the schema, and Qualytics' view of it is a subset (it has excluded, missing and masked
fields), so writing ours would overwrite an authoritative schema with a partial copy on
every run. Field-level metadata still reaches DataHub through field profiles and the
schemaField URNs on assertions, neither of which requires owning the schema. See
``skill_docs/_PLANNING.md``.
"""

import math
from collections.abc import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qualytics.models import ContainerProfile, FieldProfile
from datahub.ingestion.source.qualytics.report import QualyticsSourceReport
from datahub.ingestion.source.qualytics.timeutil import parse_timestamp_millis
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    QuantileClass,
    ValueFrequencyClass,
)

# Histogram buckets per field. Qualytics can produce a long tail on high-cardinality
# string columns, and every bucket is a row in the DataHub UI plus payload on every
# profile run. DataHub's own profilers cap similarly.
MAX_DISTINCT_VALUE_FREQUENCIES = 100


def _as_str(value: float | None) -> str | None:
    """DataHub types the profile statistics as strings, so render them predictably.

    Whole-valued floats are rendered without the trailing ``.0``: a row count or a
    min of ``42`` reading as ``42.0`` in the UI looks like a precision artefact.
    """
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


class ProfileMapper:
    """Builds DataHub profile aspects from Qualytics profile payloads."""

    def __init__(self, report: QualyticsSourceReport) -> None:
        self.report = report

    def field_profile(
        self, profile: FieldProfile, row_count: int | None
    ) -> DatasetFieldProfileClass:
        """Map one Qualytics field profile.

        ``row_count`` comes from the enclosing container profile and is needed to turn
        Qualytics' completeness *ratio* into DataHub's absolute null count.
        """
        null_proportion: float | None = None
        null_count: int | None = None
        if profile.completeness is not None:
            # Qualytics completeness is a 0..1 ratio of populated values.
            null_proportion = max(0.0, min(1.0, 1.0 - profile.completeness))
            if row_count is not None:
                null_count = round(row_count * null_proportion)

        # isfinite: JSON allows 1e400, which parses to inf, and int(inf) raises.
        distinct = profile.approximate_distinct_values
        unique_count = (
            int(distinct) if distinct is not None and math.isfinite(distinct) else None
        )
        unique_proportion: float | None = None
        if unique_count is not None and row_count:
            unique_proportion = min(1.0, unique_count / row_count)

        return DatasetFieldProfileClass(
            fieldPath=profile.name,
            uniqueCount=unique_count,
            uniqueProportion=unique_proportion,
            nullCount=null_count,
            nullProportion=null_proportion,
            min=_as_str(profile.min),
            max=_as_str(profile.max),
            mean=_as_str(profile.mean),
            median=_as_str(profile.median),
            stdev=_as_str(profile.std_dev),
            quantiles=self._quantiles(profile),
            distinctValueFrequencies=self._value_frequencies(profile),
        )

    @staticmethod
    def _quantiles(profile: FieldProfile) -> list[QuantileClass] | None:
        # Qualytics reports the quartiles individually. The median is also exposed as
        # its own DataHub field, but including it here keeps the quantile series
        # contiguous for anything plotting it.
        pairs = [("0.25", profile.q1), ("0.5", profile.median), ("0.75", profile.q3)]
        quantiles = [
            QuantileClass(quantile=q, value=rendered)
            for q, value in pairs
            if (rendered := _as_str(value)) is not None
        ]
        return quantiles or None

    def _value_frequencies(
        self, profile: FieldProfile
    ) -> list[ValueFrequencyClass] | None:
        if not profile.histogram_buckets:
            return None

        buckets = profile.histogram_buckets
        if len(buckets) > MAX_DISTINCT_VALUE_FREQUENCIES:
            self.report.profile_histograms_truncated += 1
            # Most frequent first, so truncation drops the least informative tail
            # rather than an arbitrary slice.
            buckets = sorted(buckets, key=lambda b: b.count, reverse=True)[
                :MAX_DISTINCT_VALUE_FREQUENCIES
            ]

        return [ValueFrequencyClass(value=b.value, frequency=b.count) for b in buckets]

    def dataset_profile(
        self,
        container_profile: ContainerProfile,
        field_profiles: list[FieldProfile],
    ) -> DatasetProfileClass:
        timestamp = parse_timestamp_millis(container_profile.created)
        if timestamp is None:
            # Falling back to "now" would date a months-old profile to this run and
            # corrupt the trend line, so the profile's own time is required.
            raise ValueError(
                f"container profile {container_profile.id} has an unparseable "
                f"created timestamp: {container_profile.created!r}"
            )

        row_count = container_profile.records_count

        return DatasetProfileClass(
            timestampMillis=timestamp,
            rowCount=row_count,
            columnCount=len(field_profiles) or None,
            fieldProfiles=[self.field_profile(fp, row_count) for fp in field_profiles]
            or None,
        )

    def workunits(
        self,
        dataset_urn: str,
        container_profile: ContainerProfile,
        field_profiles: list[FieldProfile],
    ) -> Iterable[MetadataWorkUnit]:
        """The datasetProfile for one container, or nothing if it cannot be built."""
        try:
            aspect = self.dataset_profile(container_profile, field_profiles)
        except ValueError as e:
            self.report.profiles_failed += 1
            self.report.warning(
                title="Could not build a dataset profile",
                message="The container's profile will be skipped; other metadata is unaffected.",
                context=f"dataset={dataset_urn}",
                exc=e,
            )
            return

        self.report.profiles_emitted += 1
        # is_primary_source=False is load-bearing, not a detail. The default (True)
        # makes auto_stale_entity_removal add this URN to our checkpoint state -- and
        # this URN is the *customer's* Snowflake/BigQuery dataset, which we do not own.
        # On the next run, any container that drops out (renamed, filtered, unprofiled,
        # emit_profiles turned off) would emit Status(removed=True) against their
        # dataset. Assertions and run events are ours and stay primary.
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=aspect
        ).as_workunit(is_primary_source=False)
