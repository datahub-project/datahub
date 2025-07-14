from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetUsageStatisticsClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    StatusClass,
    SubTypesClass,
    TimeWindowSizeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn


def _get_urn(table_name: str = "fooIndex") -> str:
    return str(
        DatasetUrn.create_from_ids(
            platform_id="elasticsearch",
            table_name=table_name,
            env="PROD",
        )
    )


class FakeSource(Source):
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        return [
            MetadataWorkUnit(
                id="test-workunit",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=_get_urn(),
                    aspect=StatusClass(removed=False),
                ),
            )
        ]

    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report = SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FakeSource":
        return FakeSource(ctx)

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self) -> None:
        return super().close()


def test_aspects_by_subtypes():
    source = FakeSource(PipelineContext(run_id="test_aspects_by_subtypes"))
    for wu in source.get_workunits_internal():
        source.source_report.report_workunit(wu)

    source.source_report.compute_stats()
    assert source.source_report.get_aspects_by_subtypes_dict() == {
        "dataset": {
            "unknown": {"status": 1},
        }
    }
    source.source_report.report_workunit(
        MetadataChangeProposalWrapper(
            entityUrn=_get_urn(),
            aspect=SubTypesClass(typeNames=["Table"]),
        ).as_workunit()
    )
    source.source_report.compute_stats()
    assert source.source_report.get_aspects_by_subtypes_dict() == {
        "dataset": {
            "Table": {"status": 1, "subTypes": 1},
        }
    }


def test_lineage_in_aspects_by_subtypes():
    # _urn_1 is upstream of _urn_2
    _urn_1 = _get_urn()
    _urn_2 = _get_urn(table_name="barIndex")

    source = FakeSource(PipelineContext(run_id="test_lineage_in_aspects_by_subtypes"))
    for wu in source.get_workunits_internal():
        source.source_report.report_workunit(wu)

    source.source_report.report_workunit(
        MetadataChangeProposalWrapper(
            entityUrn=_urn_2,
            aspect=SubTypesClass(typeNames=["Table"]),
        ).as_workunit()
    )

    source.source_report.report_workunit(
        MetadataChangeProposalWrapper(
            entityUrn=_urn_2,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=_urn_1, type=DatasetLineageTypeClass.TRANSFORMED
                    ),
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
                        upstreams=[
                            _urn_1,
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    )
                ],
            ),
        ).as_workunit()
    )
    source.source_report.compute_stats()
    assert source.source_report.get_aspects_by_subtypes_dict() == {
        "dataset": {
            "Table": {
                "subTypes": 1,
                "upstreamLineage": 1,
                "fineGrainedLineages": 1,
            },
            "unknown": {
                "status": 1,
            },
        }
    }
    assert source.source_report.get_aspects_dict() == {
        "dataset": {
            "subTypes": 1,
            "upstreamLineage": 1,
            "fineGrainedLineages": 1,
            "status": 1,
        },
    }
    assert source.source_report.samples == {
        "lineage": {"Table": [_urn_2]},
    }

    # Now let's add usage and profiling and see if the samples are updated
    source.source_report.report_workunit(
        MetadataChangeProposalWrapper(
            entityUrn=_urn_2,
            aspect=DatasetProfileClass(
                timestampMillis=0,
                rowCount=100,
                columnCount=10,
                sizeInBytes=1000,
            ),
        ).as_workunit()
    )
    source.source_report.report_workunit(
        MetadataChangeProposalWrapper(
            entityUrn=_urn_2,
            aspect=DatasetUsageStatisticsClass(
                timestampMillis=0,
                eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
                uniqueUserCount=0,
                totalSqlQueries=0,
                topSqlQueries=[],
                userCounts=[],
                fieldCounts=[],
            ),
        ).as_workunit()
    )
    source.source_report.compute_stats()
    assert source.source_report.get_aspects_by_subtypes_dict() == {
        "dataset": {
            "Table": {
                "subTypes": 1,
                "upstreamLineage": 1,
                "fineGrainedLineages": 1,
                "datasetProfile": 1,
                "datasetUsageStatistics": 1,
            },
            "unknown": {
                "status": 1,
            },
        }
    }
    assert source.source_report.get_aspects_dict() == {
        "dataset": {
            "subTypes": 1,
            "upstreamLineage": 1,
            "fineGrainedLineages": 1,
            "status": 1,
            "datasetProfile": 1,
            "datasetUsageStatistics": 1,
        },
    }
    assert source.source_report.samples == {
        "lineage": {"Table": [_urn_2]},
        "profiling": {"Table": [_urn_2]},
        "usage": {"Table": [_urn_2]},
        "all_3": {"Table": [_urn_2]},
    }


def test_samples_with_overlapping_aspects():
    """Test samples collection with overlapping aspects: 25 lineage, 50 profile, 25 usage with 13 overlapping."""
    source = FakeSource(PipelineContext(run_id="test_samples_with_overlapping_aspects"))

    # Generate URNs for different categories
    # 13 entities with all three aspects (lineage + profile + usage)
    all_3_urns = [_get_urn(f"all3_table_{i}") for i in range(13)]

    # 12 entities with only lineage (25 total lineage - 13 overlapping)
    lineage_only_urns = [_get_urn(f"lineage_table_{i}") for i in range(12)]

    # 37 entities with only profile (50 total profile - 13 overlapping)
    profile_only_urns = [_get_urn(f"profile_table_{i}") for i in range(37)]

    # 12 entities with only usage (25 total usage - 13 overlapping)
    usage_only_urns = [_get_urn(f"usage_table_{i}") for i in range(12)]

    # Add SubTypes for all entities to make them "Table" subtype
    all_urns = all_3_urns + lineage_only_urns + profile_only_urns + usage_only_urns
    for urn in all_urns:
        source.source_report.report_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=SubTypesClass(typeNames=["Table"]),
            ).as_workunit()
        )

    # Add lineage aspects to all_3_urns + lineage_only_urns (25 total)
    lineage_urns = all_3_urns + lineage_only_urns
    for i, urn in enumerate(lineage_urns):
        upstream_urn = _get_urn(f"upstream_{i}")
        source.source_report.report_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        ),
                    ],
                    fineGrainedLineages=[
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
                            upstreams=[upstream_urn],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        )
                    ],
                ),
            ).as_workunit()
        )

    # Add profile aspects to all_3_urns + profile_only_urns (50 total)
    profile_urns = all_3_urns + profile_only_urns
    for urn in profile_urns:
        source.source_report.report_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetProfileClass(
                    timestampMillis=0,
                    rowCount=100,
                    columnCount=10,
                    sizeInBytes=1000,
                ),
            ).as_workunit()
        )

    # Add usage aspects to all_3_urns + usage_only_urns (25 total)
    usage_urns = all_3_urns + usage_only_urns
    for urn in usage_urns:
        source.source_report.report_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetUsageStatisticsClass(
                    timestampMillis=0,
                    eventGranularity=TimeWindowSizeClass(
                        unit=CalendarIntervalClass.DAY
                    ),
                    uniqueUserCount=0,
                    totalSqlQueries=0,
                    topSqlQueries=[],
                    userCounts=[],
                    fieldCounts=[],
                ),
            ).as_workunit()
        )

    source.source_report.compute_stats()

    # Verify samples - each category should have up to 10 samples (default _samples_to_add)
    samples = source.source_report.samples

    # Lineage samples: should include from both all_3_urns and lineage_only_urns (up to 10)
    assert "lineage" in samples
    assert "Table" in samples["lineage"]
    lineage_samples = samples["lineage"]["Table"]
    assert len(lineage_samples) == 20  # limited by _samples_to_add

    # Profile samples: should include from both all_3_urns and profile_only_urns (up to 10)
    assert "profiling" in samples
    assert "Table" in samples["profiling"]
    profile_samples = samples["profiling"]["Table"]
    assert len(profile_samples) == 20  # limited by _samples_to_add

    # Usage samples: should include from both all_3_urns and usage_only_urns (up to 10)
    assert "usage" in samples
    assert "Table" in samples["usage"]
    usage_samples = samples["usage"]["Table"]
    assert len(usage_samples) == 20  # limited by _samples_to_add

    # All_3 samples: should only include from all_3_urns (up to 10, but we have exactly 13)
    assert "all_3" in samples
    assert "Table" in samples["all_3"]
    all_3_samples = samples["all_3"]["Table"]
    assert len(all_3_samples) == 13  # limited by _samples_to_add

    # Verify that all_3 samples are actually from the all_3_urns
    for sample_urn in all_3_samples:
        assert sample_urn in all_3_urns


def test_discretize_dict_values():
    """Test the _discretize_dict_values static method."""
    test_dict = {
        "dataset": {
            "schemaMetadata": 5,
            "status": 12,
            "ownership": 3,
        },
        "chart": {
            "status": 8,
            "ownership": 1,
        },
    }

    result = SourceReport._discretize_dict_values(test_dict)
    assert result == {
        "dataset": {
            "schemaMetadata": 4,
            "status": 8,
            "ownership": 2,
        },
        "chart": {
            "status": 8,
            "ownership": 1,
        },
    }


def test_multiple_same_aspects_count_correctly():
    source = FakeSource(PipelineContext(run_id="test_multiple_same_aspects"))
    urn = _get_urn()

    for _ in range(5):
        source.source_report.report_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()
        )

    source.source_report.compute_stats()

    assert source.source_report.aspects == {"dataset": {"status": 5}}
    assert source.source_report.aspects_by_subtypes == {
        "dataset": {"unknown": {"status": 5}}
    }
