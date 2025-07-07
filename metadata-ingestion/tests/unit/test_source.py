from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    StatusClass,
    SubTypesClass,
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
        "lineage": [_urn_2],
        "usage": [],
        "profiling": [],
        "all_3": [],
    }


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
