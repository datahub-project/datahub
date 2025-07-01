from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import StatusClass
from datahub.utilities.urns.dataset_urn import DatasetUrn


class FakeSource(Source):
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        return [
            MetadataWorkUnit(
                id="test-workunit",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=str(
                        DatasetUrn.create_from_ids(
                            platform_id="elasticsearch",
                            table_name="fooIndex",
                            env="PROD",
                        )
                    ),
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


def test_discretize_dict_values():
    """Test the _discretize_dict_values static method."""
    # Test data with various count values
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

    # Call the static method
    result = SourceReport._discretize_dict_values(test_dict)

    # Verify the structure is preserved
    assert "dataset" in result
    assert "chart" in result
    assert "schemaMetadata" in result["dataset"]
    assert "status" in result["dataset"]
    assert "ownership" in result["dataset"]
    assert "status" in result["chart"]
    assert "ownership" in result["chart"]

    # Verify that all values are discretized (should be different from original)
    # The discretize function should convert exact counts to bucketed values
    assert result["dataset"]["schemaMetadata"] == 4
    assert result["dataset"]["status"] == 8
    assert result["dataset"]["ownership"] == 2
    assert result["chart"]["status"] == 8
    assert result["chart"]["ownership"] == 1

    # Verify that discretized values are still integers
    assert isinstance(result["dataset"]["schemaMetadata"], int)
    assert isinstance(result["dataset"]["status"], int)
    assert isinstance(result["dataset"]["ownership"], int)
    assert isinstance(result["chart"]["status"], int)
    assert isinstance(result["chart"]["ownership"], int)
