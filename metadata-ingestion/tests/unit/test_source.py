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
