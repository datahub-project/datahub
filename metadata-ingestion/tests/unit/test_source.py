from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api import workunit
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.source import Source, SourceReport
from datahub.metadata.schema_classes import ChangeTypeClass, StatusClass
from datahub.utilities.urns.dataset_urn import DatasetUrn


class TestSource(Source):
    def get_workunits(self) -> Iterable[WorkUnit]:
        return [
            workunit.MetadataWorkUnit(
                id="test-workunit",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=str(
                        DatasetUrn.create_from_ids(
                            platform_id="elasticsearch",
                            table_name="fooIndex",
                            env="PROD",
                        )
                    ),
                    aspectName="status",
                    aspect=StatusClass(removed=False),
                ),
            )
        ]

    def __init__(self, ctx: PipelineContext):
        self.source_report = SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "TestSource":
        return TestSource(ctx)

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self) -> None:
        return super().close()
