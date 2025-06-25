import logging
from typing import Iterable

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class LineageConfig(ConfigModel):
    """
    Configuration for generating mock lineage data for testing purposes.

    This configuration controls how the mock data source generates a hierarchical
    lineage graph with multiple levels of upstream/downstream relationships.

    The lineage graph is structured as follows:
    - Level 0: 1 table (root)
    - Level 1: lineage_fan_out tables (each connected to the root)
    - Level 2: lineage_fan_out^2 tables (each connected to a level 1 table)
    - ... and so on for lineage_hops levels

    Examples:
        - With lineage_fan_out=2, lineage_hops=1: Creates 3 tables total
          (1 root + 2 downstream) with 2 lineage relationships
        - With lineage_fan_out=3, lineage_hops=2: Creates 13 tables total
          (1 + 3 + 9) with 12 lineage relationships
        - With lineage_fan_out=4, lineage_hops=1: Creates 5 tables total
          (1 + 4) with 4 lineage relationships

    Table naming convention: "hops_{lineage_hops}_f_{lineage_fan_out}_h{level}_t{table_index}"
    """

    emit_lineage: bool = Field(
        default=False,
        description="Whether to emit lineage data for testing purposes. When False, no lineage data is generated regardless of other settings.",
    )

    lineage_fan_out: int = Field(
        default=3,
        description="Number of downstream tables that each upstream table connects to. This controls the 'width' of the lineage graph. Higher values create more parallel downstream tables per level.",
    )

    lineage_hops: int = Field(
        default=2,
        description="Number of hops (levels) in the lineage graph. This controls the 'depth' of the lineage graph. Level 0 is the root table, and each subsequent level contains downstream tables. Higher values create deeper lineage chains.",
    )


class DataHubMockDataConfig(ConfigModel):
    enabled: bool = Field(
        default=True,
        description="Whether this source is enabled",
    )

    lineage_gen_1: LineageConfig = Field(
        default_factory=LineageConfig,
        description="Configuration for lineage data generation",
    )


@platform_name("DataHubMockData")
@config_class(DataHubMockDataConfig)
@support_status(SupportStatus.TESTING)
class DataHubMockDataSource(Source):
    """
    This source is for generating mock data for testing purposes.
    Expect breaking changes as we iterate on the mock data source.
    """

    def __init__(self, ctx: PipelineContext, config: DataHubMockDataConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.config.lineage.emit_lineage:
            yield from self._generate_lineage_data_gen_1()

        yield from []

    def _generate_lineage_data_gen_1(self) -> Iterable[MetadataWorkUnit]:
        """Generate mock lineage data for testing purposes."""
        lineage_gen_1 = self.config.lineage_gen_1
        fan_out = lineage_gen_1.lineage_fan_out
        hops = lineage_gen_1.lineage_hops

        logger.info(f"Generating lineage data with fan_out={fan_out}, hops={hops}")

        # Calculate total tables to be created
        tables_to_be_created = 0
        for i in range(hops + 1):
            tables_to_be_created += fan_out**i

        logger.info(
            f"About to create {tables_to_be_created} tables for lineage testing"
        )

        current_progress = 0
        for i in range(hops + 1):
            for j in range(fan_out**i):
                table_name = f"hops_{hops}_f_{fan_out}_h{i}_t{j}"

                yield self._get_status_aspect(table_name)

                # Emit upstream lineage for downstream tables
                for downstream in range(j * fan_out, (j + 1) * fan_out):
                    if i + 1 <= hops:  # Make sure we don't exceed the number of hops
                        downstream_table_name = (
                            f"hops_{hops}_f_{fan_out}_h{i + 1}_t{downstream}"
                        )
                        yield self._get_upstream_aspect(
                            upstream_table=table_name,
                            downstream_table=downstream_table_name,
                        )

                current_progress += 1
                if current_progress % 10 == 0:
                    logger.info(
                        f"Progress: {current_progress}/{tables_to_be_created} tables processed"
                    )

    def _get_status_aspect(self, table: str) -> MetadataWorkUnit:
        """Create a status aspect for a table."""
        urn = make_dataset_urn(
            platform="fake",
            name=table,
        )
        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            entityType="dataset",
            aspect=StatusClass(removed=False),
        )
        return mcp.as_workunit()

    def _get_upstream_aspect(
        self, upstream_table: str, downstream_table: str
    ) -> MetadataWorkUnit:
        """Create an upstream lineage aspect between two tables."""
        mcp = MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(
                platform="fake",
                name=downstream_table,
            ),
            entityType="dataset",
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=make_dataset_urn(
                            platform="fake",
                            name=upstream_table,
                        ),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                ],
            ),
        )
        return mcp.as_workunit()

    def get_report(self) -> SourceReport:
        return self.report
