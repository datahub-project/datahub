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


class LineageConfigGen1(ConfigModel):
    """
    Configuration for generating mock lineage data for testing purposes.

    This configuration controls how the mock data source generates a hierarchical
    lineage graph with multiple levels of upstream/downstream relationships.

    The lineage graph is structured as follows:
    - Level 0: 1 table (root)
    - Level 1: lineage_fan_out tables (each connected to the root)
    - Level 2+: If lineage_fan_out_after_first_hop is set, uses that value;
                otherwise uses lineage_fan_out^level tables (each connected to a level 1 table)
    - ... and so on for lineage_hops levels

    Examples:
        - With lineage_fan_out=2, lineage_hops=1: Creates 3 tables total
          (1 root + 2 downstream) with 2 lineage relationships
        - With lineage_fan_out=3, lineage_hops=2: Creates 13 tables total
          (1 + 3 + 9) with 12 lineage relationships
        - With lineage_fan_out=4, lineage_hops=1: Creates 5 tables total
          (1 + 4) with 4 lineage relationships
        - With lineage_fan_out=3, lineage_hops=3, lineage_fan_out_after_first_hop=2:
          Creates 1 + 3 + 6 + 12 = 22 tables total (prevents exponential growth)

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

    lineage_fan_out_after_first_hop: int = Field(
        default=None,
        description="Optional limit on fanout for hops after the first hop. When set, prevents exponential growth by limiting the number of downstream tables per upstream table at levels 2 and beyond. When None, uses the standard exponential growth (lineage_fan_out^level).",
    )


class DataHubMockDataConfig(ConfigModel):
    enabled: bool = Field(
        default=True,
        description="Whether this source is enabled",
    )

    lineage_gen_1: LineageConfigGen1 = Field(
        default_factory=LineageConfigGen1,
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
        if self.config.lineage_gen_1.emit_lineage:
            yield from self._generate_lineage_data_gen_1()

        yield from []

    def _calculate_lineage_tables(
        self, fan_out: int, hops: int, fan_out_after_first: int = None
    ) -> tuple[int, list[int]]:
        """
        Calculate the total number of tables and tables at each level for lineage generation.

        Args:
            fan_out: Number of downstream tables per upstream table at level 1
            hops: Number of hops (levels) in the lineage graph
            fan_out_after_first: Optional limit on fanout for hops after the first hop

        Returns:
            Tuple of (total_tables, tables_at_levels) where tables_at_levels is a list
            containing the number of tables at each level (index 0 = level 0, etc.)
        """
        tables_to_be_created = 0
        tables_at_levels = []

        for i in range(hops + 1):
            if i == 0:
                # Level 0: always 1 table
                tables_at_level = 1
            elif i == 1:
                # Level 1: uses lineage_fan_out
                tables_at_level = fan_out
            else:
                # Level 2+: use fan_out_after_first_hop if set, otherwise exponential growth
                if fan_out_after_first is not None:
                    # Each table at previous level creates fan_out_after_first tables
                    tables_at_level = tables_at_levels[i - 1] * fan_out_after_first
                else:
                    # Original exponential behavior
                    tables_at_level = fan_out**i

            tables_at_levels.append(tables_at_level)
            tables_to_be_created += tables_at_level

        return tables_to_be_created, tables_at_levels

    def _calculate_fanout_for_level(
        self, level: int, fan_out: int, fan_out_after_first: int = None
    ) -> int:
        """
        Calculate the fanout (number of downstream tables) for a specific level.

        Args:
            level: The current level (0-based)
            fan_out: Number of downstream tables per upstream table at level 1
            fan_out_after_first: Optional limit on fanout for hops after the first hop

        Returns:
            The number of downstream tables that each table at this level should connect to
        """
        if level == 0:
            # Level 0: uses the standard fan_out
            return fan_out
        else:
            # Level 1+: use fan_out_after_first if set, otherwise use fan_out
            return fan_out_after_first if fan_out_after_first is not None else fan_out

    def _generate_lineage_data_gen_1(self) -> Iterable[MetadataWorkUnit]:
        """Generate mock lineage data for testing purposes."""
        lineage_gen_1 = self.config.lineage_gen_1
        fan_out = lineage_gen_1.lineage_fan_out
        hops = lineage_gen_1.lineage_hops
        fan_out_after_first = lineage_gen_1.lineage_fan_out_after_first_hop

        logger.info(
            f"Generating lineage data with fan_out={fan_out}, hops={hops}, fan_out_after_first={fan_out_after_first}"
        )

        tables_to_be_created, tables_at_levels = self._calculate_lineage_tables(
            fan_out, hops, fan_out_after_first
        )

        logger.info(
            f"About to create {tables_to_be_created} tables for lineage testing"
        )

        current_progress = 0
        for i in range(hops + 1):
            tables_at_level = tables_at_levels[i]

            for j in range(tables_at_level):
                table_name = f"hops_{hops}_f_{fan_out}_h{i}_t{j}"

                yield self._get_status_aspect(table_name)

                yield from self._generate_lineage_for_table(
                    table_name=table_name,
                    table_level=i,
                    table_index=j,
                    hops=hops,
                    fan_out=fan_out,
                    fan_out_after_first=fan_out_after_first,
                    tables_at_levels=tables_at_levels,
                )

                current_progress += 1
                if current_progress % 1000 == 0:
                    logger.info(
                        f"Progress: {current_progress}/{tables_to_be_created} tables processed"
                    )

    def _generate_lineage_for_table(
        self,
        table_name: str,
        table_level: int,
        table_index: int,
        hops: int,
        fan_out: int,
        fan_out_after_first: int,
        tables_at_levels: list[int],
    ) -> Iterable[MetadataWorkUnit]:
        """Generate lineage relationships for a specific table."""
        # Only generate lineage if there are downstream levels
        if table_level + 1 > hops:
            return

        current_fan_out = self._calculate_fanout_for_level(
            table_level, fan_out, fan_out_after_first
        )

        yield from self._generate_downstream_lineage(
            upstream_table_name=table_name,
            upstream_table_index=table_index,
            upstream_table_level=table_level,
            current_fan_out=current_fan_out,
            hops=hops,
            fan_out=fan_out,
            tables_at_levels=tables_at_levels,
        )

    def _generate_downstream_lineage(
        self,
        upstream_table_name: str,
        upstream_table_index: int,
        upstream_table_level: int,
        current_fan_out: int,
        hops: int,
        fan_out: int,
        tables_at_levels: list[int],
    ) -> Iterable[MetadataWorkUnit]:
        """Generate lineage relationships to downstream tables."""
        downstream_level = upstream_table_level + 1
        downstream_tables_count = tables_at_levels[downstream_level]

        # Calculate range of downstream tables this upstream table connects to
        start_downstream = upstream_table_index * current_fan_out
        end_downstream = min(
            (upstream_table_index + 1) * current_fan_out, downstream_tables_count
        )

        for downstream_index in range(start_downstream, end_downstream):
            downstream_table_name = (
                f"hops_{hops}_f_{fan_out}_h{downstream_level}_t{downstream_index}"
            )
            yield self._get_upstream_aspect(
                upstream_table=upstream_table_name,
                downstream_table=downstream_table_name,
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
