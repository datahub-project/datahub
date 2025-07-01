import logging
from typing import Dict, Iterable, List, Optional, Tuple

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
from datahub.ingestion.source.mock_data.datahub_mock_data_report import (
    DataHubMockDataReport,
)
from datahub.ingestion.source.mock_data.table_naming_helper import TableNamingHelper
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class SubTypePattern(StrEnum):
    ALTERNATING = "alternating"
    ALL_TABLE = "all_table"
    ALL_VIEW = "all_view"
    LEVEL_BASED = "level_based"


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

    lineage_fan_out_after_first_hop: Optional[int] = Field(
        default=None,
        description="Optional limit on fanout for hops after the first hop. When set, prevents exponential growth by limiting the number of downstream tables per upstream table at levels 2 and beyond. When None, uses the standard exponential growth (lineage_fan_out^level).",
    )

    subtype_pattern: SubTypePattern = Field(
        default=SubTypePattern.ALTERNATING,
        description="Pattern for determining SubTypes. Options: 'alternating', 'all_table', 'all_view', 'level_based'",
    )

    level_subtypes: Dict[int, str] = Field(
        default={0: "Table", 1: "View", 2: "Table"},
        description="Mapping of level to subtype for level_based pattern",
    )


class DataHubMockDataConfig(ConfigModel):
    enabled: bool = Field(
        default=True,
        description="Whether this source is enabled",
    )

    gen_1: LineageConfigGen1 = Field(
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
        self.report = DataHubMockDataReport()

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # We don't want any implicit aspects to be produced
        # so we are not using get_workunits_internal
        if self.config.gen_1.emit_lineage:
            for wu in self._data_gen_1():
                if self.report.first_urn_seen is None:
                    self.report.first_urn_seen = wu.get_urn()
                self.report.report_workunit(wu)
                yield wu

        yield from []

    def _calculate_lineage_tables(
        self, fan_out: int, hops: int, fan_out_after_first: Optional[int] = None
    ) -> Tuple[int, List[int]]:
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
        tables_at_levels: List[int] = []

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
        self, level: int, fan_out: int, fan_out_after_first: Optional[int] = None
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

    def _determine_subtype(
        self, table_name: str, table_level: int, table_index: int
    ) -> str:
        """
        Determine subtype based on configured pattern.

        Args:
            table_name: Name of the table
            table_level: Level of the table in the lineage graph
            table_index: Index of the table within its level

        Returns:
            The determined subtype ("Table" or "View")
        """
        pattern = self.config.gen_1.subtype_pattern

        if pattern == SubTypePattern.ALTERNATING:
            return "Table" if table_index % 2 == 0 else "View"
        elif pattern == SubTypePattern.LEVEL_BASED:
            return self.config.gen_1.level_subtypes.get(table_level, "Table")
        elif pattern == SubTypePattern.ALL_TABLE:
            return "Table"
        elif pattern == SubTypePattern.ALL_VIEW:
            return "View"
        else:
            return "Table"  # default

    def _get_subtypes_aspect(
        self, table_name: str, table_level: int, table_index: int
    ) -> MetadataWorkUnit:
        """
        Create a SubTypes aspect for a table based on deterministic pattern.

        Args:
            table_name: Name of the table
            table_level: Level of the table in the lineage graph
            table_index: Index of the table within its level

        Returns:
            MetadataWorkUnit containing the SubTypes aspect
        """
        # Determine subtype based on pattern
        subtype = self._determine_subtype(table_name, table_level, table_index)

        urn = make_dataset_urn(platform="fake", name=table_name)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            entityType="dataset",
            aspect=SubTypesClass(typeNames=[subtype]),
        )
        return mcp.as_workunit()

    def _data_gen_1(self) -> Iterable[MetadataWorkUnit]:
        """Generate mock lineage data for testing purposes."""
        gen_1 = self.config.gen_1
        fan_out = gen_1.lineage_fan_out
        hops = gen_1.lineage_hops
        fan_out_after_first = gen_1.lineage_fan_out_after_first_hop

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
                table_name = TableNamingHelper.generate_table_name(hops, fan_out, i, j)

                yield self._get_status_aspect(table_name)

                yield self._get_subtypes_aspect(table_name, i, j)

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
        fan_out_after_first: Optional[int],
        tables_at_levels: List[int],
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
        tables_at_levels: List[int],
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
            downstream_table_name = TableNamingHelper.generate_table_name(
                hops, fan_out, downstream_level, downstream_index
            )
            yield self._get_upstream_aspect(
                upstream_table=upstream_table_name,
                downstream_table=downstream_table_name,
            )

    def _get_status_aspect(self, table: str) -> MetadataWorkUnit:
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
