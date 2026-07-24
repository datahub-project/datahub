import logging
from functools import partial
from typing import Dict, Iterable, List, Optional, Set, Type, Union

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.zipline.client import ZiplineRepositoryReader
from datahub.ingestion.source.zipline.config import ZiplineConfig
from datahub.ingestion.source.zipline.constants import PLATFORM_NAME
from datahub.ingestion.source.zipline.lineage import (
    SourceResolver,
    StagingQueryLineageExtractor,
)
from datahub.ingestion.source.zipline.mapper import ZiplineMapper
from datahub.ingestion.source.zipline.models import Join, StagingQuery
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob

logger = logging.getLogger(__name__)

_STAGE_FEATURE_TABLES = "Zipline Feature Table Extraction"
_STAGE_JOINS = "Zipline Join Extraction"
_STAGE_STAGING_QUERIES = "Zipline Staging Query Extraction"


@platform_name("Zipline")
@config_class(ZiplineConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Enabled via platform_instance for backing source datasets and jobs",
)
@capability(
    SourceCapability.TAGS,
    "Extracted from MetaData.customJson when enable_tag_extraction is set",
)
@capability(
    SourceCapability.OWNERSHIP,
    "Extracted via owner_mappings when enable_owner_extraction is set",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class ZiplineSource(StatefulIngestionSourceBase):
    """Extracts ML feature metadata from a compiled Chronon/Zipline repository.

    Reads the thrift-as-JSON output of `compile.py`: GroupBys become feature
    tables, their backing sources become lineage, and Joins/StagingQueries
    become DataJobs.
    """

    config: ZiplineConfig
    report: ZiplineSourceReport

    def __init__(self, config: ZiplineConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.platform = PLATFORM_NAME
        self.report = ZiplineSourceReport()
        self.reader = ZiplineRepositoryReader(config.path, self.report)
        self.source_resolver = SourceResolver(config, self.report)
        self.mapper = ZiplineMapper(config, self.report, self.source_resolver)
        self.staging_lineage = StagingQueryLineageExtractor(
            config, self.report, ctx.graph
        )
        self._emitted_flow_teams: Set[str] = set()
        # Joins embed a nested GroupBy copy that drops `outputNamespace`, so cache
        # each GroupBy's namespaced output table to wire Join inlets correctly.
        self._group_by_output_tables: Dict[str, str] = {}
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.report,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=PLATFORM_NAME,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ZiplineSource":
        config = ZiplineConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(self) -> List[Union[str, Type]]:
        # Stale removal is wired manually with a shared handler, so drop the
        # framework's automatic processor.
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(auto_stale_entity_removal, self.stale_entity_removal_handler),
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if not self.reader.is_valid():
            self.report.failure(
                title="Compiled config directory not found",
                message=(
                    "`path` exists but contains no compiled Zipline output "
                    "(no group_bys/, joins/ or staging_queries/). Point it at the "
                    "`production/` output of compile.py, not the Python config repo."
                ),
                context=self.config.path,
            )
            return

        yield from self._extract_feature_tables()

        if self.config.include_joins:
            yield from self._extract_joins()

        if self.config.include_staging_queries:
            yield from self._extract_staging_queries()

    def _extract_feature_tables(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(_STAGE_FEATURE_TABLES):
            for group_by in self.reader.read_group_bys():
                name = group_by.meta_data.name
                output_table = group_by.meta_data.output_table_name()
                if name is not None and output_table is not None:
                    self._group_by_output_tables[name] = output_table
                if not self._team_allowed(group_by.meta_data.team):
                    self.report.filtered_teams += 1
                    continue
                if name is not None and not self.config.feature_table_pattern.allowed(
                    name
                ):
                    self.report.filtered_feature_tables += 1
                    continue
                yield from self.mapper.map_group_by(group_by)

    def _extract_joins(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(_STAGE_JOINS):
            for join in self.reader.read_joins():
                if not self._team_allowed(join.meta_data.team):
                    self.report.filtered_joins += 1
                    continue
                if join.meta_data.name is None:
                    self.report.warning(
                        title="Join missing name",
                        message="Skipped a compiled Join with no metaData.name — cannot form a DataJob URN",
                        context=join.source_file,
                    )
                    continue
                yield from self._emit_join(join)
                self.report.report_join_scanned()

    def _extract_staging_queries(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(_STAGE_STAGING_QUERIES):
            for staging_query in self.reader.read_staging_queries():
                if not self._team_allowed(staging_query.meta_data.team):
                    self.report.filtered_staging_queries += 1
                    continue
                if staging_query.meta_data.name is None:
                    self.report.warning(
                        title="StagingQuery missing name",
                        message="Skipped a compiled StagingQuery with no metaData.name — cannot form a DataJob URN",
                        context=staging_query.source_file,
                    )
                    continue
                yield from self._emit_staging_query(staging_query)
                self.report.report_staging_query_scanned()

    def _team_allowed(self, team: Optional[str]) -> bool:
        if team is None:
            return True
        return self.config.team_pattern.allowed(team)

    def _get_or_create_flow(self, team: str) -> DataFlow:
        return DataFlow(
            name=team,
            platform=PLATFORM_NAME,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            subtype=DataFlowSubTypes.ZIPLINE_TEAM,
            description=f"Zipline/Chronon jobs owned by team {team}",
        )

    def _emit_flow_once(self, flow: DataFlow) -> Iterable[MetadataWorkUnit]:
        if str(flow.urn) not in self._emitted_flow_teams:
            self._emitted_flow_teams.add(str(flow.urn))
            self.report.teams_scanned += 1
            yield from flow.as_workunits()

    def _emit_join(self, join: Join) -> Iterable[MetadataWorkUnit]:
        name = join.meta_data.name
        team = join.meta_data.team or PLATFORM_NAME
        if name is None:
            return

        flow = self._get_or_create_flow(team)
        yield from self._emit_flow_once(flow)

        inlets: List[str] = []
        if join.left is not None:
            inlets.extend(self.source_resolver.resolve_source_urns(join.left))
        for join_part in join.join_parts:
            part_name = join_part.group_by.meta_data.name
            group_by_table = (
                self._group_by_output_tables.get(part_name or "")
                or join_part.group_by.meta_data.output_table_name()
            )
            if group_by_table:
                inlets.append(self.source_resolver.resolve_table_urn(group_by_table))
            else:
                self.report.unresolved_join_part_inlets += 1
                self.report.warning(
                    title="Unresolved join-part inlet",
                    message="A Join part's GroupBy has no resolvable output table; its upstream edge was omitted",
                    context=f"{name} (part: {part_name})",
                )

        outlets: List[str] = []
        output_table = join.meta_data.output_table_name()
        if output_table:
            outlets.append(self.source_resolver.resolve_table_urn(output_table))

        datajob = DataJob(
            name=name,
            flow=flow,
            display_name=name,
            description=join.meta_data.description,
            subtype=DataJobSubTypes.ZIPLINE_JOIN,
            inlets=list(dict.fromkeys(inlets)) or None,
            outlets=list(dict.fromkeys(outlets)) or None,
        )
        yield from datajob.as_workunits()

    def _emit_staging_query(
        self, staging_query: StagingQuery
    ) -> Iterable[MetadataWorkUnit]:
        name = staging_query.meta_data.name
        team = staging_query.meta_data.team or PLATFORM_NAME
        if name is None:
            return

        flow = self._get_or_create_flow(team)
        yield from self._emit_flow_once(flow)

        inlets: List[str] = []
        if self.config.include_staging_query_lineage and staging_query.query:
            inlets.extend(
                self.staging_lineage.extract_input_urns(staging_query.query, name)
            )

        outlets: List[str] = []
        output_table = staging_query.meta_data.output_table_name()
        if output_table:
            outlets.append(self.source_resolver.resolve_table_urn(output_table))

        datajob = DataJob(
            name=name,
            flow=flow,
            display_name=name,
            description=staging_query.meta_data.description,
            subtype=DataJobSubTypes.ZIPLINE_STAGING_QUERY,
            inlets=list(dict.fromkeys(inlets)) or None,
            outlets=list(dict.fromkeys(outlets)) or None,
        )
        yield from datajob.as_workunits()

    def get_report(self) -> ZiplineSourceReport:
        return self.report
