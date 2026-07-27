import logging
from typing import Iterable, Iterator, List, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema_gen import (
    HanaCalculationViewExtractor,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource, SqlWorkUnit
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.ingestion.source_report.ingestion_stage import (
    LINEAGE_EXTRACTION,
    METADATA_EXTRACTION,
    QUERIES_EXTRACTION,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


@platform_name("SAP HANA", id="hana")
@config_class(HanaConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Optionally enabled via `include_query_usage` (queries) and "
    "`include_usage_stats` (rollup) — mines "
    "`_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`",
)
@capability(
    SourceCapability.OPERATION_CAPTURE,
    "Derived from observed queries when `include_operational_stats` "
    "and `include_query_usage` are enabled",
)
class HanaSource(SQLAlchemySource):
    """DataHub source for SAP HANA.

    Inherits regular table/view extraction, profiling, descriptions,
    classification, container hierarchy, stateful ingestion, and test
    connection from :class:`SQLAlchemySource`. Additional behaviour:

    - When ``include_stored_procedures`` is true (default), HANA procedures
      are emitted as ``DataJob`` entities with parsed lineage.
    - When ``include_calculation_views`` is true, activated calc views are
      read from ``_SYS_REPO.ACTIVE_OBJECT`` and emitted with column-level
      lineage parsed from their XML definitions.
    - When ``include_query_usage`` is true, observed queries are mined
      from ``_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`` and fed through the
      SQL parsing aggregator (driving query-level lineage, usage roll-up
      when ``include_usage_stats`` is set, and operations when
      ``include_operational_stats`` is set).
    """

    config: HanaConfig

    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())
        self.identifiers = HanaIdentifierBuilder(config)

        # Replace the parent's aggregator with one that has usage and
        # operations hooks wired in. ``SQLAlchemySource.__init__`` always
        # constructs a usage-disabled aggregator
        self.aggregator = SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_lineage=self.include_lineage,
            generate_usage_statistics=self.config.include_usage_stats,
            generate_operations=(
                self.config.include_operational_stats
                and self.config.include_query_usage
            ),
            usage_config=(self.config if self.config.include_usage_stats else None),
            eager_graph_load=False,
        )
        self.report.sql_aggregator = self.aggregator.report

        self.calc_view_extractor: Optional[HanaCalculationViewExtractor] = None
        if config.include_calculation_views:
            self.calc_view_extractor = HanaCalculationViewExtractor(
                config=config,
                report=self.report,
                identifiers=self.identifiers,
                engine_factory=self._build_engine,
                aggregator=self.aggregator,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return "hana"

    def _build_engine(self) -> Engine:
        return create_engine(self.config.get_sql_alchemy_url(), **self.config.options)

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        with inspector.engine.connect() as conn:
            data_dict = HanaDataDictionary(conn, self.report)
            return list(data_dict.get_stored_procedures(schema))

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Suppress the base class's eager flush. We re-emit explicitly in
        # ``get_workunits_internal`` after the calc-view extractor and
        # query-usage extractor have had a chance to add lineage.
        return iter([])

    def _extract_queries_from_plan_cache(
        self, engine: Engine
    ) -> Iterator[ObservedQuery]:
        """Yield ``ObservedQuery`` per row from ``HOST_SQL_PLAN_CACHE``.

        Each row is one ``(statement_hash, last_execution_timestamp)``
        observation — i.e. one moment a cached plan was executed within
        the configured window. ``usage_multiplier`` stays at the default
        of 1; we knowingly trade absolute execution-count accuracy for
        the simpler "one observation per execution event" semantics
        (Oracle's V$SQL path makes the same trade-off). The aggregator
        attributes each observation to the bucket containing
        ``last_execution_timestamp``.
        """
        with engine.connect() as conn:
            data_dict = HanaDataDictionary(conn, self.report)
            for row in data_dict.iter_observed_queries(
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                top_n=self.config.usage_max_queries,
            ):
                yield ObservedQuery(
                    query=row.statement_string,
                    session_id=f"hana_stmt:{row.statement_hash}",
                    timestamp=row.last_execution_timestamp,
                    user=CorpUserUrn(row.user_name) if row.user_name else None,
                    default_db=None,
                    default_schema=row.schema_name,
                )

    def _populate_aggregator_from_queries(self) -> None:
        if not self.config.include_query_usage:
            return

        engine: Optional[Engine] = None
        try:
            engine = self._build_engine()
            with self.report.new_stage(QUERIES_EXTRACTION):
                logger.info(
                    "Extracting observed queries from "
                    "_SYS_STATISTICS.HOST_SQL_PLAN_CACHE "
                    "(top_n=%s, window=[%s, %s])",
                    self.config.usage_max_queries,
                    self.config.start_time,
                    self.config.end_time,
                )
                count = 0
                for observed in self._extract_queries_from_plan_cache(engine):
                    try:
                        self.aggregator.add(observed)
                        count += 1
                    except Exception as e:
                        self.report.warning(
                            title="Failed to add observed query to aggregator",
                            message="One HANA query could not be ingested "
                            "into the SQL parsing aggregator.",
                            context=observed.session_id,
                            exc=e,
                        )
                logger.info(
                    "Added %s observed HANA queries to the SQL aggregator",
                    count,
                )
        except Exception as e:
            self.report.warning(
                title="Query usage extraction unavailable",
                message="Could not connect to HANA or initialise the query "
                "usage extractor. Metadata ingestion will complete without "
                "usage / operational aspects.",
                exc=e,
            )
        finally:
            if engine is not None:
                engine.dispose()

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        with self.report.new_stage(METADATA_EXTRACTION):
            yield from super().get_workunits_internal()

        # Observed queries must be added to the aggregator *before* its
        # final flush, so the resolver sees their lineage and the usage
        # roll-up gets the right time-bucketed counts.
        self._populate_aggregator_from_queries()

        if self.calc_view_extractor is not None:
            with self.report.new_stage(LINEAGE_EXTRACTION):
                yield from self.calc_view_extractor.get_workunits_internal()

        # Final aggregator flush runs after all add_known_query_lineage /
        # add(ObservedQuery) calls so usage + lineage land in one rollup.
        yield from super()._generate_aggregator_workunits()
