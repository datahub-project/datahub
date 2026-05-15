"""DataHub ingestion source for SAP HANA databases."""

import logging
from typing import Iterable, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

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
from datahub.ingestion.source.sql.hana.hana_schema_gen import (
    HanaCalculationViewExtractor,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource, SqlWorkUnit
from datahub.ingestion.source_report.ingestion_stage import (
    LINEAGE_EXTRACTION,
    METADATA_EXTRACTION,
)

logger = logging.getLogger(__name__)


@platform_name("SAP HANA", id="hana")
@config_class(HanaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emitted for calculation views when `include_calculation_views` is enabled",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage emitted for calculation views when "
    "`include_calculation_views` is enabled",
)
class HanaSource(SQLAlchemySource):
    """DataHub source for SAP HANA.

    Inherits regular table / view extraction, profiling, descriptions,
    classification, container hierarchy, stateful ingestion, and test
    connection from :class:`SQLAlchemySource`. On top of that, when
    ``include_calculation_views`` is enabled, the source reads activated
    calculation views from ``_SYS_REPO.ACTIVE_OBJECT`` and emits
    column-level lineage from their XML definitions.
    """

    config: HanaConfig

    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())
        self.identifiers = HanaIdentifierBuilder(config)
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

    def _generate_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        # The default implementation would emit aggregator workunits as soon
        # as ``super().get_workunits_internal()`` finishes its schema pass.
        # We delay that emission so the calculation-view extractor can first
        # add its column-level lineage; the explicit super-call below in
        # :meth:`get_workunits_internal` flushes everything in one batch.
        return iter([])

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # Schema pass populates the aggregator's resolver with every regular
        # table and view via the standard SQLAlchemy reflection path.
        with self.report.new_stage(METADATA_EXTRACTION):
            yield from super().get_workunits_internal()

        if self.calc_view_extractor is None:
            yield from super()._generate_aggregator_workunits()
            return

        with self.report.new_stage(LINEAGE_EXTRACTION):
            yield from self.calc_view_extractor.get_workunits_internal()
            yield from super()._generate_aggregator_workunits()
