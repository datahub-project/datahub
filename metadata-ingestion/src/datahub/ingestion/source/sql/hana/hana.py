"""
SAP HANA source for DataHub.

This module provides a DataHub source for extracting metadata from SAP HANA databases,
including tables, views, calculation views, and their lineage information.
"""

import logging
from typing import Iterable

from sqlalchemy import create_engine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema_gen import HanaSchemaGenerator
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


@platform_name("SAP HANA", id="hana")
@config_class(HanaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class HanaSource(SQLAlchemySource):
    """
    DataHub source for SAP HANA database.

    This source supports ingestion of:
    - Tables and their schemas
    - Views and their definitions
    - SAP HANA Calculation Views with lineage
    - Column-level metadata and lineage
    """

    config: HanaConfig
    report: SQLSourceReport

    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        """Initialize the SAP HANA source.

        Args:
            config: Configuration for the HANA source
            ctx: Pipeline context
        """
        super().__init__(config, ctx, self.get_platform())
        self.config = config
        self.engine = self._create_engine()

        # Initialize components
        self.identifiers = HanaIdentifierBuilder(self.config)
        self.data_dictionary = HanaDataDictionary(self.engine, self.report)

        # Initialize domain registry if configured
        self.domain_registry = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=ctx.graph
            )

        # Initialize schema generator
        self.schema_generator = HanaSchemaGenerator(
            config=self.config,
            report=self.report,
            data_dictionary=self.data_dictionary,
            identifiers=self.identifiers,
            domain_registry=self.domain_registry,
        )

    def get_platform(self) -> str:
        """Get the platform name for this source."""
        return "hana"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "HanaSource":
        """Create a new HANA source instance."""
        config = HanaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _create_engine(self):
        """Create SQLAlchemy engine for HANA connection."""
        url = self.config.get_sql_alchemy_url()
        return create_engine(url)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate work units for all HANA objects.

        This is the main method that orchestrates the ingestion process:
        1. Processes databases and schemas
        2. Processes regular tables and views
        3. Processes SAP HANA calculation views

        Yields:
            MetadataWorkUnit objects for all discovered entities
        """
        yield from self.schema_generator.get_workunits_internal()

    def get_report(self) -> SourceReport:
        """Get the source report.

        Returns:
            SourceReport with ingestion statistics
        """
        return self.report

    def close(self) -> None:
        """Clean up resources."""
        if hasattr(self, "engine"):
            self.engine.dispose()
        super().close()
