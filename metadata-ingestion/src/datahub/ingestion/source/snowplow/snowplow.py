"""
Snowplow source for DataHub.

Extracts metadata from Snowplow:
- Event and entity schemas from BDP Console API or Iglu Registry
- Event specifications (BDP only)
- Tracking scenarios (BDP only)
- Lineage from warehouse atomic events table (optional)

Supports both:
- Snowplow BDP (managed) deployments
- Open-source Snowplow with Iglu registry
"""

import logging
from typing import Any, Dict, Iterable, Optional

from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
)
from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.ingestion.source.snowplow.builders.ownership_builder import (
    OwnershipBuilder,
)
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.constants import (
    PLATFORM_NAME,
    ConnectionMode,
)
from datahub.ingestion.source.snowplow.dependencies import (
    IngestionState,
    ProcessorDependencies,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    Enrichment,
)
from datahub.ingestion.source.snowplow.processors.data_product_processor import (
    DataProductProcessor,
)
from datahub.ingestion.source.snowplow.processors.event_spec_processor import (
    EventSpecProcessor,
)
from datahub.ingestion.source.snowplow.processors.pipeline_processor import (
    PipelineProcessor,
)
from datahub.ingestion.source.snowplow.processors.schema_processor import (
    SchemaProcessor,
)
from datahub.ingestion.source.snowplow.processors.standard_schema_processor import (
    StandardSchemaProcessor,
)
from datahub.ingestion.source.snowplow.processors.tracking_scenario_processor import (
    TrackingScenarioProcessor,
)
from datahub.ingestion.source.snowplow.processors.warehouse_lineage_processor import (
    WarehouseLineageProcessor,
)
from datahub.ingestion.source.snowplow.services.atomic_event_builder import (
    AtomicEventBuilder,
)
from datahub.ingestion.source.snowplow.services.column_lineage_builder import (
    ColumnLineageBuilder,
)
from datahub.ingestion.source.snowplow.services.data_structure_builder import (
    DataStructureBuilder,
)
from datahub.ingestion.source.snowplow.services.deployment_fetcher import (
    DeploymentFetcher,
)
from datahub.ingestion.source.snowplow.services.enrichment_registry_factory import (
    EnrichmentRegistryFactory,
)
from datahub.ingestion.source.snowplow.services.error_handler import ErrorHandler
from datahub.ingestion.source.snowplow.services.field_tagging import (
    FieldTagger,
)
from datahub.ingestion.source.snowplow.services.iglu_client import IgluClient
from datahub.ingestion.source.snowplow.services.property_manager import PropertyManager
from datahub.ingestion.source.snowplow.services.user_resolver import UserResolver
from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport
from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


@platform_name("Snowplow")
@config_class(SnowplowSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via configuration")
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Enabled by default for event and entity schemas",
)
@capability(
    SourceCapability.DESCRIPTIONS, "Enabled by default from schema descriptions"
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Optionally enabled via warehouse_lineage.enabled configuration (requires BDP)",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class SnowplowSource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingests metadata from Snowplow.

    Extracts:
    - Organizations (as containers)
    - Event schemas (as datasets)
    - Entity schemas (as datasets)
    - Event specifications (as datasets) - BDP only
    - Tracking scenarios (as containers) - BDP only
    - Warehouse lineage (optional) - requires warehouse connection

    Supports:
    - Snowplow BDP (Behavioral Data Platform) deployments
    - Open-source Snowplow with Iglu registry
    """

    config: SnowplowSourceConfig
    report: SnowplowSourceReport
    platform = PLATFORM_NAME

    def __init__(self, config: SnowplowSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = SnowplowSourceReport()

        # Initialize API clients
        self.bdp_client: Optional[SnowplowBDPClient] = None
        self.iglu_client: Optional[IgluClient] = None

        if config.bdp_connection:
            self.bdp_client = SnowplowBDPClient(
                config.bdp_connection, report=self.report
            )
            self.report.connection_mode = ConnectionMode.BDP.value
            self.report.organization_id = config.bdp_connection.organization_id

        if config.iglu_connection:
            self.iglu_client = IgluClient(config.iglu_connection, report=self.report)
            if self.report.connection_mode == ConnectionMode.BDP.value:
                self.report.connection_mode = ConnectionMode.BOTH.value
            else:
                self.report.connection_mode = ConnectionMode.IGLU.value

        # Initialize user resolver for ownership resolution
        self.user_resolver = UserResolver(self.bdp_client, self.report)
        self.user_resolver.load_users()

        # Initialize stale entity removal handler
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

        # Domain registry (optional)
        self.domain_registry: Optional[DomainRegistry] = None
        if hasattr(self.config, "domain") and self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        # Initialize enrichment lineage registry
        self.enrichment_lineage_registry = EnrichmentRegistryFactory.create_registry()

        # Initialize field tagger
        self.field_tagger = FieldTagger(self.config.field_tagging)

        # Initialize URN factory for centralized URN construction
        self.urn_factory = SnowplowURNFactory(
            platform=self.platform,
            config=self.config,
        )

        # Initialize cache manager for performance optimization
        self.cache = CacheManager()

        # Initialize builders for metadata construction
        self.ownership_builder = OwnershipBuilder(
            config=self.config,
            user_cache=self.user_resolver.get_user_cache(),
            user_name_cache=self.user_resolver.get_user_name_cache(),
        )
        self.lineage_builder = LineageBuilder()

        # Initialize error handler for consistent error handling
        self.error_handler = ErrorHandler(report=self.report)

        # Initialize property manager for structured properties
        self.property_manager = PropertyManager(config=self.config)

        # Create mutable ingestion state (populated during extraction)
        # This is separate from deps to maintain clean separation between
        # immutable config and mutable runtime data
        self.state = IngestionState()

        # Initialize column lineage builder
        self.column_lineage_builder = ColumnLineageBuilder(
            config=self.config,
            urn_factory=self.urn_factory,
            state=self.state,
        )

        # Initialize atomic event builder
        self.atomic_event_builder = AtomicEventBuilder(
            config=self.config,
            urn_factory=self.urn_factory,
            state=self.state,
            platform=self.platform,
        )

        # Initialize deployment fetcher service
        self.deployment_fetcher = DeploymentFetcher(
            bdp_client=self.bdp_client,
            enable_parallel=self.config.performance.enable_parallel_fetching,
            max_workers=self.config.performance.max_concurrent_api_calls,
            report=self.report,
        )

        # Create processor dependencies (dependency injection)
        # Immutable dependencies only - state is managed separately
        self.deps = ProcessorDependencies(
            config=self.config,
            report=self.report,
            cache=self.cache,
            platform=self.platform,
            urn_factory=self.urn_factory,
            ownership_builder=self.ownership_builder,
            lineage_builder=self.lineage_builder,
            enrichment_lineage_registry=self.enrichment_lineage_registry,
            error_handler=self.error_handler,
            field_tagger=self.field_tagger,
            bdp_client=self.bdp_client,
            iglu_client=self.iglu_client,
            graph=self.ctx.graph if hasattr(self.ctx, "graph") else None,
            deployment_fetcher=self.deployment_fetcher,
        )

        # Initialize data structure builder (used by SchemaProcessor)
        self.data_structure_builder = DataStructureBuilder(
            deps=self.deps,
            state=self.state,
            column_lineage_builder=self.column_lineage_builder,
        )

        # Initialize processors for entity extraction
        # Processors receive both immutable deps and mutable state
        self.schema_processor = SchemaProcessor(
            deps=self.deps,
            state=self.state,
            data_structure_builder=self.data_structure_builder,
        )
        self.pipeline_processor = PipelineProcessor(deps=self.deps, state=self.state)
        self.event_spec_processor = EventSpecProcessor(deps=self.deps, state=self.state)
        self.tracking_scenario_processor = TrackingScenarioProcessor(
            deps=self.deps, state=self.state
        )
        self.data_product_processor = DataProductProcessor(
            deps=self.deps, state=self.state
        )
        self.standard_schema_processor = StandardSchemaProcessor(
            deps=self.deps, state=self.state
        )
        self.warehouse_lineage_processor = WarehouseLineageProcessor(
            deps=self.deps, state=self.state
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SnowplowSource":
        config = SnowplowSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main extraction logic.

        Order:
        0. Register structured property definitions (if using structured properties)
        1. Emit organization container
        2. Create atomic event dataset (synthetic schema for enrichment lineage)
        3. Extract event and entity schemas
        4. Extract event specifications (BDP only)
        5. Extract tracking scenarios (BDP only)
        6. Extract warehouse lineage (optional)
        """

        # Register structured property definitions if enabled
        # Use synchronous registration via graph client to ensure definitions are
        # committed before field property assignments are validated in the same batch
        if self.config.field_tagging.use_structured_properties:
            if self.ctx.graph:
                # Sync registration ensures definitions exist before assignments
                self.property_manager.register_structured_properties_sync(
                    self.ctx.graph
                )
            else:
                # Fall back to workunit emission (may fail with batch validation)
                logger.warning(
                    "Graph client not available for synchronous property registration. "
                    "Structured property definitions will be emitted as workunits. "
                    "If validation fails, pre-register properties with: "
                    "datahub properties upsert -f snowplow_field_structured_properties.yaml"
                )
                yield from self.property_manager.register_structured_properties()

        # Emit organization container
        if self.bdp_client and self.config.bdp_connection:
            yield from self._process_organization()

        # Create atomic event dataset (for enrichment lineage)
        yield from self.atomic_event_builder.create_atomic_event_dataset()

        # Extract PII fields from enrichments BEFORE processing schemas
        # This ensures PII tags are available during schema field tagging
        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.use_pii_enrichment
        ):
            logger.info("Extracting PII fields from enrichment configuration")
            pii_fields = self._extract_pii_fields()
            if pii_fields:
                logger.info(f"Found {len(pii_fields)} PII fields from enrichments")
            else:
                logger.warning(
                    "No PII fields found in enrichment configuration. "
                    "Field tagging will fall back to pattern matching."
                )

        # Extract schemas (via processor)
        if self.schema_processor.is_enabled():
            yield from self.schema_processor.extract()

        # Extract event specifications (via processor) - MUST be before parsed events dataset creation
        # so we can capture the event spec ID and name for dataset naming
        if self.event_spec_processor.is_enabled():
            yield from self.event_spec_processor.extract()

        # Extract Snowplow standard schemas (via processor) - MUST be after event specs
        # so we have collected all referenced Iglu URIs from event specifications
        if self.standard_schema_processor.is_enabled():
            yield from self.standard_schema_processor.extract()

        # Emit schema metadata for event specs now that ALL schemas (custom + standard) are extracted
        # This is a second pass that adds complete field information to event specs
        if self.event_spec_processor.is_enabled():
            yield from self.event_spec_processor.emit_event_spec_schema_metadata()

        # Event Specs now serve as the datasets that enrichments read from (Option A architecture)
        # No need for separate Parsed Events dataset - Event Spec IS the parsed events

        # Extract tracking scenarios (via processor)
        if self.tracking_scenario_processor.is_enabled():
            yield from self.tracking_scenario_processor.extract()

        # Extract data products (via processor)
        if self.data_product_processor.is_enabled():
            yield from self.data_product_processor.extract()

        # Extract pipelines and enrichments (via processor)
        if self.pipeline_processor.is_enabled():
            yield from self.pipeline_processor.extract()

        # Extract warehouse lineage (via processor)
        if self.warehouse_lineage_processor.is_enabled():
            yield from self.warehouse_lineage_processor.extract()

        # Compute final statistics
        self.report.compute_stats()

    def _process_organization(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit organization as container.

        Organizations group all schemas and event specifications.
        """
        if not self.config.bdp_connection:
            return

        org_id = self.config.bdp_connection.organization_id

        # Create organization container key
        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Use gen_containers to emit all container aspects properly
        yield from gen_containers(
            container_key=org_key,
            name=f"Snowplow Organization ({org_id})",
            sub_types=[DatasetContainerSubTypes.DATABASE],
            description="Snowplow BDP organization containing event and entity schemas",
            extra_properties={
                "organization_id": org_id,
                "platform": "snowplow",
                "connection_mode": self.report.connection_mode,
            },
        )

    def _extract_pii_fields(self) -> set:
        """
        Extract PII fields from PII Pseudonymization enrichment configuration.

        Returns a set of field names that are configured as PII in the
        enrichment config. This is more accurate than pattern matching alone.

        Returns:
            Set of field names that should be classified as PII
        """
        # Return cached value if available
        if self.state.pii_fields_cache is not None:
            return self.state.pii_fields_cache

        pii_fields: set = set()

        # Guard clauses for early exit
        if not self.config.field_tagging.use_pii_enrichment or not self.bdp_client:
            self.state.pii_fields_cache = pii_fields
            return pii_fields

        try:
            pipelines = self.bdp_client.get_pipelines()
            for pipeline in pipelines:
                self._extract_pii_fields_from_pipeline(pipeline.id, pii_fields)
        except Exception as e:
            logger.warning(f"Failed to extract PII fields from enrichments: {e}")

        self.state.pii_fields_cache = pii_fields
        return pii_fields

    def _extract_pii_fields_from_pipeline(
        self, pipeline_id: str, pii_fields: set
    ) -> None:
        """Extract PII fields from a single pipeline's enrichments."""
        assert self.bdp_client is not None  # Caller ensures this
        try:
            enrichments = self.bdp_client.get_enrichments(pipeline_id)
            for enrichment in enrichments:
                self._extract_pii_fields_from_enrichment(enrichment, pii_fields)
        except Exception as e:
            logger.warning(
                f"Failed to extract PII fields from pipeline {pipeline_id}: {e}"
            )

    def _extract_pii_fields_from_enrichment(
        self, enrichment: Enrichment, pii_fields: set
    ) -> None:
        """Extract PII fields from a PII Pseudonymization enrichment."""
        # Skip non-PII enrichments
        if not enrichment.schema_ref:
            return
        if "pii_pseudonymization" not in enrichment.schema_ref.lower():
            return

        # Get parameters from enrichment content
        params = self._get_enrichment_parameters(enrichment)
        if not params or "pii" not in params:
            return

        pii_config = params["pii"]
        self._parse_pii_config(pii_config, pii_fields)

    @staticmethod
    def _get_enrichment_parameters(enrichment: Enrichment) -> Optional[Dict[str, Any]]:
        """Safely extract parameters from enrichment content."""
        if not enrichment.content:
            return None
        if not enrichment.content.data:
            return None
        return enrichment.content.data.parameters

    @staticmethod
    def _parse_pii_config(pii_config: Any, pii_fields: set) -> None:
        """Parse PII configuration and add field names to the set."""
        # Format 1: List of field configurations with "fieldName" key
        if isinstance(pii_config, list):
            for field_config in pii_config:
                if isinstance(field_config, dict) and "fieldName" in field_config:
                    pii_fields.add(field_config["fieldName"])
            return

        # Format 2: Dict with "fieldNames" key containing a list
        if isinstance(pii_config, dict) and "fieldNames" in pii_config:
            field_names = pii_config["fieldNames"]
            if isinstance(field_names, list):
                pii_fields.update(field_names)

    def get_report(self) -> SnowplowSourceReport:
        return self.report

    def close(self) -> None:
        """Clean up resources after ingestion completes."""
        # Clean up FileBackedDict SQLite resources in IngestionState
        self.state.close()
        super().close()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Snowplow APIs."""
        test_report = TestConnectionReport()

        # Initialize capability_report if None
        if test_report.capability_report is None:
            test_report.capability_report = {}

        try:
            config = SnowplowSourceConfig.model_validate(config_dict)

            # Test BDP connection
            if config.bdp_connection:
                try:
                    bdp_client = SnowplowBDPClient(config.bdp_connection)
                    if bdp_client.test_connection():
                        test_report.basic_connectivity = CapabilityReport(capable=True)
                        test_report.capability_report[
                            SourceCapability.SCHEMA_METADATA
                        ] = CapabilityReport(capable=True)
                    else:
                        test_report.basic_connectivity = CapabilityReport(
                            capable=False, failure_reason="BDP API connection failed"
                        )
                except Exception as e:
                    test_report.basic_connectivity = CapabilityReport(
                        capable=False, failure_reason=f"BDP connection error: {e}"
                    )

            # Test Iglu connection
            if config.iglu_connection:
                try:
                    iglu_client = IgluClient(config.iglu_connection)
                    if iglu_client.test_connection():
                        test_report.basic_connectivity = CapabilityReport(capable=True)
                        test_report.capability_report[
                            SourceCapability.SCHEMA_METADATA
                        ] = CapabilityReport(capable=True)
                    else:
                        test_report.basic_connectivity = CapabilityReport(
                            capable=False, failure_reason="Iglu connection failed"
                        )
                except Exception as e:
                    test_report.basic_connectivity = CapabilityReport(
                        capable=False, failure_reason=f"Iglu connection error: {e}"
                    )

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Configuration error: {e}"
            )

        return test_report
