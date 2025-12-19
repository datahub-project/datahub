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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.ingestion.source.snowplow.builders.ownership_builder import (
    OwnershipBuilder,
)
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.constants import (
    ConnectionMode,
    SchemaType,
    infer_schema_type,
)
from datahub.ingestion.source.snowplow.container_keys import (
    SnowplowOrganizationKey,
)
from datahub.ingestion.source.snowplow.dependencies import (
    IngestionState,
    ProcessorDependencies,
)
from datahub.ingestion.source.snowplow.field_tagging import (
    FieldTagger,
)
from datahub.ingestion.source.snowplow.iglu_client import IgluClient
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
from datahub.ingestion.source.snowplow.schema_parser import SnowplowSchemaParser
from datahub.ingestion.source.snowplow.services.atomic_event_builder import (
    AtomicEventBuilder,
)
from datahub.ingestion.source.snowplow.services.column_lineage_builder import (
    ColumnLineageBuilder,
)
from datahub.ingestion.source.snowplow.services.data_structure_builder import (
    DataStructureBuilder,
)
from datahub.ingestion.source.snowplow.services.enrichment_registry_factory import (
    EnrichmentRegistryFactory,
)
from datahub.ingestion.source.snowplow.services.error_handler import ErrorHandler
from datahub.ingestion.source.snowplow.services.property_manager import PropertyManager
from datahub.ingestion.source.snowplow.services.user_resolver import UserResolver
from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructure,
    IgluSchema,
)
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport
from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sdk.dataset import Dataset
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.sentinels import unset

logger = logging.getLogger(__name__)


# Constants
WAREHOUSE_PLATFORM_MAP = {
    "snowflake": "snowflake",
    "snowflake_db": "snowflake",  # Common variation
    "bigquery": "bigquery",
    "bigquery_enterprise": "bigquery",  # Common variation
    "redshift": "redshift",
    "red_shift": "redshift",  # Common variation
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",  # Common variation
}

# Standard Snowplow atomic event columns
# Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
SNOWPLOW_STANDARD_COLUMNS = [
    "app_id",
    "platform",
    "etl_tstamp",
    "collector_tstamp",
    "dvce_created_tstamp",
    "event",
    "event_id",
    "txn_id",
    "name_tracker",
    "v_tracker",
    "v_collector",
    "v_etl",
    "user_id",
    "user_ipaddress",
    "user_fingerprint",
    "domain_userid",
    "domain_sessionidx",
    "network_userid",
    "geo_country",
    "geo_region",
    "geo_city",
    "geo_zipcode",
    "geo_latitude",
    "geo_longitude",
    "geo_region_name",
    "ip_isp",
    "ip_organization",
    "ip_domain",
    "ip_netspeed",
    "page_url",
    "page_title",
    "page_referrer",
    "page_urlscheme",
    "page_urlhost",
    "page_urlport",
    "page_urlpath",
    "page_urlquery",
    "page_urlfragment",
    "refr_urlscheme",
    "refr_urlhost",
    "refr_urlport",
    "refr_urlpath",
    "refr_urlquery",
    "refr_urlfragment",
    "refr_medium",
    "refr_source",
    "refr_term",
    "mkt_medium",
    "mkt_source",
    "mkt_term",
    "mkt_content",
    "mkt_campaign",
    "se_category",
    "se_action",
    "se_label",
    "se_property",
    "se_value",
    "tr_orderid",
    "tr_affiliation",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "tr_city",
    "tr_state",
    "tr_country",
    "ti_orderid",
    "ti_sku",
    "ti_name",
    "ti_category",
    "ti_price",
    "ti_quantity",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "useragent",
    "br_name",
    "br_family",
    "br_version",
    "br_type",
    "br_renderengine",
    "br_lang",
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears",
    "br_features_silverlight",
    "br_cookies",
    "br_colordepth",
    "br_viewwidth",
    "br_viewheight",
    "os_name",
    "os_family",
    "os_manufacturer",
    "os_timezone",
    "dvce_type",
    "dvce_ismobile",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_charset",
    "doc_width",
    "doc_height",
    "tr_currency",
    "tr_total_base",
    "tr_tax_base",
    "tr_shipping_base",
    "ti_currency",
    "ti_price_base",
    "base_currency",
    "geo_timezone",
    "mkt_clickid",
    "mkt_network",
    "etl_tags",
    "dvce_sent_tstamp",
    "refr_domain_userid",
    "refr_dvce_tstamp",
    "domain_sessionid",
    "derived_tstamp",
    "event_vendor",
    "event_name",
    "event_format",
    "event_version",
    "event_fingerprint",
    "true_tstamp",
]


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
    platform = "snowplow"

    def __init__(self, config: SnowplowSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = SnowplowSourceReport()

        # Initialize API clients
        self.bdp_client: Optional[SnowplowBDPClient] = None
        self.iglu_client: Optional[IgluClient] = None

        if config.bdp_connection:
            self.bdp_client = SnowplowBDPClient(config.bdp_connection)
            self.report.connection_mode = ConnectionMode.BDP.value
            self.report.organization_id = config.bdp_connection.organization_id

        if config.iglu_connection:
            self.iglu_client = IgluClient(config.iglu_connection)
            if self.report.connection_mode == ConnectionMode.BDP.value:
                self.report.connection_mode = ConnectionMode.BOTH.value
            else:
                self.report.connection_mode = ConnectionMode.IGLU.value

        # Initialize user resolver for ownership resolution
        self.user_resolver = UserResolver(self.bdp_client)
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
        if self.config.field_tagging.use_structured_properties:
            logger.info("Registering Snowplow field structured property definitions")
            yield from self.property_manager.register_structured_properties()

            # Note: If properties fail to apply, ensure structured properties are registered
            # before running ingestion:
            #   datahub properties upsert -f snowplow_field_structured_properties.yaml

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

    def _get_data_structures_filtered(self) -> List[DataStructure]:
        """
        Get data structures from BDP with pagination and timestamp filtering.

        Performance optimizations (Phase 1):
        - Caching: Returns cached data if available (prevents redundant API calls)
        - Parallel fetching: Fetches deployments concurrently when enabled

        Returns:
            List of data structures, filtered by deployed_since if configured
        """
        if not self.bdp_client:
            return []

        # Check cache first (Phase 1 optimization)
        cached_data_structures = self.cache.get("data_structures")
        if cached_data_structures is not None:
            logger.info(
                f"Using cached data structures ({len(cached_data_structures)} schemas)"
            )
            return cached_data_structures

        # Fetch all data structures with pagination
        data_structures = self._fetch_all_data_structures_main()
        if not data_structures:
            return []

        # Fetch deployment history if needed
        if self.config.field_tagging.track_field_versions:
            self._fetch_deployments_main(data_structures)

        # Apply filters
        data_structures = self._filter_by_deployed_since_main(data_structures)
        data_structures = self._filter_by_schema_pattern_main(data_structures)

        # Build field version mappings if field tagging enabled
        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.track_field_versions
            and self.config.field_tagging.tag_schema_version
        ):
            logger.info("Building field version mappings for description updates")
            for data_structure in data_structures:
                schema_key = f"{data_structure.vendor}/{data_structure.name}"
                field_version_map = self._build_field_version_mapping(data_structure)
                if field_version_map:
                    self.state.field_version_cache[schema_key] = field_version_map
                    logger.debug(
                        f"Mapped {len(field_version_map)} fields for {schema_key}"
                    )

        # Cache the result (Phase 1 optimization)
        self.cache.set("data_structures", data_structures)
        logger.debug(f"Cached {len(data_structures)} data structures for reuse")

        return data_structures

    def _fetch_all_data_structures_main(self) -> List[DataStructure]:
        """Fetch all data structures with error handling."""
        if not self.bdp_client:
            return []
        try:
            return self.bdp_client.get_data_structures(
                page_size=self.config.schema_page_size
            )
        except Exception as e:
            self.report.report_failure(
                title="Failed to fetch data structures",
                message="Unable to retrieve schemas from BDP API. Check API credentials and network connectivity.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            return []

    def _fetch_deployments_main(self, data_structures: List[DataStructure]) -> None:
        """Fetch deployment history for all schemas (parallel or sequential)."""
        logger.info(
            "Field version tracking enabled - fetching full deployment history for all schemas"
        )

        schemas_needing_deployments = [ds for ds in data_structures if ds.hash]

        if (
            self.config.performance.enable_parallel_fetching
            and len(schemas_needing_deployments) > 1
        ):
            self._fetch_deployments_parallel_main(schemas_needing_deployments)
        else:
            self._fetch_deployments_sequential_main(schemas_needing_deployments)

    def _fetch_deployments_parallel_main(self, schemas: List[DataStructure]) -> None:
        """Fetch deployments in parallel using ThreadPoolExecutor."""
        max_workers = self.config.performance.max_concurrent_api_calls
        logger.info(
            f"Fetching deployments in parallel (max_workers={max_workers}) "
            f"for {len(schemas)} schemas"
        )

        def fetch_deployments_for_schema(
            ds: DataStructure,
        ) -> Tuple[DataStructure, Optional[Exception]]:
            """Thread-safe deployment fetching."""
            try:
                if self.bdp_client and ds.hash:
                    ds.deployments = self.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    logger.debug(
                        f"Fetched {len(ds.deployments) if ds.deployments else 0} deployments "
                        f"for {ds.vendor}/{ds.name}"
                    )
                return ds, None
            except Exception as e:
                return ds, e

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(fetch_deployments_for_schema, ds) for ds in schemas
            ]

            for future in as_completed(futures):
                ds, error = future.result()
                if error:
                    logger.warning(
                        f"Failed to fetch deployments for {ds.vendor}/{ds.name}: {error}"
                    )

        logger.info(
            f"Completed parallel deployment fetching for {len(schemas)} schemas"
        )

    def _fetch_deployments_sequential_main(self, schemas: List[DataStructure]) -> None:
        """Fetch deployments sequentially."""
        if not self.bdp_client:
            return
        for ds in schemas:
            try:
                if ds.hash:
                    deployments = self.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    ds.deployments = deployments
                    logger.debug(
                        f"Fetched {len(deployments)} deployments for {ds.vendor}/{ds.name}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to fetch deployments for {ds.vendor}/{ds.name}: {e}"
                )

    def _filter_by_deployed_since_main(
        self, data_structures: List[DataStructure]
    ) -> List[DataStructure]:
        """Filter data structures by deployment timestamp."""
        if not self.config.deployed_since:
            return data_structures

        try:
            since_dt = datetime.fromisoformat(
                self.config.deployed_since.replace("Z", "+00:00")
            )

            filtered_structures = []
            for ds in data_structures:
                if ds.deployments:
                    for dep in ds.deployments:
                        if dep.ts and self._is_deployment_recent_main(dep.ts, since_dt):
                            filtered_structures.append(ds)
                            break

            logger.info(
                f"Filtered schemas by deployed_since={self.config.deployed_since}: "
                f"{len(filtered_structures)}/{len(data_structures)} schemas"
            )
            return filtered_structures

        except ValueError as e:
            logger.warning(
                f"Invalid deployed_since timestamp format '{self.config.deployed_since}': {e}. "
                f"Using all schemas."
            )
            return data_structures

    def _is_deployment_recent_main(self, dep_ts: str, since_dt: datetime) -> bool:
        """Check if deployment timestamp is recent."""
        try:
            dep_dt = datetime.fromisoformat(dep_ts.replace("Z", "+00:00"))
            return dep_dt >= since_dt
        except ValueError as e:
            logger.warning(f"Failed to parse deployment timestamp '{dep_ts}': {e}")
            return False

    def _filter_by_schema_pattern_main(
        self, data_structures: List[DataStructure]
    ) -> List[DataStructure]:
        """Filter data structures by schema pattern."""
        if not self.config.schema_pattern:
            return data_structures

        filtered_by_pattern = []
        for ds in data_structures:
            if not ds.vendor or not ds.name:
                logger.warning(
                    f"Data structure missing vendor or name (hash={ds.hash}), skipping pattern check"
                )
                continue

            schema_identifier = f"{ds.vendor}/{ds.name}"

            if self.config.schema_pattern.allowed(schema_identifier):
                filtered_by_pattern.append(ds)
            else:
                schema_type = (
                    ds.meta.schema_type if ds.meta and ds.meta.schema_type else "event"
                )
                self.report.report_schema_filtered(schema_type, schema_identifier)
                logger.debug(
                    f"Schema {schema_identifier} filtered out by schema_pattern"
                )

        logger.info(
            f"Filtered schemas by schema_pattern: "
            f"{len(filtered_by_pattern)}/{len(data_structures)} schemas"
        )
        return filtered_by_pattern

    def _extract_schemas_from_bdp(self) -> Iterable[MetadataWorkUnit]:
        """Extract schemas from BDP Console API."""
        if not self.bdp_client:
            return

        # Get data structures with pagination and filtering
        data_structures = self._get_data_structures_filtered()

        for data_structure in data_structures:
            yield from self.data_structure_builder.process_data_structure(
                data_structure
            )

    def _extract_schemas_from_iglu(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract schemas from Iglu Schema Registry (Iglu-only mode).

        Uses automatic schema discovery via list_schemas() endpoint.
        Requires Iglu Server 0.6+ with /api/schemas endpoint support.
        """
        if not self.iglu_client:
            return

        # Automatic schema discovery via /api/schemas endpoint
        schema_uris = self.iglu_client.list_schemas()

        if not schema_uris:
            logger.error(
                "No schemas found in Iglu registry. Either the registry is empty or "
                "your Iglu Server doesn't support the /api/schemas endpoint (requires Iglu Server 0.6+)."
            )
            self.report.report_failure(
                title="No schemas found",
                message="Automatic schema discovery returned no results",
                context="Iglu-only mode requires Iglu Server 0.6+ with /api/schemas endpoint support",
            )
            return

        logger.info(
            f"Using automatic schema discovery: found {len(schema_uris)} schemas in Iglu registry"
        )
        yield from self._extract_schemas_from_uris(schema_uris)

    def _extract_schemas_from_uris(
        self, schema_uris: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract schemas from a list of Iglu URIs (automatic discovery).

        Args:
            schema_uris: List of schema URIs in format 'iglu:vendor/name/format/version'
        """
        for uri in schema_uris:
            if not self.iglu_client:
                logger.warning("Iglu client not configured, skipping URI extraction")
                return
            parsed = self.iglu_client.parse_iglu_uri(uri)
            if not parsed:
                logger.warning(f"Skipping invalid Iglu URI: {uri}")
                continue

            try:
                # Fetch schema from Iglu
                iglu_schema = self.iglu_client.get_schema(
                    vendor=parsed["vendor"],
                    name=parsed["name"],
                    format=parsed["format"],
                    version=parsed["version"],
                )

                if iglu_schema:
                    yield from self._process_iglu_schema(
                        iglu_schema=iglu_schema,
                        vendor=parsed["vendor"],
                        name=parsed["name"],
                        version=parsed["version"],
                    )
                else:
                    logger.warning(f"Schema not found in Iglu: {uri}")

            except (requests.RequestException, ValueError, KeyError) as e:
                # Expected errors: API failures, parsing errors, missing fields
                logger.error(f"Failed to fetch schema {uri} from Iglu: {e}")
                self.report.report_failure(
                    title=f"Failed to fetch schema {uri}",
                    message="Error fetching schema from Iglu registry",
                    context=uri,
                    exc=e,
                )
            except Exception as e:
                # Unexpected error - indicates a bug
                logger.error(
                    f"Unexpected error processing schema {uri}: {e}", exc_info=True
                )
                self.report.report_failure(
                    title=f"Unexpected error processing schema {uri}",
                    message="This may indicate a bug in the connector",
                    context=uri,
                    exc=e,
                )

    def _process_iglu_schema(
        self,
        iglu_schema: IgluSchema,
        vendor: str,
        name: str,
        version: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single schema from Iglu registry.

        Args:
            iglu_schema: Schema from Iglu API
            vendor: Schema vendor
            name: Schema name
            version: Schema version
        """
        # Determine schema type (event or entity)
        # In Iglu-only mode, we don't have meta.schema_type, so infer from schema itself
        # Snowplow convention: contexts are entities, rest are events
        schema_type = infer_schema_type(name)

        # Track found schema
        self.report.report_schema_found(schema_type)

        # Build schema identifier for filtering
        schema_identifier = f"{vendor}/{name}"

        # Apply filtering
        if not self.config.schema_pattern.allowed(schema_identifier):
            self.report.report_schema_filtered(schema_type, schema_identifier)
            return

        # Filter by schema type
        if schema_type not in self.config.schema_types_to_extract:
            logger.debug(f"Skipping schema {schema_identifier} with type {schema_type}")
            return

        # Capture first event schema for parsed events dataset naming (if not already set)
        if (
            schema_type == SchemaType.EVENT.value
            and self.state.first_event_schema_vendor is None
        ):
            self.state.first_event_schema_vendor = vendor
            self.state.first_event_schema_name = name
            logger.debug(
                f"Captured first event schema for Event dataset naming: {vendor}/{name}"
            )

        # Build dataset name
        if self.config.include_version_in_urn:
            dataset_name = f"{vendor}.{name}.{version}".replace("/", ".")
        else:
            dataset_name = f"{vendor}.{name}".replace("/", ".")

        # Dataset properties
        custom_properties = {
            "vendor": vendor,
            "schemaVersion": version,
            "schema_type": schema_type,
            "format": iglu_schema.self_descriptor.format,
            "hidden": "false",  # Iglu doesn't have hidden flag
            "igluUri": f"iglu:{vendor}/{name}/jsonschema/{version}",
        }

        # No parent container in Iglu-only mode (no organization)
        parent_container = None

        # No ownership in Iglu-only mode (no deployments)
        owners_list = None

        # SubTypes
        subtype = f"snowplow_{schema_type}_schema"

        # Prepare extra aspects
        extra_aspects: List[Any] = [
            StatusClass(removed=False),
        ]

        # Parse schema metadata
        schema_metadata = None
        try:
            schema_metadata = SnowplowSchemaParser.parse_schema(
                schema_data=iglu_schema.model_dump(),
                vendor=vendor,
                name=name,
                version=version,
            )

            # Field tagging disabled for Iglu-only mode (no deployment info available)
            # In Iglu-only mode, we don't have deployment initiators or PII enrichment config
            # so field tagging would be incomplete

            extra_aspects.append(schema_metadata)

        except Exception as e:
            error_msg = f"Failed to parse schema {schema_identifier}: {e}"
            self.report.report_schema_parsing_error(error_msg)
            logger.error(error_msg)
            return

        # Create Dataset using SDK V2
        dataset = Dataset(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=iglu_schema.description,
            display_name=name,
            external_url=None,  # No BDP Console URL in Iglu-only mode
            custom_properties=custom_properties,
            parent_container=parent_container
            if parent_container is not None
            else unset,
            subtype=subtype,
            owners=owners_list,
            extra_aspects=extra_aspects,
        )

        # Yield the dataset
        yield from dataset.as_workunits()

        # Cache the schema URN for pipeline collector job
        self.state.extracted_schema_urns.append(str(dataset.urn))

        # Cache schema fields for Event dataset along with their source URN
        if schema_metadata and schema_metadata.fields:
            for field in schema_metadata.fields:
                self.state.extracted_schema_fields.append((str(dataset.urn), field))

        logger.info(f"Emitted schema dataset: {schema_identifier} (version {version})")
        self.report.report_schema_extracted(schema_type)

        # Column-level lineage (if warehouse configured via Data Models API)
        if schema_metadata:
            yield from self.column_lineage_builder.emit_column_lineage(
                dataset_urn=str(dataset.urn),
                vendor=vendor,
                name=name,
                version=version,
                schema_metadata=schema_metadata,
            )

    def _get_warehouse_table_urn(self) -> Optional[str]:
        """
        Get URN for warehouse atomic events table.

        Delegates to PipelineProcessor which handles warehouse configuration.
        """
        return self.pipeline_processor._get_warehouse_table_urn()

    def _emit_column_lineage(
        self,
        dataset_urn: str,
        vendor: str,
        name: str,
        version: str,
        schema_metadata: SchemaMetadataClass,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit column-level lineage from Iglu schema fields to atomic.events Snowflake VARIANT column.

        Maps all fields from an Iglu schema to the single VARIANT column in Snowflake.
        Since DataHub's Snowflake connector doesn't parse VARIANT sub-fields, we create
        FineGrainedLineage showing that all Iglu fields feed into the VARIANT column.

        Args:
            dataset_urn: URN of the Iglu schema dataset
            vendor: Schema vendor
            name: Schema name
            version: Schema version
            schema_metadata: Parsed schema metadata with fields

        Yields:
            MetadataWorkUnit with UpstreamLineage containing FineGrainedLineage
        """
        # Get warehouse table URN (atomic.events from Snowflake)
        warehouse_table_urn = self._get_warehouse_table_urn()

        if not warehouse_table_urn:
            logger.debug(
                f"No warehouse table configured, skipping column lineage for {vendor}/{name}"
            )
            return

        # Check that parsed events dataset exists (intermediate hop)
        if not self.state.parsed_events_urn:
            logger.debug(
                f"Parsed events dataset not created, skipping column lineage for {vendor}/{name}"
            )
            return

        # Extract field paths from schema metadata
        if not schema_metadata.fields:
            logger.debug(f"No fields in schema metadata for {vendor}/{name}")
            return

        # Get the Snowflake VARIANT column name
        snowflake_column = self.column_lineage_builder.map_schema_to_snowflake_column(
            vendor=vendor,
            name=name,
            version=version,
        )

        # Create URNs for all Iglu fields
        iglu_field_urns = [
            make_schema_field_urn(dataset_urn, field.fieldPath)
            for field in schema_metadata.fields
        ]

        # Create URN for Snowflake VARIANT column
        snowflake_field_urn = make_schema_field_urn(
            warehouse_table_urn, snowflake_column
        )

        # Create single FineGrainedLineage mapping all Iglu fields to VARIANT column
        fine_grained_lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=iglu_field_urns,  # All Iglu schema fields
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[snowflake_field_urn],  # Single Snowflake VARIANT column
        )

        # Create UpstreamLineage with FineGrainedLineage
        # Use parsed events dataset as upstream (not the Iglu schema directly)
        # This creates the correct flow: Schema → Parsed Events → Warehouse
        upstream = UpstreamClass(
            dataset=self.state.parsed_events_urn,  # Parsed events dataset (intermediate)
            type=DatasetLineageTypeClass.TRANSFORMED,
        )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[upstream],
            fineGrainedLineages=[fine_grained_lineage],
        )

        logger.info(
            f"Emitting column-level lineage: {len(iglu_field_urns)} Iglu fields "
            f"from {vendor}/{name} → Snowflake VARIANT column '{snowflake_column}'"
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=warehouse_table_urn,  # Lineage is attached to downstream (warehouse table)
            aspect=upstream_lineage,
        ).as_workunit()

    def _build_field_version_mapping(
        self, data_structure: DataStructure
    ) -> Dict[str, str]:
        """
        Build mapping of field paths to the version they were first added in.

        Compares all versions of a schema to determine when each field was introduced.

        Args:
            data_structure: Data structure with deployments containing version history

        Returns:
            Dict mapping field_path -> version_added (e.g., {"email": "1-0-0", "phone": "1-1-0"})
        """
        schema_key = f"{data_structure.vendor}/{data_structure.name}"
        field_version_map: Dict[str, str] = {}

        # Need BDP client and deployments for version history
        if (
            not self.bdp_client
            or not data_structure.deployments
            or not data_structure.hash
        ):
            logger.debug(
                f"Cannot track field versions for {schema_key}: missing BDP client, deployments, or hash"
            )
            return {}

        # Extract all unique versions from deployments and sort
        versions = sorted(
            set(d.version for d in data_structure.deployments if d.version),
            key=lambda v: self._parse_schemaver(v),
        )

        if not versions:
            logger.debug(f"No versions found in deployments for {schema_key}")
            return {}

        logger.info(
            f"Tracking field versions for {schema_key}: {len(versions)} versions to compare"
        )

        # Track fields seen in each version
        previous_fields: set = set()

        for version in versions:
            try:
                # Fetch this version's schema
                version_ds = self.bdp_client.get_data_structure_version(
                    data_structure.hash, version
                )

                if not version_ds or not version_ds.data:
                    logger.warning(
                        f"Could not fetch schema for {schema_key} version {version}"
                    )
                    continue

                # Extract field paths from this version
                current_fields = set(version_ds.data.properties.keys())

                # Fields that are new in this version
                new_fields = current_fields - previous_fields

                # Record when each new field was added
                for field_path in new_fields:
                    if field_path not in field_version_map:
                        field_version_map[field_path] = version
                        logger.debug(
                            f"Field '{field_path}' added in version {version} of {schema_key}"
                        )

                # Update for next iteration
                previous_fields = current_fields

            except Exception as e:
                logger.warning(
                    f"Failed to process version {version} of {schema_key}: {e}"
                )
                continue

        logger.info(
            f"Field version tracking complete for {schema_key}: {len(field_version_map)} fields tracked"
        )

        return field_version_map

    @staticmethod
    def _parse_schemaver(version: str) -> Tuple[int, int, int]:
        """
        Parse SchemaVer version string into tuple for sorting.

        Args:
            version: Version string like "1-0-0" or "2-1-3"

        Returns:
            Tuple of (model, revision, addition) for comparison
        """
        try:
            parts = version.split("-")
            return (int(parts[0]), int(parts[1]), int(parts[2]))
        except (ValueError, IndexError):
            logger.warning(f"Invalid SchemaVer format: {version}, using (0,0,0)")
            return (0, 0, 0)

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

        # Only extract from enrichments if use_pii_enrichment is enabled
        if not self.config.field_tagging.use_pii_enrichment:
            self.state.pii_fields_cache = pii_fields
            return pii_fields

        # Extract from BDP enrichments
        if not self.bdp_client:
            self.state.pii_fields_cache = pii_fields
            return pii_fields

        try:
            # Get all pipelines and extract enrichments from each
            pipelines = self.bdp_client.get_pipelines()

            for pipeline in pipelines:
                try:
                    enrichments = self.bdp_client.get_enrichments(pipeline.id)

                    for enrichment in enrichments:
                        # Check if this is PII Pseudonymization enrichment
                        if not enrichment.schema_ref:
                            continue

                        if "pii_pseudonymization" not in enrichment.schema_ref.lower():
                            continue

                        # Extract PII fields from enrichment parameters
                        if (
                            enrichment.content
                            and enrichment.content.data
                            and enrichment.content.data.parameters
                        ):
                            params = enrichment.content.data.parameters

                            # PII enrichment config typically has a "pii" field with field list
                            if "pii" in params:
                                pii_config = params["pii"]
                                # pii_config is typically a list of field configurations
                                if isinstance(pii_config, list):
                                    for field_config in pii_config:
                                        if (
                                            isinstance(field_config, dict)
                                            and "fieldName" in field_config
                                        ):
                                            pii_fields.add(field_config["fieldName"])

                                # Alternative format: dict with fieldNames key
                                elif (
                                    isinstance(pii_config, dict)
                                    and "fieldNames" in pii_config
                                ):
                                    field_names = pii_config["fieldNames"]
                                    if isinstance(field_names, list):
                                        pii_fields.update(field_names)
                except Exception as e:
                    logger.warning(
                        f"Failed to extract PII fields from pipeline {pipeline.id}: {e}"
                    )
                    continue

        except Exception as e:
            logger.warning(f"Failed to extract PII fields from enrichments: {e}")

        # Cache the result
        self.state.pii_fields_cache = pii_fields
        return pii_fields

    def _map_schema_to_snowflake_column(
        self, vendor: str, name: str, version: str
    ) -> str:
        """
        Map Iglu schema to Snowflake atomic.events VARIANT column name.

        Snowflake stores context/entity schemas as VARIANT columns:
        contexts_{vendor}_{name}_{major_version}

        The VARIANT column contains JSON with all schema fields nested inside.
        Since DataHub's Snowflake connector does NOT parse VARIANT sub-fields,
        all fields from an Iglu schema map to the same VARIANT column.

        Examples:
        - vendor=com.acme, name=checkout_started, version=1-0-0
          → contexts_com_acme_checkout_started_1
        - vendor=com.snowplowanalytics.snowplow, name=web_page, version=1-0-0
          → contexts_com_snowplowanalytics_snowplow_web_page_1

        Args:
            vendor: Schema vendor (e.g., "com.acme")
            name: Schema name (e.g., "checkout_started")
            version: Schema version (e.g., "1-0-0")

        Returns:
            Snowflake VARIANT column name
        """
        # Replace dots and slashes with underscores
        vendor_clean = vendor.replace(".", "_").replace("/", "_")
        name_clean = name.replace(".", "_").replace("/", "_")

        # Version: Keep only major version (1-0-0 → 1)
        major_version = (
            version.split("-")[0] if "-" in version else version.split(".")[0]
        )

        # Build Snowflake VARIANT column name
        return f"contexts_{vendor_clean}_{name_clean}_{major_version}"

    def get_report(self) -> SnowplowSourceReport:
        return self.report

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
