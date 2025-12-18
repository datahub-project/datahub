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
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import Field

from datahub.emitter.mce_builder import (
    get_sys_time,
    make_container_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.urns import (
    DataTypeUrn,
    EntityTypeUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
)
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
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
from datahub.ingestion.source.snowplow.enrichment_lineage import (
    CampaignAttributionLineageExtractor,
    CurrencyConversionLineageExtractor,
    EnrichmentLineageRegistry,
    EventFingerprintLineageExtractor,
    IabSpidersRobotsLineageExtractor,
    IpLookupLineageExtractor,
    PiiPseudonymizationLineageExtractor,
    RefererParserLineageExtractor,
    UaParserLineageExtractor,
    YauaaLineageExtractor,
)
from datahub.ingestion.source.snowplow.field_tagging import (
    FieldTagContext,
    FieldTagger,
)
from datahub.ingestion.source.snowplow.iglu_client import IgluClient
from datahub.ingestion.source.snowplow.schema_parser import SnowplowSchemaParser
from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.snowplow_models import (
    DataStructure,
    DataStructureDeployment,
    Enrichment,
    EventSpecification,
    IgluSchema,
    TrackingScenario,
    User,
)
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    PropertyValueClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
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


# Container key definitions
class SnowplowOrganizationKey(ContainerKey):
    """Container key for Snowplow BDP organizations."""

    organization_id: str = Field(description="Snowplow organization ID")


class SnowplowTrackingScenarioKey(SnowplowOrganizationKey):
    """Container key for tracking scenarios within an organization."""

    scenario_id: str = Field(description="Tracking scenario ID")


class SnowplowDataProductKey(SnowplowOrganizationKey):
    """Container key for data products within an organization."""

    product_id: str = Field(description="Data product ID")


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

        # User cache for ownership resolution
        self._user_cache: Dict[str, User] = {}
        self._user_name_cache: Dict[str, List[User]] = {}

        # Track emitted event spec IDs to avoid linking to filtered specs
        self._emitted_event_spec_ids: set = set()

        if config.bdp_connection:
            self.bdp_client = SnowplowBDPClient(config.bdp_connection)
            self.report.connection_mode = "bdp"
            self.report.organization_id = config.bdp_connection.organization_id

            # Load users for ownership resolution
            self._load_user_cache()

        if config.iglu_connection:
            self.iglu_client = IgluClient(config.iglu_connection)
            if self.report.connection_mode == "bdp":
                self.report.connection_mode = "both"
            else:
                self.report.connection_mode = "iglu"

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
        self.enrichment_lineage_registry = EnrichmentLineageRegistry()
        self.enrichment_lineage_registry.register(IpLookupLineageExtractor())
        self.enrichment_lineage_registry.register(UaParserLineageExtractor())
        self.enrichment_lineage_registry.register(YauaaLineageExtractor())
        self.enrichment_lineage_registry.register(IabSpidersRobotsLineageExtractor())
        self.enrichment_lineage_registry.register(PiiPseudonymizationLineageExtractor())
        self.enrichment_lineage_registry.register(RefererParserLineageExtractor())
        self.enrichment_lineage_registry.register(CurrencyConversionLineageExtractor())
        self.enrichment_lineage_registry.register(CampaignAttributionLineageExtractor())
        self.enrichment_lineage_registry.register(EventFingerprintLineageExtractor())

        # Initialize field tagger
        self.field_tagger = FieldTagger(self.config.field_tagging)
        self._pii_fields_cache: Optional[set] = None  # Cache for PII fields

        # Performance optimization caches (Phase 1 improvements)
        self._cached_data_structures: Optional[List[DataStructure]] = None
        self._cached_event_schema_urns: Optional[List[str]] = None

        # Event-specific DataFlow mapping (for new architecture)
        self._event_spec_dataflow_urns: Dict[
            str, str
        ] = {}  # event_spec_id -> dataflow_urn
        self._physical_pipeline: Optional[Any] = None  # Reference to physical pipeline

        # Atomic event dataset URN (standard Snowplow event fields)
        self._atomic_event_urn: Optional[str] = None

        # Parsed Events dataset URN (intermediate dataset after collector/parser, before enrichments)
        self._parsed_events_urn: Optional[str] = None

        # Warehouse table URN cache (performance optimization - computed once, reused across enrichments/loader)
        self._warehouse_table_urn_cache: Optional[str] = None

        # Extracted schema URNs cache (for pipeline collector job)
        self._extracted_schema_urns: List[str] = []

        # Cached schema fields for Event dataset
        self._atomic_event_fields: List[SchemaFieldClass] = []
        self._extracted_schema_fields: List[
            Tuple[str, SchemaFieldClass]
        ] = []  # (schema_urn, field)

        # Track event spec for parsed events dataset naming
        self._event_spec_id: Optional[str] = (
            None  # Event spec ID (e.g., "650986b2-ad4a-453f-a0f1-4a2df337c31d")
        )
        self._event_spec_name: Optional[str] = (
            None  # Event spec name (e.g., "checkout_started")
        )

        # Track first event schema for parsed events dataset naming (fallback when no event spec)
        self._first_event_schema_vendor: Optional[str] = None
        self._first_event_schema_name: Optional[str] = None

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
            yield from self._register_structured_properties()

            # Wait for DataHub's structured properties cache to invalidate
            # This ensures the properties are available when we try to use them
            cache_invalidation_seconds = 10
            logger.info(
                f"Waiting {cache_invalidation_seconds} seconds for structured properties cache to invalidate"
            )
            time.sleep(cache_invalidation_seconds)

        # Emit organization container
        if self.bdp_client and self.config.bdp_connection:
            yield from self._process_organization()

        # Create atomic event dataset (for enrichment lineage)
        yield from self._create_atomic_event_dataset()

        # Extract schemas
        yield from self._extract_schemas()

        # Extract event specifications (BDP only) - MUST be before parsed events dataset creation
        # so we can capture the event spec ID and name for dataset naming
        if self.config.extract_event_specifications and self.bdp_client:
            yield from self._extract_event_specifications()

        # Create parsed events dataset (intermediate output of collector/parser, input to enrichments)
        yield from self._create_parsed_events_dataset()

        # Extract tracking scenarios (BDP only)
        if self.config.extract_tracking_scenarios and self.bdp_client:
            yield from self._extract_tracking_scenarios()

        # Extract data products (BDP only, experimental)
        if self.config.extract_data_products and self.bdp_client:
            yield from self._extract_data_products()

        # Extract pipelines as DataFlow (BDP only)
        if self.config.extract_pipelines and self.bdp_client:
            yield from self._extract_pipelines()

        # Extract enrichments as DataJobs (BDP only)
        if self.config.extract_enrichments and self.bdp_client:
            yield from self._extract_enrichments()

        # Extract warehouse lineage via Data Models API (BDP only)
        if self.config.warehouse_lineage.enabled and self.bdp_client:
            yield from self._extract_warehouse_lineage_via_data_models()

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

    def _create_atomic_event_dataset(self) -> Iterable[MetadataWorkUnit]:
        """
        Create a synthetic dataset representing the Snowplow Atomic Event schema.

        The atomic event schema contains ~130 standard fields defined in the canonical event model.
        Only ~12 fields are REQUIRED (always populated): app_id, platform, collector_tstamp, event,
        event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version.

        Other fields like user_ipaddress, page_urlquery, geo_country, mkt_medium are OPTIONAL but
        part of the standard schema. Enrichments read from these atomic event schema fields and write
        enriched fields to the warehouse table.

        This dataset represents the SCHEMA (what fields are available), not a guarantee that all
        fields are populated on every event. This is correct for field-level lineage purposes - we
        model what fields enrichments CAN read/write based on schema, not runtime data presence.

        Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
        Schema definition: https://github.com/snowplow/snowplow/blob/master/4-storage/redshift-storage/sql/atomic-def.sql
        """
        # Create URN for atomic event dataset (Event Core)
        # Use direct dataset name without vendor prefix (synthetic Snowplow concept, not an Iglu schema)
        self._atomic_event_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name="event_core",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Define standard Snowplow atomic event fields that enrichments commonly read/write
        # Based on: https://github.com/snowplow/snowplow/blob/master/4-storage/redshift-storage/sql/atomic-def.sql
        atomic_fields = [
            # App & Platform (REQUIRED fields)
            ("app_id", "Application ID (REQUIRED)"),
            ("platform", "Platform - web, mobile, server, etc. (REQUIRED)"),
            # Event metadata (REQUIRED fields)
            ("event_id", "Unique event ID (REQUIRED)"),
            ("event", "Event type - page_view, struct, unstruct, etc. (REQUIRED)"),
            ("event_name", "Event name for custom events (REQUIRED)"),
            ("event_vendor", "Event vendor (REQUIRED)"),
            ("event_format", "Event format (REQUIRED)"),
            ("event_version", "Event version (REQUIRED)"),
            ("collector_tstamp", "Collector timestamp (REQUIRED)"),
            # Tracker information (v_tracker REQUIRED)
            ("name_tracker", "Tracker name (optional)"),
            ("v_tracker", "Tracker version (REQUIRED)"),
            ("v_collector", "Collector version (REQUIRED)"),
            ("v_etl", "ETL version (REQUIRED)"),
            # Timestamps (optional)
            ("etl_tstamp", "ETL timestamp (optional)"),
            ("dvce_created_tstamp", "Device timestamp (optional)"),
            ("dvce_sent_tstamp", "Device sent timestamp (optional)"),
            ("derived_tstamp", "Derived timestamp (optional)"),
            ("true_tstamp", "True timestamp (optional)"),
            # User identifiers (optional)
            ("user_id", "User ID from tracker (optional)"),
            (
                "user_ipaddress",
                "User IP address (optional) - commonly read by IP Lookup enrichment",
            ),
            ("user_fingerprint", "User fingerprint (optional)"),
            ("domain_userid", "Domain user ID - 1st party cookie (optional)"),
            ("domain_sessionid", "Domain session ID (optional)"),
            ("domain_sessionidx", "Domain session index (optional)"),
            ("network_userid", "Network user ID - 3rd party cookie (optional)"),
            # Page fields (optional)
            (
                "page_url",
                "Page URL (optional) - commonly read by URL Parser enrichment",
            ),
            (
                "page_urlquery",
                "Page URL query string (optional) - commonly read by Campaign Attribution enrichment",
            ),
            (
                "page_referrer",
                "Page referrer URL (optional) - commonly read by Referer Parser enrichment",
            ),
            ("page_title", "Page title (optional)"),
            ("page_urlscheme", "Page URL scheme (optional)"),
            ("page_urlhost", "Page URL host (optional)"),
            ("page_urlport", "Page URL port (optional)"),
            ("page_urlpath", "Page URL path (optional)"),
            ("page_urlfragment", "Page URL fragment (optional)"),
            # Referrer URL components (optional)
            ("refr_urlscheme", "Referrer URL scheme (optional)"),
            ("refr_urlhost", "Referrer URL host (optional)"),
            ("refr_urlport", "Referrer URL port (optional)"),
            ("refr_urlpath", "Referrer URL path (optional)"),
            ("refr_urlquery", "Referrer URL query (optional)"),
            ("refr_urlfragment", "Referrer URL fragment (optional)"),
            # Browser/User Agent input fields (read by enrichments)
            (
                "useragent",
                "User agent string (optional) - read by UA Parser and YAUAA enrichments to derive browser/OS/device info",
            ),
            ("br_lang", "Browser language (optional)"),
            ("br_cookies", "Browser cookies enabled (optional)"),
            ("br_colordepth", "Browser color depth (optional)"),
            ("br_viewwidth", "Browser viewport width (optional)"),
            ("br_viewheight", "Browser viewport height (optional)"),
            ("refr_domain_userid", "Referrer domain user ID (optional)"),
            ("refr_dvce_tstamp", "Referrer device timestamp (optional)"),
            # Device fields (captured by tracker, not enriched)
            ("dvce_screenwidth", "Device screen width (optional)"),
            ("dvce_screenheight", "Device screen height (optional)"),
            # Note: Enriched output fields (geo_*, ip_*, mkt_*, refr_medium/source/term,
            # br_name/family/version/type/renderengine, os_*, dvce_type/ismobile, event_fingerprint)
            # are NOT included here because they don't exist until AFTER enrichments run.
            # The Event dataset represents the state BEFORE enrichments.
        ]

        # Create schema fields
        schema_fields = []
        for field_name, description in atomic_fields:
            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=field_name,
                    nativeDataType="string",  # Simplified - actual types vary
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    description=description,
                    nullable=True,
                    recursive=False,
                )
            )

        # Cache atomic event fields for Event dataset
        self._atomic_event_fields = schema_fields

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName="snowplow/atomic_event",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=schema_fields,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        )

        # Emit schema metadata
        yield MetadataChangeProposalWrapper(
            entityUrn=self._atomic_event_urn,
            aspect=schema_metadata,
        ).as_workunit()

        # Emit dataset properties
        dataset_properties = DatasetPropertiesClass(
            name="Event Core",
            description=(
                "**Event Core** represents the standard Snowplow atomic event schema as it exists BEFORE enrichments run. "
                "Contains ~50 core fields including:\n"
                "- **Required fields** (~12): app_id, platform, collector_tstamp, event, event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version\n"
                "- **Input fields for enrichments**: user_ipaddress, useragent, page_urlquery, page_referrer\n"
                "- **Other atomic fields**: timestamps, user IDs, page URLs, referrer URLs, browser/device properties\n\n"
                "**Does NOT include enriched output fields** like geo_*, ip_*, mkt_*, br_name/family/version, os_*, dvce_type/ismobile, etc. "
                "Those fields are created by enrichments and only exist in the warehouse table.\n\n"
                "Enrichments read from Event Core fields and write new enriched fields to the warehouse table. "
                "\n\n"
                "This dataset represents the SCHEMA (available fields), not a guarantee that all fields are "
                "populated on every event. This is correct for field-level lineage - we model what fields "
                "enrichments CAN read/write based on schema definition."
                "\n\n"
                "Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/"
            ),
            customProperties={
                "schema_type": "event_core",
                "platform": "snowplow",
                "field_count": str(len(atomic_fields)),
                "required_fields": "app_id, platform, collector_tstamp, event, event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self._atomic_event_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self._atomic_event_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit subTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=self._atomic_event_urn,
            aspect=SubTypesClass(typeNames=["event_core"]),
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self._make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self._atomic_event_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        logger.info(
            f"Created Event Core dataset with {len(atomic_fields)} atomic fields (excluding enriched output fields)"
        )

    def _create_parsed_events_dataset(self) -> Iterable[MetadataWorkUnit]:
        """
        Create a synthetic dataset representing parsed events after collector/parser stage.

        This dataset represents all fields from all schemas (atomic + custom events + entities)
        after they've been validated and parsed by the Snowplow pipeline, but BEFORE enrichments run.

        This serves as the intermediate output of the Collector/Parser job and input to enrichment jobs.

        Fields included:
        - All atomic event fields (~130 standard fields)
        - All custom event schema fields
        - All entity schema fields

        This dataset is synthetic and does not correspond to a real table/file.
        It's used for lineage modeling purposes only.
        """
        # Create URN for parsed events dataset using same pattern as Event Core
        # Use direct dataset name without vendor prefix (synthetic Snowplow concept, not an Iglu schema)
        # Name after the event specification ID to link it to the event spec
        # URN format: {event_spec_id}_event (e.g., "650986b2-ad4a-453f-a0f1-4a2df337c31d_event")
        # Display name: {event_spec_name} Event (e.g., "checkout_started Event")
        if self._event_spec_id and self._event_spec_name:
            dataset_name = f"{self._event_spec_id}_event"
            display_name = f"{self._event_spec_name} Event"
        elif self._first_event_schema_vendor and self._first_event_schema_name:
            # Fallback to first event schema found (more specific than generic "Event")
            # URN format: {vendor}.{name}_event (e.g., "com.example.checkout_started_event")
            # Display name: {name} Event (e.g., "checkout_started Event")
            schema_identifier = f"{self._first_event_schema_vendor}.{self._first_event_schema_name}".replace(
                "/", "."
            )
            dataset_name = f"{schema_identifier}_event"
            display_name = f"{self._first_event_schema_name} Event"
            logger.info(
                f"Using event schema for Event dataset naming: {self._first_event_schema_vendor}/{self._first_event_schema_name}"
            )
        else:
            # Final fallback if no event spec or event schema found
            dataset_name = "event"
            display_name = "Event"
            logger.warning(
                "No event spec or event schema found - using generic 'Event' dataset name"
            )

        self._parsed_events_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Create dataset properties
        dataset_properties = DatasetPropertiesClass(
            name=display_name,
            description=(
                f"**{display_name}** represents all parsed event fields after the Collector/Parser stage and BEFORE enrichments run.\n\n"
                "This dataset models the intermediate state of events after the Snowplow pipeline has:\n"
                "1. **Collected** events from trackers\n"
                "2. **Validated** against Iglu schemas\n"
                "3. **Parsed** atomic fields, custom event data, and entity data\n\n"
                "**But BEFORE** enrichments (IP Lookup, UA Parser, YAUAA, Campaign Attribution, etc.) add computed fields.\n\n"
                "## Fields Included\n"
                "This dataset contains ALL fields from:\n"
                "- **Event Core**: ~50 standard Snowplow atomic fields (user_ipaddress, page_url, useragent, etc.)\n"
                "- **Event Data Structures**: Application-specific event data\n"
                "- **Entity Data Structures**: Contextual data attached to events\n\n"
                "**Does NOT include enriched output fields** like geo_*, ip_*, mkt_*, br_name/family/version, os_*, dvce_type/ismobile, event_fingerprint, etc. "
                "Those fields are created by enrichments and only exist in the warehouse table.\n\n"
                "## Purpose\n"
                "Used for lineage modeling to show:\n"
                "- Collector/Parser job outputs to this dataset\n"
                "- Enrichment jobs read from this dataset and write new enriched fields to warehouse\n\n"
                "**Note**: This is a synthetic dataset for metadata purposes only. "
                "It does not correspond to a real table or file in your infrastructure."
            ),
            customProperties={
                "synthetic": "true",
                "purpose": "lineage_modeling",
                "stage": "post_parse_pre_enrich",
                "event_spec_id": self._event_spec_id or "",
                "event_spec_name": self._event_spec_name or "",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self._parsed_events_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # Emit schema metadata
        # The Event dataset contains ALL fields from ALL schemas (Event Core + Event Data Structures + Entity Data Structures)
        # Combine all cached fields
        all_event_fields = []

        # Add atomic event fields (Event Core)
        if self._atomic_event_fields:
            all_event_fields.extend(self._atomic_event_fields)

        # Add custom event and entity fields
        if self._extracted_schema_fields:
            # Extract just the fields from the (urn, field) tuples
            all_event_fields.extend(
                [field for _, field in self._extracted_schema_fields]
            )

        if all_event_fields:
            event_schema_metadata = SchemaMetadataClass(
                schemaName="snowplow/event",
                platform="urn:li:dataPlatform:snowplow",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=all_event_fields,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=self._parsed_events_urn,
                aspect=event_schema_metadata,
            ).as_workunit()

            logger.info(
                f"Event dataset schema contains {len(all_event_fields)} fields from all schemas "
                f"(Event Core: {len(self._atomic_event_fields)}, "
                f"Event/Entity Data Structures: {len(self._extracted_schema_fields)})"
            )
        else:
            logger.warning(
                "No schema fields available for Event dataset - schemas may not have been extracted yet"
            )

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self._parsed_events_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit subTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=self._parsed_events_urn,
            aspect=SubTypesClass(typeNames=["event"]),
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self._make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self._parsed_events_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        logger.info(
            "Created Event dataset (intermediate: collector/parser → enrichments)"
        )

    def _load_user_cache(self) -> None:
        """
        Load all users from BDP API and cache them for ownership resolution.

        This is called once during initialization to enable efficient
        initiator name/ID → email resolution for ownership tracking.
        """
        if not self.bdp_client:
            return

        try:
            users = self.bdp_client.get_users()
            for user in users:
                # Cache by ID (for initiatorId lookups)
                if user.id:
                    self._user_cache[user.id] = user

                # Cache by name (for initiator name lookups)
                if user.name:
                    if user.name not in self._user_name_cache:
                        self._user_name_cache[user.name] = []
                    self._user_name_cache[user.name].append(user)

                # Also cache by display_name
                if user.display_name:
                    if user.display_name not in self._user_name_cache:
                        self._user_name_cache[user.display_name] = []
                    self._user_name_cache[user.display_name].append(user)

            logger.info(
                f"Cached {len(self._user_cache)} users for ownership resolution"
            )
        except Exception as e:
            logger.warning(
                f"Failed to load users for ownership resolution: {e}. "
                f"Ownership tracking will use initiator names directly."
            )

    def _resolve_user_email(
        self, initiator_id: Optional[str], initiator_name: Optional[str]
    ) -> Optional[str]:
        """
        Resolve initiator to email address or identifier.

        Priority:
        1. Use initiatorId → lookup in user cache → return email (RELIABLE)
        2. If initiatorId missing: Try to match by name (UNRELIABLE - multiple matches possible)
        3. Return initiator_name directly if no resolution possible

        Args:
            initiator_id: User ID from deployment (preferred)
            initiator_name: User full name from deployment (fallback)

        Returns:
            Email address, name, or None if unresolvable
        """
        # Best case: Use initiatorId (unique identifier)
        if initiator_id and initiator_id in self._user_cache:
            user = self._user_cache[initiator_id]
            return user.email or user.name or initiator_name

        # Fallback: Try to match by name (PROBLEMATIC - names not unique!)
        if initiator_name and initiator_name in self._user_name_cache:
            matching_users = self._user_name_cache[initiator_name]

            if len(matching_users) == 1:
                # Single match - use it
                user = matching_users[0]
                logger.debug(
                    f"Resolved '{initiator_name}' to {user.email or user.name} by name match"
                )
                return user.email or user.name or initiator_name
            elif len(matching_users) > 1:
                # Multiple matches - ambiguous!
                logger.warning(
                    f"Ambiguous ownership: Found {len(matching_users)} users with name '{initiator_name}'. "
                    f"Using initiator name directly."
                )
                return initiator_name

        # Last resort: return name directly (better than nothing)
        if initiator_name:
            return initiator_name

        return None

    def _extract_ownership_from_deployments(
        self, deployments: List[DataStructureDeployment]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract createdBy and modifiedBy from deployments array.

        The oldest deployment represents the creator (DATAOWNER).
        The newest deployment represents the last modifier (PRODUCER).

        Args:
            deployments: List of deployments for a data structure

        Returns:
            Tuple of (createdBy_email, modifiedBy_email)
        """
        if not deployments:
            return None, None

        # Sort by timestamp (oldest first)
        sorted_deployments = sorted(
            deployments, key=lambda d: d.ts or "", reverse=False
        )

        # Oldest deployment = creator
        oldest = sorted_deployments[0]
        created_by = self._resolve_user_email(oldest.initiator_id, oldest.initiator)

        # Newest deployment = modifier
        newest = sorted_deployments[-1]
        modified_by = self._resolve_user_email(newest.initiator_id, newest.initiator)

        return created_by, modified_by

    def _build_ownership_list(
        self,
        created_by: Optional[str],
        modified_by: Optional[str],
        schema_identifier: str,
    ) -> Optional[List[OwnerClass]]:
        """
        Build ownership list for a schema (for SDK V2 Dataset constructor).

        Args:
            created_by: Email or name of creator
            modified_by: Email or name of last modifier
            schema_identifier: Schema identifier for logging

        Returns:
            List of OwnerClass objects, or None if no ownership data
        """
        if not created_by and not modified_by:
            return None

        owners = []

        # Primary owner (creator) - DATAOWNER
        if created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(created_by),
                    type=OwnershipTypeClass.DATAOWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        # Producer (last modifier) - PRODUCER (only if different from creator)
        if modified_by and modified_by != created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(modified_by),
                    type=OwnershipTypeClass.PRODUCER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        return owners if owners else None

    def _emit_ownership(
        self,
        dataset_urn: str,
        created_by: Optional[str],
        modified_by: Optional[str],
        schema_identifier: str,
    ) -> Optional[MetadataWorkUnit]:
        """
        Emit ownership aspect for a schema.

        Args:
            dataset_urn: URN of the dataset (schema)
            created_by: Email or name of creator
            modified_by: Email or name of last modifier
            schema_identifier: Schema identifier for logging

        Returns:
            MetadataWorkUnit for ownership, or None if no ownership data
        """
        if not created_by and not modified_by:
            return None

        owners = []

        # Primary owner (creator) - DATAOWNER
        if created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(created_by),
                    type=OwnershipTypeClass.DATAOWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        # Producer (last modifier) - PRODUCER (only if different from creator)
        if modified_by and modified_by != created_by:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(modified_by),
                    type=OwnershipTypeClass.PRODUCER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                        url=self._get_schema_url_for_ownership(schema_identifier),
                    ),
                )
            )

        if not owners:
            return None

        ownership = OwnershipClass(
            owners=owners,
            lastModified=AuditStampClass(
                time=int(time.time() * 1000), actor=make_user_urn("datahub")
            ),
        )

        logger.debug(
            f"Emitting ownership for {schema_identifier}: "
            f"created_by={created_by}, modified_by={modified_by}"
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=ownership
        ).as_workunit()

    def _get_schema_url_for_ownership(self, schema_identifier: str) -> Optional[str]:
        """Get BDP Console URL for ownership source."""
        if self.config.bdp_connection:
            org_id = self.config.bdp_connection.organization_id
            # Basic URL - actual implementation would need proper schema hash
            return f"https://console.snowplowanalytics.com/organizations/{org_id}/data-structures"
        return None

    def _extract_schemas(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event and entity schemas.

        Sources:
        - BDP Console API (if configured)
        - Iglu Registry (if configured and BDP not available)
        """
        if self.bdp_client:
            # Extract from BDP Console API
            yield from self._extract_schemas_from_bdp()
        elif self.iglu_client:
            # Extract from Iglu registry (open-source mode)
            logger.info(
                "Running in Iglu-only mode. Extracting schemas from manually configured list."
            )
            yield from self._extract_schemas_from_iglu()
        else:
            logger.error("No API client configured for schema extraction")

    def _get_data_structures_filtered(self) -> List[DataStructure]:  # noqa: C901
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
        if self._cached_data_structures is not None:
            logger.info(
                f"Using cached data structures ({len(self._cached_data_structures)} schemas)"
            )
            return self._cached_data_structures

        # Fetch all data structures with pagination
        try:
            data_structures = self.bdp_client.get_data_structures(
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

        # Fetch deployment history for field version tracking
        # Phase 1 optimization: Parallel fetching with ThreadPoolExecutor
        if self.config.field_tagging.track_field_versions:
            logger.info(
                "Field version tracking enabled - fetching full deployment history for all schemas"
            )

            # Filter schemas that need deployment fetching
            schemas_needing_deployments = [ds for ds in data_structures if ds.hash]

            if (
                self.config.performance.enable_parallel_fetching
                and len(schemas_needing_deployments) > 1
            ):
                # Phase 1: Parallel fetching (10x speedup)
                max_workers = self.config.performance.max_concurrent_api_calls
                logger.info(
                    f"Fetching deployments in parallel (max_workers={max_workers}) "
                    f"for {len(schemas_needing_deployments)} schemas"
                )

                def fetch_deployments_for_schema(
                    ds: DataStructure,
                ) -> Tuple[DataStructure, Optional[Exception]]:
                    """Thread-safe deployment fetching."""
                    try:
                        if self.bdp_client and ds.hash:
                            ds.deployments = (
                                self.bdp_client.get_data_structure_deployments(ds.hash)
                            )
                            logger.debug(
                                f"Fetched {len(ds.deployments) if ds.deployments else 0} deployments "
                                f"for {ds.vendor}/{ds.name}"
                            )
                        elif not ds.hash:
                            logger.debug(
                                f"Skipping deployment fetch for {ds.vendor}/{ds.name} - no hash"
                            )
                        return ds, None
                    except Exception as e:
                        return ds, e

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(fetch_deployments_for_schema, ds)
                        for ds in schemas_needing_deployments
                    ]

                    for future in as_completed(futures):
                        ds, error = future.result()
                        if error:
                            logger.warning(
                                f"Failed to fetch deployments for {ds.vendor}/{ds.name}: {error}"
                            )

                logger.info(
                    f"Completed parallel deployment fetching for {len(schemas_needing_deployments)} schemas"
                )
            else:
                # Sequential fetching (original behavior)
                for ds in schemas_needing_deployments:
                    try:
                        if ds.hash:
                            deployments = (
                                self.bdp_client.get_data_structure_deployments(ds.hash)
                            )
                            ds.deployments = deployments
                            logger.debug(
                                f"Fetched {len(deployments)} deployments for {ds.vendor}/{ds.name}"
                            )
                        else:
                            logger.debug(
                                f"Skipping deployment fetch for {ds.vendor}/{ds.name} - no hash"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to fetch deployments for {ds.vendor}/{ds.name}: {e}"
                        )

        # Filter by deployment timestamp if configured
        if self.config.deployed_since:
            try:
                since_dt = datetime.fromisoformat(
                    self.config.deployed_since.replace("Z", "+00:00")
                )

                filtered_structures = []
                for ds in data_structures:
                    # Check if any deployment is newer than since_dt
                    if ds.deployments:
                        for dep in ds.deployments:
                            if dep.ts:
                                try:
                                    dep_dt = datetime.fromisoformat(
                                        dep.ts.replace("Z", "+00:00")
                                    )
                                    if dep_dt >= since_dt:
                                        filtered_structures.append(ds)
                                        break  # Found a recent deployment, include this schema
                                except ValueError as e:
                                    logger.warning(
                                        f"Failed to parse deployment timestamp '{dep.ts}': {e}"
                                    )
                                    continue

                logger.info(
                    f"Filtered schemas by deployed_since={self.config.deployed_since}: "
                    f"{len(filtered_structures)}/{len(data_structures)} schemas"
                )
                data_structures = filtered_structures

            except ValueError as e:
                logger.warning(
                    f"Invalid deployed_since timestamp format '{self.config.deployed_since}': {e}. "
                    f"Using all schemas."
                )

        # Filter by schema pattern (vendor/name)
        if self.config.schema_pattern:
            filtered_by_pattern = []
            for ds in data_structures:
                # Skip schemas with missing vendor or name (shouldn't happen with valid API responses)
                if not ds.vendor or not ds.name:
                    logger.warning(
                        f"Data structure missing vendor or name (hash={ds.hash}), skipping pattern check"
                    )
                    continue

                # Format as vendor/name for pattern matching
                schema_identifier = f"{ds.vendor}/{ds.name}"

                if self.config.schema_pattern.allowed(schema_identifier):
                    filtered_by_pattern.append(ds)
                else:
                    # Determine schema type for reporting
                    schema_type = (
                        ds.meta.schema_type
                        if ds.meta and ds.meta.schema_type
                        else "event"
                    )
                    self.report.report_schema_filtered(schema_type, schema_identifier)
                    logger.debug(
                        f"Schema {schema_identifier} filtered out by schema_pattern"
                    )

            logger.info(
                f"Filtered schemas by schema_pattern: "
                f"{len(filtered_by_pattern)}/{len(data_structures)} schemas"
            )
            data_structures = filtered_by_pattern

        # Cache the result (Phase 1 optimization)
        self._cached_data_structures = data_structures
        logger.debug(f"Cached {len(data_structures)} data structures for reuse")

        return data_structures

    def _extract_schemas_from_bdp(self) -> Iterable[MetadataWorkUnit]:
        """Extract schemas from BDP Console API."""
        if not self.bdp_client:
            return

        # Get data structures with pagination and filtering
        data_structures = self._get_data_structures_filtered()

        for data_structure in data_structures:
            yield from self._process_data_structure(data_structure)

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

            except Exception as e:
                logger.error(f"Failed to fetch schema {uri} from Iglu: {e}")
                self.report.report_failure(
                    title=f"Failed to fetch schema {uri}",
                    message="Error fetching schema from Iglu registry",
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
        schema_type = "entity" if "context" in name.lower() else "event"

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
        if schema_type == "event" and self._first_event_schema_vendor is None:
            self._first_event_schema_vendor = vendor
            self._first_event_schema_name = name
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
        self._extracted_schema_urns.append(str(dataset.urn))

        # Cache schema fields for Event dataset along with their source URN
        if schema_metadata and schema_metadata.fields:
            for field in schema_metadata.fields:
                self._extracted_schema_fields.append((str(dataset.urn), field))

        logger.info(f"Emitted schema dataset: {schema_identifier} (version {version})")
        self.report.report_schema_extracted(schema_type)

        # Column-level lineage (if warehouse configured via Data Models API)
        if schema_metadata:
            yield from self._emit_column_lineage(
                dataset_urn=str(dataset.urn),
                vendor=vendor,
                name=name,
                version=version,
                schema_metadata=schema_metadata,
            )

    def _fetch_full_schema_definition(
        self, data_structure: DataStructure
    ) -> DataStructure:
        """
        Fetch full schema definition if only minimal info available.

        Args:
            data_structure: Data structure from BDP API

        Returns:
            Updated data structure with full schema definition (if available)
        """
        if (
            data_structure.data is not None
            or not data_structure.hash
            or not self.bdp_client
        ):
            return data_structure

        if not data_structure.deployments:
            logger.warning(
                f"No deployments found for {data_structure.vendor}/{data_structure.name}, cannot determine version to fetch"
            )
            return data_structure

        # Use version from most recent PROD deployment, fall back to any deployment
        prod_deployments = [d for d in data_structure.deployments if d.env == "PROD"]
        if prod_deployments:
            latest_deployment = sorted(
                prod_deployments, key=lambda d: d.ts or "", reverse=True
            )[0]
        else:
            latest_deployment = sorted(
                data_structure.deployments,
                key=lambda d: d.ts or "",
                reverse=True,
            )[0]

        version = latest_deployment.version
        env = latest_deployment.env

        logger.info(
            f"Fetching schema definition for {data_structure.vendor}/{data_structure.name} version {version} from {env}"
        )

        # Fetch using the /versions/{version} endpoint which returns full schema
        full_structure = self.bdp_client.get_data_structure_version(
            data_structure.hash, version, env
        )

        if full_structure and full_structure.data:
            logger.info(
                f"Successfully fetched schema definition with {len(full_structure.data.properties or {})} properties"
            )
            # Preserve metadata from list response
            full_structure.meta = data_structure.meta
            full_structure.deployments = data_structure.deployments
            return full_structure
        else:
            logger.warning(
                f"Could not fetch schema definition for {data_structure.hash}/{version}, skipping field-level metadata"
            )
            return data_structure

    def _get_schema_version(self, data_structure: DataStructure) -> Optional[str]:
        """
        Extract schema version from data structure.

        Args:
            data_structure: Data structure from BDP API

        Returns:
            Schema version string, or None if cannot be determined
        """
        # Get version from schema definition if available
        if data_structure.data:
            return data_structure.data.self_descriptor.version

        # Otherwise use version from latest deployment
        if data_structure.deployments:
            sorted_deployments = sorted(
                data_structure.deployments, key=lambda d: d.ts or "", reverse=True
            )
            version = sorted_deployments[0].version
            logger.info(
                f"Schema definition missing, using version from latest deployment: {version}"
            )
            return version

        return None

    def _process_data_structure(
        self, data_structure: DataStructure
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single data structure (schema).

        Args:
            data_structure: Data structure from BDP API
        """
        # Fetch full schema definition if missing
        data_structure = self._fetch_full_schema_definition(data_structure)

        # Check if we have basic required fields (vendor, name, meta)
        if (
            not data_structure.vendor
            or not data_structure.name
            or not data_structure.meta
        ):
            logger.warning(
                f"Data structure missing basic fields (vendor, name, or meta), skipping: {data_structure.hash}"
            )
            return

        vendor = data_structure.vendor
        name = data_structure.name
        schema_meta = data_structure.meta
        schema_type = schema_meta.schema_type or "event"  # "event" or "entity"

        # Get version
        version = self._get_schema_version(data_structure)
        if not version:
            logger.warning(f"Cannot determine version for {vendor}/{name}, skipping")
            return

        # Track found schema
        self.report.report_schema_found(schema_type or "event")

        # Build schema identifier for filtering
        schema_identifier = f"{vendor}/{name}"

        # Apply filtering
        if not self.config.schema_pattern.allowed(schema_identifier):
            self.report.report_schema_filtered(
                schema_type or "event", schema_identifier
            )
            return

        # Filter by schema type
        if schema_type and schema_type not in self.config.schema_types_to_extract:
            logger.debug(f"Skipping schema {schema_identifier} with type {schema_type}")
            return

        # Skip hidden schemas if configured
        if schema_meta.hidden and not self.config.include_hidden_schemas:
            self.report.report_hidden_schema(skipped=True)
            logger.debug(f"Skipping hidden schema: {schema_identifier}")
            return
        elif schema_meta.hidden:
            self.report.report_hidden_schema(skipped=False)

        # Capture first event schema for parsed events dataset naming (if not already set)
        if schema_type == "event" and self._first_event_schema_vendor is None:
            self._first_event_schema_vendor = vendor
            self._first_event_schema_name = name
            logger.debug(
                f"Captured first event schema for Event dataset naming: {vendor}/{name}"
            )

        # Build dataset name (consistent with URN generation)
        if self.config.include_version_in_urn:
            # Legacy behavior: version in URN
            dataset_name = f"{vendor}.{name}.{version}".replace("/", ".")
        else:
            # New behavior: version in properties only
            dataset_name = f"{vendor}.{name}".replace("/", ".")

        # Dataset properties
        custom_properties = {
            "vendor": vendor,
            "schemaVersion": version,  # Current schema version
            "schema_type": schema_type or "unknown",
            "hidden": str(schema_meta.hidden),
            "igluUri": f"iglu:{vendor}/{name}/jsonschema/{version}",
            **schema_meta.custom_data,
        }
        # Note: latestVersion and allVersions will be added when multi-version
        # tracking is implemented (requires fetching all versions per schema)

        # Add format if schema data available
        if data_structure.data:
            custom_properties["format"] = data_structure.data.self_descriptor.format

        # Determine parent container (as list for SDK V2 Dataset constructor)
        parent_container = None
        if self.config.bdp_connection:
            parent_container_urn = self._make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            # SDK Dataset expects parent_container as a list of URN strings for browse path
            parent_container = [parent_container_urn]

        # Determine owners (if deployments available)
        owners_list = None
        if data_structure.deployments and self.bdp_client:
            created_by, modified_by = self._extract_ownership_from_deployments(
                data_structure.deployments
            )
            owners_list = self._build_ownership_list(
                created_by, modified_by, schema_identifier
            )

        # SubTypes (event_schema or entity_schema)
        subtype = f"snowplow_{schema_type}_schema" if schema_type else "snowplow_schema"

        # Prepare extra aspects list for StatusClass and SchemaMetadata
        extra_aspects: List[Any] = [
            StatusClass(removed=False),
        ]

        # Add dataset-level tags if enabled
        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.tag_event_type
        ):
            dataset_tags = self._build_dataset_tags(name, schema_type)
            if dataset_tags:
                extra_aspects.append(dataset_tags)

        # Schema metadata (only if full schema definition available)
        schema_metadata = None
        if data_structure.data:
            try:
                schema_metadata = SnowplowSchemaParser.parse_schema(
                    schema_data=data_structure.data.model_dump(),
                    vendor=vendor,
                    name=name,
                    version=version,
                )

                # Add field tags if enabled
                if self.config.field_tagging.enabled:
                    schema_metadata = self._add_field_tags(
                        schema_metadata=schema_metadata,
                        data_structure=data_structure,
                        version=version,
                    )

                # Add schema metadata to extra aspects
                extra_aspects.append(schema_metadata)

                # Cache schema fields for Event dataset along with their source URN
                if schema_metadata and schema_metadata.fields:
                    # Will be set after dataset.as_workunits() is called
                    pass

            except Exception as e:
                error_msg = f"Failed to parse schema {schema_identifier}: {e}"
                self.report.report_schema_parsing_error(error_msg)
                logger.error(error_msg)
        else:
            logger.info(
                f"Schema definition not available for {vendor}/{name}/{version}, skipping detailed schema metadata"
            )

        # Create Dataset using SDK V2
        dataset = Dataset(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=data_structure.data.description
            if data_structure.data
            else None,
            display_name=name,
            external_url=self._get_schema_url(
                vendor, name, version, data_structure.hash
            ),
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

        # Emit field-level structured properties if enabled
        if (
            self.config.field_tagging.enabled
            and self.config.field_tagging.use_structured_properties
            and schema_metadata
        ):
            yield from self._emit_field_structured_properties(
                dataset_urn=str(dataset.urn),
                schema_metadata=schema_metadata,
                data_structure=data_structure,
                version=version,
            )

        # Cache the schema URN for pipeline collector job
        self._extracted_schema_urns.append(str(dataset.urn))

        # Cache schema fields for Event dataset along with their source URN
        if schema_metadata and schema_metadata.fields:
            for field in schema_metadata.fields:
                self._extracted_schema_fields.append((str(dataset.urn), field))

        # Column-level lineage: Iglu schema fields → atomic.events Snowflake columns
        # (must be emitted separately as it's not part of the Dataset object itself)
        if schema_metadata:
            yield from self._emit_column_lineage(
                dataset_urn=str(dataset.urn),
                vendor=vendor,
                name=name,
                version=version,
                schema_metadata=schema_metadata,
            )

        # Track extracted schema
        self.report.report_schema_extracted(schema_type or "event")

    def _extract_event_specifications(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event specifications from BDP.

        Event specifications are treated as datasets with references to schemas.
        """
        if not self.bdp_client:
            return

        try:
            event_specs = self.bdp_client.get_event_specifications()
        except Exception as e:
            self.report.report_failure(
                title="Failed to fetch event specifications",
                message="Unable to retrieve event specifications from BDP API. Check API credentials and permissions.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            return

        for event_spec in event_specs:
            self.report.report_event_spec_found()

            # Apply filtering
            if not self.config.event_spec_pattern.allowed(event_spec.name):
                self.report.report_event_spec_filtered(event_spec.name)
                continue

            # Track that this event spec was emitted (for container linking)
            self._emitted_event_spec_ids.add(event_spec.id)

            # Capture first event spec ID and name for parsed events dataset naming
            if self._event_spec_id is None:
                self._event_spec_id = event_spec.id
                self._event_spec_name = event_spec.name

            yield from self._process_event_specification(event_spec)

    def _process_event_specification(
        self, event_spec: EventSpecification
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single event specification.

        Args:
            event_spec: Event specification from BDP API
        """
        # Generate dataset URN for event spec
        dataset_urn = self._make_event_spec_dataset_urn(event_spec.id)

        # Dataset properties
        dataset_properties = DatasetPropertiesClass(
            name=event_spec.name,
            description=event_spec.description,
            customProperties={
                "event_spec_id": event_spec.id,
                "status": event_spec.status or "unknown",
                "created_at": event_spec.created_at or "",
                "updated_at": event_spec.updated_at or "",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # SubTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=["snowplow_event_spec"]),
        ).as_workunit()

        # Container (link to organization)
        if self.config.bdp_connection:
            org_urn = self._make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            container = ContainerClass(container=org_urn)

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=container,
            ).as_workunit()

        # Status
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Track extracted event spec
        self.report.report_event_spec_extracted()

        # Add lineage from event spec to referenced schemas
        if event_spec.event_schemas:
            upstream_urns = []
            for schema_ref in event_spec.event_schemas:
                # Create schema URN from vendor/name/version
                schema_urn = self._make_schema_dataset_urn(
                    vendor=schema_ref.vendor,
                    name=schema_ref.name,
                    version=schema_ref.version,
                )
                upstream_urns.append(
                    UpstreamClass(
                        dataset=schema_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )

            if upstream_urns:
                upstream_lineage = UpstreamLineageClass(upstreams=upstream_urns)
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=upstream_lineage,
                ).as_workunit()

    def _extract_tracking_scenarios(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract tracking scenarios from BDP.

        Tracking scenarios are treated as containers grouping related event specs.
        """
        if not self.bdp_client:
            return

        try:
            tracking_scenarios = self.bdp_client.get_tracking_scenarios()
        except Exception as e:
            self.report.report_failure(
                title="Failed to fetch tracking scenarios",
                message="Unable to retrieve tracking scenarios from BDP API. Check API credentials and permissions.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            return

        for scenario in tracking_scenarios:
            self.report.report_tracking_scenario_found()

            # Apply filtering
            if not self.config.tracking_scenario_pattern.allowed(scenario.name):
                self.report.report_tracking_scenario_filtered(scenario.name)
                continue

            yield from self._process_tracking_scenario(scenario)

    def _process_tracking_scenario(
        self, scenario: TrackingScenario
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single tracking scenario.

        Args:
            scenario: Tracking scenario from BDP API
        """
        if not self.config.bdp_connection:
            return

        org_id = self.config.bdp_connection.organization_id

        # Create parent organization key
        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Create tracking scenario container key (child of organization)
        scenario_key = SnowplowTrackingScenarioKey(
            organization_id=org_id,
            scenario_id=scenario.id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Use gen_containers to emit all container aspects properly
        yield from gen_containers(
            container_key=scenario_key,
            name=scenario.name,
            sub_types=["tracking_scenario"],
            parent_container_key=org_key,
            description=scenario.description,
            extra_properties={
                "scenario_id": scenario.id,
                "status": scenario.status or "unknown",
                "created_at": scenario.created_at or "",
                "updated_at": scenario.updated_at or "",
                "num_event_specs": str(len(scenario.event_specs)),
            },
        )

        # Track extracted tracking scenario
        self.report.report_tracking_scenario_extracted()

        # Add container relationships to event specs referenced in this scenario
        if scenario.event_specs:
            scenario_container_urn = str(
                make_container_urn(
                    guid=scenario_key.guid(),
                )
            )

            for event_spec_id in scenario.event_specs:
                # Only link to event specs that were actually emitted (not filtered)
                if event_spec_id not in self._emitted_event_spec_ids:
                    logger.debug(
                        f"Skipping container link for filtered event spec {event_spec_id} in scenario {scenario.name}"
                    )
                    continue

                # Create event spec dataset URN
                event_spec_urn = self._make_event_spec_dataset_urn(event_spec_id)

                # Link event spec to tracking scenario container
                container_aspect = ContainerClass(container=scenario_container_urn)
                yield MetadataChangeProposalWrapper(
                    entityUrn=event_spec_urn,
                    aspect=container_aspect,
                ).as_workunit()

    def _extract_data_products(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract data products from BDP Console API.

        Data products are high-level groupings of event specifications with
        ownership, domain, and access information. They help organize tracking
        design at a business domain level.

        Each data product is represented as a Container with subtype "Data Product".
        Event specifications are linked to data products via container relationships.
        """
        if not self.bdp_client:
            return

        try:
            # Fetch data products from API
            data_products = self.bdp_client.get_data_products()
            self.report.num_data_products_found = len(data_products)
            logger.info(f"Found {len(data_products)} data products")

            for product in data_products:
                # Apply filtering
                if not self.config.data_product_pattern.allowed(product.id):
                    logger.debug(f"Skipping filtered data product: {product.id}")
                    self.report.num_data_products_filtered += 1
                    continue

                # Get organization ID (checked at source init)
                org_id = (
                    self.config.bdp_connection.organization_id
                    if self.config.bdp_connection
                    else ""
                )

                # Create data product container
                product_key = SnowplowDataProductKey(
                    organization_id=org_id,
                    product_id=product.id,
                    platform=self.platform,
                )

                # Build custom properties for additional metadata
                custom_properties = {}
                if product.access_instructions:
                    custom_properties["accessInstructions"] = (
                        product.access_instructions
                    )
                if product.source_applications:
                    custom_properties["sourceApplications"] = ", ".join(
                        product.source_applications
                    )
                if product.type:
                    custom_properties["type"] = product.type
                if product.lock_status:
                    custom_properties["lockStatus"] = product.lock_status
                if product.status:
                    custom_properties["status"] = product.status
                if product.created_at:
                    custom_properties["createdAt"] = product.created_at
                if product.updated_at:
                    custom_properties["updatedAt"] = product.updated_at

                # Emit container with properties (SDK V2 pattern via gen_containers)
                yield from gen_containers(
                    container_key=product_key,
                    name=product.name,
                    sub_types=["Data Product"],  # Custom subtype for data products
                    domain_urn=None,  # Domain URL is not available from API
                    description=product.description,
                    owner_urn=None,  # Owner is email, handled separately below
                    external_url=None,
                    tags=None,
                    extra_properties=custom_properties if custom_properties else None,
                )

                # Create container URN
                dataset_urn = str(make_container_urn(guid=product_key.guid()))

                # Add ownership if owner specified
                if product.owner:
                    ownership_aspect = OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=make_user_urn(product.owner),
                                type=OwnershipTypeClass.DATAOWNER,
                                source=OwnershipSourceClass(
                                    type=OwnershipSourceTypeClass.SERVICE,
                                    url=None,
                                ),
                            )
                        ]
                    )
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=ownership_aspect,
                    ).as_workunit()

                # Link event specifications to this data product container
                if product.event_specs:
                    product_container_urn = str(
                        make_container_urn(guid=product_key.guid())
                    )

                    for event_spec_ref in product.event_specs:
                        # Extract event spec ID from reference object
                        event_spec_id = event_spec_ref.id

                        # Only link to event specs that were actually emitted (not filtered)
                        if event_spec_id not in self._emitted_event_spec_ids:
                            logger.debug(
                                f"Skipping container link for filtered event spec {event_spec_id} in data product {product.name}"
                            )
                            continue

                        # Create event spec dataset URN
                        event_spec_urn = self._make_event_spec_dataset_urn(
                            event_spec_id
                        )

                        # Link event spec to data product container
                        container_aspect = ContainerClass(
                            container=product_container_urn
                        )
                        yield MetadataChangeProposalWrapper(
                            entityUrn=event_spec_urn,
                            aspect=container_aspect,
                        ).as_workunit()

                logger.debug(f"Extracted data product: {product.name} ({product.id})")
                self.report.num_data_products_extracted += 1

        except Exception as e:
            self.report.report_failure(
                title="Failed to extract data products",
                message="Unable to retrieve data products from BDP API. Check API credentials and permissions.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )

    def _extract_pipelines(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event-specific pipelines as DataFlow entities.

        NEW ARCHITECTURE (per event specification):
        - One DataFlow per event specification (instead of per physical pipeline)
        - DataFlow represents the data flow for a specific event through the pipeline
        - Inputs: event schema + entity schemas from event specification
        - Tagged with physical pipeline name/ID

        This provides better visibility into which entities flow with which events.
        """
        if not self.bdp_client:
            return

        try:
            # Get event specifications (with entity mappings)
            event_specs = self.bdp_client.get_event_specifications()
            logger.info(
                f"Extracting event-specific DataFlows from {len(event_specs)} event specifications"
            )

            # Get physical pipelines (for tagging)
            physical_pipelines = self.bdp_client.get_pipelines()
            self.report.num_pipelines_found = len(physical_pipelines)

            # Use first pipeline as default (most orgs have one pipeline)
            default_pipeline = physical_pipelines[0] if physical_pipelines else None

            # Cache physical pipeline info for later use by enrichments
            self._physical_pipeline = default_pipeline

            # Create DataFlow for each event specification that has entity data
            for event_spec in event_specs:
                # Only create DataFlow if event specification has entity information
                # (otherwise it's just documentation without lineage value)
                event_iglu_uri = event_spec.get_event_iglu_uri()
                entity_iglu_uris = event_spec.get_entity_iglu_uris()

                if not event_iglu_uri:
                    logger.debug(
                        f"Skipping event spec {event_spec.name} - no event URI found"
                    )
                    continue

                self.report.report_pipeline_found()

                # Create DataFlow URN for this event specification
                # Include pipeline ID to ensure uniqueness across multiple pipelines
                pipeline_id = default_pipeline.id if default_pipeline else "unknown"
                dataflow_urn = make_data_flow_urn(
                    orchestrator="snowplow",
                    flow_id=f"{pipeline_id}_event_{event_spec.id}",
                    cluster=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                # Store mapping for enrichment extraction
                self._event_spec_dataflow_urns[event_spec.id] = dataflow_urn

                # Build custom properties
                custom_properties = {
                    "eventSpecId": event_spec.id,
                    "eventIgluUri": event_iglu_uri,
                    "entityCount": str(len(entity_iglu_uris)),
                }

                # Tag with physical pipeline info
                if default_pipeline:
                    custom_properties["physicalPipelineId"] = default_pipeline.id
                    custom_properties["physicalPipelineName"] = default_pipeline.name
                    custom_properties["pipelineStatus"] = default_pipeline.status

                    if default_pipeline.label:
                        custom_properties["pipelineLabel"] = default_pipeline.label

                    if (
                        default_pipeline.config
                        and default_pipeline.config.collector_endpoints
                    ):
                        custom_properties["collectorEndpoints"] = ", ".join(
                            default_pipeline.config.collector_endpoints
                        )

                # Add entity URIs as custom properties (for reference)
                if entity_iglu_uris:
                    custom_properties["entityIgluUris"] = ", ".join(
                        entity_iglu_uris[:5]
                    )  # Limit to avoid too long
                    if len(entity_iglu_uris) > 5:
                        custom_properties["entityIgluUris"] += (
                            f" (+{len(entity_iglu_uris) - 5} more)"
                        )

                # Build description
                pipeline_name = (
                    default_pipeline.name if default_pipeline else "Unknown Pipeline"
                )
                description = f"Snowplow event flow for '{event_spec.name}'"
                if entity_iglu_uris:
                    description += f" with {len(entity_iglu_uris)} entity context(s)"
                if default_pipeline:
                    description += f" (Pipeline: {pipeline_name})"

                # Emit DataFlow info
                dataflow_info = DataFlowInfoClass(
                    name=f"{event_spec.name} ({pipeline_name})",  # Include pipeline name for clarity
                    description=description,
                    customProperties=custom_properties,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataflow_urn,
                    aspect=dataflow_info,
                ).as_workunit()

                # Emit status
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataflow_urn,
                    aspect=StatusClass(removed=False),
                ).as_workunit()

                # Link to organization container
                if self.config.bdp_connection:
                    org_container_urn = self._make_organization_urn(
                        self.config.bdp_connection.organization_id
                    )
                    container_aspect = ContainerClass(container=org_container_urn)
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataflow_urn,
                        aspect=container_aspect,
                    ).as_workunit()

                logger.debug(
                    f"Extracted event-specific DataFlow: {event_spec.name} (event_spec_id={event_spec.id})"
                )
                self.report.report_pipeline_extracted()

        except Exception as e:
            self.report.report_failure(
                title="Failed to extract event-specific pipelines",
                message="Unable to retrieve event specifications or pipelines from BDP API. Check API credentials and permissions.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )

    def _get_warehouse_table_urn(self) -> Optional[str]:
        """
        Get URN for the warehouse atomic events table (where enriched data lands).

        Uses caching to avoid redundant API calls - the warehouse table is the same
        across all enrichments and the loader job.

        Priority:
        1. Use destinations API (most accurate - actual pipeline configuration)
        2. Fall back to organization's warehouse source (from organizations API)
        3. Return None if none available

        Returns:
            Warehouse table URN, or None if not available
        """
        # Return cached URN if available
        if self._warehouse_table_urn_cache is not None:
            return self._warehouse_table_urn_cache

        # Try destinations API first (most accurate)
        if self.bdp_client:
            try:
                destinations = self.bdp_client.get_destinations()

                if destinations:
                    # Get the first active destination
                    # TODO: Support multiple destinations per pipeline
                    destination = destinations[0]

                    # Map destination type to DataHub platform
                    # Sanitize platform name: lowercase, replace spaces/hyphens with underscores
                    destination_type_clean = (
                        destination.destination_type.lower()
                        .replace(" ", "_")
                        .replace("-", "_")
                    )
                    warehouse_platform = WAREHOUSE_PLATFORM_MAP.get(
                        destination_type_clean, destination_type_clean
                    )

                    # Extract database and schema from destination config
                    config = destination.target.config
                    database = config.get("database", "unknown_db")
                    schema = config.get("schema", "unknown_schema")

                    # Build warehouse table name: {database}.{schema}.events
                    # Snowplow typically loads to 'events' table
                    # Lowercase to match Snowflake connector URN format
                    warehouse_table_name = f"{database}.{schema}.events".lower()

                    # Generate warehouse table URN
                    warehouse_urn = make_dataset_urn_with_platform_instance(
                        platform=warehouse_platform,
                        name=warehouse_table_name,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    logger.info(
                        f"Using destination API for warehouse: {warehouse_platform}://{warehouse_table_name}"
                    )
                    # Cache the URN for subsequent calls
                    self._warehouse_table_urn_cache = warehouse_urn
                    return warehouse_urn

            except Exception as e:
                logger.warning(
                    f"Failed to get warehouse from destinations API: {e}. Falling back to organization warehouse."
                )

        # Fall back to organization's warehouse source
        if self.bdp_client:
            try:
                organization = self.bdp_client.get_organization()

                if organization and organization.source:
                    # Sanitize platform name: lowercase, replace spaces/hyphens with underscores
                    warehouse_type_clean = (
                        organization.source.name.lower()
                        .replace(" ", "_")
                        .replace("-", "_")
                    )

                    # Map warehouse type to DataHub platform
                    warehouse_platform = WAREHOUSE_PLATFORM_MAP.get(
                        warehouse_type_clean, warehouse_type_clean
                    )

                    # Use default database/schema names (can be made configurable if needed)
                    # Snowplow typically uses: database.atomic.events
                    warehouse_table_name = "prod.atomic.events"

                    # Generate warehouse table URN
                    warehouse_urn = make_dataset_urn_with_platform_instance(
                        platform=warehouse_platform,
                        name=warehouse_table_name,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    logger.debug(
                        f"Using organization's warehouse destination: {warehouse_type_clean} (from {organization.source.name})"
                    )
                    # Cache the URN for subsequent calls
                    self._warehouse_table_urn_cache = warehouse_urn
                    return warehouse_urn

            except Exception as e:
                logger.warning(f"Failed to get warehouse from organization source: {e}")

        # No warehouse configuration available
        return None

    def _get_event_schema_urns(self) -> List[str]:
        """
        Get URNs for all event schemas in the organization.

        This is used to create lineage between enrichments and the event schemas they process.
        """
        # Check cache first (Phase 1 optimization)
        if self._cached_event_schema_urns is not None:
            logger.info(
                f"Using cached event schema URNs ({len(self._cached_event_schema_urns)} schemas)"
            )
            return self._cached_event_schema_urns

        event_schema_urns: List[str] = []

        if not self.bdp_client:
            return event_schema_urns

        try:
            data_structures = self._get_data_structures_filtered()

            for data_structure in data_structures:
                # Only include event schemas
                if data_structure.meta and data_structure.meta.schema_type == "event":
                    vendor = data_structure.vendor
                    name = data_structure.name

                    # Skip if vendor or name is None
                    if not vendor or not name:
                        logger.debug("Skipping schema with missing vendor or name")
                        continue

                    # Get version from schema definition if available, otherwise from latest deployment
                    if data_structure.data and data_structure.data.self_descriptor:
                        version = data_structure.data.self_descriptor.version
                    elif data_structure.deployments:
                        # Use version from most recent deployment
                        sorted_deployments = sorted(
                            data_structure.deployments,
                            key=lambda d: d.ts or "",
                            reverse=True,
                        )
                        version = sorted_deployments[0].version
                        logger.debug(
                            f"Using version from latest deployment for {vendor}/{name}: {version}"
                        )
                    else:
                        # If no version available, skip this schema
                        logger.debug(
                            f"Skipping schema {vendor}/{name} - no version information"
                        )
                        continue

                    # Generate URN for this event schema
                    schema_urn = self._make_schema_dataset_urn(vendor, name, version)
                    event_schema_urns.append(schema_urn)
                    logger.debug(f"Added event schema URN: {schema_urn}")

        except Exception as e:
            logger.warning(f"Failed to get event schema URNs: {e}")

        # Cache the result (Phase 1 optimization)
        self._cached_event_schema_urns = event_schema_urns
        logger.debug(f"Cached {len(event_schema_urns)} event schema URNs for reuse")

        return event_schema_urns

    def _build_enrichment_description(
        self,
        enrichment_name: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
    ) -> str:
        """
        Build enrichment description with field lineage information.

        Args:
            enrichment_name: Name of the enrichment
            fine_grained_lineages: List of fine-grained lineage objects

        Returns:
            Enhanced description with field information
        """
        if not fine_grained_lineages:
            return f"{enrichment_name} enrichment"

        # Extract unique upstream and downstream field names
        upstream_fields = set()
        downstream_fields = set()

        for lineage in fine_grained_lineages:
            # Extract field names from URNs
            for upstream_urn in lineage.upstreams or []:
                # URN format: urn:li:schemaField:(urn:li:dataset:...,field_name)
                if "," in upstream_urn:
                    field_name = upstream_urn.split(",")[-1].rstrip(")")
                    upstream_fields.add(field_name)

            for downstream_urn in lineage.downstreams or []:
                if "," in downstream_urn:
                    field_name = downstream_urn.split(",")[-1].rstrip(")")
                    downstream_fields.add(field_name)

        # Build markdown-formatted description with proper structure
        description_lines = [f"## {enrichment_name} enrichment", ""]

        if downstream_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            downstream_list = sorted(downstream_fields)
            description_lines.append("**Adds fields:**")
            for field in downstream_list:
                description_lines.append(f"- `{field}`")
            description_lines.append("")

        if upstream_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            upstream_list = sorted(upstream_fields)
            description_lines.append("**From source fields:**")
            for field in upstream_list:
                description_lines.append(f"- `{field}`")

        return "\n".join(description_lines)

    def _extract_enrichment_field_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urns: List[str],
        warehouse_table_urn: Optional[str],
    ) -> List[FineGrainedLineageClass]:
        """
        Extract field-level lineage for an enrichment using registered extractors.

        Args:
            enrichment: The enrichment to extract lineage for
            event_schema_urns: Event schema URNs (inputs to enrichment) - NOT USED for atomic field enrichments
            warehouse_table_urn: Warehouse table URN (used as both input and output for atomic field enrichments)

        Returns:
            List of FineGrainedLineageClass objects representing field transformations
        """
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        # Get extractor for this enrichment type
        extractor = self.enrichment_lineage_registry.get_extractor(enrichment)
        if not extractor:
            logger.debug(
                f"No lineage extractor found for enrichment schema: {enrichment.schema_ref}"
            )
            return fine_grained_lineages

        # IMPORTANT: Enrichments read from Event dataset fields (user_ipaddress, page_urlquery, etc.),
        # which are standard Snowplow fields present on every event, and write enriched fields
        # to the Snowflake warehouse table.
        #
        # The Event dataset contains all fields from:
        # - Event Core: Standard atomic fields (user_ipaddress, page_urlquery, etc.)
        # - Event/Entity Data Structures: Custom fields
        #
        # Enrichments transform:
        # - Input: Event dataset fields (e.g., user_ipaddress from Event)
        # - Output: Enriched fields in warehouse table (e.g., geo_country in Snowflake)
        if not self._parsed_events_urn:
            logger.debug("Event URN not available - skipping field lineage extraction")
            return fine_grained_lineages

        if not warehouse_table_urn:
            logger.debug(
                "No warehouse table URN available - skipping field lineage extraction"
            )
            return fine_grained_lineages

        # Extract field lineages using the registered extractor
        # Pass parsed_events_urn as input and warehouse_table_urn as output
        try:
            field_lineages = extractor.extract_lineage(
                enrichment=enrichment,
                event_schema_urn=self._parsed_events_urn,  # Use Event as source for fields
                warehouse_table_urn=warehouse_table_urn,  # Use warehouse table as output for enriched fields
            )

            # Convert FieldLineage objects to DataHub FineGrainedLineageClass
            for field_lineage in field_lineages:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=field_lineage.upstream_fields,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=field_lineage.downstream_fields,
                        transformOperation=field_lineage.transformation_type,
                    )
                )

            if fine_grained_lineages:
                logger.debug(
                    f"Extracted {len(fine_grained_lineages)} field lineages for enrichment {enrichment.filename}"
                )

        except Exception as e:
            logger.warning(
                f"Failed to extract field lineage for enrichment {enrichment.filename}: {e}",
                exc_info=True,
            )

        return fine_grained_lineages

    def _emit_collector_datajob(
        self,
        event_schema_urns: List[str],
        warehouse_table_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit a default "collector" DataJob that represents the Snowplow event collection process.

        This DataJob creates column-level lineage for standard Snowplow event fields that are
        NOT enriched (fields that come directly from the tracker to atomic.events).

        Standard Snowplow columns include: app_id, platform, event_id, event_name, user_id,
        collector_tstamp, dvce_created_tstamp, page_url, page_title, etc.

        Lineage flow:
        - Inputs: Event schemas (from tracker)
        - Outputs: Warehouse atomic.events table (standard columns)

        Args:
            event_schema_urns: List of event schema URNs (inputs)
            warehouse_table_urn: Warehouse table URN (output)

        Yields:
            MetadataWorkUnits for the collector DataJob
        """
        try:
            # Create DataFlow URN for the "snowplow-pipeline" flow
            dataflow_urn = make_data_flow_urn(
                orchestrator="snowplow",
                flow_id="snowplow-pipeline",
                cluster=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            # Create DataJob URN for the "collector" job
            datajob_urn = make_data_job_urn_with_flow(
                flow_urn=dataflow_urn,
                job_id="collector",
            )

            # Standard Snowplow event columns (non-enriched fields)
            # Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
            standard_columns = SNOWPLOW_STANDARD_COLUMNS

            # Create field URNs for standard columns
            downstream_field_urns = [
                make_schema_field_urn(warehouse_table_urn, col)
                for col in standard_columns
            ]

            # Get all field URNs from input event schemas
            # Note: This is a simplified approach - we're creating a FIELD_SET to FIELD_SET mapping
            # showing that event schema fields flow through the collector to standard event columns
            upstream_field_urns: List[str] = []
            for event_schema_urn in event_schema_urns:
                # Get schema metadata to extract field URNs
                # For simplicity, we'll just reference the dataset itself
                # In reality, specific fields from event schemas map to specific standard columns
                upstream_field_urns.append(event_schema_urn)

            # Create fine-grained lineage for collector
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            if upstream_field_urns and downstream_field_urns:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
                        upstreams=event_schema_urns,  # Event schemas as upstreams
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=downstream_field_urns,  # Standard columns as downstreams
                        transformOperation="COLLECT",
                    )
                )

            # Create DataJob info
            datajob_info = DataJobInfoClass(
                name="collector",
                description=(
                    "Snowplow event collector that captures raw event data from trackers "
                    "and writes standard event columns to the atomic events table. "
                    "This job represents the initial data collection phase before enrichment."
                ),
                type="BATCH",
                customProperties={
                    "job_type": "collector",
                    "source": "snowplow-tracker",
                    "destination": "atomic-events",
                    "standard_columns_count": str(len(standard_columns)),
                },
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob_urn, aspect=datajob_info
            ).as_workunit()

            # Emit DataJobInputOutput with column-level lineage
            datajob_input_output = DataJobInputOutputClass(
                inputDatasets=event_schema_urns,
                outputDatasets=[warehouse_table_urn],
                fineGrainedLineages=fine_grained_lineages
                if fine_grained_lineages
                else None,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob_urn,
                aspect=datajob_input_output,
            ).as_workunit()

            # Link collector job to DataFlow
            dataflow_info_aspect = DataFlowInfoClass(
                name="snowplow-pipeline",
                description="Snowplow data pipeline including event collection and enrichment",
                customProperties={
                    "platform": "snowplow",
                    "env": self.config.env,
                },
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataflow_urn,
                aspect=dataflow_info_aspect,
            ).as_workunit()

            logger.info(
                f"Created collector DataJob with {len(standard_columns)} standard columns mapped"
            )

        except Exception as e:
            logger.warning(f"Failed to emit collector DataJob: {e}")

    def _parse_iglu_uri(self, uri: str) -> tuple[str | None, str | None, str | None]:
        """Parse Iglu URI into vendor, name, and version components."""
        uri_clean = uri.replace("iglu:", "")
        parts = uri_clean.split("/")
        if len(parts) == 4:
            return (parts[0], parts[1], parts[3])
        return (None, None, None)

    def _build_enrichment_input_urns(
        self, event_spec: "EventSpecification"
    ) -> list[str]:
        """Build list of input schema URNs (event + entities) for an event specification."""
        event_iglu_uri = event_spec.get_event_iglu_uri()
        entity_iglu_uris = event_spec.get_entity_iglu_uris()

        if not event_iglu_uri:
            return []

        input_schema_urns = []

        # Add event schema URN
        event_vendor, event_name, event_version = self._parse_iglu_uri(event_iglu_uri)
        if event_name and event_vendor and event_version:
            event_schema_urn = self._make_schema_dataset_urn(
                event_vendor, event_name, event_version
            )
            input_schema_urns.append(event_schema_urn)

        # Add entity schema URNs
        for entity_iglu_uri in entity_iglu_uris:
            entity_vendor, entity_name, entity_version = self._parse_iglu_uri(
                entity_iglu_uri
            )
            if entity_name and entity_vendor and entity_version:
                entity_schema_urn = self._make_schema_dataset_urn(
                    entity_vendor, entity_name, entity_version
                )
                input_schema_urns.append(entity_schema_urn)

        return input_schema_urns

    def _emit_enrichment_datajob(
        self,
        enrichment: "Enrichment",
        dataflow_urn: str,
        input_dataset_urn: str,
        warehouse_table_urn: str | None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit all aspects for a single enrichment DataJob."""
        # Create DataJob URN
        datajob_urn = make_data_job_urn_with_flow(
            flow_urn=dataflow_urn,
            job_id=enrichment.id,
        )

        # Extract field-level lineage
        # Enrichments read from Event dataset and write enriched fields to warehouse
        logger.debug(
            f"Extracting field lineage for {enrichment.filename}: "
            f"warehouse_urn={'SET' if warehouse_table_urn else 'NONE'}"
        )
        fine_grained_lineages = self._extract_enrichment_field_lineage(
            enrichment=enrichment,
            event_schema_urns=[input_dataset_urn],  # Event dataset as input
            warehouse_table_urn=warehouse_table_urn,  # Warehouse table as output
        )
        if fine_grained_lineages:
            logger.info(
                f"✅ Extracted {len(fine_grained_lineages)} column-level lineages for {enrichment.filename}"
            )
        else:
            logger.debug(f"No column-level lineage extracted for {enrichment.filename}")

        # Build custom properties
        custom_properties = {
            "enrichmentId": enrichment.id,
            "filename": enrichment.filename,
            "enabled": str(enrichment.enabled),
            "lastUpdate": enrichment.last_update,
        }

        if self._physical_pipeline:
            custom_properties["pipelineId"] = self._physical_pipeline.id
            custom_properties["pipelineName"] = self._physical_pipeline.name

        # Extract enrichment name and description
        enrichment_name = enrichment.filename
        description = f"Snowplow enrichment: {enrichment_name}"

        if enrichment.content and enrichment.content.data:
            enrichment_name = enrichment.content.data.name
            custom_properties["vendor"] = enrichment.content.data.vendor
            custom_properties["schema"] = enrichment.content.schema_ref

            if enrichment.content.data.parameters:
                params_str = str(enrichment.content.data.parameters)
                if len(params_str) > 500:
                    params_str = params_str[:500] + "..."
                custom_properties["parameters"] = params_str

            description = f"{enrichment_name} enrichment"

        if fine_grained_lineages:
            description = self._build_enrichment_description(
                enrichment_name=enrichment_name,
                fine_grained_lineages=fine_grained_lineages,
            )

        # Emit DataJob info
        datajob_info = DataJobInfoClass(
            name=enrichment_name,
            type="ENRICHMENT",
            description=description,
            customProperties=custom_properties,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_info,
        ).as_workunit()

        # Emit ownership if configured
        if self.config.enrichment_owner:
            ownership = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=make_user_urn(self.config.enrichment_owner),
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ],
                lastModified=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor=make_user_urn("datahub"),
                ),
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=datajob_urn,
                aspect=ownership,
            ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit input/output lineage
        # Input: Event dataset (contains all fields from all schemas)
        # Output: Warehouse table (with enriched fields added)
        output_datasets = [warehouse_table_urn] if warehouse_table_urn else []
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=[input_dataset_urn],
            outputDatasets=output_datasets,
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_input_output,
        ).as_workunit()

        if warehouse_table_urn:
            logger.debug(
                f"Linked enrichment {enrichment_name}: Event → warehouse table"
            )
        else:
            logger.debug(
                f"Linked enrichment {enrichment_name}: Event (no warehouse output)"
            )

        logger.debug(f"Extracted enrichment: {enrichment_name}")
        self.report.report_enrichment_extracted()

    def _emit_loader_datajob(
        self,
        dataflow_urn: str,
        input_dataset_urn: str,
        warehouse_table_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit Loader DataJob that loads enriched events from Event dataset to warehouse table.

        This job models the Loader stage of the Snowplow pipeline which writes the fully
        processed and enriched events to the data warehouse (Snowflake, BigQuery, etc.).

        Inputs: Event dataset (all fields including enriched fields)
        Output: Warehouse table
        """
        # Create DataJob URN
        datajob_urn = make_data_job_urn_with_flow(
            flow_urn=dataflow_urn,
            job_id="loader",
        )

        # Emit DataJob info
        datajob_info = DataJobInfoClass(
            name="Loader",
            type="BATCH_SCHEDULED",
            description=(
                "Snowplow Loader stage that writes enriched events to the data warehouse.\n\n"
                "**Input**: Event dataset (all fields from Event Core + custom schemas + enriched fields)\n"
                "**Process**: Writes all event data to warehouse table\n"
                "**Output**: Warehouse table (Snowflake, BigQuery, etc.)\n\n"
                "This stage runs after all enrichments have been applied to the events."
            ),
            customProperties={
                "pipelineId": self._physical_pipeline.id
                if self._physical_pipeline
                else "unknown",
                "pipelineName": self._physical_pipeline.name
                if self._physical_pipeline
                else "unknown",
                "stage": "loader",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_info,
        ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Build fine-grained lineages (field-level)
        # The Loader passes through all fields from Event dataset to warehouse table
        fine_grained_lineages = []

        # Map all Event fields to warehouse fields
        # Combine Event Core fields + extracted schema fields
        all_event_fields = []
        if self._atomic_event_fields:
            all_event_fields.extend(self._atomic_event_fields)
        if self._extracted_schema_fields:
            all_event_fields.extend(
                [field for _, field in self._extracted_schema_fields]
            )

        if all_event_fields:
            for field in all_event_fields:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(input_dataset_urn, field.fieldPath)
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(warehouse_table_urn, field.fieldPath)
                        ],
                    )
                )

            logger.info(
                f"Created {len(fine_grained_lineages)} field-level lineages for Loader job"
            )

        # Emit input/output lineage
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=[input_dataset_urn],
            outputDatasets=[warehouse_table_urn],
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=datajob_input_output,
        ).as_workunit()

        logger.debug(f"Linked Loader job: Event → {warehouse_table_urn}")

    def _emit_pipeline_collector_job(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit a Collector/Parser DataJob that processes schemas and outputs parsed events.

        This job models the Collector → Parser stage of the Snowplow pipeline:
        1. Collector receives events from trackers
        2. Parser validates against schemas and extracts all fields

        Inputs: All schemas (atomic event, custom events, entities)
        Output: Parsed Events dataset (all fields from all schemas)

        The parsed events then flow to enrichment jobs (IP Lookup, UA Parser, etc.)
        which add computed fields and write to the warehouse.
        """
        if not self._physical_pipeline:
            logger.debug("No physical pipeline - skipping pipeline collector job")
            return

        if not self._parsed_events_urn:
            logger.debug(
                "Parsed events dataset not available - skipping pipeline collector job"
            )
            return

        # Collect all schema URNs as inputs
        input_schema_urns = []

        # Add atomic event schema
        if self._atomic_event_urn:
            input_schema_urns.append(self._atomic_event_urn)

        # Add all extracted custom event and entity schemas
        # Get schemas from our cache
        if hasattr(self, "_extracted_schema_urns"):
            input_schema_urns.extend(self._extracted_schema_urns)

        if not input_schema_urns:
            logger.debug("No schema URNs available - skipping pipeline collector job")
            return

        # Create DataFlow for the pipeline
        pipeline_dataflow_urn = make_data_flow_urn(
            orchestrator="snowplow",
            flow_id=f"{self._physical_pipeline.id}_pipeline",
            cluster=self.config.env,
        )

        # Store for enrichments to use
        self._pipeline_dataflow_urn = pipeline_dataflow_urn

        # Emit DataFlow info
        dataflow_info = DataFlowInfoClass(
            name=f"Snowplow Pipeline ({self._physical_pipeline.name})",
            description=(
                f"Snowplow event processing pipeline for {self._physical_pipeline.name}.\n\n"
                "This pipeline processes events through:\n"
                "1. **Collector/Parser**: Receives events from trackers, validates against Iglu schemas, "
                "parses all fields (atomic + custom events + entities)\n"
                "2. **Enrichments**: Add computed fields (IP Lookup, UA Parser, Campaign Attribution, etc.)\n"
                "3. **Loader**: Writes enriched data to warehouse\n\n"
                "The pipeline contains multiple tasks:\n"
                "- Collector/Parser task: Schemas → Parsed Events\n"
                "- Enrichment tasks: Parsed Events → Warehouse (with enriched fields)"
            ),
            customProperties={
                "pipelineId": self._physical_pipeline.id,
                "pipelineName": self._physical_pipeline.name,
                "platform": "snowplow",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_dataflow_urn,
            aspect=dataflow_info,
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self._make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=pipeline_dataflow_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        # Create DataJob for the collector/parser
        pipeline_job_urn = make_data_job_urn_with_flow(
            flow_urn=pipeline_dataflow_urn,
            job_id="collector_parser",
        )

        # Emit DataJob info
        datajob_info = DataJobInfoClass(
            name="Collector/Parser",
            type="BATCH_SCHEDULED",
            description=(
                "Snowplow Collector and Parser stage that receives, validates, and parses events.\n\n"
                "**Inputs**: Event and entity schemas from Iglu registry\n"
                "**Process**:\n"
                "- Collector receives tracker events (HTTP requests)\n"
                "- Parser validates against schemas\n"
                "- Extracts all fields: atomic event fields + custom event data + entity data\n\n"
                "**Output**: Parsed Events dataset containing all fields from all schemas\n\n"
                "The parsed events then flow to enrichment jobs (IP Lookup, Campaign Attribution, etc.) "
                "which add computed fields before writing to the warehouse."
            ),
            customProperties={
                "pipelineId": self._physical_pipeline.id,
                "pipelineName": self._physical_pipeline.name,
                "stage": "collector_parser",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_job_urn,
            aspect=datajob_info,
        ).as_workunit()

        # Build fine-grained lineages (field-level)
        # The Collector/Parser passes through all fields from input schemas to the Event dataset
        fine_grained_lineages = []

        # Map fields from Event Core to Event
        if self._atomic_event_urn and self._atomic_event_fields:
            for field in self._atomic_event_fields:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(
                                self._atomic_event_urn, field.fieldPath
                            )
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(
                                self._parsed_events_urn, field.fieldPath
                            )
                        ],
                    )
                )

        # Map fields from Event/Entity Data Structures to Event
        if self._extracted_schema_fields:
            # Each tuple is (source_urn, field) - we now properly track which field came from which schema
            for source_urn, field in self._extracted_schema_fields:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[make_schema_field_urn(source_urn, field.fieldPath)],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(
                                self._parsed_events_urn, field.fieldPath
                            )
                        ],
                    )
                )

        logger.info(
            f"Created {len(fine_grained_lineages)} field-level lineages for Collector/Parser job"
        )

        # Emit input/output lineage
        datajob_input_output = DataJobInputOutputClass(
            inputDatasets=input_schema_urns,
            outputDatasets=[self._parsed_events_urn],
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_job_urn,
            aspect=datajob_input_output,
        ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        logger.info(
            f"✅ Created Collector/Parser job: {len(input_schema_urns)} schemas → Event"
        )

    def _extract_enrichments(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract enrichments as DataJob entities in the Pipeline DataFlow.

        NEW ARCHITECTURE:
        - Creates one DataJob per enrichment (not per event specification)
        - Enrichments are tasks within the Snowplow Pipeline DataFlow
        - Input: Parsed Events dataset (all fields from all schemas)
        - Output: Warehouse table (with enriched fields added)

        Flow: Schemas → Collector/Parser → Parsed Events → Enrichments → Warehouse

        Example: [Parsed Events] → [IP Lookup DataJob] → [Warehouse]
        """
        if not self.bdp_client:
            return

        try:
            # Get warehouse destination table URN (if configured)
            warehouse_table_urn = self._get_warehouse_table_urn()
            if warehouse_table_urn:
                logger.info(
                    f"✅ Enrichments will output to warehouse table: {warehouse_table_urn}"
                )
                logger.info("✅ Column-level lineage will be extracted for enrichments")
            else:
                logger.warning(
                    "⚠️  No warehouse table URN found - column-level lineage will NOT be extracted"
                )
                logger.warning(
                    "⚠️  To enable column-level lineage, ensure your Snowplow pipeline has a destination configured"
                )

            # Emit Pipeline/Collector job that processes all schemas and outputs to parsed events
            # This must be called before enrichments so enrichments can use the Pipeline DataFlow
            yield from self._emit_pipeline_collector_job()

            # Get enrichments from physical pipeline (enrichments configured once, apply to all events)
            if not self._physical_pipeline:
                logger.warning(
                    "No physical pipeline found - cannot extract enrichments. "
                    "Make sure _extract_pipelines() ran first."
                )
                return

            if (
                not hasattr(self, "_pipeline_dataflow_urn")
                or not self._pipeline_dataflow_urn
            ):
                logger.warning(
                    "Pipeline DataFlow URN not available - cannot extract enrichments. "
                    "Make sure _emit_pipeline_collector_job() ran first."
                )
                return

            if not self._parsed_events_urn:
                logger.warning(
                    "Parsed events URN not available - cannot extract enrichments."
                )
                return

            enrichments = self.bdp_client.get_enrichments(self._physical_pipeline.id)
            total_enrichments = len(enrichments)
            logger.info(
                f"Found {total_enrichments} enrichments in pipeline {self._physical_pipeline.name}"
            )

            # Enrichments apply to ALL events, so we emit them once (not per-event-spec)
            # They read from the Parsed Events dataset and write to warehouse
            for enrichment in enrichments:
                self.report.report_enrichment_found()

                if not enrichment.enabled:
                    self.report.report_enrichment_filtered(enrichment.filename)
                    logger.debug(f"Skipping disabled enrichment: {enrichment.filename}")
                    continue

                # Emit enrichment DataJob with all aspects
                # Enrichments read from Event and write enriched fields to warehouse
                yield from self._emit_enrichment_datajob(
                    enrichment=enrichment,
                    dataflow_urn=self._pipeline_dataflow_urn,
                    input_dataset_urn=self._parsed_events_urn,
                    warehouse_table_urn=warehouse_table_urn,
                )

            self.report.num_enrichments_found = total_enrichments

            # Emit Loader job (Event → Snowflake warehouse)
            if warehouse_table_urn:
                yield from self._emit_loader_datajob(
                    dataflow_urn=self._pipeline_dataflow_urn,
                    input_dataset_urn=self._parsed_events_urn,
                    warehouse_table_urn=warehouse_table_urn,
                )

        except Exception as e:
            self.report.report_failure(
                title="Failed to extract enrichments",
                message="Unable to retrieve enrichments from BDP API. Check API credentials and permissions.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )

    def _construct_warehouse_urn(
        self,
        query_engine: str,
        table_name: str,
        destination_id: Optional[str] = None,
    ) -> str:
        """
        Construct warehouse table URN with optional platform instance.

        Args:
            query_engine: Query engine type (snowflake, bigquery, databricks, redshift)
            table_name: Fully qualified table name (e.g., database.schema.table)
            destination_id: Optional Snowplow destination UUID for mapping lookup

        Returns:
            DataHub URN for warehouse table
        """
        # Find mapping for this destination
        platform_instance = None
        env = self.config.warehouse_lineage.env

        if destination_id:
            for mapping in self.config.warehouse_lineage.destination_mappings:
                if mapping.destination_id == destination_id:
                    platform_instance = mapping.platform_instance
                    env = mapping.env
                    logger.debug(
                        f"Using destination mapping for {destination_id}: "
                        f"platform_instance={platform_instance}, env={env}"
                    )
                    break

        # Fallback to global config
        if platform_instance is None:
            platform_instance = self.config.warehouse_lineage.platform_instance

        # Build dataset name with optional platform instance prefix
        dataset_name = table_name
        if platform_instance:
            dataset_name = f"{platform_instance}.{table_name}"

        urn = make_dataset_urn(
            platform=query_engine,
            name=dataset_name,
            env=env,
        )

        logger.debug(
            f"Constructed warehouse URN: {urn} "
            f"(query_engine={query_engine}, table_name={table_name}, "
            f"platform_instance={platform_instance}, env={env})"
        )

        return urn

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
        if not self._parsed_events_urn:
            logger.debug(
                f"Parsed events dataset not created, skipping column lineage for {vendor}/{name}"
            )
            return

        # Extract field paths from schema metadata
        if not schema_metadata.fields:
            logger.debug(f"No fields in schema metadata for {vendor}/{name}")
            return

        # Get the Snowflake VARIANT column name
        snowflake_column = self._map_schema_to_snowflake_column(
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
            dataset=self._parsed_events_urn,  # Parsed events dataset (intermediate)
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

    def _extract_warehouse_lineage_via_data_models(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage to warehouse destinations via Data Models API.

        Creates lineage from enrichment outputs to warehouse tables by:
        1. Fetching data products and their data models
        2. Extracting destination warehouse info (query engine, table name)
        3. Constructing warehouse table URNs
        4. Creating lineage edges

        This approach uses the DataHub Graph API to validate warehouse URNs
        and avoids needing warehouse credentials in the Snowplow connector.
        """
        if not self.config.warehouse_lineage.enabled:
            yield from []  # Empty generator
            return

        if not self.bdp_client:
            logger.warning(
                "Warehouse lineage enabled but BDP client not available. Skipping."
            )
            yield from []  # Empty generator
            return

        logger.info("Extracting warehouse lineage via Data Models API...")

        try:
            # Get all data products
            data_products = self.bdp_client.get_data_products()
            logger.info(
                f"Found {len(data_products)} data products, checking for data models..."
            )

            total_models = 0
            total_lineage_created = 0

            for data_product in data_products:
                # Get data models for this product
                data_models = self.bdp_client.get_data_models(data_product.id)

                if not data_models:
                    logger.debug(
                        f"No data models configured for data product '{data_product.name}'"
                    )
                    continue

                logger.info(
                    f"Data product '{data_product.name}' has {len(data_models)} data models"
                )
                total_models += len(data_models)

                for data_model in data_models:
                    # Skip if model doesn't have destination info
                    if not data_model.destination or not data_model.query_engine:
                        logger.debug(
                            f"Skipping data model '{data_model.name}': missing destination or query_engine"
                        )
                        continue

                    if not data_model.table_name:
                        logger.warning(
                            f"Data model '{data_model.name}' has destination but no table_name, skipping"
                        )
                        continue

                    # Construct warehouse table URN
                    warehouse_urn = self._construct_warehouse_urn(
                        query_engine=data_model.query_engine,
                        table_name=data_model.table_name,
                        destination_id=data_model.destination,
                    )

                    # Optional: Validate URN exists in DataHub
                    if self.config.warehouse_lineage.validate_urns:
                        if self.ctx.graph:
                            exists = self.ctx.graph.exists(warehouse_urn)
                            if not exists:
                                logger.warning(
                                    f"Warehouse table {warehouse_urn} not found in DataHub. "
                                    f"Lineage will be created but may appear broken until warehouse is ingested. "
                                    f"Set validate_urns=False to skip this check."
                                )
                        else:
                            logger.debug(
                                "Graph client not available, skipping URN validation"
                            )

                    # Get source table URN (atomic.events where raw events land)
                    source_table_urn = self._get_warehouse_table_urn()

                    if source_table_urn:
                        # Create lineage: atomic.events → derived table
                        upstream = UpstreamClass(
                            dataset=source_table_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )

                        upstream_lineage = UpstreamLineageClass(upstreams=[upstream])

                        yield MetadataChangeProposalWrapper(
                            entityUrn=warehouse_urn,
                            aspect=upstream_lineage,
                        ).as_workunit()

                        logger.info(
                            f"Created warehouse lineage: {source_table_urn} → {warehouse_urn} (via data model '{data_model.name}')"
                        )
                        total_lineage_created += 1
                    else:
                        logger.warning(
                            f"Cannot create lineage for data model '{data_model.name}': "
                            "source warehouse table URN (atomic.events) not available. "
                            "Ensure warehouse destination is configured in BDP."
                        )

            logger.info(
                f"Warehouse lineage extraction complete: "
                f"{total_models} data models processed, "
                f"{total_lineage_created} lineage edges created"
            )

        except Exception as e:
            self.report.warning(
                title="Failed to extract warehouse lineage via data models",
                message="Unable to retrieve data models from BDP API for lineage extraction. This is optional and won't affect other metadata.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            traceback.print_exc()

    # ============================================
    # Helper Methods - URN Generation
    # ============================================

    def _make_organization_urn(self, org_id: str) -> str:
        """Generate URN for organization container."""
        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        return org_key.as_urn()

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

    def _build_dataset_tags(
        self, name: str, schema_type: Optional[str]
    ) -> Optional[GlobalTagsClass]:
        """
        Build dataset-level tags.

        Dataset-level tags apply to the schema/dataset as a whole, not individual fields.
        Currently adds:
        - Event type tag: Category derived from schema name (e.g., "checkout" from "checkout_started")

        Args:
            name: Schema name (e.g., "checkout_started")
            schema_type: Schema type ("event" or "entity")

        Returns:
            GlobalTagsClass with dataset-level tags, or None if no tags to add
        """
        tags = set()

        # Event type tag (category derived from name)
        # Extract first word: checkout_started → checkout
        event_name = name.split("_")[0] if "_" in name else name
        event_type_tag = self.config.field_tagging.event_type_pattern.format(
            name=event_name
        )
        tags.add(event_type_tag)

        if not tags:
            return None

        # Convert to TagAssociationClass
        tag_associations = [
            TagAssociationClass(tag=f"urn:li:tag:{tag}") for tag in sorted(tags)
        ]

        return GlobalTagsClass(tags=tag_associations)

    def _add_field_tags(
        self,
        schema_metadata: SchemaMetadataClass,
        data_structure: DataStructure,
        version: str,
    ) -> SchemaMetadataClass:
        """
        Add tags to schema fields.

        Generates field-level tags for:
        - Schema version (which version this field was added in, if track_field_versions enabled)
        - Data classification (PII, Sensitive from enrichment config + patterns)
        - Authorship (who deployed the version when field was added)

        Note: Event type tags are added at dataset level, not field level (see _build_dataset_tags).
        """
        # Get PII fields from enrichments (if configured)
        pii_fields = self._extract_pii_fields()

        # Build field version mapping if enabled
        field_version_map: Dict[str, str] = {}
        if self.config.field_tagging.track_field_versions:
            field_version_map = self._build_field_version_mapping(data_structure)

        # Build deployment version -> initiator and timestamp mappings
        deployment_initiators: Dict[str, Optional[str]] = {}
        deployment_timestamps: Dict[str, Optional[str]] = {}
        if data_structure.deployments:
            for dep in data_structure.deployments:
                if dep.version:
                    deployment_initiators[dep.version] = dep.initiator
                    deployment_timestamps[dep.version] = dep.ts

        # Get latest deployment initiator and timestamp as fallback
        fallback_initiator = None
        fallback_timestamp = None
        if data_structure.deployments:
            sorted_deps = sorted(
                data_structure.deployments,
                key=lambda d: d.ts or "",
                reverse=True,
            )
            fallback_initiator = sorted_deps[0].initiator
            fallback_timestamp = sorted_deps[0].ts

        # Determine the initial version (oldest version) for this schema
        initial_version = None
        if self.config.field_tagging.track_field_versions and field_version_map:
            initial_version = min(
                field_version_map.values(),
                key=lambda v: self._parse_schemaver(v),
                default=None,
            )

        # Add tags and descriptions to each field
        for field in schema_metadata.fields:
            # Determine which version to use for this field
            field_version = version  # Default: current version
            field_initiator = fallback_initiator  # Default: latest initiator
            field_timestamp = fallback_timestamp  # Default: latest timestamp
            skip_version_tag = False  # Whether to skip version tag for this field

            if self.config.field_tagging.track_field_versions and field_version_map:
                # Use the version when field was added
                added_in_version = field_version_map.get(field.fieldPath)
                if added_in_version:
                    # Check if this is the initial version
                    if added_in_version == initial_version:
                        # Don't tag initial version fields - they were there from the start
                        skip_version_tag = True
                    else:
                        # Field was added later - tag it with the version it was added
                        field_version = added_in_version
                        field_initiator = deployment_initiators.get(
                            added_in_version, fallback_initiator
                        )
                        field_timestamp = deployment_timestamps.get(
                            added_in_version, fallback_timestamp
                        )

                        # Add "Added in version X" to description
                        version_note = f" (Added in version {added_in_version})"
                        if field.description:
                            field.description = field.description + version_note
                        else:
                            field.description = f"Added in version {added_in_version}"

            # Determine event type based on data structure schema type
            event_type = None
            if data_structure.meta and data_structure.meta.schema_type:
                if data_structure.meta.schema_type == "event":
                    event_type = "self_describing"
                elif data_structure.meta.schema_type == "entity":
                    event_type = "context"
                # Note: atomic events don't have data structures

            # Create tagging context
            context = FieldTagContext(
                schema_version=field_version,
                vendor=data_structure.vendor or "",
                name=data_structure.name or "",
                field_name=field.fieldPath,
                field_type=field.nativeDataType,
                field_description=field.description,
                deployment_initiator=field_initiator,
                deployment_timestamp=field_timestamp,
                pii_fields=pii_fields,
                skip_version_tag=skip_version_tag,
                event_type=event_type,
            )

            # Generate and apply tags
            field_tags = self.field_tagger.generate_tags(context)
            if field_tags:
                field.globalTags = field_tags

        return schema_metadata

    def _emit_field_structured_properties(
        self,
        dataset_urn: str,
        schema_metadata: SchemaMetadataClass,
        data_structure: DataStructure,
        version: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit structured properties for schema fields.

        Args:
            dataset_urn: URN of the dataset containing the fields
            schema_metadata: Schema metadata with field information
            data_structure: Data structure with deployment information
            version: Schema version

        Yields:
            MetadataWorkUnit containing structured properties for fields
        """
        # Skip if structured properties not enabled
        if not self.config.field_tagging.use_structured_properties:
            return

        # Get PII fields from enrichments (if configured)
        pii_fields = self._extract_pii_fields()

        # Build field version mapping if enabled
        field_version_map: Dict[str, str] = {}
        if self.config.field_tagging.track_field_versions:
            field_version_map = self._build_field_version_mapping(data_structure)

        # Build deployment version -> initiator and timestamp mappings
        deployment_initiators: Dict[str, Optional[str]] = {}
        deployment_timestamps: Dict[str, Optional[str]] = {}
        if data_structure.deployments:
            for dep in data_structure.deployments:
                if dep.version:
                    deployment_initiators[dep.version] = dep.initiator
                    deployment_timestamps[dep.version] = dep.ts

        # Get latest deployment initiator and timestamp as fallback
        fallback_initiator = None
        fallback_timestamp = None
        if data_structure.deployments:
            sorted_deps = sorted(
                data_structure.deployments,
                key=lambda d: d.ts or "",
                reverse=True,
            )
            fallback_initiator = sorted_deps[0].initiator
            fallback_timestamp = sorted_deps[0].ts

        # Determine the initial version (oldest version) for this schema
        initial_version = None
        if self.config.field_tagging.track_field_versions and field_version_map:
            initial_version = min(
                field_version_map.values(),
                key=lambda v: self._parse_schemaver(v),
                default=None,
            )

        # Emit structured properties for each field
        for field in schema_metadata.fields:
            # Determine which version to use for this field
            field_version = version  # Default: current version
            field_initiator = fallback_initiator  # Default: latest initiator
            field_timestamp = fallback_timestamp  # Default: latest timestamp
            skip_version_tag = False  # Whether to skip version tag for this field

            if self.config.field_tagging.track_field_versions and field_version_map:
                # Use the version when field was added
                added_in_version = field_version_map.get(field.fieldPath)
                if added_in_version:
                    # Check if this is the initial version
                    if added_in_version == initial_version:
                        # Don't emit structured properties for initial version fields
                        skip_version_tag = True
                    else:
                        # Field was added later
                        field_version = added_in_version
                        field_initiator = deployment_initiators.get(
                            added_in_version, fallback_initiator
                        )
                        field_timestamp = deployment_timestamps.get(
                            added_in_version, fallback_timestamp
                        )

            # Determine event type based on data structure schema type
            event_type = None
            if data_structure.meta and data_structure.meta.schema_type:
                if data_structure.meta.schema_type == "event":
                    event_type = "self_describing"
                elif data_structure.meta.schema_type == "entity":
                    event_type = "context"
                # Note: atomic events don't have data structures

            # Create context for structured properties
            context = FieldTagContext(
                schema_version=field_version,
                vendor=data_structure.vendor or "",
                name=data_structure.name or "",
                field_name=field.fieldPath,
                field_type=field.nativeDataType,
                field_description=field.description,
                deployment_initiator=field_initiator,
                deployment_timestamp=field_timestamp,
                pii_fields=pii_fields,
                skip_version_tag=skip_version_tag,
                event_type=event_type,
            )

            # Generate and emit structured properties
            yield from self.field_tagger.generate_field_structured_properties(
                dataset_urn=dataset_urn,
                field=field,
                context=context,
            )

    def _register_structured_properties(self) -> Iterable[MetadataWorkUnit]:
        """
        Register structured property definitions for Snowplow field metadata.

        These structured properties are used to track field authorship, versioning,
        and classification. This method automatically creates the property definitions
        in DataHub if they don't already exist.

        Yields:
            MetadataWorkUnit containing structured property definitions
        """
        if not self.config.field_tagging.use_structured_properties:
            return

        logger.info("Registering Snowplow field structured property definitions")

        # Define structured properties
        properties: List[Dict[str, Any]] = [
            {
                "id": "io.acryl.snowplow.fieldAuthor",
                "display_name": "Field Author",
                "description": "The person who added this field to the Snowplow event specification or data structure. "
                "This tracks the initiator of the deployment that introduced the field.",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": "io.acryl.snowplow.fieldVersionAdded",
                "display_name": "Version Added",
                "description": "The schema version when this field was first added to the event specification. "
                "Format: Major-Minor-Patch (e.g., '1-0-2' for version 1.0.2)",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": "io.acryl.snowplow.fieldAddedTimestamp",
                "display_name": "Added Timestamp",
                "description": "ISO 8601 timestamp when this field was first added to the event specification. "
                "Format: YYYY-MM-DDTHH:MM:SSZ (e.g., '2024-01-15T10:30:00Z')",
                "value_type": "string",
                "cardinality": "SINGLE",
            },
            {
                "id": "io.acryl.snowplow.fieldDataClass",
                "display_name": "Data Classification",
                "description": "The data classification level for this field based on PII enrichment analysis. "
                "Values: 'pii', 'sensitive', 'public', 'internal'",
                "value_type": "string",
                "cardinality": "SINGLE",
                "allowed_values": [
                    {
                        "value": "pii",
                        "description": "Contains personally identifiable information",
                    },
                    {
                        "value": "sensitive",
                        "description": "Contains sensitive business data",
                    },
                    {
                        "value": "public",
                        "description": "Public information, safe to share externally",
                    },
                    {
                        "value": "internal",
                        "description": "Internal information, not for external sharing",
                    },
                ],
            },
            {
                "id": "io.acryl.snowplow.fieldEventType",
                "display_name": "Event Type",
                "description": "The type of Snowplow event this field belongs to. "
                "Values: 'self_describing', 'atomic', 'context'",
                "value_type": "string",
                "cardinality": "SINGLE",
                "allowed_values": [
                    {
                        "value": "self_describing",
                        "description": "Custom self-describing event field",
                    },
                    {"value": "atomic", "description": "Snowplow atomic event field"},
                    {"value": "context", "description": "Context entity field"},
                ],
            },
        ]

        # Create structured property definitions
        for prop in properties:
            urn = StructuredPropertyUrn(prop["id"]).urn()

            # Build allowed values if provided
            allowed_values = None
            if "allowed_values" in prop:
                allowed_values = [
                    PropertyValueClass(value=av["value"], description=av["description"])
                    for av in prop["allowed_values"]
                ]

            aspect = StructuredPropertyDefinition(
                qualifiedName=prop["id"],
                displayName=prop["display_name"],
                description=prop["description"],
                valueType=DataTypeUrn(f"datahub.{prop['value_type']}").urn(),
                entityTypes=[
                    EntityTypeUrn(f"datahub.{SchemaFieldUrn.ENTITY_TYPE}").urn(),
                ],
                cardinality=prop["cardinality"],
                allowedValues=allowed_values,
                lastModified=AuditStamp(
                    time=get_sys_time(), actor="urn:li:corpuser:datahub"
                ),
            )

            # Emit the structured property definition
            # Using If-None-Match: * header ensures we only create if it doesn't exist
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=aspect,
                changeType=ChangeTypeClass.CREATE,
                headers={"If-None-Match": "*"},
            ).as_workunit()

        logger.info("Registered 5 Snowplow field structured property definitions")

    def _extract_pii_fields(self) -> set:
        """
        Extract PII fields from PII Pseudonymization enrichment configuration.

        Returns a set of field names that are configured as PII in the
        enrichment config. This is more accurate than pattern matching alone.

        Returns:
            Set of field names that should be classified as PII
        """
        # Return cached value if available
        if self._pii_fields_cache is not None:
            return self._pii_fields_cache

        pii_fields: set = set()

        # Only extract from enrichments if use_pii_enrichment is enabled
        if not self.config.field_tagging.use_pii_enrichment:
            self._pii_fields_cache = pii_fields
            return pii_fields

        # Extract from BDP enrichments
        if not self.bdp_client:
            self._pii_fields_cache = pii_fields
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
                    logger.debug(
                        f"Failed to extract PII fields from pipeline {pipeline.id}: {e}"
                    )
                    continue

        except Exception as e:
            logger.warning(f"Failed to extract PII fields from enrichments: {e}")

        # Cache the result
        self._pii_fields_cache = pii_fields
        return pii_fields

    def _make_schema_dataset_urn(self, vendor: str, name: str, version: str) -> str:
        """
        Generate dataset URN for schema.

        By default (include_version_in_urn=False), version is NOT included in URN.
        This creates a single dataset entity per schema, with version tracked in properties.

        When include_version_in_urn=True (legacy), version is included in URN for
        backwards compatibility with existing metadata.
        """
        if self.config.include_version_in_urn:
            # Legacy behavior: version in URN
            dataset_name = f"{vendor}.{name}.{version}".replace("/", ".")
        else:
            # New behavior: version in properties only
            dataset_name = f"{vendor}.{name}".replace("/", ".")

        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

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

    def _make_event_spec_dataset_urn(self, event_spec_id: str) -> str:
        """Generate dataset URN for event specification."""
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"event_spec.{event_spec_id}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _make_tracking_scenario_urn(self, scenario_id: str) -> str:
        """Generate container URN for tracking scenario."""
        if not self.config.bdp_connection:
            # Fallback for non-BDP mode (shouldn't happen)
            return make_container_urn(f"snowplow_scenario_{scenario_id}")

        org_id = self.config.bdp_connection.organization_id
        scenario_key = SnowplowTrackingScenarioKey(
            organization_id=org_id,
            scenario_id=scenario_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        return scenario_key.as_urn()

    def _get_schema_url(
        self,
        vendor: str,
        name: str,
        version: str,
        data_structure_hash: Optional[str] = None,
    ) -> Optional[str]:
        """
        Generate BDP Console URL for schema (if BDP configured).

        Args:
            vendor: Schema vendor (for fallback if hash not available)
            name: Schema name (for fallback if hash not available)
            version: Schema version
            data_structure_hash: Data structure hash/ID from BDP API

        Returns:
            BDP Console URL or None if not in BDP mode
        """
        if self.config.bdp_connection:
            org_id = self.config.bdp_connection.organization_id

            # Use hash-based URL if available (correct format)
            if data_structure_hash:
                return (
                    f"https://console.snowplowanalytics.com/{org_id}/"
                    f"data-structures/{data_structure_hash}?version={version}"
                )

            # Fallback to vendor/name format (may not work, but better than nothing)
            logger.warning(
                f"Data structure hash not available for {vendor}/{name}, using fallback URL format"
            )
            return (
                f"https://console.snowplowanalytics.com/{org_id}/"
                f"data-structures/{vendor}/{name}/{version}"
            )
        return None

    # ============================================
    # Source Interface Methods
    # ============================================

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
