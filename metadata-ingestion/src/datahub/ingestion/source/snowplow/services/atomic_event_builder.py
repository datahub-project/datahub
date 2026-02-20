"""
Atomic Event Builder for Snowplow connector.

Handles creation of the synthetic Atomic Event dataset representing Snowplow's canonical event model.
"""

import logging
from typing import TYPE_CHECKING, Iterable, List, Tuple

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import IngestionState

logger = logging.getLogger(__name__)

# Atomic field definitions: (field_name, description)
# Based on: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0
ATOMIC_FIELD_DEFINITIONS: List[Tuple[str, str]] = [
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


class AtomicEventBuilder:
    """
    Builder for creating the synthetic Atomic Event dataset.

    The atomic event dataset represents Snowplow's canonical event model - the standard
    schema containing ~130 fields that exist BEFORE enrichments run.
    """

    def __init__(
        self,
        config: SnowplowSourceConfig,
        urn_factory: SnowplowURNFactory,
        state: "IngestionState",
        platform: str,
    ):
        """
        Initialize atomic event builder.

        Args:
            config: Source configuration
            urn_factory: URN factory for creating organization URNs
            state: Shared mutable state to populate atomic_event_urn and atomic_event_fields
            platform: Platform identifier (e.g., "snowplow")
        """
        self.config = config
        self.urn_factory = urn_factory
        self.state = state
        self.platform = platform

    def create_atomic_event_dataset(self) -> Iterable[MetadataWorkUnit]:
        """
        Create a synthetic dataset representing the Snowplow Atomic Event schema.

        The atomic event schema contains ~50 standard fields defined in the canonical event model.
        Only ~12 fields are REQUIRED (always populated): app_id, platform, collector_tstamp, event,
        event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version.

        Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
        Schema definition: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0

        Yields:
            MetadataWorkUnit: Work units for atomic event dataset metadata
        """
        # Create URN and cache to state
        self.state.atomic_event_urn = self._create_atomic_event_urn()

        # Create and cache schema fields
        schema_fields = self._create_schema_fields()
        self.state.atomic_event_fields = schema_fields

        # Emit all aspects
        yield from self._emit_schema_metadata(schema_fields)
        yield from self._emit_dataset_properties()
        yield from self._emit_status()
        yield from self._emit_subtypes()
        yield from self._emit_container()

        logger.info(
            f"Created Atomic Event dataset with {len(ATOMIC_FIELD_DEFINITIONS)} atomic fields (excluding enriched output fields)"
        )

    def _create_atomic_event_urn(self) -> str:
        """Create URN for the atomic event dataset."""
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name="atomic_event",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _create_schema_fields(self) -> List[SchemaFieldClass]:
        """Convert atomic field definitions to SchemaFieldClass objects."""
        return [
            SchemaFieldClass(
                fieldPath=field_name,
                nativeDataType="string",  # Simplified - actual types vary
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                description=description,
                nullable=True,
                recursive=False,
            )
            for field_name, description in ATOMIC_FIELD_DEFINITIONS
        ]

    def _emit_schema_metadata(
        self, schema_fields: List[SchemaFieldClass]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit schema metadata aspect."""
        schema_metadata = SchemaMetadataClass(
            schemaName="snowplow/atomic_event",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=schema_fields,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=schema_metadata,
        ).as_workunit()

    def _emit_dataset_properties(self) -> Iterable[MetadataWorkUnit]:
        """Emit dataset properties aspect with description and custom properties."""
        dataset_properties = DatasetPropertiesClass(
            name="Atomic Event",
            description=(
                "**Atomic Event** represents the standard Snowplow atomic event schema as it exists BEFORE enrichments run. "
                "Contains ~50 core fields including:\n"
                "- **Required fields** (~12): app_id, platform, collector_tstamp, event, event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version\n"
                "- **Input fields for enrichments**: user_ipaddress, useragent, page_urlquery, page_referrer\n"
                "- **Other atomic fields**: timestamps, user IDs, page URLs, referrer URLs, browser/device properties\n\n"
                "**Does NOT include enriched output fields** like geo_*, ip_*, mkt_*, br_name/family/version, os_*, dvce_type/ismobile, etc. "
                "Those fields are created by enrichments and only exist in the warehouse table.\n\n"
                "Enrichments read from Atomic Event fields and write new enriched fields to the warehouse table. "
                "\n\n"
                "This dataset represents the SCHEMA (available fields), not a guarantee that all fields are "
                "populated on every event. This is correct for field-level lineage - we model what fields "
                "enrichments CAN read/write based on schema definition."
                "\n\n"
                "Schema source: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0\n"
                "Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/"
            ),
            customProperties={
                "schema_type": "atomic_event",
                "platform": "snowplow",
                "field_count": str(len(ATOMIC_FIELD_DEFINITIONS)),
                "required_fields": "app_id, platform, collector_tstamp, event, event_id, v_tracker, v_collector, v_etl, event_vendor, event_name, event_format, event_version",
                "schema_source": "https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0",
            },
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=dataset_properties,
        ).as_workunit()

    def _emit_status(self) -> Iterable[MetadataWorkUnit]:
        """Emit status aspect."""
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

    def _emit_subtypes(self) -> Iterable[MetadataWorkUnit]:
        """Emit subTypes aspect."""
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=SubTypesClass(typeNames=["atomic_event"]),
        ).as_workunit()

    def _emit_container(self) -> Iterable[MetadataWorkUnit]:
        """Emit container aspect linking to organization."""
        if self.config.bdp_connection:
            org_container_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self.state.atomic_event_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()
