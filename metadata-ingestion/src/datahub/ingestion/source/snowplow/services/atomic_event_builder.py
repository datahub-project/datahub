"""
Atomic Event Builder for Snowplow connector.

Handles creation of the synthetic Atomic Event dataset representing Snowplow's canonical event model.
"""

import logging
from typing import TYPE_CHECKING, Iterable

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

        Yields:
            MetadataWorkUnit: Work units for atomic event dataset metadata
        """
        # Create URN for atomic event dataset (Event Core)
        # Use direct dataset name without vendor prefix (synthetic Snowplow concept, not an Iglu schema)
        self.state.atomic_event_urn = make_dataset_urn_with_platform_instance(
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
        self.state.atomic_event_fields = schema_fields

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
            entityUrn=self.state.atomic_event_urn,
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
            entityUrn=self.state.atomic_event_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit subTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.atomic_event_urn,
            aspect=SubTypesClass(typeNames=["event_core"]),
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self.state.atomic_event_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        logger.info(
            f"Created Event Core dataset with {len(atomic_fields)} atomic fields (excluding enriched output fields)"
        )
