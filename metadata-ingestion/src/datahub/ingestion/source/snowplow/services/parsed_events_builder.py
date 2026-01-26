"""
Parsed Events Builder for Snowplow connector.

Handles creation of the synthetic Parsed Events dataset representing all event fields
after Collector/Parser stage and before enrichments run.
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
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import IngestionState

logger = logging.getLogger(__name__)


class ParsedEventsBuilder:
    """
    Builder for creating the synthetic Parsed Events dataset.

    The parsed events dataset represents all event fields after the Collector/Parser stage
    and before enrichments run. It combines atomic event fields with custom event and
    entity schema fields.
    """

    def __init__(
        self,
        config: SnowplowSourceConfig,
        urn_factory: SnowplowURNFactory,
        state: "IngestionState",
        platform: str,
    ):
        """
        Initialize parsed events builder.

        Args:
            config: Source configuration
            urn_factory: URN factory for creating organization URNs
            state: Shared mutable state containing event spec info and schema fields
            platform: Platform identifier (e.g., "snowplow")
        """
        self.config = config
        self.urn_factory = urn_factory
        self.state = state
        self.platform = platform

    def create_parsed_events_dataset(self) -> Iterable[MetadataWorkUnit]:
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

        Yields:
            MetadataWorkUnit: Work units for parsed events dataset metadata
        """
        # Create URN for parsed events dataset using same pattern as Atomic Event
        # Use direct dataset name without vendor prefix (synthetic Snowplow concept, not an Iglu schema)
        # Name after the event specification ID to link it to the event spec
        # URN format: {event_spec_id}_event (e.g., "650986b2-ad4a-453f-a0f1-4a2df337c31d_event")
        # Display name: {event_spec_name} Event (e.g., "checkout_started Event")
        if self.state.event_spec_id and self.state.event_spec_name:
            dataset_name = f"{self.state.event_spec_id}_event"
            display_name = f"{self.state.event_spec_name} Event"
        elif (
            self.state.first_event_schema_vendor and self.state.first_event_schema_name
        ):
            # Fallback to first event schema found (more specific than generic "Event")
            # URN format: {vendor}.{name}_event (e.g., "com.example.checkout_started_event")
            # Display name: {name} Event (e.g., "checkout_started Event")
            schema_identifier = f"{self.state.first_event_schema_vendor}.{self.state.first_event_schema_name}".replace(
                "/", "."
            )
            dataset_name = f"{schema_identifier}_event"
            display_name = f"{self.state.first_event_schema_name} Event"
            logger.info(
                f"Using event schema for Event dataset naming: {self.state.first_event_schema_vendor}/{self.state.first_event_schema_name}"
            )
        else:
            # Final fallback if no event spec or event schema found
            dataset_name = "event"
            display_name = "Event"
            logger.warning(
                "No event spec or event schema found - using generic 'Event' dataset name"
            )

        self.state.parsed_events_urn = make_dataset_urn_with_platform_instance(
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
                "- **Atomic Event**: ~50 standard Snowplow atomic fields (user_ipaddress, page_url, useragent, etc.)\n"
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
                "event_spec_id": self.state.event_spec_id or "",
                "event_spec_name": self.state.event_spec_name or "",
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.parsed_events_urn,
            aspect=dataset_properties,
        ).as_workunit()

        # Emit schema metadata
        # The Event dataset contains ALL fields from ALL schemas (Atomic Event + Event Data Structures + Entity Data Structures)
        # Combine all cached fields
        all_event_fields = []

        # Add atomic event fields (Atomic Event)
        if self.state.atomic_event_fields:
            all_event_fields.extend(self.state.atomic_event_fields)

        # Add custom event and entity fields (uses efficient accessor method)
        extracted_fields = self.state.get_all_extracted_fields()
        all_event_fields.extend(extracted_fields)

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
                entityUrn=self.state.parsed_events_urn,
                aspect=event_schema_metadata,
            ).as_workunit()

            logger.info(
                f"Event dataset schema contains {len(all_event_fields)} fields from all schemas "
                f"(Atomic Event: {len(self.state.atomic_event_fields)}, "
                f"Event/Entity Data Structures: {self.state.get_extracted_fields_count()})"
            )
        else:
            logger.warning(
                "No schema fields available for Event dataset - schemas may not have been extracted yet"
            )

        # Emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.parsed_events_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit subTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=self.state.parsed_events_urn,
            aspect=SubTypesClass(typeNames=["event"]),
        ).as_workunit()

        # Emit container (organization)
        if self.config.bdp_connection:
            org_container_urn = self.urn_factory.make_organization_urn(
                self.config.bdp_connection.organization_id
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=self.state.parsed_events_urn,
                aspect=ContainerClass(container=org_container_urn),
            ).as_workunit()

        logger.info(
            "Created Event dataset (intermediate: collector/parser → enrichments)"
        )
