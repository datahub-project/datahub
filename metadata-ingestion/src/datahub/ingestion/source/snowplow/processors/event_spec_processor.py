"""
Event Specification Processor for Snowplow connector.

Handles extraction of event specifications from Snowplow BDP.
"""

import logging
from typing import TYPE_CHECKING, Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.ingestion.source.snowplow.snowplow_models import EventSpecification
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class EventSpecProcessor(EntityProcessor):
    """
    Processor for extracting event specification metadata from Snowplow BDP.

    Event specifications define the structure and rules for events.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize event spec processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if event specification extraction is enabled."""
        return (
            self.config.extract_event_specifications
            and self.deps.bdp_client is not None
        )

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract event specifications from BDP.

        Event specifications are treated as datasets with references to schemas.

        Yields:
            MetadataWorkUnit: Event specification metadata work units
        """
        if not self.deps.bdp_client:
            return

        try:
            event_specs = self.deps.bdp_client.get_event_specifications()
        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="fetch event specifications",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )
            return

        for event_spec in event_specs:
            self.report.report_event_spec_found()

            # Apply filtering
            if not self.config.event_spec_pattern.allowed(event_spec.name):
                self.report.report_event_spec_filtered(event_spec.name)
                continue

            # Track that this event spec was emitted (for container linking)
            self.state.emitted_event_spec_ids.add(event_spec.id)

            # Capture first event spec ID and name for parsed events dataset naming
            if self.state.event_spec_id is None:
                self.state.event_spec_id = event_spec.id
                self.state.event_spec_name = event_spec.name

            yield from self._process_event_specification(event_spec)

    def _process_event_specification(
        self, event_spec: EventSpecification
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single event specification.

        Args:
            event_spec: Event specification from BDP API

        Yields:
            MetadataWorkUnit: Event specification metadata work units
        """
        # Generate dataset URN for event spec
        dataset_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec.id)

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
            org_urn = self.urn_factory.make_organization_urn(
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
                schema_urn = self.urn_factory.make_schema_dataset_urn(
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
