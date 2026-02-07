"""
Tracking Plan Processor for Snowplow connector.

Handles extraction of tracking plans from Snowplow BDP.
"""

import logging
from typing import TYPE_CHECKING, Iterable

from datahub.emitter.mce_builder import make_container_urn, make_user_urn
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
    SnowplowTrackingPlanKey,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import TrackingPlan
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import (
    ContainerClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class TrackingPlanProcessor(EntityProcessor):
    """
    Processor for extracting tracking plan metadata from Snowplow BDP.

    Tracking plans represent logical groupings of event specifications.
    Uses the /data-products/v2 API which provides richer metadata including
    ownership, domain, and access information.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if tracking plan extraction is enabled."""
        return self.config.extract_tracking_plans and self.deps.bdp_client is not None

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract tracking plans from BDP.

        Tracking plans are treated as containers grouping related event specs.

        Yields:
            MetadataWorkUnit: Tracking plan metadata work units
        """
        if not self.deps.bdp_client:
            return

        try:
            tracking_plans = self.deps.bdp_client.get_tracking_plans()
        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="fetch tracking plans",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )
            return

        for plan in tracking_plans:
            self.report.report_tracking_plan_found()

            # Apply filtering
            if not self.config.tracking_plan_pattern.allowed(plan.name):
                self.report.report_tracking_plan_filtered(plan.name)
                continue

            yield from self._process_tracking_plan(plan)

    def _process_tracking_plan(self, plan: TrackingPlan) -> Iterable[MetadataWorkUnit]:
        """
        Process a single tracking plan.

        Args:
            plan: Tracking plan from BDP API

        Yields:
            MetadataWorkUnit: Tracking plan metadata work units
        """
        if not self.config.bdp_connection:
            return

        org_id = self.config.bdp_connection.organization_id

        # Create parent organization key
        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Create tracking plan container key (child of organization)
        plan_key = SnowplowTrackingPlanKey(
            organization_id=org_id,
            plan_id=plan.id,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Build custom properties for additional metadata
        custom_properties = {
            "plan_id": plan.id,
            "status": plan.status or "unknown",
            "created_at": plan.created_at or "",
            "updated_at": plan.updated_at or "",
            "num_event_specs": str(len(plan.event_specs)),
        }
        if plan.access_instructions:
            custom_properties["accessInstructions"] = plan.access_instructions
        if plan.source_applications:
            custom_properties["sourceApplications"] = ", ".join(
                plan.source_applications
            )
        if plan.type:
            custom_properties["type"] = plan.type
        if plan.lock_status:
            custom_properties["lockStatus"] = plan.lock_status

        # Use gen_containers to emit all container aspects properly
        yield from gen_containers(
            container_key=plan_key,
            name=plan.name,
            sub_types=["tracking_plan"],
            parent_container_key=org_key,
            description=plan.description,
            extra_properties=custom_properties,
        )

        plan_container_urn = str(make_container_urn(guid=plan_key.guid()))

        # Emit ownership if owner specified
        if plan.owner:
            yield from self.emit_aspects(
                entity_urn=plan_container_urn,
                aspects=[
                    OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=make_user_urn(plan.owner),
                                type=OwnershipTypeClass.DATAOWNER,
                                source=OwnershipSourceClass(
                                    type=OwnershipSourceTypeClass.SERVICE,
                                    url=None,
                                ),
                            )
                        ]
                    )
                ],
            )

        # Track extracted tracking plan
        self.report.report_tracking_plan_extracted()

        # Add container relationships to event specs referenced in this plan
        if plan.event_specs:
            for event_spec_ref in plan.event_specs:
                event_spec_id = event_spec_ref.id

                # Only link to event specs that were actually emitted (not filtered)
                if event_spec_id not in self.state.emitted_event_spec_ids:
                    logger.debug(
                        f"Skipping container link for filtered event spec {event_spec_id} in plan {plan.name}"
                    )
                    continue

                # Create event spec dataset URN
                event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(
                    event_spec_id
                )

                # Link event spec to tracking plan container
                yield from self.emit_aspects(
                    entity_urn=event_spec_urn,
                    aspects=[ContainerClass(container=plan_container_urn)],
                )
