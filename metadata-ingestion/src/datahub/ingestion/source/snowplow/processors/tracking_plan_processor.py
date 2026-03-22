"""
Tracking Plan Processor for Snowplow connector.

Handles extraction of tracking plans from Snowplow BDP.
"""

import logging
from typing import TYPE_CHECKING, Iterable, List, Optional

from datahub.emitter.mce_builder import make_container_urn, make_user_urn
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
    SnowplowTrackingPlanKey,
)
from datahub.ingestion.source.snowplow.constants import DatasetSubtype
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
        self._cached_tracking_plans: Optional[List[TrackingPlan]] = None

    def is_enabled(self) -> bool:
        """Check if tracking plan extraction is enabled."""
        return self.config.extract_tracking_plans and self.deps.bdp_client is not None

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract tracking plans from BDP (both containers and event spec links).

        For correct browse paths, call emit_containers() before event specs
        and link_event_specs_to_containers() after. This method is a convenience
        that does both in sequence for backward compatibility.

        Yields:
            MetadataWorkUnit: Tracking plan metadata work units
        """
        yield from self.emit_containers()
        yield from self.link_event_specs_to_containers()

    def emit_containers(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit tracking plan containers and ownership.

        Must be called BEFORE event spec extraction so that auto_browse_path_v2
        can resolve the full container chain when computing browse paths for
        event spec datasets.

        Yields:
            MetadataWorkUnit: Container and ownership work units
        """
        tracking_plans = self._fetch_tracking_plans()
        if tracking_plans is None:
            return

        for plan in tracking_plans:
            self.report.report_tracking_plan_found()

            if not self.config.tracking_plan_pattern.allowed(plan.name):
                self.report.report_tracking_plan_filtered(plan.name)
                continue

            yield from self._emit_plan_container(plan)

    def link_event_specs_to_containers(self) -> Iterable[MetadataWorkUnit]:
        """
        Link event spec datasets to their tracking plan containers.

        Must be called AFTER event spec extraction so that
        state.emitted_event_spec_ids is populated.

        Yields:
            MetadataWorkUnit: ContainerClass work units linking event specs
        """
        # Use cached tracking plans from emit_containers()
        if self._cached_tracking_plans is None:
            return

        for plan in self._cached_tracking_plans:
            if not self.config.tracking_plan_pattern.allowed(plan.name):
                continue
            yield from self._link_plan_event_specs(plan)

    def _fetch_tracking_plans(self) -> Optional[List[TrackingPlan]]:
        """Fetch and cache tracking plans from BDP API."""
        if not self.deps.bdp_client:
            return None

        try:
            plans = self.deps.bdp_client.get_tracking_plans()
            self._cached_tracking_plans = plans
            return plans
        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="fetch tracking plans",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )
            return None

    def _emit_plan_container(self, plan: TrackingPlan) -> Iterable[MetadataWorkUnit]:
        """
        Emit container and ownership aspects for a single tracking plan.

        Args:
            plan: Tracking plan from BDP API

        Yields:
            MetadataWorkUnit: Container and ownership work units
        """
        if not self.config.bdp_connection:
            return

        org_id = self.config.bdp_connection.organization_id

        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        plan_key = SnowplowTrackingPlanKey(
            organization_id=org_id,
            plan_id=plan.id,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

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

        yield from gen_containers(
            container_key=plan_key,
            name=plan.name,
            sub_types=[DatasetSubtype.TRACKING_PLAN],
            parent_container_key=org_key,
            description=plan.description,
            extra_properties=custom_properties,
        )

        plan_container_urn = str(make_container_urn(guid=plan_key.guid()))

        # Pre-populate event_spec → container mapping so the event spec processor
        # can set the correct ContainerClass in its first (and only) batch.
        for event_spec_ref in plan.event_specs:
            self.state.event_spec_container_map[event_spec_ref.id] = plan_container_urn

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

        self.report.report_tracking_plan_extracted()

    def _link_plan_event_specs(self, plan: TrackingPlan) -> Iterable[MetadataWorkUnit]:
        """
        Link event specs in a tracking plan to its container.

        Args:
            plan: Tracking plan from BDP API

        Yields:
            MetadataWorkUnit: ContainerClass work units
        """
        if not self.config.bdp_connection or not plan.event_specs:
            return

        org_id = self.config.bdp_connection.organization_id

        plan_key = SnowplowTrackingPlanKey(
            organization_id=org_id,
            plan_id=plan.id,
            platform=self.deps.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        plan_container_urn = str(make_container_urn(guid=plan_key.guid()))

        for event_spec_ref in plan.event_specs:
            event_spec_id = event_spec_ref.id

            if event_spec_id not in self.state.emitted_event_spec_ids:
                logger.debug(
                    f"Skipping container link for filtered event spec {event_spec_id} in plan {plan.name}"
                )
                continue

            event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec_id)

            yield from self.emit_aspects(
                entity_urn=event_spec_urn,
                aspects=[ContainerClass(container=plan_container_urn)],
            )
