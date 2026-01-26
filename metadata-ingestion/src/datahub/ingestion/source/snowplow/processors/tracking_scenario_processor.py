"""
Tracking Scenario Processor for Snowplow connector.

Handles extraction of tracking scenarios from Snowplow BDP.
"""

import logging
from typing import TYPE_CHECKING, Iterable

from datahub.emitter.mce_builder import make_container_urn
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
    SnowplowTrackingScenarioKey,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import TrackingScenario
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import ContainerClass

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class TrackingScenarioProcessor(EntityProcessor):
    """
    Processor for extracting tracking scenario metadata from Snowplow BDP.

    Tracking scenarios represent logical groupings of event specifications.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize tracking scenario processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if tracking scenario extraction is enabled."""
        return (
            self.config.extract_tracking_scenarios and self.deps.bdp_client is not None
        )

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract tracking scenarios from BDP.

        Tracking scenarios are treated as containers grouping related event specs.

        Yields:
            MetadataWorkUnit: Tracking scenario metadata work units
        """
        if not self.deps.bdp_client:
            return

        try:
            tracking_scenarios = self.deps.bdp_client.get_tracking_scenarios()
        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="fetch tracking scenarios",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
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

        Yields:
            MetadataWorkUnit: Tracking scenario metadata work units
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

        # Create tracking scenario container key (child of organization)
        scenario_key = SnowplowTrackingScenarioKey(
            organization_id=org_id,
            scenario_id=scenario.id,
            platform=self.deps.platform,
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
                if event_spec_id not in self.state.emitted_event_spec_ids:
                    logger.debug(
                        f"Skipping container link for filtered event spec {event_spec_id} in scenario {scenario.name}"
                    )
                    continue

                # Create event spec dataset URN
                event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(
                    event_spec_id
                )

                # Link event spec to tracking scenario container using SDK V2 pattern
                yield from self.emit_aspects(
                    entity_urn=event_spec_urn,
                    aspects=[ContainerClass(container=scenario_container_urn)],
                )
