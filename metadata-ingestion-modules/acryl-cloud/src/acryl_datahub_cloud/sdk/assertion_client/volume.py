from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    VolumeAssertion,
)
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    DEFAULT_CREATED_BY,
    _merge_field,
    _print_experimental_warning,
    retrieve_assertion_and_monitor_by_urn,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCondition,
    VolumeAssertionCriteria,
    VolumeAssertionDefinitionParameters,
    _VolumeAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class VolumeAssertionClient:
    """Client for managing volume assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def sync_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        criteria_condition: Optional[Union[str, VolumeAssertionCondition]] = None,
        criteria_parameters: Optional[VolumeAssertionDefinitionParameters] = None,
    ) -> VolumeAssertion:
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. Validate criteria parameters if any are provided
        if (criteria_condition is not None or criteria_parameters is not None) and (
            criteria_condition is None or criteria_parameters is None
        ):
            raise SDKUsageError(
                "When providing volume assertion criteria, both criteria_condition and criteria_parameters must be provided"
            )

        # Assert the invariant: if criteria_condition is provided, criteria_parameters is also provided
        assert criteria_condition is None or criteria_parameters is not None, (
            "criteria fields already validated"
        )

        # 2. Retrieve and merge the assertion input with any existing assertion and monitor entities,
        # or build a new assertion input if the assertion does not exist:
        assertion_input = self._retrieve_and_merge_native_volume_assertion_and_monitor(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            criteria_condition=criteria_condition,
            criteria_parameters=criteria_parameters,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        # 3. Upsert the assertion and monitor entities:
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion upsert fails, we won't try to upsert the monitor
        self.client.entities.upsert(assertion_entity)
        # TODO: Wrap monitor upsert in a try-except and delete the assertion if monitor upsert fails (once delete is implemented https://linear.app/acryl-data/issue/OBS-1350/add-delete-method-to-entity-clientpy)
        # try:
        self.client.entities.upsert(monitor_entity)
        # except Exception as e:
        #     logger.error(f"Error upserting monitor: {e}")
        #     self.client.entities.delete(assertion_entity)
        #     raise e
        return VolumeAssertion._from_entities(assertion_entity, monitor_entity)

    def _create_new_assertion_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        criteria_condition: Union[str, VolumeAssertionCondition],
        criteria_parameters: VolumeAssertionDefinitionParameters,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _VolumeAssertionInput:
        """Create a new volume assertion input with given criteria."""
        criteria: dict[str, Any] = {
            "condition": criteria_condition,
            "parameters": criteria_parameters,
        }
        return _VolumeAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled if enabled is not None else True,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,
            created_at=now_utc,
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            criteria=criteria,
        )

    def _create_existing_assertion_from_entities(
        self,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        monitor_urn: MonitorUrn,
        enabled: Optional[bool],
    ) -> Optional[VolumeAssertion]:
        """Create existing assertion object from entities, handling missing monitor case."""
        if maybe_assertion_entity and maybe_monitor_entity:
            return VolumeAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )

        if maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            return VolumeAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )

        return None

    def _retrieve_and_merge_native_volume_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        criteria_condition: Optional[Union[str, VolumeAssertionCondition]],
        criteria_parameters: Optional[VolumeAssertionDefinitionParameters],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _VolumeAssertionInput:
        use_backend_criteria = criteria_condition is None

        # 1. If urn is not provided, build and return a new assertion input directly
        if urn is None:
            if use_backend_criteria:
                raise SDKUsageError(
                    "Volume assertion criteria are required when creating a new assertion"
                )
            logger.info("URN is not set, building a new assertion input")
            assert criteria_condition is not None and criteria_parameters is not None
            return self._create_new_assertion_input(
                dataset_urn,
                None,
                display_name,
                enabled,
                detection_mechanism,
                criteria_condition,
                criteria_parameters,
                incident_behavior,
                tags,
                updated_by,
                now_utc,
                schedule,
            )

        # 2. Prepare temporary criteria for validation
        if criteria_condition is not None:
            temp_criteria: dict[str, Any] = {
                "condition": criteria_condition,
                "parameters": criteria_parameters,
            }
        else:
            temp_criteria = {
                "condition": VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                "parameters": 0,
            }

        # 3. Build initial assertion input for validation
        assertion_input = _VolumeAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,
            created_at=now_utc,
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            criteria=temp_criteria,
        )

        # 4. Retrieve any existing assertion and monitor entities
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 5. If the assertion does not exist, build and return a new assertion input
        if not maybe_assertion_entity:
            if use_backend_criteria:
                raise SDKUsageError(
                    f"Cannot sync assertion {urn}: no existing definition found in backend and no definition provided in request"
                )
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, building a new assertion input"
            )
            assert criteria_condition is not None and criteria_parameters is not None
            return self._create_new_assertion_input(
                dataset_urn,
                urn,
                display_name,
                enabled,
                detection_mechanism,
                criteria_condition,
                criteria_parameters,
                incident_behavior,
                tags,
                updated_by,
                now_utc,
                schedule,
            )

        # 6. Create existing assertion from entities
        existing_assertion = self._create_existing_assertion_from_entities(
            maybe_assertion_entity, maybe_monitor_entity, monitor_urn, enabled
        )
        assert existing_assertion is not None, (
            "existing_assertion is guaranteed non-None after early return check"
        )

        # 7. Check for any issues e.g. different dataset urns
        if (
            existing_assertion
            and hasattr(existing_assertion, "dataset_urn")
            and existing_assertion.dataset_urn != assertion_input.dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        # 8. Determine effective criteria
        if use_backend_criteria:
            backend_criteria = VolumeAssertionCriteria.from_assertion(
                maybe_assertion_entity
            )
            assertion_input.criteria = backend_criteria
            effective_criteria = backend_criteria
            logger.info("Using criteria from backend assertion")
        else:
            effective_criteria = assertion_input.criteria

        # 9. Merge the existing assertion with the validated input
        merged_assertion_input = self._merge_volume_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            maybe_monitor_entity=maybe_monitor_entity,
            existing_assertion=existing_assertion,
            schedule=schedule,
            criteria=effective_criteria,
        )

        return merged_assertion_input

    def _retrieve_assertion_and_monitor(
        self,
        assertion_input: _VolumeAssertionInput,
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance.

        Args:
            assertion_input: The validated input to the function.

        Returns:
            The assertion and monitor entities.
        """
        assert assertion_input.urn is not None, "URN is required"
        return retrieve_assertion_and_monitor_by_urn(
            self.client, assertion_input.urn, assertion_input.dataset_urn
        )

    def _merge_volume_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _VolumeAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: VolumeAssertion,
        schedule: Optional[Union[str, models.CronScheduleClass]],
        criteria: Optional[VolumeAssertionCriteria],
    ) -> _VolumeAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            detection_mechanism: The detection mechanism to be used for the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            maybe_monitor_entity: The existing monitor entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.
            schedule: The schedule to be applied to the assertion.
            definition: The volume assertion definition to be applied to the assertion.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _VolumeAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=_merge_field(
                display_name,
                "display_name",
                assertion_input,
                existing_assertion,
                maybe_assertion_entity.description if maybe_assertion_entity else None,
            ),
            enabled=_merge_field(
                enabled,
                "enabled",
                assertion_input,
                existing_assertion,
                existing_assertion.mode == AssertionMode.ACTIVE
                if existing_assertion
                else None,
            ),
            schedule=_merge_field(
                schedule,
                "schedule",
                assertion_input,
                existing_assertion,
                existing_assertion.schedule if existing_assertion else None,
            ),
            detection_mechanism=_merge_field(
                detection_mechanism,
                "detection_mechanism",
                assertion_input,
                existing_assertion,
                VolumeAssertion._get_detection_mechanism(
                    maybe_assertion_entity, maybe_monitor_entity, default=None
                )
                if maybe_assertion_entity and maybe_monitor_entity
                else None,
            ),
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                VolumeAssertion._get_incident_behavior(maybe_assertion_entity)
                if maybe_assertion_entity
                else None,
            ),
            tags=_merge_field(
                tags,
                "tags",
                assertion_input,
                existing_assertion,
                maybe_assertion_entity.tags if maybe_assertion_entity else None,
            ),
            criteria=_merge_field(
                criteria,
                "criteria",
                assertion_input,
                existing_assertion,
                existing_assertion.criteria if existing_assertion else None,
            ),
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
        )
        return merged_assertion_input
