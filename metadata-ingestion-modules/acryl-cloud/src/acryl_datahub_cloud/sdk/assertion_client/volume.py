from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    VolumeAssertion,
    _AssertionPublic,
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
    VolumeAssertionCriteriaInputTypes,
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
        """Upsert and merge a volume assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default daily schedule ("0 0 * * *") or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are (additional_filter is optional):
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - {"type": "query", "additional_filter": "value > 1000"} or DetectionMechanism.QUERY(additional_filter='value > 1000')
                - "dataset_profile" or DetectionMechanism.DATASET_PROFILE
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.
            criteria_condition (Optional[Union[str, VolumeAssertionCondition]]): Optional condition for the volume assertion. Valid values are:
                - "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO" -> The row count is less than or equal to the threshold.
                - "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO" -> The row count is greater than or equal to the threshold.
                - "ROW_COUNT_IS_WITHIN_A_RANGE" -> The row count is within the specified range.
                - "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE" -> The row count growth is at most the threshold (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE" -> The row count growth is at least the threshold (absolute change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The row count growth is within the specified range (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE" -> The row count growth is at most the threshold (percentage change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE" -> The row count growth is at least the threshold (percentage change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The row count growth is within the specified range (percentage change).
                If not provided, the existing definition from the backend will be preserved (for update operations). Required when creating a new assertion (when urn is None).
            criteria_parameters (Optional[VolumeAssertionDefinitionParameters]): Optional threshold parameters to be used for the assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (ROW_COUNT_IS_WITHIN_A_RANGE, ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE, ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.
                If not provided, existing value is preserved for updates. Required when creating a new assertion.

        Returns:
            VolumeAssertion: The created or updated assertion.
        """
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

        # 2. If urn is not set, create a new assertion
        if urn is None:
            if criteria_condition is None:
                raise SDKUsageError(
                    "Volume assertion criteria are required when creating a new assertion"
                )
            logger.info("URN is not set, creating a new assertion")
            # Type narrowing: we know these are not None because of validation above
            assert criteria_parameters is not None
            return self._create_volume_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                criteria_condition=criteria_condition,
                criteria_parameters=criteria_parameters,
            )

        # 2. If urn is set, prepare definition for validation
        # If criteria parameters are provided, create definition from them
        # Otherwise, we use temporary default definition if None is provided, just to pass the _VolumeAssertionInput validation.
        # However, we keep memory of this in use_backend_definition flag, so we can later
        # fail if there is no definition in backend (basically, there is no assertion). That would mean that
        # this is a creation case and the user missed the definition parameter, which is required.
        # Likely this pattern never happened before because there is no a publicly documented default definition
        # that we can use as fallback.
        if criteria_condition is not None:
            # Create criteria from criteria_condition and parameters
            temp_criteria: dict[str, Any] = {
                "condition": criteria_condition,
                "parameters": criteria_parameters,
            }

            use_backend_criteria = False
        else:
            # No criteria provided, use backend criteria
            use_backend_criteria = True
            temp_criteria = {
                "condition": VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                "parameters": 0,  # Temporary placeholder
            }

        # 3. Create assertion input with effective definition
        assertion_input = _VolumeAssertionInput(
            urn=urn,
            dataset_urn=dataset_urn,
            entity_client=self.client.entities,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            criteria=temp_criteria,
        )

        # 4. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_native_volume_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
                criteria=temp_criteria,
                use_backend_criteria=use_backend_criteria,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
                schedule=schedule,
            )
        )

        # Return early if we created a new assertion in the merge:
        if isinstance(merged_assertion_input_or_created_assertion, _AssertionPublic):
            # We know this is the correct type because we passed the assertion_class parameter
            assert isinstance(
                merged_assertion_input_or_created_assertion, VolumeAssertion
            )
            return merged_assertion_input_or_created_assertion

        # 4. Upsert the assertion and monitor entities:
        assertion_entity, monitor_entity = (
            merged_assertion_input_or_created_assertion.to_assertion_and_monitor_entities()
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

    def _retrieve_and_merge_native_volume_assertion_and_monitor(
        self,
        assertion_input: _VolumeAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
        criteria: VolumeAssertionCriteriaInputTypes,
        use_backend_criteria: bool = False,
    ) -> Union[VolumeAssertion, _VolumeAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = VolumeAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = VolumeAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            if use_backend_criteria:
                raise SDKUsageError(
                    f"Cannot sync assertion {urn}: no existing definition found in backend and no definition provided in request"
                )
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            # Extract criteria from definition to call the new signature
            parsed_criteria = VolumeAssertionCriteria.parse(criteria)
            return self._create_volume_assertion(
                urn=urn,
                dataset_urn=dataset_urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                criteria_condition=parsed_criteria.condition,
                criteria_parameters=parsed_criteria.parameters,
                enabled=enabled if enabled is not None else True,
            )

        # 3. Check for any issues e.g. different dataset urns
        if (
            existing_assertion
            and hasattr(existing_assertion, "dataset_urn")
            and existing_assertion.dataset_urn != assertion_input.dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        # 4. Handle criteria: use backend criteria if flag is set and backend has one
        if use_backend_criteria:
            if maybe_assertion_entity is not None:
                # Use criteria from backend
                backend_criteria = VolumeAssertionCriteria.from_assertion(
                    maybe_assertion_entity
                )
                # Update the assertion_input with the real criteria from backend
                assertion_input.criteria = backend_criteria
                effective_criteria = backend_criteria
                logger.info("Using criteria from backend assertion")
            else:
                # No backend assertion and no user-provided criteria - this is an error
                raise SDKUsageError(
                    f"Cannot sync assertion {urn}: no existing criteria found in backend and no criteria provided in request"
                )
        else:
            # Use the already-parsed criteria from assertion_input
            effective_criteria = assertion_input.criteria

        # 5. Merge the existing assertion with the validated input:
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

    def _create_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        criteria_condition: Union[str, VolumeAssertionCondition],
        criteria_parameters: VolumeAssertionDefinitionParameters,
    ) -> VolumeAssertion:
        """Create a volume assertion.

        Note: keyword arguments are required.

        The created assertion will use the default daily schedule ("0 0 * * *").

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            display_name: The display name of the assertion. If not provided, a random display
                name will be generated.
            enabled: Whether the assertion is enabled. Defaults to True.
            detection_mechanism: The detection mechanism to be used for the assertion. Information
                schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {
                    "type": "last_modified_column",
                    "column_name": "last_modified",
                    "additional_filter": "last_modified > '2021-01-01'",
                } or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified',
                additional_filter='last_modified > 2021-01-01')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
                - A list of the above values (strings or enum values)
                - None (default behavior)
            tags: The tags to be applied to the assertion. Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            created_by: Optional urn of the user who created the assertion. The format is
                "urn:li:corpuser:<username>", which you can find on the Users & Groups page.
                The default is the datahub system user.
                TODO: Retrieve the SDK user as the default instead of the datahub system user.
            schedule: Optional cron formatted schedule for the assertion. If not provided, a default
                schedule will be used. The schedule determines when the assertion will be evaluated.
                The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone.
                Alternatively, a models.CronScheduleClass object can be provided with string parameters
                cron and timezone. Use `from datahub.metadata import schema_classes as models` to import the class.
            criteria_condition: The condition for the volume assertion. Valid values are:
                - "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO" -> The row count is less than or equal to the threshold.
                - "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO" -> The row count is greater than or equal to the threshold.
                - "ROW_COUNT_IS_WITHIN_A_RANGE" -> The row count is within the specified range.
                - "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE" -> The row count growth is at most the threshold (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE" -> The row count growth is at least the threshold (absolute change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The row count growth is within the specified range (absolute change).
                - "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE" -> The row count growth is at most the threshold (percentage change).
                - "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE" -> The row count growth is at least the threshold (percentage change).
                - "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The row count growth is within the specified range (percentage change).
            criteria_parameters: The threshold parameters to be used for the assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (ROW_COUNT_IS_WITHIN_A_RANGE, ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE, ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.

        Returns:
            VolumeAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY

        # Create criteria from criteria_condition and parameters
        # The dictionary object will be fully validated down in the _VolumeAssertionInput class
        criteria: dict[str, Any] = {
            "condition": criteria_condition,
            "parameters": criteria_parameters,
        }

        assertion_input = _VolumeAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=now_utc,
            updated_by=created_by,
            updated_at=now_utc,
            schedule=schedule,
            criteria=criteria,
        )
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion creation fails, we won't try to create the monitor
        self.client.entities.create(assertion_entity)
        # TODO: Wrap monitor creation in a try-except and delete the assertion if monitor creation fails (once delete is implemented https://linear.app/acryl-data/issue/OBS-1350/add-delete-method-to-entity-clientpy)
        # try:
        self.client.entities.create(monitor_entity)
        # except Exception as e:
        #     logger.error(f"Error creating monitor: {e}")
        #     self.client.entities.delete(assertion_entity)
        #     raise e
        return VolumeAssertion._from_entities(assertion_entity, monitor_entity)
