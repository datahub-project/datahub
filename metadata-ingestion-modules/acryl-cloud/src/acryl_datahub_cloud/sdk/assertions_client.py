from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

from acryl_datahub_cloud.sdk.assertion import (
    AssertionMode,
    FreshnessAssertion,
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    _AssertionPublic,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
    TimeWindowSizeInputTypes,
    _AssertionInput,
    _SmartFreshnessAssertionInput,
    _SmartVolumeAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    _FreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.errors import ItemNotFoundError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

# TODO: Replace __datahub_system with the actual datahub system user https://linear.app/acryl-data/issue/OBS-1351/auditstamp-actor-hydration-pattern-for-sdk-calls
DEFAULT_CREATED_BY = CorpUserUrn.from_string("urn:li:corpuser:__datahub_system")


class AssertionsClient:
    def __init__(self, client: "DataHubClient"):
        self.client = client
        _print_experimental_warning()

    def sync_smart_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
    ) -> SmartFreshnessAssertion:
        """Upsert and merge a smart freshness assertion.

        Note: keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set e.g. by passing an empty list or
        empty string.

        Schedule behavior:
        - Create case: Uses default hourly schedule ("0 * * * *")
        - Update case: Preserves existing schedule from backend (not modifiable)

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion. If not provided, a urn will be generated and the
                assertion will be _created_ in the DataHub instance.
            display_name: The display name of the assertion. If not provided, a random display
                name will be generated.
            enabled: Whether the assertion is enabled. If not provided, the existing value
                will be preserved.
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
            sensitivity: The sensitivity to be applied to the assertion. Valid values are:
                - "low" or InferenceSensitivity.LOW
                - "medium" or InferenceSensitivity.MEDIUM
                - "high" or InferenceSensitivity.HIGH
            exclusion_windows: The exclusion windows to be applied to the assertion, currently only
                fixed range exclusion windows are supported. Valid values are:
                - from datetime.datetime objects: {
                    "start": "datetime(2025, 1, 1, 0, 0, 0)",
                    "end": "datetime(2025, 1, 2, 0, 0, 0)",
                }
                - from string datetimes: {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
                - from FixedRangeExclusionWindow objects: FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0)
                )
            training_data_lookback_days: The training data lookback days to be applied to the
                assertion as an integer.
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
            tags: The tags to be applied to the assertion. Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            updated_by: Optional urn of the user who updated the assertion. The format is
                "urn:li:corpuser:<username>", which you can find on the Users & Groups page.
                The default is the datahub system user.
                TODO: Retrieve the SDK user as the default instead of the datahub system user.

        Returns:
            SmartFreshnessAssertion: The created or updated assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. If urn is not set, create a new assertion
        if urn is None:
            logger.info("URN is not set, creating a new assertion")
            return self._create_smart_freshness_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
            )

        # 2. If urn is set, first validate the input:
        assertion_input = _SmartFreshnessAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
        )

        # 3. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_smart_freshness_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
            )
        )

        # Return early if we created a new assertion in the merge:
        if isinstance(merged_assertion_input_or_created_assertion, _AssertionPublic):
            # We know this is the correct type because we passed the assertion_class parameter
            assert isinstance(
                merged_assertion_input_or_created_assertion, SmartFreshnessAssertion
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

        return SmartFreshnessAssertion._from_entities(assertion_entity, monitor_entity)

    def _retrieve_and_merge_smart_freshness_assertion_and_monitor(
        self,
        assertion_input: _SmartFreshnessAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
    ) -> Union[SmartFreshnessAssertion, _SmartFreshnessAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SmartFreshnessAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = SmartFreshnessAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_smart_freshness_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
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

        # 4. Merge the existing assertion with the validated input:
        merged_assertion_input = self._merge_smart_freshness_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            maybe_monitor_entity=maybe_monitor_entity,
            existing_assertion=existing_assertion,
        )

        return merged_assertion_input

    def _retrieve_and_merge_volume_assertion_and_monitor(
        self,
        assertion_input: _SmartVolumeAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> Union[SmartVolumeAssertion, _SmartVolumeAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SmartVolumeAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = SmartVolumeAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_smart_volume_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
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

        # 4. Merge the existing assertion with the validated input:
        merged_assertion_input = self._merge_smart_volume_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            schedule=schedule,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            maybe_monitor_entity=maybe_monitor_entity,
            existing_assertion=existing_assertion,
        )

        return merged_assertion_input

    def _retrieve_and_merge_freshness_assertion_and_monitor(
        self,
        assertion_input: _FreshnessAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
    ) -> Union[FreshnessAssertion, _FreshnessAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = FreshnessAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = FreshnessAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_freshness_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                freshness_schedule_check_type=freshness_schedule_check_type,
                lookback_window=lookback_window,
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

        # 4. Merge the existing assertion with the validated input:
        merged_assertion_input = self._merge_freshness_input(
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
            freshness_schedule_check_type=freshness_schedule_check_type,
            lookback_window=lookback_window,
        )

        return merged_assertion_input

    def _retrieve_assertion_and_monitor(
        self,
        assertion_input: _AssertionInput,
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance.

        Args:
            assertion_input: The validated input to the function.

        Returns:
            The assertion and monitor entities.
        """
        assert assertion_input.urn is not None, "URN is required"

        # Get assertion entity
        maybe_assertion_entity: Optional[Assertion] = None
        try:
            entity = self.client.entities.get(assertion_input.urn)
            if entity is not None:
                assert isinstance(entity, Assertion)
                maybe_assertion_entity = entity
        except ItemNotFoundError:
            pass

        # Get monitor entity
        monitor_urn = Monitor._ensure_id(
            id=(assertion_input.dataset_urn, assertion_input.urn)
        )
        maybe_monitor_entity: Optional[Monitor] = None
        try:
            entity = self.client.entities.get(monitor_urn)
            if entity is not None:
                assert isinstance(entity, Monitor)
                maybe_monitor_entity = entity
        except ItemNotFoundError:
            pass

        return maybe_assertion_entity, monitor_urn, maybe_monitor_entity

    def _merge_smart_freshness_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _SmartFreshnessAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: SmartFreshnessAssertion,
    ) -> _SmartFreshnessAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            detection_mechanism: The detection mechanism to be used for the assertion.
            sensitivity: The sensitivity to be applied to the assertion.
            exclusion_windows: The exclusion windows to be applied to the assertion.
            training_data_lookback_days: The training data lookback days to be applied to the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            maybe_monitor_entity: The existing monitor entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _SmartFreshnessAssertionInput(
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
                None,  # Don't allow schedule modification in updates - always preserve existing
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
                SmartFreshnessAssertion._get_detection_mechanism(  # TODO: Consider moving this conversion to DetectionMechanism.parse(), it could avoid having to use Optional on the return type of SmartFreshnessAssertion.get_detection_mechanism()
                    maybe_assertion_entity, maybe_monitor_entity, default=None
                )
                if maybe_assertion_entity and maybe_monitor_entity
                else None,
            ),
            sensitivity=_merge_field(
                sensitivity,
                "sensitivity",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.sensitivity if maybe_monitor_entity else None,
            ),
            exclusion_windows=_merge_field(
                exclusion_windows,
                "exclusion_windows",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.exclusion_windows
                if maybe_monitor_entity
                else None,
            ),
            training_data_lookback_days=_merge_field(
                training_data_lookback_days,
                "training_data_lookback_days",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.training_data_lookback_days
                if maybe_monitor_entity
                else None,
            ),
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                SmartFreshnessAssertion._get_incident_behavior(maybe_assertion_entity)
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
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
        )

        return merged_assertion_input

    def _merge_freshness_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _FreshnessAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: FreshnessAssertion,
        schedule: Optional[Union[str, models.CronScheduleClass]],
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
    ) -> _FreshnessAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            maybe_monitor_entity: The existing monitor entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.
            schedule: The schedule to be applied to the assertion.
            freshness_schedule_check_type: The freshness schedule check type to be applied to the assertion.
            lookback_window: The lookback window to be applied to the assertion.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _FreshnessAssertionInput(
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
            freshness_schedule_check_type=_merge_field(
                freshness_schedule_check_type,
                "freshness_schedule_check_type",
                assertion_input,
                existing_assertion,
                existing_assertion._freshness_schedule_check_type
                if existing_assertion
                else None,
            ),
            lookback_window=_merge_field(
                lookback_window,
                "lookback_window",
                assertion_input,
                existing_assertion,
                existing_assertion.lookback_window if existing_assertion else None,
            ),
            detection_mechanism=_merge_field(
                detection_mechanism,
                "detection_mechanism",
                assertion_input,
                existing_assertion,
                FreshnessAssertion._get_detection_mechanism(
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
                FreshnessAssertion._get_incident_behavior(maybe_assertion_entity)
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
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
        )
        return merged_assertion_input

    def _merge_smart_volume_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ],
        tags: Optional[TagsInputType],
        schedule: Optional[Union[str, models.CronScheduleClass]],
        now_utc: datetime,
        assertion_input: _SmartVolumeAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: SmartVolumeAssertion,
    ) -> _SmartVolumeAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            detection_mechanism: The detection mechanism to be used for the assertion.
            sensitivity: The sensitivity to be applied to the assertion.
            exclusion_windows: The exclusion windows to be applied to the assertion.
            training_data_lookback_days: The training data lookback days to be applied to the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            maybe_monitor_entity: The existing monitor entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _SmartVolumeAssertionInput(
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
                SmartVolumeAssertion._get_detection_mechanism(
                    maybe_assertion_entity, maybe_monitor_entity, default=None
                )
                if maybe_assertion_entity and maybe_monitor_entity
                else None,
            ),
            sensitivity=_merge_field(
                sensitivity,
                "sensitivity",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.sensitivity if maybe_monitor_entity else None,
            ),
            exclusion_windows=_merge_field(
                exclusion_windows,
                "exclusion_windows",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.exclusion_windows
                if maybe_monitor_entity
                else None,
            ),
            training_data_lookback_days=_merge_field(
                training_data_lookback_days,
                "training_data_lookback_days",
                assertion_input,
                existing_assertion,
                maybe_monitor_entity.training_data_lookback_days
                if maybe_monitor_entity
                else None,
            ),
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                SmartVolumeAssertion._get_incident_behavior(maybe_assertion_entity)
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
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
        )

        return merged_assertion_input

    def _create_smart_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
    ) -> SmartFreshnessAssertion:
        """Create a smart freshness assertion.

        Note: keyword arguments are required.

        The created assertion will use the default hourly schedule ("0 * * * *").

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
            sensitivity: The sensitivity to be applied to the assertion. Valid values are:
                - "low" or InferenceSensitivity.LOW
                - "medium" or InferenceSensitivity.MEDIUM
                - "high" or InferenceSensitivity.HIGH
            exclusion_windows: The exclusion windows to be applied to the assertion, currently only
                fixed range exclusion windows are supported. Valid values are:
                - from datetime.datetime objects: {
                    "start": "datetime(2025, 1, 1, 0, 0, 0)",
                    "end": "datetime(2025, 1, 2, 0, 0, 0)",
                }
                - from string datetimes: {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
                - from FixedRangeExclusionWindow objects: FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0)
                )
            training_data_lookback_days: The training data lookback days to be applied to the
                assertion as an integer.
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
            tags: The tags to be applied to the assertion. Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            created_by: Optional urn of the user who created the assertion. The format is
                "urn:li:corpuser:<username>", which you can find on the Users & Groups page.
                The default is the datahub system user.
                TODO: Retrieve the SDK user as the default instead of the datahub system user.

        Returns:
            SmartFreshnessAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _SmartFreshnessAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=now_utc,
            updated_by=created_by,
            updated_at=now_utc,
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
        return SmartFreshnessAssertion._from_entities(assertion_entity, monitor_entity)

    def _create_smart_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartVolumeAssertion:
        """Create a smart volume assertion.

        Note: keyword arguments are required.

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
                - {
                    "type": "high_watermark_column",
                    "column_name": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id',
                additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            sensitivity: The sensitivity to be applied to the assertion. Valid values are:
                - "low" or InferenceSensitivity.LOW
                - "medium" or InferenceSensitivity.MEDIUM
                - "high" or InferenceSensitivity.HIGH
            exclusion_windows: The exclusion windows to be applied to the assertion, currently only
                fixed range exclusion windows are supported. Valid values are:
                - from datetime.datetime objects: {
                    "start": "datetime(2025, 1, 1, 0, 0, 0)",
                    "end": "datetime(2025, 1, 2, 0, 0, 0)",
                }
                - from string datetimes: {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
                - from FixedRangeExclusionWindow objects: FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0)
                )
            training_data_lookback_days: The training data lookback days to be applied to the
                assertion as an integer.
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
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

        Returns:
            SmartVolumeAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _SmartVolumeAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=now_utc,
            updated_by=created_by,
            updated_at=now_utc,
            schedule=schedule,
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
        return SmartVolumeAssertion._from_entities(assertion_entity, monitor_entity)

    def _create_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        display_name: Optional[str] = None,
        enabled: bool = True,
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> FreshnessAssertion:
        """Create a freshness assertion.

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
            freshness_schedule_check_type: The freshness schedule check type to be applied to the assertion. Valid values are:
                - "since_the_last_check" or models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK
                - "cron" or models.FreshnessAssertionScheduleTypeClass.CRON
            lookback_window: The lookback window to be applied to the assertion. Valid values are:
                - from models.TimeWindowSize objects: models.TimeWindowSizeClass(
                    unit=models.CalendarIntervalClass.DAY,
                    multiple=1)
                - from TimeWindowSize objects: TimeWindowSize(unit='DAY', multiple=1)
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
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

        Returns:
            FreshnessAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _FreshnessAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled,
            detection_mechanism=detection_mechanism,
            freshness_schedule_check_type=freshness_schedule_check_type,
            lookback_window=lookback_window,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=created_by,
            created_at=now_utc,
            updated_by=created_by,
            updated_at=now_utc,
            schedule=schedule,
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
        return FreshnessAssertion._from_entities(assertion_entity, monitor_entity)

    def sync_smart_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartVolumeAssertion:
        """Upsert and merge a smart volume assertion.

        Note: keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set e.g. by passing an empty list or
        empty string.

        Schedule behavior:
        - Create case: Uses default hourly schedule (\"0 * * * *\") or provided schedule
        - Update case: Different than `sync_smart_freshness_assertion`, schedule is updated.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion. If not provided, a urn will be generated and the assertion
                will be _created_ in the DataHub instance.
            display_name: The display name of the assertion. If not provided, a random display name
                will be generated.
            enabled: Whether the assertion is enabled. If not provided, the existing value
                will be preserved.
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
                - {
                    "type": "high_watermark_column",
                    "column_name": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id',
                additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            sensitivity: The sensitivity to be applied to the assertion. Valid values are:
                - "low" or InferenceSensitivity.LOW
                - "medium" or InferenceSensitivity.MEDIUM
                - "high" or InferenceSensitivity.HIGH
            exclusion_windows: The exclusion windows to be applied to the assertion, currently only
                fixed range exclusion windows are supported. Valid values are:
                - from datetime.datetime objects: {
                    "start": "datetime(2025, 1, 1, 0, 0, 0)",
                    "end": "datetime(2025, 1, 2, 0, 0, 0)",
                }
                - from string datetimes: {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
                - from FixedRangeExclusionWindow objects: FixedRangeExclusionWindow(
                    start=datetime(2025, 1, 1, 0, 0, 0),
                    end=datetime(2025, 1, 2, 0, 0, 0)
                )
            training_data_lookback_days: The training data lookback days to be applied to the
                assertion as an integer.
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
            tags: The tags to be applied to the assertion. Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            updated_by: Optional urn of the user who updated the assertion. The format is
                "urn:li:corpuser:<username>", which you can find on the Users & Groups page.
                The default is the datahub system user.
                TODO: Retrieve the SDK user as the default instead of the datahub system user.
            schedule: Optional cron formatted schedule for the assertion. If not provided, a default
                schedule will be used. The schedule determines when the assertion will be evaluated.
                The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone.
                Alternatively, a models.CronScheduleClass object can be provided with string parameters
                cron and timezone. Use `from datahub.metadata import schema_classes as models` to import the class.

        Returns:
            SmartVolumeAssertion: The created or updated assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. If urn is not set, create a new assertion
        if urn is None:
            logger.info("URN is not set, creating a new assertion")
            return self._create_smart_volume_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
            )

        # 2. If urn is set, first validate the input:
        assertion_input = _SmartVolumeAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
        )

        # 3. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_volume_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
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
                merged_assertion_input_or_created_assertion, SmartVolumeAssertion
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

        return SmartVolumeAssertion._from_entities(assertion_entity, monitor_entity)

    def sync_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[
            Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
        ] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
    ) -> FreshnessAssertion:
        """Upsert and merge a freshness assertion.

        Note: keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set e.g. by passing an empty list or
        empty string.

        Schedule behavior:
        - Create case: Uses default daily schedule (\"0 0 * * *\") or provided schedule
        - Update case: Uses existing schedule or provided schedule.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion. If not provided, a urn will be generated and the assertion
                will be _created_ in the DataHub instance.
            display_name: The display name of the assertion. If not provided, a random display name
                will be generated.
            enabled: Whether the assertion is enabled. If not provided, the existing value
                will be preserved.
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
                - {
                    "type": "high_watermark_column",
                    "column_name": "id",
                    "additional_filter": "id > 1000",
                } or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id',
                additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            incident_behavior: The incident behavior to be applied to the assertion. Valid values are:
                - "raise_on_fail" or AssertionIncidentBehavior.RAISE_ON_FAIL
                - "resolve_on_pass" or AssertionIncidentBehavior.RESOLVE_ON_PASS
            tags: The tags to be applied to the assertion. Valid values are:
                - a list of strings (strings will be converted to TagUrn objects)
                - a list of TagUrn objects
                - a list of TagAssociationClass objects
            updated_by: Optional urn of the user who updated the assertion. The format is
                "urn:li:corpuser:<username>", which you can find on the Users & Groups page.
                The default is the datahub system user.
                TODO: Retrieve the SDK user as the default instead of the datahub system user.
            schedule: Optional cron formatted schedule for the assertion. If not provided, a default
                schedule will be used. The schedule determines when the assertion will be evaluated.
                The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone.
                Alternatively, a models.CronScheduleClass object can be provided with string parameters
                cron and timezone. Use `from datahub.metadata import schema_classes as models` to import the class.

        Returns:
            FreshnessAssertion: The created or updated assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. If urn is not set, create a new assertion
        if urn is None:
            logger.info("URN is not set, creating a new assertion")
            return self._create_freshness_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                freshness_schedule_check_type=freshness_schedule_check_type,
                lookback_window=lookback_window,
            )

        # 2. If urn is set, first validate the input:
        assertion_input = _FreshnessAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            freshness_schedule_check_type=freshness_schedule_check_type,
            lookback_window=lookback_window,
        )

        # 3. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_freshness_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
                schedule=schedule,
                freshness_schedule_check_type=freshness_schedule_check_type,
                lookback_window=lookback_window,
            )
        )

        # Return early if we created a new assertion in the merge:
        if isinstance(merged_assertion_input_or_created_assertion, _AssertionPublic):
            # We know this is the correct type because we passed the assertion_class parameter
            assert isinstance(
                merged_assertion_input_or_created_assertion, FreshnessAssertion
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

        return FreshnessAssertion._from_entities(assertion_entity, monitor_entity)


def _merge_field(
    input_field_value: Any,
    input_field_name: str,
    validated_assertion_input: _AssertionInput,
    validated_existing_assertion: _AssertionPublic,
    existing_entity_value: Optional[Any] = None,  # TODO: Can we do better than Any?
) -> Any:
    """Merge the input field value with any existing entity value or default value.

    The merge logic is as follows:
    - If the input is None, use the existing value
    - If the input is not None, use the input value
    - If the input is an empty list or empty string, still use the input value (falsy values can be used to unset fields)
    - If the input is a non-empty list or non-empty string, use the input value
    - If the input is None and the existing value is None, use the default value from _AssertionInput

    Args:
        input_field_value: The value of the field in the input e.g. passed to the function.
        input_field_name: The name of the field in the input.
        validated_assertion_input: The *validated* input to the function.
        validated_existing_assertion: The *validated* existing assertion from the DataHub instance.
        existing_entity_value: The value of the field in the existing entity from the DataHub instance, directly retrieved from the entity.

        Returns:
            The merged value of the field.

    """
    if input_field_value is None:  # Input value default
        if existing_entity_value is not None:  # Existing entity value set
            return existing_entity_value
        elif (
            getattr(validated_existing_assertion, input_field_name) is None
        ):  # Validated existing value not set
            return getattr(validated_assertion_input, input_field_name)
        else:  # Validated existing value set
            return getattr(validated_existing_assertion, input_field_name)
    else:  # Input value set
        return input_field_value


def _print_experimental_warning() -> None:
    print(
        "Warning: The assertions client is experimental and under heavy development. Expect breaking changes."
    )
