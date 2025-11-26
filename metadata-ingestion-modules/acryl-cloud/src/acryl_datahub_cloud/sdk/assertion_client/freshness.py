from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    FreshnessAssertion,
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
    TimeWindowSizeInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    FreshnessAssertionScheduleCheckType,
    _FreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class FreshnessAssertionClient:
    """Client for managing freshness assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def sync_freshness_assertion(
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
        freshness_schedule_check_type: Optional[
            Union[
                str,
                FreshnessAssertionScheduleCheckType,
                models.FreshnessAssertionScheduleTypeClass,
            ]
        ] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
    ) -> FreshnessAssertion:
        """Upsert and merge a freshness assertion.

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
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {"type": "last_modified_column", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
                - {"type": "high_watermark_column", "column_name": "id", "additional_filter": "id > 1000"} or DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name='id', additional_filter='id > 1000')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            freshness_schedule_check_type (Optional[Union[str, FreshnessAssertionScheduleCheckType, models.FreshnessAssertionScheduleTypeClass]]): The freshness schedule check type to be applied to the assertion. Valid values are: "since_the_last_check", "fixed_interval".
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.
            lookback_window (Optional[TimeWindowSizeInputTypes]): The lookback window to be applied to the assertion. Valid values are:
                - TimeWindowSize(unit=CalendarInterval.MINUTE, multiple=10) for 10 minutes
                - TimeWindowSize(unit=CalendarInterval.HOUR, multiple=2) for 2 hours
                - TimeWindowSize(unit=CalendarInterval.DAY, multiple=1) for 1 day
                - {"unit": "MINUTE", "multiple": 30} for 30 minutes (using dict)
                - {"unit": "HOUR", "multiple": 6} for 6 hours (using dict)
                - {"unit": "DAY", "multiple": 7} for 7 days (using dict)
                Valid values for CalendarInterval are: "MINUTE", "HOUR", "DAY" and for multiple, the integer number of units.

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

    def _retrieve_and_merge_freshness_assertion_and_monitor(
        self,
        assertion_input: _FreshnessAssertionInput,
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
                urn=urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                freshness_schedule_check_type=freshness_schedule_check_type,
                lookback_window=lookback_window,
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
        assertion_input: _FreshnessAssertionInput,
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

    def _merge_freshness_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
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

    def _create_freshness_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        freshness_schedule_check_type: Optional[
            Union[str, models.FreshnessAssertionScheduleTypeClass]
        ] = None,
        lookback_window: Optional[TimeWindowSizeInputTypes] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
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
            urn=urn,
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
