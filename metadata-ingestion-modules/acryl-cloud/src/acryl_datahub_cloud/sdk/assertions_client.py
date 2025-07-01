from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, TypedDict, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    FreshnessAssertion,
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    SqlAssertion,
    VolumeAssertion,
    _AssertionPublic,
    _HasColumnMetricFunctionality,
)
from acryl_datahub_cloud.sdk.assertion.column_metric_assertion import (
    ColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion.smart_column_metric_assertion import (
    SmartColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    DetectionMechanismInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
    TimeWindowSizeInputTypes,
    _AssertionInput,
    _SmartFreshnessAssertionInput,
    _SmartVolumeAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    ColumnMetricAssertionParameters,
    _ColumnMetricAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    FreshnessAssertionScheduleCheckType,
    _FreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_column_metric_assertion_input import (
    _SmartColumnMetricAssertionInput,
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
    _SqlAssertionInput,
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
from datahub.errors import ItemNotFoundError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

# TODO: Replace __datahub_system with the actual datahub system user https://linear.app/acryl-data/issue/OBS-1351/auditstamp-actor-hydration-pattern-for-sdk-calls
DEFAULT_CREATED_BY = CorpUserUrn.from_string("urn:li:corpuser:__datahub_system")


class _AssertionLookupInfo(TypedDict):
    """Minimal info needed to look up an assertion and monitor."""

    dataset_urn: Union[str, DatasetUrn]
    urn: Union[str, AssertionUrn]


class AssertionsClient:
    def __init__(self, client: "DataHubClient"):
        self.client = client
        _print_experimental_warning()

    def _validate_required_field(
        self, field_value: Optional[Any], field_name: str, context: str
    ) -> None:
        """Validate that a required field is not None and raise SDKUsageError if it is."""
        if field_value is None:
            raise SDKUsageError(f"{field_name} is required {context}")

    def _validate_required_smart_column_fields_for_creation(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
    ) -> None:
        """Validate required fields for smart column metric assertion creation."""
        self._validate_required_field(
            column_name, "column_name", "when creating a new assertion (urn is None)"
        )
        self._validate_required_field(
            metric_type, "metric_type", "when creating a new assertion (urn is None)"
        )

    def _validate_required_smart_column_fields_for_update(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        assertion_urn: Union[str, AssertionUrn],
    ) -> None:
        """Validate required fields after attempting to fetch from existing assertion."""
        context = f"and not found in existing assertion {assertion_urn}. The existing assertion may be invalid or corrupted."
        self._validate_required_field(column_name, "column_name", context)
        self._validate_required_field(metric_type, "metric_type", context)

    def _validate_criteria_parameters_for_creation(
        self,
        urn: Optional[Union[str, AssertionUrn]],
    ) -> None:
        """Validate criteria_parameters for creation scenario."""
        # Smart assertions always use BETWEEN operator with (0, 0) criteria_parameters
        # No validation needed since these values are fixed
        pass

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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
    ) -> SmartFreshnessAssertion:
        """Upsert and merge a smart freshness assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default hourly schedule ("0 * * * *")
            - Update case: Preserves existing schedule from backend (not modifiable)

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - "audit_log" or DetectionMechanism.AUDIT_LOG
                - {"type": "last_modified_column", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.LAST_MODIFIED_COLUMN(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
                - "datahub_operation" or DetectionMechanism.DATAHUB_OPERATION
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported. Valid values are:
                - {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"} (using ISO strings)
                - {"start": datetime(2025, 1, 1, 0, 0, 0), "end": datetime(2025, 1, 2, 0, 0, 0)} (using datetime objects)
                - FixedRangeExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0)) (using typed object)
                - A list of any of the above formats
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass" or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.

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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
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
                dataset_urn=dataset_urn,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
                criteria_condition=parsed_criteria.condition,
                criteria_parameters=parsed_criteria.parameters,
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

    def _retrieve_and_merge_sql_assertion_and_monitor(
        self,
        assertion_input: _SqlAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        criteria: SqlAssertionCriteria,
        statement: str,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> Union[SqlAssertion, _SqlAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SqlAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = SqlAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_sql_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                criteria_condition=criteria.condition,
                criteria_parameters=criteria.parameters,
                statement=statement,
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
        merged_assertion_input = self._merge_sql_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            incident_behavior=incident_behavior,
            tags=tags,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            existing_assertion=existing_assertion,
            schedule=schedule,
            criteria=criteria,
            statement=statement,
        )

        return merged_assertion_input

    def _retrieve_assertion_and_monitor(
        self,
        assertion_input: Union[_AssertionInput, _AssertionLookupInfo],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance.

        Args:
            assertion_input: The validated input to the function or minimal lookup info.

        Returns:
            The assertion and monitor entities.
        """
        # Extract URN and dataset URN from input
        _urn: Union[str, AssertionUrn]
        _dataset_urn: Union[str, DatasetUrn]
        if isinstance(assertion_input, dict):
            _urn = assertion_input["urn"]
            _dataset_urn = assertion_input["dataset_urn"]
        else:
            assert assertion_input.urn is not None, "URN is required"
            _urn = assertion_input.urn
            _dataset_urn = assertion_input.dataset_urn

        urn: AssertionUrn = (
            _urn if isinstance(_urn, AssertionUrn) else AssertionUrn.from_string(_urn)
        )
        dataset_urn: DatasetUrn = (
            _dataset_urn
            if isinstance(_dataset_urn, DatasetUrn)
            else DatasetUrn.from_string(_dataset_urn)
        )

        # Get assertion entity
        maybe_assertion_entity: Optional[Assertion] = None
        try:
            entity = self.client.entities.get(urn)
            if entity is not None:
                assert isinstance(entity, Assertion)
                maybe_assertion_entity = entity
        except ItemNotFoundError:
            pass

        # Get monitor entity
        monitor_urn = Monitor._ensure_id(id=(dataset_urn, urn))
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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
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

    def _merge_sql_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        criteria: SqlAssertionCriteria,
        statement: str,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _SqlAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        # not used: maybe_monitor_entity: Optional[Monitor], as schedule is already set in existing_assertion
        existing_assertion: SqlAssertion,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SqlAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            criteria: The criteria of the assertion.
            statement: The statement of the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            now_utc: The current UTC time from when the function is called.
            assertion_input: The validated input to the function.
            maybe_assertion_entity: The existing assertion entity from the DataHub instance.
            existing_assertion: The existing assertion from the DataHub instance.
            schedule: The schedule to be applied to the assertion.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _SqlAssertionInput(
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
                # TODO should this use maybe_monitor_entity.schedule?
                existing_assertion.schedule if existing_assertion else None,
            ),
            criteria=_merge_field(
                criteria,
                "criteria",
                assertion_input,
                existing_assertion,
                existing_assertion._criteria if existing_assertion else None,
            ),
            statement=_merge_field(
                statement,
                "statement",
                assertion_input,
                existing_assertion,
                existing_assertion.statement if existing_assertion else None,
            ),
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                SqlAssertion._get_incident_behavior(maybe_assertion_entity)
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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
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

    def _create_volume_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
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
            urn=None,
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

    def _create_sql_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        display_name: Optional[str] = None,
        enabled: bool = True,
        criteria_condition: Union[SqlAssertionCondition, str],
        criteria_parameters: Union[
            Union[float, int], tuple[Union[float, int], Union[float, int]]
        ],
        statement: str,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SqlAssertion:
        """Create a sql assertion.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            display_name: The display name of the assertion. If not provided, a random display
                name will be generated.
            enabled: Whether the assertion is enabled. Defaults to True.
            criteria_condition: The condition for the sql assertion. Valid values are:
                - "IS_EQUAL_TO" -> The metric value equals the threshold.
                - "IS_NOT_EQUAL_TO" -> The metric value does not equal the threshold.
                - "IS_GREATER_THAN" -> The metric value is greater than the threshold.
                - "IS_LESS_THAN" -> The metric value is less than the threshold.
                - "IS_WITHIN_A_RANGE" -> The metric value is within the specified range.
                - "GROWS_AT_MOST_ABSOLUTE" -> The metric growth is at most the threshold (absolute change).
                - "GROWS_AT_MOST_PERCENTAGE" -> The metric growth is at most the threshold (percentage change).
                - "GROWS_AT_LEAST_ABSOLUTE" -> The metric growth is at least the threshold (absolute change).
                - "GROWS_AT_LEAST_PERCENTAGE" -> The metric growth is at least the threshold (percentage change).
                - "GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The metric growth is within the specified range (absolute change).
                - "GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The metric growth is within the specified range (percentage change).
            criteria_parameters: The threshold parameters to be used for the assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (IS_WITHIN_A_RANGE, GROWS_WITHIN_A_RANGE_ABSOLUTE, GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.
            statement: The statement to be used for the assertion.
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
            schedule: Optional cron formatted schedule for the assertion. If not provided, a default
                schedule will be used. The schedule determines when the assertion will be evaluated.
                The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone.
                Alternatively, a models.CronScheduleClass object can be provided with string parameters
                cron and timezone. Use `from datahub.metadata import schema_classes as models` to import the class.

        Returns:
            SqlAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        criteria = SqlAssertionCriteria(
            condition=criteria_condition,
            parameters=criteria_parameters,
        )
        assertion_input = _SqlAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled,
            criteria=criteria,
            statement=statement,
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
        return SqlAssertion._from_entities(assertion_entity, monitor_entity)

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
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartVolumeAssertion:
        """Upsert and merge a smart volume assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated. Existing assertion fields will
        be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default hourly schedule ("0 * * * *") or provided schedule
            - Update case: Schedule is updated if provided, otherwise existing schedule is preserved.

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Information schema is recommended. Valid values are:
                - "information_schema" or DetectionMechanism.INFORMATION_SCHEMA
                - {"type": "query", "additional_filter": "value > 1000"} or DetectionMechanism.QUERY(additional_filter='value > 1000')
                - "dataset_profile" or DetectionMechanism.DATASET_PROFILE
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported. Valid values are:
                - {"start": "2025-01-01T00:00:00", "end": "2025-01-02T00:00:00"} (using ISO strings)
                - {"start": datetime(2025, 1, 1, 0, 0, 0), "end": datetime(2025, 1, 2, 0, 0, 0)} (using datetime objects)
                - FixedRangeExclusionWindow(start=datetime(2025, 1, 1, 0, 0, 0), end=datetime(2025, 1, 2, 0, 0, 0)) (using typed object)
                - A list of any of the above formats
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

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

    def sync_column_metric_assertion(  # noqa: C901  # TODO: Refactor
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: Optional[str] = None,
        metric_type: Optional[MetricInputType] = None,
        operator: Optional[OperatorInputType] = None,
        criteria_parameters: Optional[ColumnMetricAssertionParameters] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> ColumnMetricAssertion:
        """Upsert and merge a column metric assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated.

        Existing assertion fields will be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default schedule of every 6 hours or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Examples:
            # Using enum values (recommended for type safety)
            from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import MetricType, OperatorType
            client.sync_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="user_id",
                metric_type=MetricType.NULL_COUNT,
                operator=OperatorType.GREATER_THAN,
                criteria_parameters=10
            )

            # Using case-insensitive strings (more flexible)
            client.sync_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="price",
                metric_type="mean",
                operator="between",
                criteria_parameters=(100.0, 500.0)
            )

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            column_name (Optional[str]): The name of the column to be monitored. Required for creation, optional for updates.
            metric_type (Optional[MetricInputType]): The type of the metric to be monitored. Required for creation, optional for updates. Valid values are:
                - Using MetricType enum: MetricType.NULL_COUNT, MetricType.NULL_PERCENTAGE, MetricType.UNIQUE_COUNT,
                  MetricType.UNIQUE_PERCENTAGE, MetricType.MAX_LENGTH, MetricType.MIN_LENGTH, MetricType.EMPTY_COUNT,
                  MetricType.EMPTY_PERCENTAGE, MetricType.MIN, MetricType.MAX, MetricType.MEAN, MetricType.MEDIAN,
                  MetricType.STDDEV, MetricType.NEGATIVE_COUNT, MetricType.NEGATIVE_PERCENTAGE, MetricType.ZERO_COUNT,
                  MetricType.ZERO_PERCENTAGE
                - Using case-insensitive strings: "null_count", "MEAN", "Max_Length", etc.
                - Using models enum: models.FieldMetricTypeClass.NULL_COUNT, etc. (import with: from datahub.metadata import schema_classes as models)
            operator (Optional[OperatorInputType]): The operator to be used for the assertion. Required for creation, optional for updates. Valid values are:
                - Using OperatorType enum: OperatorType.EQUAL_TO, OperatorType.NOT_EQUAL_TO, OperatorType.GREATER_THAN,
                  OperatorType.GREATER_THAN_OR_EQUAL_TO, OperatorType.LESS_THAN, OperatorType.LESS_THAN_OR_EQUAL_TO,
                  OperatorType.BETWEEN, OperatorType.IN, OperatorType.NOT_IN, OperatorType.NULL, OperatorType.NOT_NULL,
                  OperatorType.IS_TRUE, OperatorType.IS_FALSE, OperatorType.CONTAIN, OperatorType.END_WITH,
                  OperatorType.START_WITH, OperatorType.REGEX_MATCH
                - Using case-insensitive strings: "equal_to", "not_equal_to", "greater_than", "greater_than_or_equal_to",
                  "less_than", "less_than_or_equal_to", "between", "in", "not_in", "null", "not_null", "is_true",
                  "is_false", "contain", "end_with", "start_with", "regex_match"
                - Using models enum: models.AssertionStdOperatorClass.EQUAL_TO, models.AssertionStdOperatorClass.GREATER_THAN, etc.
            criteria_parameters (Optional[ColumnMetricAssertionParameters]): The criteria parameters for the assertion. Required for creation (except for operators that don't need parameters), optional for updates.
                - Single value operators (EQUAL_TO, NOT_EQUAL_TO, GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO, CONTAIN, END_WITH, START_WITH, REGEX_MATCH): pass a single number or string
                - Range operators (BETWEEN): pass a tuple of two numbers (min_value, max_value)
                - List operators (IN, NOT_IN): pass a list of values
                - No parameter operators (NULL, NOT_NULL, IS_TRUE, IS_FALSE): pass None or omit this parameter
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Valid values are (additional_filter is optional):
                - "all_rows_query_datahub_dataset_profile" or DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE
                - "all_rows_query" or DetectionMechanism.ALL_ROWS_QUERY(), or with additional_filter: {"type": "all_rows_query", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.ALL_ROWS_QUERY(additional_filter='last_modified > 2021-01-01')
                - {"type": "changed_rows_query", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.CHANGED_ROWS_QUERY(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule of every 6 hours will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            ColumnMetricAssertion: The created or updated assertion.
        """
        now_utc = datetime.now(timezone.utc)
        gms_criteria_type_info = None

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. If urn is not set, create a new assertion
        if urn is None:
            self._validate_required_column_fields_for_creation(
                column_name, metric_type, operator
            )
            assert (
                column_name is not None
                and metric_type is not None
                and operator is not None
            ), "Fields guaranteed non-None after validation"
            logger.info("URN is not set, creating a new assertion")
            return self._create_column_metric_assertion(
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                detection_mechanism=detection_mechanism,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
            )

        # 2.1 If urn is set, fetch missing required parameters from backend if needed:
        # NOTE: This is a tactical solution. The problem is we fetch twice (once for validation,
        # once for merge). Strategic solution would be to merge first, then validate after,
        # but that requires heavy refactor and is skipped for now.
        if urn is not None and (
            column_name is None
            or metric_type is None
            or operator is None
            or criteria_parameters is None
        ):
            # Fetch existing assertion to get missing required parameters
            maybe_assertion_entity, _, maybe_monitor_entity = (
                self._retrieve_assertion_and_monitor(
                    {"dataset_urn": dataset_urn, "urn": urn}
                )
            )

            if maybe_assertion_entity is not None:
                assertion_info = maybe_assertion_entity.info
                if (
                    hasattr(assertion_info, "fieldMetricAssertion")
                    and assertion_info.fieldMetricAssertion
                ):
                    field_metric_assertion = assertion_info.fieldMetricAssertion
                    # Use existing values for missing required parameters
                    if (
                        column_name is None
                        and hasattr(field_metric_assertion, "field")
                        and hasattr(field_metric_assertion.field, "path")
                    ):
                        column_name = field_metric_assertion.field.path
                    if metric_type is None and hasattr(
                        field_metric_assertion, "metric"
                    ):
                        metric_type = field_metric_assertion.metric
                    if operator is None and hasattr(field_metric_assertion, "operator"):
                        operator = field_metric_assertion.operator
                    if criteria_parameters is None and hasattr(
                        field_metric_assertion, "parameters"
                    ):
                        # Extract criteria_parameters from existing assertion
                        # This logic should match the parameter extraction in the assertion input class
                        params = field_metric_assertion.parameters
                        if params and hasattr(params, "value") and params.value:
                            criteria_parameters = params.value.value
                        elif (
                            params
                            and hasattr(params, "minValue")
                            and hasattr(params, "maxValue")
                            and params.minValue
                            and params.maxValue
                        ):
                            criteria_parameters = (
                                params.minValue.value,
                                params.maxValue.value,
                            )

                # Extract gms_criteria_type_info to preserve original parameter types
                gms_criteria_type_info = (
                    _HasColumnMetricFunctionality._get_criteria_parameters_with_type(
                        maybe_assertion_entity
                    )
                )

            self._validate_required_column_fields_for_update(
                column_name, metric_type, operator, urn
            )
            assert (
                column_name is not None
                and metric_type is not None
                and operator is not None
            ), "Fields guaranteed non-None after validation"

        # 2.2 Now validate the input with all required parameters:
        assertion_input = _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=display_name,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        # 3. Merge the assertion input with the existing assertion and monitor entities or create a new assertion
        # if the assertion does not exist:
        merged_assertion_input_or_created_assertion = (
            self._retrieve_and_merge_column_metric_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                detection_mechanism=detection_mechanism,
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
                merged_assertion_input_or_created_assertion, ColumnMetricAssertion
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

        return ColumnMetricAssertion._from_entities(assertion_entity, monitor_entity)

    def sync_smart_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: Optional[str] = None,
        metric_type: Optional[MetricInputType] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartColumnMetricAssertion:
        """Upsert and merge a smart column metric assertion.

        Note:
            Keyword arguments are required.

        Upsert and merge is a combination of create and update. If the assertion does not exist,
        it will be created. If it does exist, it will be updated.

        Existing assertion fields will be updated if the input value is not None. If the input value is None, the existing value
        will be preserved. If the input value can be un-set (e.g. by passing an empty list or
        empty string), it will be unset.

        Schedule behavior:
            - Create case: Uses default schedule of every 6 hours or provided schedule
            - Update case: Uses existing schedule or provided schedule.

        Examples:
            # Using enum values (recommended for type safety)
            client.sync_smart_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="user_id",
                metric_type=MetricType.NULL_COUNT
            )

            # Using case-insensitive strings (more flexible)
            client.sync_smart_column_metric_assertion(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)",
                column_name="price",
                metric_type="mean"
            )

        Args:
            dataset_urn (Union[str, DatasetUrn]): The urn of the dataset to be monitored.
            column_name (Optional[str]): The name of the column to be monitored. Required for creation, optional for updates.
            metric_type (Optional[MetricInputType]): The type of the metric to be monitored. Required for creation, optional for updates. Valid values are:
                - Using MetricType enum: MetricType.NULL_COUNT, MetricType.NULL_PERCENTAGE, MetricType.UNIQUE_COUNT,
                  MetricType.UNIQUE_PERCENTAGE, MetricType.MAX_LENGTH, MetricType.MIN_LENGTH, MetricType.EMPTY_COUNT,
                  MetricType.EMPTY_PERCENTAGE, MetricType.MIN, MetricType.MAX, MetricType.MEAN, MetricType.MEDIAN,
                  MetricType.STDDEV, MetricType.NEGATIVE_COUNT, MetricType.NEGATIVE_PERCENTAGE, MetricType.ZERO_COUNT,
                  MetricType.ZERO_PERCENTAGE
                - Using case-insensitive strings: "null_count", "MEAN", "Max_Length", etc.
                - Using models enum: models.FieldMetricTypeClass.NULL_COUNT, etc. (import with: from datahub.metadata import schema_classes as models)
            urn (Optional[Union[str, AssertionUrn]]): The urn of the assertion. If not provided, a urn will be generated and the assertion will be created in the DataHub instance.
            display_name (Optional[str]): The display name of the assertion. If not provided, a random display name will be generated.
            enabled (Optional[bool]): Whether the assertion is enabled. If not provided, the existing value will be preserved.
            detection_mechanism (DetectionMechanismInputTypes): The detection mechanism to be used for the assertion. Valid values are (additional_filter is optional):
                - "all_rows_query_datahub_dataset_profile" or DetectionMechanism.ALL_ROWS_QUERY_DATAHUB_DATASET_PROFILE
                - "all_rows_query" or DetectionMechanism.ALL_ROWS_QUERY(), or with additional_filter: {"type": "all_rows_query", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.ALL_ROWS_QUERY(additional_filter='last_modified > 2021-01-01')
                - {"type": "changed_rows_query", "column_name": "last_modified", "additional_filter": "last_modified > '2021-01-01'"} or DetectionMechanism.CHANGED_ROWS_QUERY(column_name='last_modified', additional_filter='last_modified > 2021-01-01')
            sensitivity (Optional[Union[str, InferenceSensitivity]]): The sensitivity to be applied to the assertion. Valid values are: "low", "medium", "high".
            exclusion_windows (Optional[ExclusionWindowInputTypes]): The exclusion windows to be applied to the assertion. Only fixed range exclusion windows are supported.
            training_data_lookback_days (Optional[int]): The training data lookback days to be applied to the assertion as an integer.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule of every 6 hours will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            SmartColumnMetricAssertion: The created or updated assertion.
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
            self._validate_required_smart_column_fields_for_creation(
                column_name, metric_type
            )
            assert column_name is not None and metric_type is not None, (
                "Fields guaranteed non-None after validation"
            )
            logger.info("URN is not set, creating a new assertion")
            return self._create_smart_column_metric_assertion(
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
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

        # 2.1 If urn is set, fetch missing required parameters from backend if needed:
        # NOTE: This is a tactical solution. The problem is we fetch twice (once for validation,
        # once for merge). Strategic solution would be to merge first, then validate after,
        # but that requires heavy refactor and is skipped for now.
        if urn is not None and (column_name is None or metric_type is None):
            # Fetch existing assertion to get missing required parameters
            maybe_assertion_entity, _, maybe_monitor_entity = (
                self._retrieve_assertion_and_monitor(
                    {"dataset_urn": dataset_urn, "urn": urn}
                )
            )

            if maybe_assertion_entity is not None:
                assertion_info = maybe_assertion_entity.info
                if (
                    hasattr(assertion_info, "fieldMetricAssertion")
                    and assertion_info.fieldMetricAssertion
                ):
                    field_metric_assertion = assertion_info.fieldMetricAssertion
                    # Use existing values for missing required parameters
                    if (
                        column_name is None
                        and hasattr(field_metric_assertion, "field")
                        and hasattr(field_metric_assertion.field, "path")
                    ):
                        column_name = field_metric_assertion.field.path
                    if metric_type is None and hasattr(
                        field_metric_assertion, "metric"
                    ):
                        metric_type = field_metric_assertion.metric
                    # Smart assertions always use BETWEEN operator - no need to fetch from existing assertion

            self._validate_required_smart_column_fields_for_update(
                column_name, metric_type, urn
            )
            assert column_name is not None and metric_type is not None, (
                "Fields guaranteed non-None after validation"
            )

        # 2.1.1 Validate criteria_parameters for creation scenario
        self._validate_criteria_parameters_for_creation(urn)

        # 2.2 Now validate the input with all required parameters:
        assertion_input = _SmartColumnMetricAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
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
            self._retrieve_and_merge_smart_column_metric_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
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
                merged_assertion_input_or_created_assertion, SmartColumnMetricAssertion
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

        return SmartColumnMetricAssertion._from_entities(
            assertion_entity, monitor_entity
        )

    def _create_smart_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartColumnMetricAssertion:
        """Create a smart column metric assertion.

        Note: keyword arguments are required.

        Args:
            dataset_urn: The urn of the dataset to be monitored. (Required)
            column_name: The name of the column to be monitored. (Required)
            metric_type: The type of the metric to be monitored. (Required)
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
            SmartVolumeAssertion: The created assertion.
        """
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _SmartColumnMetricAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
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
        return SmartColumnMetricAssertion._from_entities(
            assertion_entity, monitor_entity
        )

    def _retrieve_and_merge_smart_column_metric_assertion_and_monitor(
        self,
        assertion_input: _SmartColumnMetricAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> Union[SmartColumnMetricAssertion, _SmartColumnMetricAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SmartColumnMetricAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = SmartColumnMetricAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_smart_column_metric_assertion(
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                schedule=schedule,
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

        # 4. Smart assertions always use fixed criteria_parameters (0, 0) and BETWEEN operator
        # No GMS type info needed since values are fixed

        # 5. Merge the existing assertion with the validated input:
        merged_assertion_input = self._merge_smart_column_metric_input(
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
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

    def _merge_smart_column_metric_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        schedule: Optional[Union[str, models.CronScheduleClass]],
        now_utc: datetime,
        assertion_input: _SmartColumnMetricAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: SmartColumnMetricAssertion,
    ) -> _SmartColumnMetricAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            column_name: The name of the column to be monitored.
            metric_type: The type of the metric to be monitored.
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
        merged_assertion_input = _SmartColumnMetricAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=_merge_field(
                input_field_value=column_name,
                input_field_name="column_name",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=SmartColumnMetricAssertion._get_column_name(
                    maybe_assertion_entity
                )
                if maybe_assertion_entity
                else None,
            ),
            metric_type=_merge_field(
                input_field_value=metric_type,
                input_field_name="metric_type",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=SmartColumnMetricAssertion._get_metric_type(
                    maybe_assertion_entity
                )
                if maybe_assertion_entity
                else None,
            ),
            display_name=_merge_field(
                input_field_value=display_name,
                input_field_name="display_name",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=maybe_assertion_entity.description
                if maybe_assertion_entity
                else None,
            ),
            enabled=_merge_field(
                input_field_value=enabled,
                input_field_name="enabled",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=existing_assertion.mode == AssertionMode.ACTIVE
                if existing_assertion
                else None,
            ),
            schedule=_merge_field(
                input_field_value=schedule,
                input_field_name="schedule",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=existing_assertion.schedule
                if existing_assertion
                else None,
            ),
            detection_mechanism=_merge_field(
                input_field_value=detection_mechanism,
                input_field_name="detection_mechanism",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=SmartColumnMetricAssertion._get_detection_mechanism(
                    maybe_assertion_entity, maybe_monitor_entity, default=None
                )
                if maybe_assertion_entity and maybe_monitor_entity
                else None,
            ),
            sensitivity=_merge_field(
                input_field_value=sensitivity,
                input_field_name="sensitivity",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=maybe_monitor_entity.sensitivity
                if maybe_monitor_entity
                else None,
            ),
            exclusion_windows=_merge_field(
                input_field_value=exclusion_windows,
                input_field_name="exclusion_windows",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=maybe_monitor_entity.exclusion_windows
                if maybe_monitor_entity
                else None,
            ),
            training_data_lookback_days=_merge_field(
                input_field_value=training_data_lookback_days,
                input_field_name="training_data_lookback_days",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=maybe_monitor_entity.training_data_lookback_days
                if maybe_monitor_entity
                else None,
            ),
            incident_behavior=_merge_field(
                input_field_value=incident_behavior,
                input_field_name="incident_behavior",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=SmartColumnMetricAssertion._get_incident_behavior(
                    maybe_assertion_entity
                )
                if maybe_assertion_entity
                else None,
            ),
            tags=_merge_field(
                input_field_value=tags,
                input_field_name="tags",
                validated_assertion_input=assertion_input,
                validated_existing_assertion=existing_assertion,
                existing_entity_value=maybe_assertion_entity.tags
                if maybe_assertion_entity
                else None,
            ),
            created_by=existing_assertion.created_by
            or DEFAULT_CREATED_BY,  # Override with the existing assertion's created_by or the default created_by if not set
            created_at=existing_assertion.created_at
            or now_utc,  # Override with the existing assertion's created_at or now if not set
            updated_by=assertion_input.updated_by,  # Override with the input's updated_by
            updated_at=assertion_input.updated_at,  # Override with the input's updated_at (now)
        )

        return merged_assertion_input

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

    def _validate_sql_assertion_creation_params(
        self,
        statement: Optional[str],
        criteria_condition: Optional[Union[SqlAssertionCondition, str]],
        criteria_parameters: Optional[
            Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]
        ],
    ) -> None:
        """Validate required parameters for SQL assertion creation."""
        self._validate_required_field(
            statement, "statement", "when creating a new assertion (urn is None)"
        )
        self._validate_required_field(
            criteria_condition,
            "criteria_condition",
            "when creating a new assertion (urn is None)",
        )
        self._validate_required_field(
            criteria_parameters,
            "criteria_parameters",
            "when creating a new assertion (urn is None)",
        )

    def _validate_required_sql_fields_for_update(
        self,
        statement: Optional[str],
        criteria_condition: Optional[Union[SqlAssertionCondition, str]],
        criteria_parameters: Optional[
            Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]
        ],
        assertion_urn: Union[str, AssertionUrn],
    ) -> None:
        """Validate required fields after attempting to fetch from existing assertion."""
        context = f"and not found in existing assertion {assertion_urn}. The existing assertion may be invalid or corrupted."
        self._validate_required_field(statement, "statement", context)
        self._validate_required_field(criteria_condition, "criteria_condition", context)
        self._validate_required_field(
            criteria_parameters, "criteria_parameters", context
        )

    def sync_sql_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        statement: Optional[str] = None,
        criteria_condition: Optional[Union[SqlAssertionCondition, str]] = None,
        criteria_parameters: Optional[
            Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]
        ] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SqlAssertion:
        """Upsert and merge a sql assertion.

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
            statement (Optional[str]): The SQL statement to be used for the assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion.
            criteria_condition (Optional[Union[SqlAssertionCondition, str]]): The condition for the sql assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion. Valid values are:
                - "IS_EQUAL_TO" -> The metric value equals the threshold.
                - "IS_NOT_EQUAL_TO" -> The metric value does not equal the threshold.
                - "IS_GREATER_THAN" -> The metric value is greater than the threshold.
                - "IS_LESS_THAN" -> The metric value is less than the threshold.
                - "IS_WITHIN_A_RANGE" -> The metric value is within the specified range.
                - "GROWS_AT_MOST_ABSOLUTE" -> The metric growth is at most the threshold (absolute change).
                - "GROWS_AT_MOST_PERCENTAGE" -> The metric growth is at most the threshold (percentage change).
                - "GROWS_AT_LEAST_ABSOLUTE" -> The metric growth is at least the threshold (absolute change).
                - "GROWS_AT_LEAST_PERCENTAGE" -> The metric growth is at least the threshold (percentage change).
                - "GROWS_WITHIN_A_RANGE_ABSOLUTE" -> The metric growth is within the specified range (absolute change).
                - "GROWS_WITHIN_A_RANGE_PERCENTAGE" -> The metric growth is within the specified range (percentage change).
            criteria_parameters (Optional[Union[float, int, tuple[float, int]]]): The threshold parameters to be used for the assertion. Required when creating a new assertion (urn=None), optional when updating an existing assertion. This can be a single threshold value or a tuple range.
                - If the condition is range-based (IS_WITHIN_A_RANGE, GROWS_WITHIN_A_RANGE_ABSOLUTE, GROWS_WITHIN_A_RANGE_PERCENTAGE), the value is a tuple of two threshold values, with format (min, max).
                - For other conditions, the value is a single numeric threshold value.
            incident_behavior (Optional[Union[str, list[str], AssertionIncidentBehavior, list[AssertionIncidentBehavior]]]): The incident behavior to be applied to the assertion. Valid values are: "raise_on_fail", "resolve_on_pass", or the typed ones (AssertionIncidentBehavior.RAISE_ON_FAIL and AssertionIncidentBehavior.RESOLVE_ON_PASS).
            tags (Optional[TagsInputType]): The tags to be applied to the assertion. Valid values are: a list of strings, TagUrn objects, or TagAssociationClass objects.
            updated_by (Optional[Union[str, CorpUserUrn]]): Optional urn of the user who updated the assertion. The format is "urn:li:corpuser:<username>". The default is the datahub system user.
            schedule (Optional[Union[str, models.CronScheduleClass]]): Optional cron formatted schedule for the assertion. If not provided, a default schedule will be used. The format is a cron expression, e.g. "0 * * * *" for every hour using UTC timezone. Alternatively, a models.CronScheduleClass object can be provided.

        Returns:
            SqlAssertion: The created or updated assertion.
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

            # Validate required parameters for creation
            self._validate_sql_assertion_creation_params(
                statement, criteria_condition, criteria_parameters
            )
            # After validation, these cannot be None
            assert statement is not None
            assert criteria_condition is not None
            assert criteria_parameters is not None
            return self._create_sql_assertion(
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                criteria_condition=criteria_condition,
                criteria_parameters=criteria_parameters,
                statement=statement,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
            )

        # 2.1 If urn is set, fetch missing required parameters from backend if needed:
        # NOTE: This is a tactical solution. The problem is we fetch twice (once for validation,
        # once for merge). Strategic solution would be to merge first, then validate after,
        # but that requires heavy refactor and is skipped for now.
        if urn is not None and (
            statement is None
            or criteria_condition is None
            or criteria_parameters is None
        ):
            # Fetch existing assertion to get missing required parameters
            maybe_assertion_entity, _, maybe_monitor_entity = (
                self._retrieve_assertion_and_monitor(
                    {"dataset_urn": dataset_urn, "urn": urn}
                )
            )

            if maybe_assertion_entity is not None and maybe_monitor_entity is not None:
                existing_assertion = SqlAssertion._from_entities(
                    maybe_assertion_entity, maybe_monitor_entity
                )
                # Use existing values for missing required parameters
                if statement is None:
                    statement = existing_assertion.statement
                if criteria_condition is None or criteria_parameters is None:
                    criteria = existing_assertion._criteria
                    if criteria_condition is None:
                        criteria_condition = criteria.condition
                    if criteria_parameters is None:
                        criteria_parameters = criteria.parameters

            self._validate_required_sql_fields_for_update(
                statement, criteria_condition, criteria_parameters, urn
            )
            assert (
                statement is not None
                and criteria_condition is not None
                and criteria_parameters is not None
            ), "Fields guaranteed non-None after validation"

        # 2.2 Now validate the input with all required parameters:
        criteria = SqlAssertionCriteria(
            condition=criteria_condition,
            parameters=criteria_parameters,
        )

        assertion_input = _SqlAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            criteria=criteria,
            statement=statement,
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
            self._retrieve_and_merge_sql_assertion_and_monitor(
                assertion_input=assertion_input,
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                criteria=criteria,
                statement=statement,
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
            assert isinstance(merged_assertion_input_or_created_assertion, SqlAssertion)
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

        return SqlAssertion._from_entities(assertion_entity, monitor_entity)

    def _validate_required_column_fields_for_creation(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
    ) -> None:
        """Validate required fields for column metric assertion creation."""
        self._validate_required_field(
            column_name, "column_name", "when creating a new assertion (urn is None)"
        )
        self._validate_required_field(
            metric_type, "metric_type", "when creating a new assertion (urn is None)"
        )
        self._validate_required_field(
            operator, "operator", "when creating a new assertion (urn is None)"
        )

    def _validate_required_column_fields_for_update(
        self,
        column_name: Optional[str],
        metric_type: Optional[MetricInputType],
        operator: Optional[OperatorInputType],
        assertion_urn: Union[str, AssertionUrn],
    ) -> None:
        """Validate required fields after attempting to fetch from existing assertion."""
        context = f"and not found in existing assertion {assertion_urn}. The existing assertion may be invalid or corrupted."
        self._validate_required_field(column_name, "column_name", context)
        self._validate_required_field(metric_type, "metric_type", context)
        self._validate_required_field(operator, "operator", context)

    def _create_column_metric_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        detection_mechanism: DetectionMechanismInputTypes = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> ColumnMetricAssertion:
        """Create a column metric assertion.

        Note: keyword arguments are required.

        Args:
            dataset_urn: The urn of the dataset to be monitored. (Required)
            column_name: The name of the column to be monitored. (Required)
            metric_type: The type of the metric to be monitored. (Required)
            operator: The operator to be used for the assertion. (Required)
            criteria_parameters: The criteria parameters for the assertion. Required for most operators.
            display_name: The display name of the assertion. If not provided, a random display
                name will be generated.
            enabled: Whether the assertion is enabled. Defaults to True.
            detection_mechanism: The detection mechanism to be used for the assertion.
            incident_behavior: The incident behavior to be applied to the assertion.
            tags: The tags to be applied to the assertion.
            created_by: Optional urn of the user who created the assertion.
            schedule: Optional cron formatted schedule for the assertion.

        Returns:
            ColumnMetricAssertion: The created assertion.
        """
        now_utc = datetime.now(timezone.utc)
        if created_by is None:
            logger.warning(
                f"Created by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            created_by = DEFAULT_CREATED_BY
        assertion_input = _ColumnMetricAssertionInput(
            urn=None,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
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
            gms_criteria_type_info=None,
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
        return ColumnMetricAssertion._from_entities(assertion_entity, monitor_entity)

    def _retrieve_and_merge_column_metric_assertion_and_monitor(
        self,
        assertion_input: _ColumnMetricAssertionInput,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Optional[Union[str, CorpUserUrn]],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> Union[ColumnMetricAssertion, _ColumnMetricAssertionInput]:
        # 1. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        # Extract gms_criteria_type_info from existing assertion if available
        gms_criteria_type_info = None
        if maybe_assertion_entity is not None:
            gms_criteria_type_info = (
                _HasColumnMetricFunctionality._get_criteria_parameters_with_type(
                    maybe_assertion_entity
                )
            )

        # 2.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = ColumnMetricAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 2.2 If the assertion exists but the monitor does not, create a placeholder monitor entity to be able to create the assertion:
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = ColumnMetricAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 2.3 If the assertion does not exist, create a new assertion with a generated urn and return the assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, creating a new assertion with a generated urn"
            )
            return self._create_column_metric_assertion(
                dataset_urn=dataset_urn,
                column_name=column_name,
                metric_type=metric_type,
                operator=operator,
                criteria_parameters=criteria_parameters,
                schedule=schedule,
                display_name=display_name,
                detection_mechanism=detection_mechanism,
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
        merged_assertion_input = self._merge_column_metric_input(
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
            detection_mechanism=detection_mechanism,
            incident_behavior=incident_behavior,
            tags=tags,
            now_utc=now_utc,
            assertion_input=assertion_input,
            maybe_assertion_entity=maybe_assertion_entity,
            maybe_monitor_entity=maybe_monitor_entity,
            existing_assertion=existing_assertion,
            gms_criteria_type_info=gms_criteria_type_info,
        )

        return merged_assertion_input

    def _merge_column_metric_input(  # TODO: Refactor
        self,
        dataset_urn: Union[str, DatasetUrn],
        column_name: str,
        metric_type: MetricInputType,
        operator: OperatorInputType,
        criteria_parameters: Optional[ColumnMetricAssertionParameters],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        schedule: Optional[Union[str, models.CronScheduleClass]],
        detection_mechanism: DetectionMechanismInputTypes,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _ColumnMetricAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: ColumnMetricAssertion,
        gms_criteria_type_info: Optional[tuple] = None,
    ) -> _ColumnMetricAssertionInput:
        """Merge the validated assertion input with the existing assertion to create an upsert."""

        # Extract existing values from entities for merging
        existing_display_name = None
        existing_enabled = None
        existing_schedule = None
        existing_detection_mechanism = None
        existing_incident_behavior = None
        existing_tags = None

        if maybe_assertion_entity and maybe_assertion_entity.info:
            if hasattr(maybe_assertion_entity.info, "displayName"):
                existing_display_name = maybe_assertion_entity.info.displayName
            if hasattr(maybe_assertion_entity.info, "tags"):
                existing_tags = maybe_assertion_entity.info.tags

        if maybe_monitor_entity and maybe_monitor_entity.info:
            if (
                hasattr(maybe_monitor_entity.info, "status")
                and maybe_monitor_entity.info.status
            ):
                existing_enabled = maybe_monitor_entity.info.status == "ACTIVE"
            if (
                hasattr(maybe_monitor_entity.info, "config")
                and maybe_monitor_entity.info.config
            ):
                if hasattr(maybe_monitor_entity.info.config, "schedule"):
                    existing_schedule = maybe_monitor_entity.info.config.schedule
                if hasattr(maybe_monitor_entity.info.config, "executorId"):
                    existing_detection_mechanism = (
                        maybe_monitor_entity.info.config.executorId
                    )
                if hasattr(maybe_monitor_entity.info.config, "actions"):
                    existing_incident_behavior = (
                        maybe_monitor_entity.info.config.actions
                    )

        # Merge each field using the merge logic
        merged_display_name = _merge_field(
            display_name,
            "display_name",
            assertion_input,
            existing_assertion,
            existing_display_name,
        )
        merged_enabled = _merge_field(
            enabled, "mode", assertion_input, existing_assertion, existing_enabled
        )
        merged_schedule = _merge_field(
            schedule, "schedule", assertion_input, existing_assertion, existing_schedule
        )
        merged_detection_mechanism = _merge_field(
            detection_mechanism,
            "detection_mechanism",
            assertion_input,
            existing_assertion,
            existing_detection_mechanism,
        )
        merged_incident_behavior = _merge_field(
            incident_behavior,
            "incident_behavior",
            assertion_input,
            existing_assertion,
            existing_incident_behavior,
        )
        merged_tags = _merge_field(
            tags, "tags", assertion_input, existing_assertion, existing_tags
        )

        # Create the merged assertion input
        return _ColumnMetricAssertionInput(
            urn=urn,
            entity_client=assertion_input.entity_client,
            dataset_urn=dataset_urn,
            column_name=column_name,
            metric_type=metric_type,
            operator=operator,
            criteria_parameters=criteria_parameters,
            display_name=merged_display_name,
            enabled=merged_enabled,
            detection_mechanism=merged_detection_mechanism,
            incident_behavior=merged_incident_behavior,
            tags=merged_tags,
            created_by=existing_assertion.created_by
            if existing_assertion.created_by
            else assertion_input.created_by,
            created_at=existing_assertion.created_at
            if existing_assertion.created_at
            else assertion_input.created_at,
            updated_by=assertion_input.updated_by,
            updated_at=now_utc,
            schedule=merged_schedule,
            gms_criteria_type_info=gms_criteria_type_info,
        )


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
