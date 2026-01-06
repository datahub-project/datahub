from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.smart_sql_assertion import SmartSqlAssertion
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    DEFAULT_CREATED_BY,
    _merge_field,
    _print_experimental_warning,
    _validate_required_field,
    resolve_updated_by,
    retrieve_assertion_and_monitor_by_urn,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    ExclusionWindowInputTypes,
    InferenceSensitivity,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_sql_assertion_input import (
    _SmartSqlAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class SmartSqlAssertionClient:
    """Client for managing smart SQL assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def sync_smart_sql_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        statement: Optional[str] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        sensitivity: Optional[Union[str, InferenceSensitivity]] = None,
        exclusion_windows: Optional[ExclusionWindowInputTypes] = None,
        training_data_lookback_days: Optional[int] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SmartSqlAssertion:
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        resolved_updated_by = resolve_updated_by(updated_by)

        # Validate statement is provided for new assertions, since we can't fetch it from an existing one
        if urn is None:
            logger.info("URN is not set, creating a new assertion")
            _validate_required_field(
                statement, "statement", "when creating a new assertion (urn is None)"
            )
            assert statement is not None

        merged_assertion_input = (
            self._retrieve_and_merge_smart_sql_assertion_and_monitor(
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                statement=statement,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=resolved_updated_by,
                now_utc=now_utc,
                schedule=schedule,
            )
        )

        # After merge, ensure statement exists (either provided or fetched from existing assertion)
        if merged_assertion_input.statement is None:
            raise SDKUsageError(
                f"statement is required but was not provided and not found in existing assertion {urn}. "
                "The existing assertion may be invalid or corrupted."
            )

        assertion_entity, monitor_entity = (
            merged_assertion_input.to_assertion_and_monitor_entities()
        )
        # Upsert assertion first to fail fast if there's an issue before touching the monitor
        self.client.entities.upsert(assertion_entity)
        self.client.entities.upsert(monitor_entity)

        return SmartSqlAssertion._from_entities(assertion_entity, monitor_entity)

    def _retrieve_assertion_and_monitor_by_urn(
        self,
        urn: Union[str, AssertionUrn],
        dataset_urn: Union[str, DatasetUrn],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve assertion and monitor entities by URN."""
        return retrieve_assertion_and_monitor_by_urn(self.client, urn, dataset_urn)

    def _build_smart_sql_assertion_input(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        statement: Optional[str],
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SmartSqlAssertionInput:
        """Build a SmartSqlAssertionInput from the given parameters."""
        return _SmartSqlAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled if enabled is not None else True,
            statement=statement,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,
            created_at=now_utc,
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
        )

    # TODO: Refactoring planned - See https://github.com/acryldata/datahub-fork/pull/7585
    # This method (lines 156-270) contains duplicated orchestration logic that exists
    # in 7 other assertion client files. A future PR will extract this into a
    # BaseAssertionClient with template method pattern. See PR #7585 for details.
    def _retrieve_and_merge_smart_sql_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        statement: Optional[str],
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SmartSqlAssertionInput:
        if urn is None:
            logger.info("URN is not set, building a new assertion input")
            return self._build_smart_sql_assertion_input(
                dataset_urn=dataset_urn,
                urn=None,
                display_name=display_name,
                enabled=enabled,
                statement=statement,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
                schedule=schedule,
            )

        assertion_input = self._build_smart_sql_assertion_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            statement=statement,
            sensitivity=sensitivity,
            exclusion_windows=exclusion_windows,
            training_data_lookback_days=training_data_lookback_days,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        existing_assertion: Optional[SmartSqlAssertion] = None

        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SmartSqlAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = "INACTIVE" if enabled is False else "ACTIVE"
            existing_assertion = SmartSqlAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, building a new assertion input"
            )
            return self._build_smart_sql_assertion_input(
                dataset_urn=dataset_urn,
                urn=urn,
                display_name=display_name,
                enabled=enabled,
                statement=statement,
                sensitivity=sensitivity,
                exclusion_windows=exclusion_windows,
                training_data_lookback_days=training_data_lookback_days,
                incident_behavior=incident_behavior,
                tags=tags,
                updated_by=updated_by,
                now_utc=now_utc,
                schedule=schedule,
            )

        # At this point, existing_assertion must be set (we return early if not maybe_assertion_entity)
        assert existing_assertion is not None

        # Validate dataset URN hasn't changed to prevent assertion corruption
        if existing_assertion.dataset_urn != assertion_input.dataset_urn:
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        merged_assertion_input = self._merge_smart_sql_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            statement=statement,
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
            schedule=schedule,
        )

        return merged_assertion_input

    def _retrieve_assertion_and_monitor(
        self,
        assertion_input: _SmartSqlAssertionInput,
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

    def _merge_smart_sql_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        statement: Optional[str],
        sensitivity: Optional[Union[str, InferenceSensitivity]],
        exclusion_windows: Optional[ExclusionWindowInputTypes],
        training_data_lookback_days: Optional[int],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _SmartSqlAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: SmartSqlAssertion,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SmartSqlAssertionInput:
        """Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The urn of the dataset to be monitored.
            urn: The urn of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            statement: The SQL statement for the assertion.
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
            schedule: The schedule to be applied to the assertion.

        Returns:
            The merged assertion input.
        """
        merged_assertion_input = _SmartSqlAssertionInput(
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
            statement=_merge_field(
                statement,
                "statement",
                assertion_input,
                existing_assertion,
                existing_assertion.statement if existing_assertion else None,
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
                SmartSqlAssertion._get_incident_behavior(maybe_assertion_entity)
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
            created_by=existing_assertion.created_by or DEFAULT_CREATED_BY,
            created_at=existing_assertion.created_at or now_utc,
            updated_by=assertion_input.updated_by,
            updated_at=assertion_input.updated_at,
        )

        return merged_assertion_input
