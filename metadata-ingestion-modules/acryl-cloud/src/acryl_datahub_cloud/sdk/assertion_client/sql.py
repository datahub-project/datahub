from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    SqlAssertion,
)
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
)
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
    SqlAssertionCriteria,
    _SqlAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class SqlAssertionClient:
    """Client for managing SQL assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def _validate_sql_assertion_creation_params(
        self,
        statement: Optional[str],
        criteria_condition: Optional[Union[SqlAssertionCondition, str]],
        criteria_parameters: Optional[
            Union[Union[float, int], tuple[Union[float, int], Union[float, int]]]
        ],
    ) -> None:
        """Validate required parameters for SQL assertion creation."""
        _validate_required_field(
            statement,
            "statement",
            "when creating a new assertion (urn is None). Provide a SQL query like 'SELECT COUNT(*) FROM table'",
        )
        _validate_required_field(
            criteria_condition,
            "criteria_condition",
            "when creating a new assertion (urn is None). Must be one of: IS_EQUAL_TO, IS_GREATER_THAN, IS_LESS_THAN, IS_WITHIN_A_RANGE, etc.",
        )
        _validate_required_field(
            criteria_parameters,
            "criteria_parameters",
            "when creating a new assertion (urn is None). Provide a single value (e.g., 100) or a range (e.g., (10, 100))",
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
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        resolved_updated_by = resolve_updated_by(updated_by)

        # Validate that criteria_condition and criteria_parameters are both provided or both omitted
        if (criteria_condition is None) != (criteria_parameters is None):
            raise SDKUsageError(
                "criteria_condition and criteria_parameters must both be provided or both omitted"
            )

        # Validate required params for new assertions upfront, since we can't fetch them from an existing one
        if urn is None:
            logger.info("URN is not set, creating a new assertion")
            self._validate_sql_assertion_creation_params(
                statement, criteria_condition, criteria_parameters
            )

        # Build criteria if condition and parameters are provided
        criteria: Optional[SqlAssertionCriteria] = None
        if criteria_condition is not None and criteria_parameters is not None:
            criteria = SqlAssertionCriteria(
                condition=criteria_condition,
                parameters=criteria_parameters,
            )

        merged_assertion_input = self._retrieve_and_merge_sql_assertion_and_monitor(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            criteria=criteria,
            statement=statement,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=resolved_updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        # Validate after merge to catch two scenarios:
        # 1. Developer forgot required fields when creating new assertion (caught earlier)
        # 2. Developer provided a URN that doesn't exist or points to invalid assertion
        if merged_assertion_input.statement is None:
            raise SDKUsageError(
                f"statement is required but was not provided and not found in existing assertion {urn}. "
                "The existing assertion may be invalid or corrupted."
            )
        if merged_assertion_input.criteria is None:
            raise SDKUsageError(
                f"criteria (condition and parameters) is required but was not provided and not found in existing assertion {urn}. "
                "The existing assertion may be invalid or corrupted."
            )

        assertion_entity, monitor_entity = (
            merged_assertion_input.to_assertion_and_monitor_entities()
        )
        # Upsert assertion first to fail fast if there's an issue before touching the monitor
        self.client.entities.upsert(assertion_entity)
        self.client.entities.upsert(monitor_entity)

        return SqlAssertion._from_entities(assertion_entity, monitor_entity)

    def _build_sql_assertion_input(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        criteria: Optional[SqlAssertionCriteria],
        statement: Optional[str],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SqlAssertionInput:
        """Build a SqlAssertionInput from the given parameters."""
        return _SqlAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            enabled=enabled if enabled is not None else True,
            criteria=criteria,
            statement=statement,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,
            created_at=now_utc,
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
        )

    # TODO: Refactoring planned - See https://github.com/acryldata/datahub-fork/pull/7585
    # This method (lines 187-290) contains duplicated orchestration logic that exists
    # in 7 other assertion client files. A future PR will extract this into a
    # BaseAssertionClient with template method pattern. See PR #7585 for details.
    def _retrieve_and_merge_sql_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        criteria: Optional[SqlAssertionCriteria],
        statement: Optional[str],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SqlAssertionInput:
        if urn is None:
            logger.info("URN is not set, building a new assertion input")
            return self._build_sql_assertion_input(
                dataset_urn=dataset_urn,
                urn=None,
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

        assertion_input = self._build_sql_assertion_input(
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

        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(assertion_input)
        )

        existing_assertion: Optional[SqlAssertion] = None

        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SqlAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = "INACTIVE" if enabled is False else "ACTIVE"
            existing_assertion = SqlAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, building a new assertion input"
            )
            return self._build_sql_assertion_input(
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

        # At this point, existing_assertion must be set (we return early if not maybe_assertion_entity)
        assert existing_assertion is not None

        # Validate dataset URN hasn't changed to prevent assertion corruption
        if existing_assertion.dataset_urn != assertion_input.dataset_urn:
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

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
        assertion_input: _SqlAssertionInput,
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

    def _merge_sql_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        criteria: Optional[SqlAssertionCriteria],
        statement: Optional[str],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _SqlAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
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
                # Note: existing_assertion.schedule is already derived from the monitor via _get_schedule()
                existing_assertion.schedule if existing_assertion else None,
            ),
            criteria=_merge_field(
                criteria,
                "criteria",
                assertion_input,
                existing_assertion,
                existing_assertion.criteria if existing_assertion else None,
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
