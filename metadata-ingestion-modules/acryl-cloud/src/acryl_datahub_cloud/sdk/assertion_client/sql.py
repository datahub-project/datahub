from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, TypedDict, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    SqlAssertion,
    _AssertionPublic,
)
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    DEFAULT_CREATED_BY,
    _merge_field,
    _print_experimental_warning,
    _validate_required_field,
    retrieve_assertion_and_monitor_by_urn,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
    _AssertionInput,
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


class _AssertionLookupInfo(TypedDict):
    """Minimal lookup info for retrieving assertion and monitor."""

    urn: Union[str, AssertionUrn]
    dataset_urn: Union[str, DatasetUrn]


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
            statement, "statement", "when creating a new assertion (urn is None)"
        )
        _validate_required_field(
            criteria_condition,
            "criteria_condition",
            "when creating a new assertion (urn is None)",
        )
        _validate_required_field(
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
        _validate_required_field(statement, "statement", context)
        _validate_required_field(criteria_condition, "criteria_condition", context)
        _validate_required_field(criteria_parameters, "criteria_parameters", context)

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
                urn=urn,
                display_name=display_name,
                criteria_condition=criteria.condition,
                criteria_parameters=criteria.parameters,
                statement=statement,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                schedule=schedule,
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

        return retrieve_assertion_and_monitor_by_urn(self.client, _urn, _dataset_urn)

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

    def _create_sql_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
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
            urn=urn,
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
