"""
Schema assertion client module.

This module provides the SchemaAssertionClient for creating and managing
schema assertions that validate dataset schemas match expected field definitions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.schema_assertion import SchemaAssertion
from acryl_datahub_cloud.sdk.assertion_client.helpers import (
    DEFAULT_CREATED_BY,
    _merge_field,
    _print_experimental_warning,
    _validate_required_field,
    retrieve_assertion_and_monitor_by_urn,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehaviorInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaAssertionFieldsInputType,
    _parse_schema_assertion_fields,
    _SchemaAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion, TagsInputType
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)


class SchemaAssertionClient:
    """Client for managing schema assertions."""

    def __init__(self, client: "DataHubClient"):
        self.client = client

    def sync_schema_assertion(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        compatibility: Optional[Union[str, SchemaAssertionCompatibility]] = None,
        fields: Optional[SchemaAssertionFieldsInputType] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        updated_by: Optional[Union[str, CorpUserUrn]] = None,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
    ) -> SchemaAssertion:
        _print_experimental_warning()
        now_utc = datetime.now(timezone.utc)

        if updated_by is None:
            logger.warning(
                f"updated_by is not set, using {DEFAULT_CREATED_BY} as a placeholder"
            )
            updated_by = DEFAULT_CREATED_BY

        # 1. Retrieve and merge the assertion input with any existing assertion and monitor entities,
        # or build a new assertion input if the assertion does not exist:
        assertion_input = self._retrieve_and_merge_schema_assertion_and_monitor(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            compatibility=compatibility,
            fields=fields,
            incident_behavior=incident_behavior,
            tags=tags,
            updated_by=updated_by,
            now_utc=now_utc,
            schedule=schedule,
        )

        # 2. Upsert the assertion and monitor entities:
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )
        # If assertion upsert fails, we won't try to upsert the monitor
        self.client.entities.upsert(assertion_entity)
        self.client.entities.upsert(monitor_entity)

        return SchemaAssertion._from_entities(assertion_entity, monitor_entity)

    def _retrieve_and_merge_schema_assertion_and_monitor(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Optional[Union[str, AssertionUrn]],
        display_name: Optional[str],
        enabled: Optional[bool],
        compatibility: Optional[Union[str, SchemaAssertionCompatibility]],
        fields: Optional[SchemaAssertionFieldsInputType],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        updated_by: Union[str, CorpUserUrn],
        now_utc: datetime,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SchemaAssertionInput:
        """Retrieve existing assertion if URN provided and merge with new values."""
        # 1. If urn is not provided, build and return a new assertion input directly
        if urn is None:
            logger.info("URN is not set, building a new assertion input")
            # For creation, fields is required
            _validate_required_field(
                fields, "fields", "when creating a new assertion (urn is None)"
            )
            return _SchemaAssertionInput(
                urn=None,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                compatibility=compatibility,
                fields=fields,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                created_at=now_utc,
                updated_by=updated_by,
                updated_at=now_utc,
                schedule=schedule,
            )

        # 2. Build initial assertion input for validation
        assertion_input = _SchemaAssertionInput(
            urn=urn,
            entity_client=self.client.entities,
            dataset_urn=dataset_urn,
            display_name=display_name,
            compatibility=compatibility,
            fields=fields,
            incident_behavior=incident_behavior,
            tags=tags,
            created_by=updated_by,  # This will be overridden by the actual created_by
            created_at=now_utc,  # This will be overridden by the actual created_at
            updated_by=updated_by,
            updated_at=now_utc,
            schedule=schedule,
        )

        # 3. Retrieve any existing assertion and monitor entities:
        maybe_assertion_entity, monitor_urn, maybe_monitor_entity = (
            self._retrieve_assertion_and_monitor(urn, dataset_urn)
        )

        # 4.1 If the assertion and monitor entities exist, create an assertion object from them:
        if maybe_assertion_entity and maybe_monitor_entity:
            existing_assertion = SchemaAssertion._from_entities(
                maybe_assertion_entity, maybe_monitor_entity
            )
        # 4.2 If the assertion exists but the monitor does not, create a placeholder monitor entity
        elif maybe_assertion_entity and not maybe_monitor_entity:
            monitor_mode = (
                "ACTIVE" if enabled else "INACTIVE" if enabled is not None else "ACTIVE"
            )
            existing_assertion = SchemaAssertion._from_entities(
                maybe_assertion_entity,
                Monitor(id=monitor_urn, info=("ASSERTION", monitor_mode)),
            )
        # 4.3 If the assertion does not exist, build and return a new assertion input:
        elif not maybe_assertion_entity:
            logger.info(
                f"No existing assertion entity found for assertion urn {urn}, building a new assertion input"
            )
            # For creation, fields is required
            _validate_required_field(
                fields,
                "fields",
                "when creating a new assertion (no existing assertion found)",
            )
            return _SchemaAssertionInput(
                urn=urn,
                entity_client=self.client.entities,
                dataset_urn=dataset_urn,
                display_name=display_name,
                enabled=enabled if enabled is not None else True,
                compatibility=compatibility,
                fields=fields,
                incident_behavior=incident_behavior,
                tags=tags,
                created_by=updated_by,
                created_at=now_utc,
                updated_by=updated_by,
                updated_at=now_utc,
                schedule=schedule,
            )

        # 5. Validate that we have field definitions (either from input or existing assertion)
        if fields is None and existing_assertion and not existing_assertion.fields:
            raise SDKUsageError(
                f"Cannot update assertion {urn}: existing assertion has no field definitions. "
                "Please provide 'fields' parameter."
            )

        # 6. Check for any issues e.g. different dataset urns
        if (
            existing_assertion
            and hasattr(existing_assertion, "dataset_urn")
            and existing_assertion.dataset_urn != assertion_input.dataset_urn
        ):
            raise SDKUsageError(
                f"Dataset URN mismatch, existing assertion: {existing_assertion.dataset_urn} != new assertion: {dataset_urn}"
            )

        # 7. Merge the existing assertion with the validated input:
        merged_assertion_input = self._merge_schema_input(
            dataset_urn=dataset_urn,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            compatibility=compatibility,
            fields=fields,
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
        urn: Union[str, AssertionUrn],
        dataset_urn: Union[str, DatasetUrn],
    ) -> tuple[Optional[Assertion], MonitorUrn, Optional[Monitor]]:
        """Retrieve the assertion and monitor entities from the DataHub instance."""
        return retrieve_assertion_and_monitor_by_urn(self.client, urn, dataset_urn)

    def _merge_schema_input(
        self,
        dataset_urn: Union[str, DatasetUrn],
        urn: Union[str, AssertionUrn],
        display_name: Optional[str],
        enabled: Optional[bool],
        compatibility: Optional[Union[str, SchemaAssertionCompatibility]],
        fields: Optional[SchemaAssertionFieldsInputType],
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes],
        tags: Optional[TagsInputType],
        now_utc: datetime,
        assertion_input: _SchemaAssertionInput,
        maybe_assertion_entity: Optional[Assertion],
        maybe_monitor_entity: Optional[Monitor],
        existing_assertion: SchemaAssertion,
        schedule: Optional[Union[str, models.CronScheduleClass]],
    ) -> _SchemaAssertionInput:
        """
        Merge the input with the existing assertion and monitor entities.

        Args:
            dataset_urn: The URN of the dataset to validate.
            urn: The URN of the assertion.
            display_name: The display name of the assertion.
            enabled: Whether the assertion is enabled.
            compatibility: The compatibility mode for schema validation.
            fields: The expected schema fields.
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
        # Merge fields - convert to list of SchemaAssertionField if needed
        merged_fields = _merge_field(
            fields,
            "fields",
            assertion_input,
            existing_assertion,
            existing_assertion.fields if existing_assertion else None,
        )

        # Ensure fields are parsed if they came from the existing assertion
        if merged_fields and not all(
            isinstance(f, SchemaAssertionField) for f in merged_fields
        ):
            merged_fields = _parse_schema_assertion_fields(merged_fields)

        merged_assertion_input = _SchemaAssertionInput(
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
            compatibility=_merge_field(
                compatibility,
                "compatibility",
                assertion_input,
                existing_assertion,
                existing_assertion.compatibility if existing_assertion else None,
            ),
            fields=merged_fields,
            incident_behavior=_merge_field(
                incident_behavior,
                "incident_behavior",
                assertion_input,
                existing_assertion,
                SchemaAssertion._get_incident_behavior(maybe_assertion_entity)
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
