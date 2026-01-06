"""
Schema assertion module.

This module provides the SchemaAssertion class for representing and working with
schema assertions that validate dataset schemas match expected field definitions.
"""

import logging
from datetime import datetime
from typing import Optional, Union

from typing_extensions import Self

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    _AssertionPublic,
    _HasSchedule,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    _DetectionMechanismTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY,
    SCHEMA_FIELD_CLASS_TO_TYPE_MAP,
    SchemaAssertionCompatibility,
    SchemaAssertionField,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, TagUrn

logger = logging.getLogger(__name__)


class SchemaAssertion(_HasSchedule, _AssertionPublic):
    """
    A class that represents a schema assertion.

    Schema assertions validate that a dataset's schema matches expected
    field definitions with configurable compatibility modes.
    """

    def __init__(
        self,
        *,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        display_name: str,
        mode: AssertionMode,
        schedule: models.CronScheduleClass,
        compatibility: SchemaAssertionCompatibility,
        fields: list[SchemaAssertionField],
        tags: list[TagUrn],
        incident_behavior: list[AssertionIncidentBehavior],
        created_by: Optional[CorpUserUrn] = None,
        created_at: Union[datetime, None] = None,
        updated_by: Optional[CorpUserUrn] = None,
        updated_at: Optional[datetime] = None,
    ):
        """
        Initialize a schema assertion.

        Note: Values can be accessed, but not set on the assertion object.
        To update an assertion, use the `sync_schema_assertion` method.

        Args:
            urn: The URN of the assertion.
            dataset_urn: The URN of the dataset that the assertion validates.
            display_name: The display name of the assertion.
            mode: The mode of the assertion (active, inactive).
            schedule: The evaluation schedule of the assertion.
            compatibility: The compatibility mode (EXACT_MATCH, SUPERSET, SUBSET).
            fields: The expected schema fields to validate.
            tags: The tags applied to the assertion.
            incident_behavior: Actions to take on assertion pass/fail.
            created_by: The user who created the assertion.
            created_at: The timestamp when the assertion was created.
            updated_by: The user who last updated the assertion.
            updated_at: The timestamp when the assertion was last updated.
        """
        _HasSchedule.__init__(self, schedule=schedule)
        _AssertionPublic.__init__(
            self,
            urn=urn,
            dataset_urn=dataset_urn,
            display_name=display_name,
            mode=mode,
            incident_behavior=incident_behavior,
            detection_mechanism=None,  # Schema assertions don't use detection mechanism
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
            tags=tags,
        )
        self._compatibility = compatibility
        self._fields = fields

    @property
    def compatibility(self) -> SchemaAssertionCompatibility:
        """The compatibility mode for the schema assertion."""
        return self._compatibility

    @property
    def fields(self) -> list[SchemaAssertionField]:
        """The expected schema fields to validate."""
        return self._fields

    @staticmethod
    def _get_detection_mechanism(
        assertion: Assertion,
        monitor: Monitor,
        default: Optional[_DetectionMechanismTypes] = None,
    ) -> Optional[_DetectionMechanismTypes]:
        """
        Schema assertions don't have a detection mechanism.
        They always use DATAHUB_SCHEMA as the source type.
        """
        return None

    @staticmethod
    def _get_compatibility(assertion: Assertion) -> SchemaAssertionCompatibility:
        """Extract compatibility from the assertion info."""
        if assertion.info is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have info, "
                f"defaulting to {DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY}"
            )
            return DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY

        if not isinstance(assertion.info, models.SchemaAssertionInfoClass):
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a schema assertion"
            )

        compatibility_value = assertion.info.compatibility
        if compatibility_value is None:
            return DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY

        # Handle enum value extraction
        try:
            if hasattr(compatibility_value, "name"):
                # It's an enum-like object
                return SchemaAssertionCompatibility(compatibility_value.name)
            elif isinstance(compatibility_value, str):
                return SchemaAssertionCompatibility(compatibility_value.upper())
            else:
                logger.warning(
                    f"Unknown compatibility value type {type(compatibility_value)}, "
                    f"defaulting to {DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY}"
                )
                return DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY
        except ValueError:
            logger.warning(
                f"Unknown compatibility value '{compatibility_value}', "
                f"defaulting to {DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY}"
            )
            return DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY

    @staticmethod
    def _get_fields(assertion: Assertion) -> list[SchemaAssertionField]:
        """Extract schema fields from the assertion info."""
        if assertion.info is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have info, returning empty fields"
            )
            return []

        if not isinstance(assertion.info, models.SchemaAssertionInfoClass):
            raise SDKNotYetSupportedError(
                f"Assertion {assertion.urn} is not a schema assertion"
            )

        schema = assertion.info.schema
        if schema is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have schema metadata, "
                "returning empty fields"
            )
            return []

        fields = []
        failed_fields = []
        for schema_field in schema.fields or []:
            field = SchemaAssertion._parse_schema_field(schema_field)
            if field is not None:
                fields.append(field)
            else:
                failed_fields.append(schema_field.fieldPath)

        if failed_fields:
            logger.warning(
                f"Failed to parse {len(failed_fields)} field(s) from assertion {assertion.urn}: "
                f"{', '.join(failed_fields)}. These fields will be excluded from the assertion object. "
                f"This may indicate unsupported field types or corrupted data."
            )

        return fields

    @staticmethod
    def _parse_schema_field(
        schema_field: models.SchemaFieldClass,
    ) -> Optional[SchemaAssertionField]:
        """Parse a SchemaFieldClass into a SchemaAssertionField."""
        # Explicit attribute checks to fail fast on missing required attributes
        if not hasattr(schema_field, "fieldPath") or not hasattr(schema_field, "type"):
            logger.warning(
                "Schema field missing required attributes: fieldPath or type, skipping"
            )
            return None

        # Extract the type from the SchemaFieldDataTypeClass
        field_data_type = schema_field.type
        if field_data_type is None or field_data_type.type is None:
            logger.warning(
                f"Schema field {schema_field.fieldPath} has no type, skipping"
            )
            return None

        # Get the actual type class instance
        type_instance = field_data_type.type
        type_class = type(type_instance)

        # Look up the SchemaFieldDataType enum value
        field_type = SCHEMA_FIELD_CLASS_TO_TYPE_MAP.get(type_class)
        if field_type is None:
            logger.warning(
                f"Unknown schema field type {type_class.__name__} "
                f"for field {schema_field.fieldPath}, skipping"
            )
            return None

        return SchemaAssertionField(
            path=schema_field.fieldPath,
            type=field_type,
            native_type=getattr(schema_field, "nativeDataType", None),
        )

    @classmethod
    def _from_entities(cls, assertion: Assertion, monitor: Monitor) -> Self:
        """
        Create a SchemaAssertion from the assertion and monitor entities.

        Args:
            assertion: The assertion entity from the backend.
            monitor: The monitor entity from the backend.

        Returns:
            A SchemaAssertion instance populated from the entities.
        """
        return cls(
            urn=assertion.urn,
            dataset_urn=assertion.dataset,
            display_name=assertion.description or "",
            mode=cls._get_mode(monitor),
            schedule=cls._get_schedule(monitor),
            compatibility=cls._get_compatibility(assertion),
            fields=cls._get_fields(assertion),
            incident_behavior=cls._get_incident_behavior(assertion),
            created_by=cls._get_created_by(assertion),
            created_at=cls._get_created_at(assertion),
            updated_by=cls._get_updated_by(assertion),
            updated_at=cls._get_updated_at(assertion),
            tags=cls._get_tags(assertion),
        )
