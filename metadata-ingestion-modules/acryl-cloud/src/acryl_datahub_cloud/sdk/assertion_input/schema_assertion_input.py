"""
Schema assertion input module.

This module provides the input types and classes for creating schema assertions
that validate dataset schemas match expected field definitions.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Union

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehaviorInputTypes,
    FieldSpecType,
    _AssertionInput,
    _SchemaMetadata,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    AssertionInfoInputType,
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError, SDKUsageErrorWithExamples
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


class SchemaFieldDataType(str, Enum):
    """The type of a schema field for schema assertions."""

    BYTES = "BYTES"
    FIXED = "FIXED"
    BOOLEAN = "BOOLEAN"
    STRING = "STRING"
    NUMBER = "NUMBER"
    DATE = "DATE"
    TIME = "TIME"
    ENUM = "ENUM"
    NULL = "NULL"
    ARRAY = "ARRAY"
    MAP = "MAP"
    STRUCT = "STRUCT"  # Maps to RecordType in the backend
    UNION = "UNION"


class SchemaAssertionCompatibility(str, Enum):
    """The compatibility level required for a schema assertion to pass."""

    EXACT_MATCH = "EXACT_MATCH"
    SUPERSET = "SUPERSET"
    SUBSET = "SUBSET"


DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY = SchemaAssertionCompatibility.EXACT_MATCH

# Default detection mechanism for schema assertions
# Schema assertions validate against DataHub's schema metadata (DATAHUB_SCHEMA source)
DEFAULT_SCHEMA_DETECTION_MECHANISM = _SchemaMetadata()


SCHEMA_FIELD_DATA_TYPE_EXAMPLES = {
    "String from enum": "SchemaFieldDataType.STRING",
    "String from string": "STRING",
    "Number from enum": "SchemaFieldDataType.NUMBER",
    "Number from string": "NUMBER",
    "Boolean from enum": "SchemaFieldDataType.BOOLEAN",
    "Struct from enum": "SchemaFieldDataType.STRUCT",
}


def _parse_schema_field_data_type(
    field_type: Union[str, SchemaFieldDataType],
) -> SchemaFieldDataType:
    """Parse a schema field data type from string or enum."""
    if isinstance(field_type, SchemaFieldDataType):
        return field_type

    if isinstance(field_type, str):
        try:
            return SchemaFieldDataType(field_type.upper())
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid schema field data type: {field_type}. "
                f"Valid options are: {[t.value for t in SchemaFieldDataType]}",
                examples=SCHEMA_FIELD_DATA_TYPE_EXAMPLES,
            ) from e

    raise SDKUsageErrorWithExamples(
        msg=f"Invalid schema field data type: {field_type}",
        examples=SCHEMA_FIELD_DATA_TYPE_EXAMPLES,
    )


SCHEMA_ASSERTION_COMPATIBILITY_EXAMPLES = {
    "Exact match from enum": "SchemaAssertionCompatibility.EXACT_MATCH",
    "Exact match from string": "EXACT_MATCH",
    "Superset from enum": "SchemaAssertionCompatibility.SUPERSET",
    "Subset from enum": "SchemaAssertionCompatibility.SUBSET",
}


def _parse_schema_assertion_compatibility(
    compatibility: Optional[Union[str, SchemaAssertionCompatibility]],
) -> SchemaAssertionCompatibility:
    """Parse a schema assertion compatibility from string or enum."""
    if compatibility is None:
        return DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY

    if isinstance(compatibility, SchemaAssertionCompatibility):
        return compatibility

    if isinstance(compatibility, str):
        try:
            return SchemaAssertionCompatibility(compatibility.upper())
        except ValueError as e:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid schema assertion compatibility: {compatibility}. "
                f"Valid options are: {[c.value for c in SchemaAssertionCompatibility]}",
                examples=SCHEMA_ASSERTION_COMPATIBILITY_EXAMPLES,
            ) from e

    raise SDKUsageErrorWithExamples(
        msg=f"Invalid schema assertion compatibility: {compatibility}",
        examples=SCHEMA_ASSERTION_COMPATIBILITY_EXAMPLES,
    )


@dataclass(frozen=True)
class SchemaAssertionField:
    """
    A field definition for schema assertions.

    Args:
        path: The field path within the schema (e.g., "id", "struct.nestedField").
        type: The expected data type of the field.
        native_type: Optional platform-specific native type (e.g., "VARCHAR(255)").
    """

    path: str
    type: SchemaFieldDataType
    native_type: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "SchemaAssertionField":
        """Create a SchemaAssertionField from a dictionary."""
        if "path" not in data:
            raise SDKUsageError("SchemaAssertionField requires 'path' field")
        if "type" not in data:
            raise SDKUsageError("SchemaAssertionField requires 'type' field")

        return cls(
            path=data["path"],
            type=_parse_schema_field_data_type(data["type"]),
            native_type=data.get("native_type") or data.get("nativeType"),
        )


SchemaAssertionFieldInputType = Union[SchemaAssertionField, dict]
SchemaAssertionFieldsInputType = Sequence[SchemaAssertionFieldInputType]


SCHEMA_ASSERTION_FIELD_EXAMPLES: dict[str, Union[dict[str, str], str]] = {
    "Field from dict": {"path": "id", "type": "STRING"},
    "Field from dict with native type": {
        "path": "count",
        "type": "NUMBER",
        "native_type": "BIGINT",
    },
    "Field from SchemaAssertionField": "SchemaAssertionField(path='id', type=SchemaFieldDataType.STRING)",
    "List of fields": '[{"path": "id", "type": "STRING"}, {"path": "count", "type": "NUMBER"}]',
}


def _parse_schema_assertion_fields(
    fields: Optional[SchemaAssertionFieldsInputType],
) -> Optional[list[SchemaAssertionField]]:
    """Parse a list of schema assertion fields from various input types."""
    if fields is None:
        return None

    if not isinstance(fields, list):
        raise SDKUsageErrorWithExamples(
            msg=f"Schema assertion fields must be a list, got {type(fields).__name__}",
            examples=SCHEMA_ASSERTION_FIELD_EXAMPLES,
        )

    if len(fields) == 0:
        raise SDKUsageError("Schema assertion fields cannot be empty")

    parsed_fields = []
    for field in fields:
        if isinstance(field, SchemaAssertionField):
            parsed_fields.append(field)
        elif isinstance(field, dict):
            parsed_fields.append(SchemaAssertionField.from_dict(field))
        else:
            raise SDKUsageErrorWithExamples(
                msg=f"Invalid schema assertion field: {field}",
                examples=SCHEMA_ASSERTION_FIELD_EXAMPLES,
            )

    return parsed_fields


# Mapping from SchemaFieldDataType to schema_classes type
SCHEMA_FIELD_TYPE_TO_CLASS_MAP: dict[SchemaFieldDataType, type] = {
    SchemaFieldDataType.BYTES: models.BytesTypeClass,
    SchemaFieldDataType.FIXED: models.FixedTypeClass,
    SchemaFieldDataType.BOOLEAN: models.BooleanTypeClass,
    SchemaFieldDataType.STRING: models.StringTypeClass,
    SchemaFieldDataType.NUMBER: models.NumberTypeClass,
    SchemaFieldDataType.DATE: models.DateTypeClass,
    SchemaFieldDataType.TIME: models.TimeTypeClass,
    SchemaFieldDataType.ENUM: models.EnumTypeClass,
    SchemaFieldDataType.NULL: models.NullTypeClass,
    SchemaFieldDataType.ARRAY: models.ArrayTypeClass,
    SchemaFieldDataType.MAP: models.MapTypeClass,
    SchemaFieldDataType.STRUCT: models.RecordTypeClass,  # STRUCT maps to RecordType
    SchemaFieldDataType.UNION: models.UnionTypeClass,
}


# Reverse mapping for parsing from backend
SCHEMA_FIELD_CLASS_TO_TYPE_MAP: dict[type, SchemaFieldDataType] = {
    models.BytesTypeClass: SchemaFieldDataType.BYTES,
    models.FixedTypeClass: SchemaFieldDataType.FIXED,
    models.BooleanTypeClass: SchemaFieldDataType.BOOLEAN,
    models.StringTypeClass: SchemaFieldDataType.STRING,
    models.NumberTypeClass: SchemaFieldDataType.NUMBER,
    models.DateTypeClass: SchemaFieldDataType.DATE,
    models.TimeTypeClass: SchemaFieldDataType.TIME,
    models.EnumTypeClass: SchemaFieldDataType.ENUM,
    models.NullTypeClass: SchemaFieldDataType.NULL,
    models.ArrayTypeClass: SchemaFieldDataType.ARRAY,
    models.MapTypeClass: SchemaFieldDataType.MAP,
    models.RecordTypeClass: SchemaFieldDataType.STRUCT,
    models.UnionTypeClass: SchemaFieldDataType.UNION,
}


class _SchemaAssertionInput(_AssertionInput):
    """Input class for creating schema assertions."""

    def _assertion_type(self) -> str:
        """Get the assertion type."""
        return models.AssertionTypeClass.DATA_SCHEMA

    def __init__(
        self,
        *,
        dataset_urn: Union[str, DatasetUrn],
        entity_client: EntityClient,
        compatibility: Optional[Union[str, SchemaAssertionCompatibility]] = None,
        fields: Optional[SchemaAssertionFieldsInputType] = None,
        urn: Optional[Union[str, AssertionUrn]] = None,
        display_name: Optional[str] = None,
        enabled: bool = True,
        schedule: Optional[Union[str, models.CronScheduleClass]] = None,
        incident_behavior: Optional[AssertionIncidentBehaviorInputTypes] = None,
        tags: Optional[TagsInputType] = None,
        created_by: Union[str, CorpUserUrn],
        created_at: datetime,
        updated_by: Union[str, CorpUserUrn],
        updated_at: datetime,
    ):
        """
        Create a SchemaAssertionInput object.

        Args:
            dataset_urn: The URN of the dataset to validate.
            entity_client: The entity client for API operations.
            compatibility: The compatibility mode for schema validation
                (EXACT_MATCH, SUPERSET, SUBSET). Defaults to EXACT_MATCH.
            fields: The expected schema fields to validate.
            urn: Optional assertion URN for updates.
            display_name: Display name for the assertion.
            enabled: Whether the assertion is enabled.
            schedule: Cron schedule for evaluation.
            incident_behavior: Incident behavior on pass/fail.
            tags: Tags to apply to the assertion.
            created_by: User who created the assertion.
            created_at: Creation timestamp.
            updated_by: User who last updated the assertion.
            updated_at: Last update timestamp.
        """
        # Schema assertions validate against DataHub's schema metadata
        # They always use DATAHUB_SCHEMA as the source type
        super().__init__(
            dataset_urn=dataset_urn,
            entity_client=entity_client,
            urn=urn,
            display_name=display_name,
            enabled=enabled,
            schedule=schedule,
            detection_mechanism=None,
            default_detection_mechanism=DEFAULT_SCHEMA_DETECTION_MECHANISM,
            incident_behavior=incident_behavior,
            tags=tags,
            source_type=models.AssertionSourceTypeClass.NATIVE,
            created_by=created_by,
            created_at=created_at,
            updated_by=updated_by,
            updated_at=updated_at,
        )

        self.compatibility = _parse_schema_assertion_compatibility(compatibility)
        self.fields = _parse_schema_assertion_fields(fields)

    def _convert_schedule(self) -> models.CronScheduleClass:
        """Convert schedule to CronScheduleClass, using default if not provided."""
        if self.schedule is None:
            return DEFAULT_EVERY_SIX_HOURS_SCHEDULE
        return models.CronScheduleClass(
            cron=self.schedule.cron,
            timezone=self.schedule.timezone,
        )

    def _create_assertion_info(
        self, filter: Optional[models.DatasetFilterClass]
    ) -> AssertionInfoInputType:
        """Create a SchemaAssertionInfoClass for the schema assertion."""
        if self.fields is None:
            raise SDKUsageError(
                "Schema assertion requires 'fields' to be specified. "
                "Please provide a list of expected schema fields."
            )

        return models.SchemaAssertionInfoClass(
            entity=str(self.dataset_urn),
            compatibility=getattr(
                models.SchemaAssertionCompatibilityClass,
                self.compatibility.value,
                models.SchemaAssertionCompatibilityClass.EXACT_MATCH,
            ),
            schema=self._create_schema_metadata(),
        )

    def _create_schema_metadata(self) -> models.SchemaMetadataClass:
        """Convert fields to SchemaMetadata for the assertion."""
        if self.fields is None:
            raise SDKUsageError("Schema assertion requires fields to be specified.")

        schema_fields = [self._create_schema_field(field) for field in self.fields]

        # Create SchemaMetadata with required fields
        # These are placeholder values since the actual schema metadata
        # is only used for the assertion definition, not for real schema tracking
        return models.SchemaMetadataClass(
            schemaName="assertion-schema-name",
            platform=str(self.dataset_urn.platform),
            version=0,
            hash="assertion-schema-hash",
            platformSchema=models.OtherSchemaClass(rawSchema=""),
            fields=schema_fields,
        )

    def _create_schema_field(
        self, field: SchemaAssertionField
    ) -> models.SchemaFieldClass:
        """Convert a SchemaAssertionField to a SchemaFieldClass."""
        type_class = SCHEMA_FIELD_TYPE_TO_CLASS_MAP.get(field.type)
        if type_class is None:
            raise SDKUsageError(f"Unknown schema field type: {field.type}")

        return models.SchemaFieldClass(
            fieldPath=field.path,
            type=models.SchemaFieldDataTypeClass(type=type_class()),
            nativeDataType=field.native_type or "",
            nullable=False,
        )

    def _create_monitor_info(
        self,
        assertion_urn: AssertionUrn,
        status: models.MonitorStatusClass,
        schedule: models.CronScheduleClass,
    ) -> models.MonitorInfoClass:
        """Create a MonitorInfoClass for the schema assertion."""
        return models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=status,
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(assertion_urn),
                        schedule=schedule,
                        parameters=self._get_assertion_evaluation_parameters(
                            models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA, None
                        ),
                    )
                ]
            ),
        )

    def _get_assertion_evaluation_parameters(
        self, source_type: str, field: Optional[FieldSpecType]
    ) -> models.AssertionEvaluationParametersClass:
        """Get evaluation parameters for schema assertion."""
        return models.AssertionEvaluationParametersClass(
            type=models.AssertionEvaluationParametersTypeClass.DATASET_SCHEMA,
            datasetSchemaParameters=models.DatasetSchemaAssertionParametersClass(
                sourceType=source_type,
            ),
        )

    def _convert_assertion_source_type_and_field(
        self,
    ) -> tuple[str, Optional[FieldSpecType]]:
        """Schema assertions always use DATAHUB_SCHEMA source type."""
        return models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA, None
