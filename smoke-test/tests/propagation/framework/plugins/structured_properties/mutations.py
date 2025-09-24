"""Structured property mutation classes for dataset and field-level structured properties."""

import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from tests.propagation.framework.core.mutations import BaseMutation


@dataclass(frozen=True)
class DatasetStructuredPropertyAdditionMutation(BaseMutation):
    """Typed mutation for adding a structured property to a dataset."""

    structured_property_urn: str
    value: Any
    value_type: str = "string"

    def get_mutation_type(self) -> str:
        return "dataset_structured_property_addition"

    def explain(self) -> str:
        return f"Adding structured property {self.structured_property_urn} = {self.value} to dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset structured property addition mutation."""
        # Format the value based on type
        formatted_value = self._format_value(self.value, self.value_type)

        structured_property_assignment = StructuredPropertyValueAssignmentClass(
            propertyUrn=self.structured_property_urn,
            values=[formatted_value],
            created=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:test_user",
            ),
            lastModified=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:test_user",
            ),
        )

        structured_properties = StructuredPropertiesClass(
            properties=[structured_property_assignment]
        )

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=structured_properties,
            )
        ]

    def _format_value(self, value: Any, value_type: str) -> Any:
        """Format the value based on its type."""
        if value_type == "string":
            return str(value)
        elif value_type == "number":
            return (
                float(value) if isinstance(value, (int, float)) else float(str(value))
            )
        elif value_type == "date":
            return str(value)  # Should be in ISO format
        elif value_type == "urn":
            return str(value)
        else:
            return str(value)


@dataclass(frozen=True)
class DatasetStructuredPropertyUpdateMutation(BaseMutation):
    """Typed mutation for updating a structured property on a dataset."""

    structured_property_urn: str
    new_value: Any
    old_value: Optional[Any] = None
    value_type: str = "string"

    def get_mutation_type(self) -> str:
        return "dataset_structured_property_update"

    def explain(self) -> str:
        if self.old_value:
            return f"Updating structured property {self.structured_property_urn} on dataset {self.dataset_name} from {self.old_value} to {self.new_value}"
        return f"Setting structured property {self.structured_property_urn} = {self.new_value} on dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset structured property update mutation."""
        # Format the value based on type
        formatted_value = self._format_value(self.new_value, self.value_type)

        structured_property_assignment = StructuredPropertyValueAssignmentClass(
            propertyUrn=self.structured_property_urn,
            values=[formatted_value],
            created=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:test_user",
            ),
            lastModified=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:test_user",
            ),
        )

        structured_properties = StructuredPropertiesClass(
            properties=[structured_property_assignment]
        )

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=structured_properties,
            )
        ]

    def _format_value(self, value: Any, value_type: str) -> Any:
        """Format the value based on its type."""
        if value_type == "string":
            return str(value)
        elif value_type == "number":
            return (
                float(value) if isinstance(value, (int, float)) else float(str(value))
            )
        elif value_type == "date":
            return str(value)  # Should be in ISO format
        elif value_type == "urn":
            return str(value)
        else:
            return str(value)


@dataclass(frozen=True)
class DatasetStructuredPropertyRemovalMutation(BaseMutation):
    """Typed mutation for removing a structured property from a dataset."""

    structured_property_urn: str

    def get_mutation_type(self) -> str:
        return "dataset_structured_property_removal"

    def explain(self) -> str:
        return f"Removing structured property {self.structured_property_urn} from dataset {self.dataset_name}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply dataset structured property removal mutation."""
        # Create empty structured properties to remove the property
        structured_properties = StructuredPropertiesClass(properties=[])

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=structured_properties,
            )
        ]


@dataclass(frozen=True)
class MultipleStructuredPropertiesAdditionMutation(BaseMutation):
    """Typed mutation for adding multiple structured properties to a dataset at once."""

    structured_properties: List[
        Dict[str, Any]
    ]  # List of {urn, value, value_type} dicts

    def get_mutation_type(self) -> str:
        return "multiple_structured_properties_addition"

    def explain(self) -> str:
        props = [
            f"{prop['urn']} = {prop['value']}" for prop in self.structured_properties
        ]
        return f"Adding multiple structured properties to dataset {self.dataset_name}: {', '.join(props)}"

    def apply_mutation(self) -> List[MetadataChangeProposalWrapper]:
        """Apply multiple structured properties addition mutation."""
        property_assignments = []

        for prop_data in self.structured_properties:
            urn = prop_data["urn"]
            value = prop_data["value"]
            value_type = prop_data.get("value_type", "string")

            # Format the value based on type
            formatted_value = self._format_value(value, value_type)

            assignment = StructuredPropertyValueAssignmentClass(
                propertyUrn=urn,
                values=[formatted_value],
                created=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
                lastModified=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
            )
            property_assignments.append(assignment)

        structured_properties = StructuredPropertiesClass(
            properties=property_assignments
        )

        return [
            MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn,
                aspect=structured_properties,
            )
        ]

    def _format_value(self, value: Any, value_type: str) -> Any:
        """Format the value based on its type."""
        if value_type == "string":
            return str(value)
        elif value_type == "number":
            return (
                float(value) if isinstance(value, (int, float)) else float(str(value))
            )
        elif value_type == "date":
            return str(value)  # Should be in ISO format
        elif value_type == "urn":
            return str(value)
        else:
            return str(value)
