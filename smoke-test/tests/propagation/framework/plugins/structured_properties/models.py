"""Data models for structured properties in the propagation framework."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class StructuredProperty:
    """Represents a structured property with its name/URN, value, and type."""

    name_or_urn: str  # Can be either a short name or full URN
    value: Any
    value_type: str = "string"

    def resolve_urn(self, available_properties: Dict[str, str]) -> str:
        """Resolve the property name to URN, or return as-is if already a URN."""
        if self.name_or_urn.startswith("urn:li:structuredProperty:"):
            return self.name_or_urn
        elif self.name_or_urn in available_properties:
            return available_properties[self.name_or_urn]
        else:
            # Assume it's a URN or create a default URN
            if not self.name_or_urn.startswith("urn:"):
                return f"urn:li:structuredProperty:{self.name_or_urn}"
            return self.name_or_urn

    def format_value(self) -> Any:
        """Format the value based on its type."""
        if self.value_type == "string":
            return str(self.value)
        elif self.value_type == "number":
            return (
                float(self.value)
                if isinstance(self.value, (int, float))
                else float(str(self.value))
            )
        elif self.value_type == "date":
            return str(self.value)  # Should be in ISO format
        elif self.value_type == "urn":
            return str(self.value)
        else:
            return str(self.value)


@dataclass
class DatasetStructuredProperties:
    """Container for all structured properties of a dataset."""

    dataset_name: str
    properties: List[StructuredProperty]

    def add_property(
        self, name_or_urn: str, value: Any, value_type: str = "string"
    ) -> None:
        """Add a structured property."""
        self.properties.append(StructuredProperty(name_or_urn, value, value_type))

    def get_properties_dict(
        self, available_properties: Optional[Dict[str, str]] = None
    ) -> Dict[str, StructuredProperty]:
        """Get properties as a dictionary keyed by resolved URN."""
        if available_properties is None:
            available_properties = {}
        return {
            prop.resolve_urn(available_properties): prop for prop in self.properties
        }

    def has_properties(self) -> bool:
        """Check if this dataset has any structured properties."""
        return len(self.properties) > 0
