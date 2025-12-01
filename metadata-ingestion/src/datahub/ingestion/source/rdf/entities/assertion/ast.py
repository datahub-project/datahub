"""
AST classes for Assertion entity.

Defines RDF and DataHub AST representations for assertions.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DataQualityRule:
    """Represents a data quality rule derived from SHACL constraints."""

    rule_name: str
    rule_type: str  # "length", "pattern", "range", "required", "datatype"
    field_name: str
    constraint_value: Any
    description: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrossFieldConstraint:
    """Represents a cross-field constraint between two fields."""

    constraint_name: str
    constraint_type: str  # "lessThan", "notEquals", "equals"
    field1_path: str
    field2_path: str
    description: str
    severity: str = "ERROR"
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RDFAssertion:
    """Represents a DataHub assertion derived from SHACL constraints."""

    assertion_key: str
    assertion_type: str  # "FIELD_METRIC", "FIELD_VALUES", "DATASET", "SCHEMA"
    dataset_urn: str
    field_name: Optional[str] = None
    description: Optional[str] = None
    operator: Optional[str] = None  # "EQUAL", "GREATER_THAN", "LESS_THAN", etc.
    parameters: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubAssertion:
    """Internal representation of a DataHub assertion."""

    assertion_key: str
    assertion_type: str  # "FIELD_METRIC", "FIELD_VALUES", "DATASET", "SCHEMA"
    dataset_urn: str
    field_name: Optional[str] = None
    description: Optional[str] = None
    operator: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataHubCrossFieldConstraint:
    """DataHub-specific cross-field constraint representation."""

    constraint_key: str
    constraint_type: str  # "lessThan", "notEquals", "equals"
    dataset_urn: str
    field1_path: str
    field2_path: str
    description: str
    severity: str = "ERROR"
    properties: Dict[str, Any] = field(default_factory=dict)
