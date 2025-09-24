"""Structured property template creation utilities for propagation testing."""

import datetime
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import DatasetUrn, EntityTypeUrn, StructuredPropertyUrn


def create_structured_property_template(
    qualified_name: str,
    display_name: str,
    value_type: str = "string",
    description: Optional[str] = None,
    allowed_values: Optional[List[str]] = None,
    cardinality: str = "SINGLE",
    entity_types: Optional[List[str]] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create a structured property template definition.

    Args:
        qualified_name: Unique identifier for the structured property
        display_name: Human-readable name
        value_type: Type of values (string, number, date, urn)
        description: Optional description
        allowed_values: Optional list of allowed values
        cardinality: SINGLE or MULTIPLE
        entity_types: List of entity types that can have this property

    Returns:
        MetadataChangeProposalWrapper for the structured property definition
    """
    if entity_types is None:
        entity_types = ["dataset"]  # Structured properties are dataset-level only

    # Create the structured property URN
    structured_prop_urn = StructuredPropertyUrn(qualified_name).urn()

    # Map simple type names to URN format
    type_mapping = {
        "string": "datahub.string",
        "number": "datahub.number",
        "date": "datahub.date",
        "urn": "datahub.urn",
    }

    value_type_urn = f"urn:li:dataType:{type_mapping.get(value_type, value_type)}"

    # Convert entity types to URNs
    entity_type_urns = []
    for entity_type in entity_types:
        if entity_type == "dataset":
            entity_type_urns.append(
                EntityTypeUrn(f"datahub.{DatasetUrn.ENTITY_TYPE}").urn()
            )
        # Note: Structured properties are typically dataset-level only
        else:
            # Allow custom entity types
            entity_type_urns.append(EntityTypeUrn(f"datahub.{entity_type}").urn())

    # Convert allowed values to PropertyValueClass objects if provided
    property_allowed_values = None
    if allowed_values:
        property_allowed_values = [
            PropertyValueClass(value=str(val)) for val in allowed_values
        ]

    # Create the structured property definition
    definition = StructuredPropertyDefinitionClass(
        qualifiedName=qualified_name,
        displayName=display_name,
        description=description,
        valueType=value_type_urn,
        allowedValues=property_allowed_values,
        cardinality=cardinality,
        entityTypes=entity_type_urns,
        lastModified=AuditStampClass(
            time=int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000),
            actor="urn:li:corpuser:test_user",
        ),
    )

    return MetadataChangeProposalWrapper(
        entityUrn=structured_prop_urn,
        aspect=definition,
        changeType=ChangeTypeClass.CREATE,
        headers={"If-None-Match": "*"},
    )


def create_common_structured_property_templates() -> List[
    MetadataChangeProposalWrapper
]:
    """
    Create a set of common structured property templates for testing.

    Returns:
        List of MetadataChangeProposalWrapper objects for common structured properties
    """
    templates = []

    # Data Classification
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.data_classification",
            display_name="Data Classification",
            value_type="string",
            description="Classification level of the data",
            allowed_values=["Public", "Internal", "Confidential", "Restricted"],
            cardinality="SINGLE",
            entity_types=["dataset"],
        )
    )

    # Business Owner
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.business_owner",
            display_name="Business Owner",
            value_type="string",
            description="Business owner of the data asset",
            cardinality="SINGLE",
            entity_types=["dataset"],
        )
    )

    # Data Retention Period
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.retention_period",
            display_name="Data Retention Period",
            value_type="number",
            description="Data retention period in days",
            cardinality="SINGLE",
            entity_types=["dataset"],
        )
    )

    # PII Fields
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.contains_pii",
            display_name="Contains PII",
            value_type="string",
            description="Whether the field contains personally identifiable information",
            allowed_values=["true", "false"],
            cardinality="SINGLE",
            entity_types=[
                "dataset"
            ],  # Note: Moving to dataset level since field-level structured properties don't exist
        )
    )

    # Business Critical
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.business_critical",
            display_name="Business Critical",
            value_type="string",
            description="Whether this asset is business critical",
            allowed_values=["true", "false"],
            cardinality="SINGLE",
            entity_types=["dataset"],
        )
    )

    # Data Source System
    templates.append(
        create_structured_property_template(
            qualified_name="io.datahub.test.source_system",
            display_name="Source System",
            value_type="string",
            description="The source system that provides this data",
            cardinality="SINGLE",
            entity_types=["dataset"],
        )
    )

    return templates


def create_structured_property_value(
    structured_property_urn: str, value: Any, value_type: str = "string"
) -> Dict[str, Any]:
    """
    Create a structured property value for use in dataset/field mutations.

    Args:
        structured_property_urn: URN of the structured property definition
        value: The actual value to set
        value_type: Type of the value (string, number, date, urn)

    Returns:
        Dictionary representing the structured property value
    """
    # Format the value based on type
    if value_type == "string":
        formatted_value = str(value)
    elif value_type == "number":
        formatted_value = str(
            float(value) if isinstance(value, (int, float)) else float(str(value))
        )
    elif value_type == "date":
        formatted_value = str(value)  # Should be in ISO format
    elif value_type == "urn":
        formatted_value = str(value)
    else:
        formatted_value = str(value)

    return {"propertyUrn": structured_property_urn, "value": formatted_value}
