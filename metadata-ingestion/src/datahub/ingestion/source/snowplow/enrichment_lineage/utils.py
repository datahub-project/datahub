"""
Utility functions for enrichment lineage extraction.

This module provides common helper functions used by enrichment lineage extractors,
such as field name conversions and URN construction.
"""

import re
from typing import Optional


def camel_to_snake(name: str) -> str:
    """
    Convert camelCase or PascalCase to snake_case.

    Used for converting enrichment configuration field names (camelCase) to
    warehouse column names (snake_case).

    Examples:
        >>> camel_to_snake("mktMedium")
        'mkt_medium'
        >>> camel_to_snake("deviceClass")
        'device_class'
        >>> camel_to_snake("IPAddress")
        'ip_address'

    Args:
        name: The camelCase or PascalCase string to convert

    Returns:
        The snake_case version of the string
    """
    # Insert underscore before uppercase letters (except at the start)
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Insert underscore before uppercase letters preceded by lowercase
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def make_field_urn(dataset_urn: str, field_name: str) -> str:
    """
    Create a schemaField URN for a dataset field.

    Args:
        dataset_urn: The URN of the dataset (e.g., 'urn:li:dataset:(...)')
        field_name: The name of the field (e.g., 'user_ipaddress')

    Returns:
        The schemaField URN (e.g., 'urn:li:schemaField:(...,user_ipaddress)')
    """
    # Remove urn:li:dataset: prefix and wrap in schemaField format
    if dataset_urn.startswith("urn:li:dataset:"):
        dataset_part = dataset_urn[len("urn:li:dataset:") :]
        return f"urn:li:schemaField:({dataset_part},{field_name})"
    else:
        # Fallback: just append field name
        return f"{dataset_urn}.{field_name}"


def parse_json_config(parameters: Optional[str]) -> dict:
    """
    Safely parse enrichment parameters JSON string.

    Args:
        parameters: JSON string containing enrichment configuration

    Returns:
        Parsed configuration dictionary, or empty dict if parsing fails
    """
    import json

    if not parameters:
        return {}

    try:
        return json.loads(parameters)
    except (json.JSONDecodeError, TypeError):
        return {}
