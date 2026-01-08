import json
import logging
from typing import Any, Dict, List, Type, TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def parse_extra_properties_for_model(
    extra_properties: List[Dict[str, str]], model_class: Type[T]
) -> Dict[str, Any]:
    """
    Parse GraphQL extraProperties into a dictionary suitable for Pydantic model instantiation.

    GraphQL returns extra properties where values are JSON-encoded strings. This function:
    1. Filters to only include fields defined in the model
    2. Parses JSON-encoded values back to Python objects
    3. Handles parsing errors gracefully with logging

    Args:
        extra_properties: List of dicts with 'name' and 'value' keys from GraphQL response
        model_class: Pydantic model class to parse properties for

    Returns:
        Dictionary mapping field names to parsed values, ready for model instantiation

    Example:
        extra_props = [
            {"name": "urn", "value": '"urn:li:dataset:..."'},
            {"name": "owners", "value": '["urn:li:corpuser:user1"]'},
            {"name": "scrollId", "value": "eyJzb3J0Ijp..."}  # Not in model, will be skipped
        ]
        parsed = parse_extra_properties_for_model(extra_props, MyModel)
        instance = MyModel(**parsed)
    """
    # Only parse fields that are defined in the model.
    # Extra fields like scrollId are ignored.
    model_fields = model_class.model_fields.keys()
    extra_properties_map: Dict[str, Any] = {}

    for prop in extra_properties:
        name = prop["name"]
        # Skip fields that aren't in the model (e.g., scrollId)
        if name not in model_fields:
            continue

        # All model fields should be JSON-encoded, parse them
        value = prop["value"]
        try:
            extra_properties_map[name] = json.loads(value)
        except json.JSONDecodeError as e:
            # This indicates a data contract violation - the field should be JSON-encoded
            logger.warning(
                f"Failed to parse field '{name}' as JSON. "
                f"Value: '{value[:100]}...'. Error: {e}. "
                f"Skipping field and using default value."
            )
            # Skip this field - Pydantic will use the default value if available,
            # or fail validation if it's a required field

    return extra_properties_map
