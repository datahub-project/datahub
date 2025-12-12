"""Utilities for redacting sensitive information from ingestion recipes."""

from typing import Any

import yaml
from datahub.configuration.common import redact_raw_config
from loguru import logger


def redact_recipe(recipe: str | None) -> str | None:
    """
    Redact sensitive fields in an ingestion recipe.

    Takes a recipe string (YAML or JSON format) and redacts sensitive fields
    like passwords, tokens, secrets, and API keys using DataHub's standard
    redaction logic.

    Args:
        recipe: Recipe string in YAML or JSON format, or None

    Returns:
        Redacted recipe string in the same format as input, or None if input is None

    Examples:
        >>> recipe = '''
        ... source:
        ...   type: snowflake
        ...   config:
        ...     account_id: abc123
        ...     password: my-secret-password
        ... '''
        >>> redacted = redact_recipe(recipe)
        >>> 'my-secret-password' in redacted
        False
        >>> '********' in redacted
        True
    """
    if recipe is None:
        return None

    if not recipe or not recipe.strip():
        return recipe

    try:
        # Try parsing as YAML first (supports both YAML and JSON)
        parsed = yaml.safe_load(recipe)

        if parsed is None:
            return recipe

        # Apply redaction
        redacted = redact_raw_config(parsed)

        # Convert back to YAML to preserve original format
        return yaml.dump(redacted, default_flow_style=False, sort_keys=False)

    except yaml.YAMLError as e:
        logger.warning(f"Failed to parse recipe as YAML for redaction: {e}")
        # If parsing fails, try to be safe and return a placeholder
        # rather than potentially exposing secrets
        return "[Unable to parse recipe for redaction]"
    except Exception as e:
        logger.error(f"Unexpected error during recipe redaction: {e}")
        return "[Error during recipe redaction]"


def redact_recipe_in_dict(data: dict[str, Any], recipe_path: list[str]) -> None:
    """
    Redact recipe field in a nested dictionary structure in-place.

    This is useful for redacting recipes in GraphQL response dictionaries
    without having to manually traverse the nested structure.

    Args:
        data: Dictionary containing recipe data
        recipe_path: List of keys forming the path to the recipe field
                     e.g., ['config', 'recipe'] or ['source', 'config', 'recipe']

    Examples:
        >>> data = {'config': {'recipe': 'source:\\n  config:\\n    password: secret'}}
        >>> redact_recipe_in_dict(data, ['config', 'recipe'])
        >>> 'secret' in str(data)
        False
    """
    if not recipe_path:
        return

    # Navigate to the parent of the recipe field
    current = data
    for key in recipe_path[:-1]:
        if not isinstance(current, dict) or key not in current:
            return
        current = current[key]

    # Redact the recipe field
    recipe_key = recipe_path[-1]
    if isinstance(current, dict) and recipe_key in current:
        current[recipe_key] = redact_recipe(current[recipe_key])
