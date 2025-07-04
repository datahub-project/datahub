import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

# Global cache for lineage data to avoid repeated file reads
_lineage_data: Optional[Dict] = None


def _load_lineage_data() -> Dict:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Load lineage data from the autogenerated lineage.json file.

    Returns:
        Dict containing the lineage information

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
    """
    global _lineage_data

    if _lineage_data is not None:
        return _lineage_data

    # Get the path to lineage.json relative to this file
    current_file = Path(__file__)
    lineage_file = current_file.parent / "lineage.json"

    if not lineage_file.exists():
        raise FileNotFoundError(f"Lineage file not found: {lineage_file}")

    try:
        with open(lineage_file, "r") as f:
            _lineage_data = json.load(f)
        return _lineage_data
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Failed to parse lineage.json: {e}", e.doc, e.pos
        ) from e


def get_lineage_fields(entity_type: str, aspect_name: str) -> List[Dict]:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Get lineage fields for a specific entity type and aspect.

    Args:
        entity_type: The entity type (e.g., 'dataset', 'dataJob')
        aspect_name: The aspect name (e.g., 'upstreamLineage', 'dataJobInputOutput')

    Returns:
        List of lineage field dictionaries, each containing:
        - name: field name
        - path: dot-notation path to the field
        - isLineage: boolean indicating if it's lineage
        - relationship: relationship information

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
    """
    lineage_data = _load_lineage_data()

    entity_data = lineage_data.get("entities", {}).get(entity_type, {})
    aspect_data = entity_data.get(aspect_name, {})

    return aspect_data.get("fields", [])


def is_lineage_field(urn: str, aspect_name: str, field_path: str) -> bool:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Check if a specific field path is lineage-related.

    Args:
        urn: The entity URN (e.g., 'urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)')
        aspect_name: The aspect name (e.g., 'upstreamLineage', 'dataJobInputOutput')
        field_path: The dot-notation path to the field (e.g., 'upstreams.dataset')

    Returns:
        True if the field is lineage-related, False otherwise

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
        AssertionError: If URN doesn't start with 'urn:li:'
    """
    entity_type = guess_entity_type(urn)
    lineage_fields = get_lineage_fields(entity_type, aspect_name)

    for field in lineage_fields:
        if field.get("path") == field_path:
            return field.get("isLineage", False)

    return False


def has_lineage(urn: str, aspect: Any) -> bool:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Check if an aspect has any lineage fields.

    Args:
        urn: The entity URN (e.g., 'urn:li:dataset:(urn:li:dataPlatform:mysql,test_db.test_table,PROD)')
        aspect: The aspect object

    Returns:
        True if the aspect has lineage fields, False otherwise

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
        AssertionError: If URN doesn't start with 'urn:li:'
    """
    entity_type = guess_entity_type(urn)
    aspect_class = getattr(aspect, "__class__", None)
    aspect_name = (
        aspect_class.__name__ if aspect_class is not None else str(type(aspect))
    )

    lineage_fields = get_lineage_fields(entity_type, aspect_name)
    return len(lineage_fields) > 0


def has_lineage_aspect(entity_type: str, aspect_name: str) -> bool:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Check if an aspect has any lineage fields.

    Args:
        entity_type: The entity type (e.g., 'dataset', 'dataJob')
        aspect_name: The aspect name (e.g., 'upstreamLineage', 'dataJobInputOutput')

    Returns:
        True if the aspect has lineage fields, False otherwise

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
    """
    lineage_fields = get_lineage_fields(entity_type, aspect_name)
    return len(lineage_fields) > 0


def get_all_lineage_aspects(entity_type: str) -> Set[str]:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Get all aspects that have lineage fields for a given entity type.

    Args:
        entity_type: The entity type (e.g., 'dataset', 'dataJob')

    Returns:
        Set of aspect names that have lineage fields

    Raises:
        FileNotFoundError: If lineage.json doesn't exist
        json.JSONDecodeError: If lineage.json is malformed
    """
    lineage_data = _load_lineage_data()

    entity_data = lineage_data.get("entities", {}).get(entity_type, {})
    lineage_aspects = set()

    for aspect_name, aspect_data in entity_data.items():
        if aspect_data.get("fields"):
            lineage_aspects.add(aspect_name)

    return lineage_aspects


def clear_cache() -> None:
    """
    This is experimental internal API subject to breaking changes without prior notice.

    Clear the internal cache of lineage data.

    This is useful for testing or when the lineage.json file has been updated.
    """
    global _lineage_data
    _lineage_data = None
