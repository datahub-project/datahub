"""
Text generator for shaping entities into natural language for reranking.

Converts GraphQL entity responses into natural language descriptions suitable
for semantic reranking.
"""

import re
from typing import Any, Callable, Dict, List, TypeVar

from loguru import logger

from datahub_integrations.smart_search.entity_normalizer import (
    EntityNormalizer,
)

T = TypeVar("T")


class EntityTextGenerator:
    """Generates natural language descriptions optimized for semantic reranking."""

    def __init__(self):
        self.platform_names = {
            "urn:li:dataPlatform:snowflake": "Snowflake",
            "urn:li:dataPlatform:dbt": "dbt",
            "urn:li:dataPlatform:looker": "Looker",
            "urn:li:dataPlatform:databricks": "Databricks",
            "urn:li:dataPlatform:postgres": "PostgreSQL",
            "urn:li:dataPlatform:bigquery": "BigQuery",
            "urn:li:dataPlatform:redshift": "Redshift",
        }

    def _generate_for_schema_field(self, entity: Dict[str, Any], urn: str) -> str:
        """
        Generate description for a schemaField (column) entity.

        Args:
            entity: SchemaField entity dict
            urn: The URN string

        Returns:
            Natural language description
        """
        # Parse URN to extract field name and parent dataset
        # Format: urn:li:schemaField:(urn:li:dataset:(...),fieldPath)
        try:
            # Extract field path (last part after final comma)
            field_path = urn.split(",")[-1].rstrip(")")

            # Extract parent dataset URN (everything between schemaField:( and the last comma)
            dataset_part = urn.split("schemaField:(")[1]
            dataset_urn = dataset_part.rsplit(",", 1)[0]

            # Try to get dataset name from URN
            # Format: urn:li:dataset:(urn:li:dataPlatform:platform,database.schema.table,env)
            if "dataset:(" in dataset_urn:
                dataset_name = (
                    dataset_urn.split(",")[1] if "," in dataset_urn else "unknown"
                )
            else:
                dataset_name = "unknown dataset"

            # Get field description using normalizer
            desc = EntityNormalizer.get_description(entity)

            parts = [f'Column "{field_path}" in table {dataset_name}']

            if desc:
                parts.append(f"Description: {desc}")

            return ". ".join(parts)

        except Exception as e:
            # Fallback if URN parsing fails
            logger.error(f"Failed to parse schemaField URN: {urn}, error: {e}")
            desc = EntityNormalizer.get_description(entity)
            if desc:
                return f"Schema field. {desc[:200]}"
            return f"Schema field: {urn}" if urn else "Schema field"

    def generate(self, entity: Dict[str, Any], search_keywords: List[str]) -> str:
        """
        Generate natural language description for an entity.

        Args:
            entity: Entity dict from GraphQL search results
            search_keywords: Keywords from the search query to prioritize matching fields

        Returns:
            Natural language description suitable for reranking
        """
        # Check entity type from URN
        urn = entity.get("urn", "")

        # Handle schemaField (column) entities specially
        if "schemaField:" in urn:
            return self._generate_for_schema_field(entity, urn)

        parts = []

        # Get name using normalizer that checks all possible locations
        name = EntityNormalizer.get_name(entity)

        # Get description using normalizer that checks all possible locations
        description = EntityNormalizer.get_description(entity)

        # Start with description if available
        if description:
            clean_desc = description.replace("\n", ". ").replace("..", ".")
            parts.append(clean_desc)

        # Add name context
        if name:
            if not description:
                parts.append(f'This is the "{name}" dataset')
            elif name.lower() not in description.lower():
                parts.append(f'The "{name}" table')

        # Add qualified name/location
        qualified_name = EntityNormalizer.get_qualified_name(entity)
        if qualified_name and qualified_name != name:
            parts.append(f"Located at {qualified_name}")

        # Add platform
        platform = entity.get("platform") or {}
        if platform:
            platform_urn = platform.get("urn", "")
            platform_name = self.platform_names.get(
                platform_urn, platform.get("name", "")
            )
            if platform_name:
                parts.append(f"Stored in {platform_name}")

        # Add schema fields with descriptions
        schema_metadata = entity.get("schemaMetadata") or {}
        fields = schema_metadata.get("fields", [])

        if fields:
            # Sort fields by keyword match count, take up to 50
            selected_fields = _sort_by_keyword_matches(
                items=fields,
                keywords=search_keywords,
                text_extractor=lambda f: [
                    f.get("fieldPath", ""),
                    f.get("description", ""),
                ],
                max_items=50,
            )

            field_details = []
            for field in selected_fields:
                field_path = field.get("fieldPath", "")
                field_desc = field.get("description") or ""

                if field_desc:
                    # Truncate field description to 120 chars
                    truncated_desc = (
                        field_desc[:120] if len(field_desc) > 120 else field_desc
                    )
                    field_details.append(f'"{field_path}": {truncated_desc}')
                else:
                    field_details.append(f'"{field_path}"')

            if field_details:
                if len(fields) <= 50:
                    parts.append(f"Fields: {'; '.join(field_details)}")
                else:
                    parts.append(
                        f"Contains {len(fields)} fields including: {'; '.join(field_details)}"
                    )

        # Add tags
        tags_container = entity.get("tags") or {}
        tags_data = tags_container.get("tags", [])
        if tags_data:
            tag_names = []
            for tag_item in tags_data[:5]:
                tag = tag_item.get("tag") or {}
                tag_props = tag.get("properties") or {}
                if tag_name := tag_props.get("name"):
                    tag_names.append(tag_name)

            if tag_names:
                parts.append(f"Tagged as: {', '.join(tag_names)}")

        # Add glossary terms
        glossary_container = entity.get("glossaryTerms") or {}
        glossary_data = glossary_container.get("terms", [])
        if glossary_data:
            term_names = []
            for term_item in glossary_data[:5]:
                term = term_item.get("term") or {}
                term_props = term.get("properties") or {}
                if term_name := term_props.get("name"):
                    term_names.append(term_name)

            if term_names:
                parts.append(f"Glossary terms: {', '.join(term_names)}")

        # Add ownership
        ownership_container = entity.get("ownership") or {}
        ownership_data = ownership_container.get("owners", [])
        if ownership_data:
            owner_names = []
            for owner_item in ownership_data[:3]:
                owner = owner_item.get("owner") or {}
                owner_props = owner.get("properties") or {}
                # Try displayName first, fallback to email
                display_name = owner_props.get("displayName") or owner_props.get(
                    "email"
                )
                if display_name:
                    owner_names.append(display_name)

            if owner_names:
                parts.append(f"Owned by: {', '.join(owner_names)}")

        # Add usage stats if available
        stats = entity.get("statsSummary", {})
        if query_count := stats.get("queryCountLast30Days"):
            parts.append(f"Queried {query_count} times in last 30 days")

        if row_count := stats.get("rowCount"):
            parts.append(f"Contains {row_count:,} rows")

        # Add custom properties, prioritizing keyword-matching ones
        properties = entity.get("properties") or {}
        custom_props = properties.get("customProperties", [])
        if custom_props:
            # Sort by keyword matches, take up to 10
            selected_props = _sort_by_keyword_matches(
                items=custom_props,
                keywords=search_keywords,
                text_extractor=lambda p: [p.get("key", ""), p.get("value", "")],
                max_items=10,
            )

            # Include selected custom properties
            for prop in selected_props:
                key = prop.get("key", "")
                value = prop.get("value", "")
                if key and value:
                    parts.append(f"{key.replace('_', ' ').title()}: {value}")

        # Join with proper punctuation
        result = ". ".join(parts)
        result = re.sub(r"\.\s*\.", ".", result)  # Remove double periods
        result = re.sub(r"\s+", " ", result)  # Normalize whitespace

        # Fallback if no parts generated (empty entity)
        if not result.strip():
            if urn:
                return f"Entity: {urn}"
            return "Unknown entity"

        return result.strip()


# Private helper functions


def _sort_by_keyword_matches(
    items: List[T],
    keywords: List[str],
    text_extractor: Callable[[T], List[str]],
    max_items: int | None = None,
) -> List[T]:
    """
    Sort items by number of keyword matches (descending).

    Generic function that works with any item type via lambda text extraction.

    Args:
        items: List of items to sort (fields, properties, etc.)
        keywords: Keywords to match against
        text_extractor: Lambda that returns list of searchable strings from an item
        max_items: Optional cap on number of items to return

    Returns:
        Sorted list with items matching most keywords first
    """
    keywords_lower = [kw.lower() for kw in keywords]

    # Score each item by keyword match count
    scored = []
    for item in items:
        text_parts = text_extractor(item)
        # Count matches in each part separately, then sum
        # Handle None values gracefully
        match_count = sum(
            1
            for kw in keywords_lower
            for text_part in text_parts
            if text_part and kw in text_part.lower()
        )
        scored.append((match_count, item))

    # Sort by match count (descending)
    scored.sort(key=lambda x: x[0], reverse=True)

    # Extract items
    sorted_items = [item for _, item in scored]

    # Apply cap if specified
    if max_items is not None:
        return sorted_items[:max_items]

    return sorted_items
