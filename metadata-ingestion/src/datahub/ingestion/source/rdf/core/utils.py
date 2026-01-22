"""
Utility functions for RDF ingestion.
"""


def entity_type_to_field_name(entity_type: str) -> str:
    """
    Convert entity_type to field name for graph classes.

    Examples:
        'glossary_term' -> 'glossary_terms'
        'relationship' -> 'relationships'

    Args:
        entity_type: The entity type name

    Returns:
        Field name (typically plural form)
    """
    # Default: pluralize (add 's' if not already plural)
    if entity_type.endswith("s"):
        return entity_type
    elif entity_type.endswith("y"):
        return entity_type[:-1] + "ies"
    else:
        return f"{entity_type}s"
