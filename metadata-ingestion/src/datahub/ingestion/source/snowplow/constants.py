"""
Constants and enums for Snowplow connector.

This module centralizes all magic strings and constant values used throughout
the Snowplow connector to improve maintainability and reduce errors.
"""

from enum import Enum


class SchemaType(str, Enum):
    """Snowplow schema types."""

    EVENT = "event"
    ENTITY = "entity"
    CONTEXT = "context"  # Alias for entity (used in schema names)


class ConnectionMode(str, Enum):
    """Snowplow connection modes."""

    BDP = "bdp"  # BDP Console API only
    IGLU = "iglu"  # Iglu Registry only
    BOTH = "both"  # Both BDP and Iglu


class DataClassification(str, Enum):
    """Data classification levels for field tagging."""

    PII = "pii"  # Personally Identifiable Information
    SENSITIVE = "sensitive"  # Sensitive business data
    PUBLIC = "public"  # Public information
    INTERNAL = "internal"  # Internal information


class EventFieldType(str, Enum):
    """Event field types for structured properties."""

    SELF_DESCRIBING = "self_describing"  # Custom self-describing event
    ATOMIC = "atomic"  # Snowplow atomic event field
    CONTEXT = "context"  # Context entity field


class SchemaFormat(str, Enum):
    """Schema format types."""

    JSON_SCHEMA = "jsonschema"


class Cardinality(str, Enum):
    """Structured property cardinality."""

    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class Environment(str, Enum):
    """Deployment environments."""

    PROD = "PROD"
    DEV = "DEV"
    TEST = "TEST"


# Subtype patterns for DataHub
class DatasetSubtype:
    """Dataset subtypes for Snowplow schemas."""

    EVENT_SCHEMA = "snowplow_event_schema"
    ENTITY_SCHEMA = "snowplow_entity_schema"
    EVENT_SPEC = "snowplow_event_spec"
    TRACKING_SCENARIO = "snowplow_tracking_scenario"
    DATA_PRODUCT = "snowplow_data_product"
    PIPELINE = "snowplow_pipeline"
    ENRICHMENT = "snowplow_enrichment"


# Platform constants
PLATFORM_NAME = "snowplow"

# Warehouse platform mappings
WAREHOUSE_PLATFORM_MAP = {
    "snowflake": "snowflake",
    "snowflake_db": "snowflake",
    "bigquery": "bigquery",
    "bigquery_enterprise": "bigquery",
    "redshift": "redshift",
    "red_shift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
}

# Schema-related constants
IGLU_URI_PREFIX = "iglu:"
IGLU_CENTRAL_URL = "http://iglucentral.com"


# Structured property IDs
class StructuredPropertyId:
    """Structured property identifiers."""

    FIELD_AUTHOR = "io.acryl.snowplow.fieldAuthor"
    FIELD_VERSION_ADDED = "io.acryl.snowplow.fieldVersionAdded"
    FIELD_ADDED_TIMESTAMP = "io.acryl.snowplow.fieldAddedTimestamp"
    FIELD_DATA_CLASS = "io.acryl.snowplow.fieldDataClass"
    FIELD_EVENT_TYPE = "io.acryl.snowplow.fieldEventType"


# Default values
DEFAULT_SCHEMA_FORMAT = SchemaFormat.JSON_SCHEMA.value
DEFAULT_SCHEMA_TYPES = [SchemaType.EVENT.value, SchemaType.ENTITY.value]


# Helper functions
def infer_schema_type(name: str) -> str:
    """
    Infer schema type from schema name.

    Snowplow convention: schemas with 'context' in the name are entity schemas,
    all others are event schemas.

    Args:
        name: Schema name (e.g., 'link_click', 'user_context')

    Returns:
        Schema type: 'event' or 'entity'
    """
    return (
        SchemaType.ENTITY.value
        if SchemaType.CONTEXT.value in name.lower()
        else SchemaType.EVENT.value
    )
