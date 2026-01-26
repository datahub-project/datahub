"""
Constants and enums for Snowplow connector.

This module centralizes all magic strings and constant values used throughout
the Snowplow connector to improve maintainability and reduce errors.
"""

from datahub.utilities.str_enum import StrEnum


class SchemaType(StrEnum):
    """Snowplow schema types."""

    EVENT = "event"
    ENTITY = "entity"
    CONTEXT = "context"  # Alias for entity (used in schema names)


class ConnectionMode(StrEnum):
    """Snowplow connection modes."""

    BDP = "bdp"  # BDP Console API only
    IGLU = "iglu"  # Iglu Registry only
    BOTH = "both"  # Both BDP and Iglu


class DataClassification(StrEnum):
    """Data classification levels for field tagging."""

    PII = "pii"  # Personally Identifiable Information
    SENSITIVE = "sensitive"  # Sensitive business data
    PUBLIC = "public"  # Public information
    INTERNAL = "internal"  # Internal information


class EventFieldType(StrEnum):
    """Event field types for structured properties."""

    SELF_DESCRIBING = "self_describing"  # Custom self-describing event
    ATOMIC = "atomic"  # Snowplow atomic event field
    CONTEXT = "context"  # Context entity field


class SchemaFormat(StrEnum):
    """Schema format types."""

    JSON_SCHEMA = "jsonschema"


class Cardinality(StrEnum):
    """Structured property cardinality."""

    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class Environment(StrEnum):
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

# API URL constants
BDP_CONSOLE_BASE_URL = "https://console.snowplowanalytics.com"
BDP_API_BASE_URL = f"{BDP_CONSOLE_BASE_URL}/api/msc/v1"

# Default timeout values (in seconds)
DEFAULT_BDP_TIMEOUT_SECONDS = 60
DEFAULT_IGLU_TIMEOUT_SECONDS = 30

# Schema-related constants
IGLU_URI_PREFIX = "iglu:"
IGLU_CENTRAL_URL = "http://iglucentral.com"

# Standard Snowplow event columns (non-enriched fields)
# Reference: https://docs.snowplow.io/docs/fundamentals/canonical-event/
SNOWPLOW_STANDARD_COLUMNS = [
    "app_id",
    "platform",
    "etl_tstamp",
    "collector_tstamp",
    "dvce_created_tstamp",
    "event",
    "event_id",
    "txn_id",
    "name_tracker",
    "v_tracker",
    "v_collector",
    "v_etl",
    "user_id",
    "user_ipaddress",
    "user_fingerprint",
    "domain_userid",
    "domain_sessionidx",
    "network_userid",
    "geo_country",
    "geo_region",
    "geo_city",
    "geo_zipcode",
    "geo_latitude",
    "geo_longitude",
    "geo_region_name",
    "ip_isp",
    "ip_organization",
    "ip_domain",
    "ip_netspeed",
    "page_url",
    "page_title",
    "page_referrer",
    "page_urlscheme",
    "page_urlhost",
    "page_urlport",
    "page_urlpath",
    "page_urlquery",
    "page_urlfragment",
    "refr_urlscheme",
    "refr_urlhost",
    "refr_urlport",
    "refr_urlpath",
    "refr_urlquery",
    "refr_urlfragment",
    "refr_medium",
    "refr_source",
    "refr_term",
    "mkt_medium",
    "mkt_source",
    "mkt_term",
    "mkt_content",
    "mkt_campaign",
    "se_category",
    "se_action",
    "se_label",
    "se_property",
    "se_value",
    "tr_orderid",
    "tr_affiliation",
    "tr_total",
    "tr_tax",
    "tr_shipping",
    "tr_city",
    "tr_state",
    "tr_country",
    "ti_orderid",
    "ti_sku",
    "ti_name",
    "ti_category",
    "ti_price",
    "ti_quantity",
    "pp_xoffset_min",
    "pp_xoffset_max",
    "pp_yoffset_min",
    "pp_yoffset_max",
    "useragent",
    "br_name",
    "br_family",
    "br_version",
    "br_type",
    "br_renderengine",
    "br_lang",
    "br_features_pdf",
    "br_features_flash",
    "br_features_java",
    "br_features_director",
    "br_features_quicktime",
    "br_features_realplayer",
    "br_features_windowsmedia",
    "br_features_gears",
    "br_features_silverlight",
    "br_cookies",
    "br_colordepth",
    "br_viewwidth",
    "br_viewheight",
    "os_name",
    "os_family",
    "os_manufacturer",
    "os_timezone",
    "dvce_type",
    "dvce_ismobile",
    "dvce_screenwidth",
    "dvce_screenheight",
    "doc_charset",
    "doc_width",
    "doc_height",
]


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
