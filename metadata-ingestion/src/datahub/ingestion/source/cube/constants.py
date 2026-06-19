"""Constants for the Cube (cube.dev) semantic layer connector."""

from typing import Dict

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

CUBE_PLATFORM = "cube"

# REST (JSON) API endpoint exposed by both Cube Core and Cube Cloud.
# Returns cubes/views with their measures, dimensions, segments and joins.
API_ENDPOINT_META = "v1/meta"

# Cube Cloud "Metadata API" endpoints. These are only available on Cube Cloud
# and additionally expose data lineage (table/column references).
API_ENDPOINT_DATA_SOURCES = "v1/data-sources"
API_ENDPOINT_ENTITIES = "v1/entities"
API_ENDPOINT_ENTITIES_ALL = "v1/entities/all"

# Cube Cloud Control Plane API: mints a metadata-scoped JWT for the Metadata API.
CONTROL_PLANE_META_SYNC_TOKEN_PATH = (
    "api/v1/deployments/{deployment_id}/environments/{environment_id}/"
    "tokens-for-meta-sync"
)

# Default base path. Configurable in Cube via the `basePath` option, but
# `/cubejs-api` is the out-of-the-box value for the vast majority of deployments.
DEFAULT_BASE_PATH = "/cubejs-api"

HTTP_METHOD_GET = "GET"
HTTP_METHOD_POST = "POST"
HTTP_PROTOCOL_HTTP = "http://"
HTTP_PROTOCOL_HTTPS = "https://"
HTTP_HEADER_AUTHORIZATION = "Authorization"
HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_CONTENT_TYPE_JSON = "application/json"

HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1
HTTP_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

# The Metadata API paginates /v1/entities/all; this is the page size we request.
METADATA_API_PAGE_SIZE = 100

DEFAULT_REQUEST_TIMEOUT_SEC = 30
MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD = 300

# Entity discriminator returned by both APIs.
ENTITY_TYPE_CUBE = "cube"
ENTITY_TYPE_VIEW = "view"

# Cube member data types (the `type` field on a dimension/measure) mapped to
# DataHub schema field types. Cube's primitive set is small and stable.
CUBE_TYPE_TO_SCHEMA_FIELD_TYPE: Dict[str, type] = {
    "string": StringTypeClass,
    "number": NumberTypeClass,
    "time": TimeTypeClass,
    "boolean": BooleanTypeClass,
    "date": DateTypeClass,
    # `geo` has no dedicated DataHub primitive; surface it as a string.
    "geo": StringTypeClass,
}

# Measure aggregation types reported by Cube. Used to annotate measure fields.
KNOWN_MEASURE_AGG_TYPES = {
    "count",
    "countDistinct",
    "countDistinctApprox",
    "sum",
    "avg",
    "min",
    "max",
    "runningTotal",
    "number",
}

# Maps the `type` reported by Cube's /v1/data-sources (the underlying warehouse
# driver) to the corresponding DataHub data platform name. Used to build
# upstream lineage URNs that point at the real warehouse tables.
DATA_SOURCE_TYPE_TO_PLATFORM: Dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "mysql": "mysql",
    "mssql": "mssql",
    "sqlserver": "mssql",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "databricks-jdbc": "databricks",
    "athena": "athena",
    "trino": "trino",
    "presto": "presto",
    "druid": "druid",
    "oracle": "oracle",
    "hive": "hive",
    "duckdb": "duckdb",
    "firebolt": "firebolt",
    "materialize": "postgres",
}

# Tag names applied to schema fields to distinguish the two kinds of Cube members.
TAG_MEASURE = "Measure"
TAG_DIMENSION = "Dimension"
TAG_TEMPORAL = "Temporal"
