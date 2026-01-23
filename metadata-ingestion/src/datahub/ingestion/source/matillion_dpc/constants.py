MATILLION_PLATFORM = "matillion"
MATILLION_NAMESPACE_PREFIX = "matillion://"

API_ENDPOINT_PROJECTS = "v1/projects"
API_ENDPOINT_ENVIRONMENTS = "v1/projects/{projectId}/environments"
API_ENDPOINT_PIPELINES = "v1/projects/{projectId}/published-pipelines"
API_ENDPOINT_PIPELINE_EXECUTIONS = "v1/pipeline-executions"
API_ENDPOINT_SCHEDULES = "v1/projects/{projectId}/schedules"
API_ENDPOINT_STREAMING_PIPELINES = "v1/projects/{projectId}/streaming-pipelines"
API_ENDPOINT_LINEAGE_EVENTS = "v1/lineage/events"

HTTP_METHOD_GET = "GET"
HTTP_PROTOCOL_HTTP = "http://"
HTTP_PROTOCOL_HTTPS = "https://"
HTTP_HEADER_AUTHORIZATION = "Authorization"
HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_CONTENT_TYPE_JSON = "application/json"

HTTP_RETRY_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_FACTOR = 1
HTTP_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
HTTP_RETRY_ALLOWED_METHODS = [HTTP_METHOD_GET]

API_RESPONSE_FIELD_RESULTS = "results"
API_RESPONSE_FIELD_TOTAL = "total"
API_RESPONSE_FIELD_PAGE = "page"
API_RESPONSE_FIELD_SIZE = "size"

# API pagination limits (per Matillion DPC API specification)
API_MAX_PAGE_SIZE = 100

MATILLION_EU1_URL = "https://eu1.api.matillion.com/dpc"
MATILLION_US1_URL = "https://us1.api.matillion.com/dpc"
MATILLION_OAUTH_TOKEN_URL = "https://id.core.matillion.com/oauth/dpc/token"

API_PATH_SUFFIX = "/dpc"

# OAuth2 configuration
OAUTH_GRANT_TYPE = "client_credentials"
OAUTH_AUDIENCE = "https://api.matillion.com"
OAUTH_TOKEN_EXPIRY_SECONDS = 1800  # 30 minutes
OAUTH_TOKEN_REFRESH_BUFFER_SECONDS = 300  # Refresh 5 minutes before expiry
UI_PATH_PIPELINES = "pipelines"
UI_PATH_STREAMING_PIPELINES = "streaming-pipelines"

DEFAULT_REQUEST_TIMEOUT_SEC = 30
MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD = 300
MAX_EXECUTIONS_PER_PIPELINE_WARNING_THRESHOLD = 100

# Platform-related constants
# Maps platform identifiers from various sources (connection types, OpenLineage namespaces)
# to DataHub platform names
PLATFORM_MAPPING = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlserver": "mssql",
    "mssql": "mssql",
    "oracle": "oracle",
    "s3": "s3",
    "azure": "abs",
    "gcs": "gcs",
    "databricks": "databricks",
    "db2": "db2",
    "teradata": "teradata",
    "sap-hana": "sap-hana",
    "saphana": "sap-hana",
    "mongodb": "mongodb",
    "mongo": "mongodb",
    "cassandra": "cassandra",
    "elasticsearch": "elasticsearch",
    "elastic": "elasticsearch",
    "kafka": "kafka",
    "delta-lake": "delta-lake",
    "delta": "delta-lake",
    "deltalake": "delta-lake",
    "dremio": "dremio",
    "firebolt": "firebolt",
}

# Platforms that use 2-tier naming (schema.table) vs 3-tier (database.schema.table)
# Based on DataHub's TwoTierSQLAlchemySource pattern
TWO_TIER_PLATFORMS = {
    "mysql",
    "hive",
    "teradata",
    "clickhouse",
    "glue",
    "iceberg",
}

# Platforms that require lowercase field names for schema field URNs
# to match DataHub connector behavior (e.g., Snowflake's convert_urns_to_lowercase)
LOWERCASE_FIELD_PLATFORMS = {"snowflake"}
