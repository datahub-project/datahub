from typing import Dict, Sequence

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult

# Default Airbyte Cloud URLs
DEFAULT_CLOUD_API_URL = "https://api.airbyte.com/v1"
DEFAULT_CLOUD_OAUTH_TOKEN_URL = "https://auth.airbyte.com/oauth/token"
DEFAULT_CLOUD_UI_URL = "https://cloud.airbyte.com"

HTTP_CONTENT_TYPE_JSON = "application/json"
HTTP_CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
HTTP_HEADER_AUTHORIZATION = "Authorization"
HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_HEADER_BEARER_PREFIX = "Bearer "
HTTP_METHOD_GET = "GET"
HTTP_METHOD_POST = "POST"
HTTP_PROTOCOL_HTTP = "http://"
HTTP_PROTOCOL_HTTPS = "https://"
# 401/403: classified as auth in _make_request. Source hard-stops the run on
# 401; a per-resource 403 fails that connection/workspace and continues.
HTTP_AUTH_STATUS_CODES = frozenset({401, 403})

DEFAULT_TOKEN_EXPIRY_SECONDS = 3600
# Refresh 10 minutes before expiry to avoid races between the "still valid"
# check and the actual API call landing on the server.
TOKEN_REFRESH_BUFFER_SECONDS = 600

# See https://docs.airbyte.com/developers/api-documentation
API_ENDPOINT_WORKSPACES = "/workspaces"
API_ENDPOINT_CONNECTIONS = "/connections"
API_ENDPOINT_SOURCES = "/sources"
API_ENDPOINT_DESTINATIONS = "/destinations"
API_ENDPOINT_STREAMS = "/streams"
API_ENDPOINT_JOBS = "/jobs"
API_ENDPOINT_TAGS = "/tags"
API_ENDPOINT_APPLICATIONS_TOKEN = "/applications/token"

API_RESPONSE_KEY_DATA = "data"
API_RESPONSE_KEY_JOBS = "jobs"
API_RESPONSE_KEY_TAGS = "tags"
API_RESPONSE_KEY_STREAMS = "streams"
API_RESPONSE_KEY_NEXT = "next"
API_RESPONSE_KEY_ACCESS_TOKEN = "access_token"
API_RESPONSE_KEY_EXPIRES_IN = "expires_in"
API_RESPONSE_KEY_ERROR_DESCRIPTION = "error_description"

API_QUERY_WORKSPACE_ID = "workspaceId"
API_QUERY_WORKSPACE_IDS = "workspaceIds"
API_QUERY_LIMIT = "limit"
API_QUERY_OFFSET = "offset"
API_QUERY_UPDATED_AT_START = "updatedAtStart"
API_QUERY_UPDATED_AT_END = "updatedAtEnd"

API_FIELD_NAME = "name"
API_FIELD_STATUS = "status"
API_FIELD_NAMESPACE = "namespace"
API_FIELD_STREAM_NAME = "streamName"
API_FIELD_STREAM_NAMESPACE_LOWER = "streamnamespace"
API_FIELD_STREAM_NAMESPACE_CAMEL = "streamNamespace"
API_FIELD_PROPERTY_FIELDS = "propertyFields"
API_FIELD_SYNC_CATALOG = "syncCatalog"
API_FIELD_CONFIGURATIONS = "configurations"
API_FIELD_SOURCE_ID = "sourceId"
API_FIELD_DESTINATION_ID = "destinationId"
API_FIELD_CONNECTION_ID = "connectionId"
API_FIELD_SYNC_MODE = "syncMode"
API_FIELD_DESTINATION_SYNC_MODE = "destinationSyncMode"
API_FIELD_PRIMARY_KEY = "primaryKey"
API_FIELD_CURSOR_FIELD = "cursorField"
API_FIELD_DESTINATION_NAMESPACE = "destinationNamespace"
API_FIELD_ALIAS_NAME = "aliasName"
API_FIELD_SELECTED_FIELDS = "selectedFields"
API_FIELD_FIELD_SELECTION_ENABLED = "fieldSelectionEnabled"
API_FIELD_FIELD_SELECTION = "fieldSelection"
API_FIELD_JSON_SCHEMA = "jsonSchema"
API_FIELD_JSON_SCHEMA_SNAKE = "json_schema"
API_FIELD_NAMESPACE_DEFINITION = "namespaceDefinition"
API_FIELD_NAMESPACE_FORMAT = "namespaceFormat"
API_FIELD_PREFIX = "prefix"
API_FIELD_TABLES = "tables"
API_FIELD_SCHEMAS = "schemas"
API_FIELD_SCHEMA = "schema"
API_FIELD_CONFIG_ID = "configId"
API_FIELD_CONFIG_TYPES = "configTypes"
API_FIELD_CLIENT_ID = "client_id"
API_FIELD_CLIENT_SECRET = "client_secret"
API_FIELD_GRANT_TYPE = "grant_type"
API_FIELD_REFRESH_TOKEN = "refresh_token"

API_STATUS_INACTIVE = "inactive"
API_JOB_CONFIG_TYPE_SYNC = "sync"
API_JOB_CONFIG_TYPE_RESET = "reset_connection"

SYNC_MODE_FULL_REFRESH = "full_refresh"
SYNC_MODE_INCREMENTAL = "incremental"
SYNC_MODE_DESTINATION_OVERWRITE = "overwrite"
SYNC_MODE_NULL = "null"

JSON_SCHEMA_TYPE_OBJECT = "object"
JSON_SCHEMA_TYPE_NULL = "null"
JSON_SCHEMA_TYPE_STRING = "string"
JSON_SCHEMA_KEY_TYPE = "type"
JSON_SCHEMA_KEY_PROPERTIES = "properties"

NAMESPACE_DEFINITION_CUSTOM_FORMAT = "customformat"
SOURCE_NAMESPACE_PLACEHOLDER = "${SOURCE_NAMESPACE}"

# Schema-name keys seen across Airbyte connector configurations. Order matters —
# more-specific keys first so the generic "schema" doesn't shadow MSSQL/Oracle's
# "default_schema".
SCHEMA_CONFIG_FIELDS: Sequence[str] = (
    "schema",
    "default_schema",
    "schema_name",
)

# Database-name keys. Destinations additionally treat BigQuery's "dataset" as
# the database tier — see DESTINATION_DATABASE_CONFIG_FIELDS.
SOURCE_DATABASE_CONFIG_FIELDS: Sequence[str] = (
    "database",
    "db",
    "db_name",
    "database_name",
    "dbname",
    "service_name",
    "sid",
    "project",
    "project_id",
    "catalog",
    "keyspace",
)

DESTINATION_DATABASE_CONFIG_FIELDS: Sequence[str] = (
    *SOURCE_DATABASE_CONFIG_FIELDS,
    "dataset",
)

# Known source type to DataHub platform mapping
KNOWN_SOURCE_TYPE_MAPPING: Dict[str, str] = {
    # Relational Databases
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mssql": "mssql",
    "sql-server": "mssql",
    "sqlserver": "mssql",
    "oracle": "oracle",
    "db2": "db2",
    # Cloud Data Warehouses
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "synapse": "mssql",
    # NoSQL Databases
    "mongodb": "mongodb",
    "mongo": "mongodb",
    "cassandra": "cassandra",
    "dynamodb": "dynamodb",
    "elasticsearch": "elasticsearch",
    "opensearch": "opensearch",
    "clickhouse": "clickhouse",
    # Big Data & Analytics
    "hive": "hive",
    "presto": "presto",
    "trino": "trino",
    "athena": "athena",
    "vertica": "vertica",
    "teradata": "teradata",
    "druid": "druid",
    # Cloud storage
    "s3": "s3",
    "gcs": "gcs",
    "google-cloud-storage": "gcs",
    "azure-blob-storage": "abs",
    "abs": "abs",
    # Streaming & Messaging
    "kafka": "kafka",
    "pulsar": "pulsar",
    "kinesis": "kinesis",
    # File Formats & Data Lakes
    "delta-lake": "delta-lake",
    "iceberg": "iceberg",
    "hudi": "hudi",
    # Other
    "glue": "glue",
    "salesforce": "salesforce",
    "netsuite": "netsuite",
    "sap-hana": "hana",
    "hana": "hana",
}

AIRBYTE_JOB_STATUS_MAP = {
    "succeeded": InstanceRunResult.SUCCESS,
    "completed": InstanceRunResult.SUCCESS,
    "success": InstanceRunResult.SUCCESS,
    "failed": InstanceRunResult.FAILURE,
    "failure": InstanceRunResult.FAILURE,
    "error": InstanceRunResult.FAILURE,
    "cancelled": InstanceRunResult.SKIPPED,
    "canceled": InstanceRunResult.SKIPPED,
    "running": InstanceRunResult.UP_FOR_RETRY,
    "incomplete": InstanceRunResult.UP_FOR_RETRY,
    "pending": InstanceRunResult.UP_FOR_RETRY,
}
