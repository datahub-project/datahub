import re

WORKDAY_PLATFORM = "workday"

# Prism Analytics REST API is versioned in the path; v3 is current (SQL-style
# tables replaced the legacy v1/v2 dataset-only model). The version is a config
# knob so a tenant pinned to an older API can override it without code changes.
PRISM_API_VERSION_DEFAULT = "v3"

# Endpoint path templates. Every path that is referenced in more than one place
# (the request plus its response-shape check or error message) is centralized so
# the uses cannot drift apart. `{tenant}` and `{version}` are filled once by the
# client; `{id}` is filled per object.
WORKDAY_OAUTH_TOKEN_PATH = "/ccx/oauth2/{tenant}/token"
PRISM_BASE_PATH = "/api/prismAnalytics/{version}/{tenant}"
PRISM_TABLES_PATH = "/tables"
PRISM_TABLE_PATH = "/tables/{id}"
PRISM_DATASETS_PATH = "/datasets"
PRISM_DATASET_PATH = "/datasets/{id}"
PRISM_DATA_SOURCES_PATH = "/dataSources"
PRISM_DATA_SOURCE_PATH = "/dataSources/{id}"

# OAuth 2.0 client-credentials grant. Workday does not issue a refresh token for
# this grant, so the client re-requests a token from the client_id/secret when
# the cached one nears expiry or a call returns 401.
WORKDAY_GRANT_TYPE_CLIENT_CREDENTIALS = "client_credentials"

# Prism `sourceType` on a table/data source: data originating inside Workday
# (reports, business objects) versus data uploaded from outside.
PRISM_SOURCE_TYPE_WORKDAY = "Workday"
PRISM_SOURCE_TYPE_EXTERNAL = "External"

# DataHub dataset subtypes. Kept as constants so the mapper and tests agree on
# the exact strings.
SUBTYPE_PRISM_TABLE = "Prism Table"
SUBTYPE_PRISM_DATASET = "Prism Dataset"
SUBTYPE_DATA_SOURCE = "Data Source"
SUBTYPE_REPORT = "Report"

# Container subtype for the tenant-level grouping.
SUBTYPE_TENANT = "Tenant"

# Field tags: Prism marks fields that participate in a table's business key.
PRIMARY_KEY_TAG_URN = "urn:li:tag:PrimaryKey"

# Prism field base type -> DataHub SchemaFieldDataType. Prism reports the
# semantic type under `type.descriptor`/`type.id`; we normalize the descriptor
# to lowercase before lookup. Unknown types fall back to a string type and are
# counted in the report.
PRISM_TYPE_TO_DATAHUB = {
    "text": "string",
    "string": "string",
    "boolean": "boolean",
    "numeric": "number",
    "number": "number",
    "decimal": "number",
    "integer": "number",
    "long": "number",
    "currency": "number",
    "percent": "number",
    "date": "date",
    "datetime": "time",
    "timestamp": "time",
    "instant": "time",
    "multi_instance": "string",
    "single_instance": "string",
    "reference": "string",
}

# API response field-name fallbacks. Prism carries the same logical field under
# different keys across API versions and object shapes, so each lookup tries an
# ordered list of aliases. Declared once here so the models that share them do
# not drift apart.
WORKDAY_KEYS_ID = ("id", "wid", "objectId")
WORKDAY_KEYS_NAME = ("name", "displayName", "label")
WORKDAY_KEYS_DESCRIPTION = ("description", "descriptor")
WORKDAY_KEYS_OWNER = ("createdBy", "owner", "updatedBy")

# Tenant names and Prism object ids appear in URNs; a permissive validator keeps
# obviously malformed ids from producing broken URNs. Prism ids are 32-char hex
# WIDs, but external/older objects can differ, so we only require non-empty,
# whitespace-free, reasonably bounded strings.
VALID_OBJECT_ID_RE = re.compile(r"^\S{1,255}$")

# Bearer token responses put the token under `access_token` (OAuth standard);
# expiry under `expires_in` (seconds). Declared for the client's token parsing.
OAUTH_ACCESS_TOKEN_KEY = "access_token"
OAUTH_EXPIRES_IN_KEY = "expires_in"

# Refresh the cached token this many seconds before its stated expiry so a call
# never fires with a token that expires in-flight.
TOKEN_EXPIRY_SKEW_SECONDS = 60
