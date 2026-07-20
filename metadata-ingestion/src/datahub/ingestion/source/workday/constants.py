import re

WORKDAY_PLATFORM = "workday"

# Prism Analytics REST API is versioned in the path; v3 is current (SQL-style
# tables replaced the legacy v1/v2 dataset-only model). The version is a config
# knob so a tenant pinned to an older API can override it without code changes.
PRISM_API_VERSION_DEFAULT = "v3"

# WQL metadata API version segment. Independent of the Prism API version.
WQL_API_VERSION_DEFAULT = "v1"

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
# Prism buckets are the staging areas uploads land in before being published to
# a table; listing them exposes the upload path feeding each table.
PRISM_BUCKETS_PATH = "/buckets"

# Workday Query Language (WQL) metadata API. `GET /dataSources` lists WQL data
# sources (Workday business objects) and `/dataSources/{id}/fields` lists their
# fields — the source of the business-object catalog and RaaS report enumeration.
WQL_BASE_PATH = "/api/wql/v1/{tenant}"
WQL_DATA_SOURCES_PATH = "/dataSources"
WQL_DATA_SOURCE_FIELDS_PATH = "/dataSources/{id}/fields"

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
# A WQL data source is a Workday business object exposed for querying.
SUBTYPE_BUSINESS_OBJECT = "Business Object"
# A Prism bucket is the staging area an upload lands in before it becomes a table.
SUBTYPE_BUCKET = "Bucket"

# Container subtypes: the tenant-level grouping and the optional functional-area
# (subject-area) sub-group that business objects and reports can carry.
SUBTYPE_TENANT = "Tenant"
SUBTYPE_FUNCTIONAL_AREA = "Functional Area"

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
# A security group that owns the object, distinct from the individual owner.
WORKDAY_KEYS_OWNER_GROUP = ("ownerGroup", "securityGroup", "ownerSecurityGroup")
# Catalog classifications surfaced as tags, and business-glossary term links.
WORKDAY_KEYS_TAGS = ("tags", "labels", "classifications")
WORKDAY_KEYS_TERMS = ("glossaryTerms", "businessGlossaryTerms", "terms")
# Functional / subject area an object belongs to (HCM, Financials, Recruiting).
WORKDAY_KEYS_CATEGORY = ("category", "subjectArea", "functionalArea")
# Creation / last-change timestamps. Prism reports these under several keys
# across object shapes; values are ISO-8601 strings.
WORKDAY_KEYS_CREATED = ("created", "creationDate", "createdDate", "createdOn")
WORKDAY_KEYS_UPDATED = (
    "updated",
    "lastRefreshed",
    "lastRefreshDate",
    "lastModified",
    "updatedDate",
    "modifiedOn",
)
# Row-count aliases on a Prism table detail response (used for profiling).
WORKDAY_KEYS_ROW_COUNT = ("rows", "rowCount", "recordCount", "totalRows", "numRows")

# Prism dataset detail: the transformation pipeline text (Data Prep Language)
# and the per-output-field source mappings that drive column-level lineage.
WORKDAY_KEYS_TRANSFORM_LOGIC = ("dpl", "pipeline", "transformationLogic", "logic")
WORKDAY_KEYS_FIELD_MAPPINGS = (
    "fieldMappings",
    "columnMappings",
    "mappings",
    "outputFields",
)

# Other business objects a WQL data source references, for object-to-object lineage.
WORKDAY_KEYS_RELATED_OBJECTS = (
    "relatedBusinessObjects",
    "relatedDataSources",
    "relatedObjects",
    "references",
)
# The output columns a custom report exposes (report schema + report CLL).
WORKDAY_KEYS_REPORT_FIELDS = ("outputFields", "reportFields", "fields", "columns")

# WQL: the business object whose rows are custom-report definitions. Querying it
# enumerates RaaS reports without a bespoke per-tenant endpoint.
WQL_CUSTOM_REPORTS_DATA_SOURCE = "allCustomReports"

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
