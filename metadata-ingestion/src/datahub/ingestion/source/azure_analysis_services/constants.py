import re
from typing import Pattern

# --- XMLA / SOAP protocol -------------------------------------------------

XMLA_NAMESPACE = "urn:schemas-microsoft-com:xml-analysis"
XMLA_ROWSET_NAMESPACE = "urn:schemas-microsoft-com:xml-analysis:rowset"
SOAP_ENVELOPE_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"
XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
XSD_NAMESPACE = "http://www.w3.org/2001/XMLSchema"

SOAP_ACTION_EXECUTE = "urn:schemas-microsoft-com:xml-analysis:Execute"
SOAP_ACTION_DISCOVER = "urn:schemas-microsoft-com:xml-analysis:Discover"

# Request headers. ``x-ms-xmlaserver`` names the logical AS server the gateway
# should route to; the negotiation-flags header is required by the webapi/xmla
# gateway to accept a raw SOAP client (observed empirically — undocumented).
HEADER_SOAP_ACTION = "SOAPAction"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_AUTHORIZATION = "Authorization"
HEADER_XMLA_SERVER = "x-ms-xmlaserver"
HEADER_XMLA_NEGOTIATION_FLAGS = "x-ms-xmlacaps-negotiation-flags"
HEADER_USER_AGENT = "User-Agent"

CONTENT_TYPE_XML = "text/xml; charset=utf-8"
XMLA_NEGOTIATION_FLAGS = "1,0,0,0,0"
USER_AGENT = "DataHub-AAS-Ingestion/XmlaClient"
BEARER_PREFIX = "Bearer "

# Property names inside the XMLA <PropertyList>.
PROPERTY_CATALOG = "Catalog"
PROPERTY_FORMAT = "Format"
PROPERTY_CONTENT = "Content"
FORMAT_TABULAR = "Tabular"
CONTENT_SCHEMA_DATA = "SchemaData"

# --- Endpoints ------------------------------------------------------------

ASAZURE_CLUSTER_RESOLVE_URL = (
    "https://{region}.asazure.windows.net/webapi/clusterResolve"
)
ASAZURE_XMLA_URL = "https://{fqdn}/webapi/xmla"
ASAZURE_DEFAULT_SCOPE = "https://{region}.asazure.windows.net/.default"
ASAZURE_SERVER_NAME_KEY = "serverName"
ASAZURE_CLUSTER_FQDN_KEY = "clusterFQDN"
ASAZURE_CORE_SERVER_KEY = "coreServerName"
ASAZURE_TENANT_ID_KEY = "tenantId"

# Power BI Premium XMLA endpoint (used when connecting to a Premium workspace
# exposed as an AS-compatible endpoint).
POWERBI_XMLA_HOST = "https://api.powerbi.com"
POWERBI_XMLA_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
POWERBI_AZURE_AD_AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"

# --- DMV / rowset queries -------------------------------------------------
# Tabular metadata DMVs. Selecting explicit columns keeps the rowset stable
# across engine versions (SELECT * ordering is not guaranteed).

DMV_MODEL = "$SYSTEM.TMSCHEMA_MODEL"
DMV_TABLES = "$SYSTEM.TMSCHEMA_TABLES"
DMV_COLUMNS = "$SYSTEM.TMSCHEMA_COLUMNS"
DMV_MEASURES = "$SYSTEM.TMSCHEMA_MEASURES"
DMV_PARTITIONS = "$SYSTEM.TMSCHEMA_PARTITIONS"
DMV_RELATIONSHIPS = "$SYSTEM.TMSCHEMA_RELATIONSHIPS"
DMV_HIERARCHIES = "$SYSTEM.TMSCHEMA_HIERARCHIES"
DMV_LEVELS = "$SYSTEM.TMSCHEMA_LEVELS"
DMV_KPIS = "$SYSTEM.TMSCHEMA_KPIS"
DMV_ROLES = "$SYSTEM.TMSCHEMA_ROLES"
DMV_DATA_SOURCES = "$SYSTEM.TMSCHEMA_DATA_SOURCES"
DMV_EXPRESSIONS = "$SYSTEM.TMSCHEMA_EXPRESSIONS"
DMV_CALC_DEPENDENCY = "$SYSTEM.DISCOVER_CALC_DEPENDENCY"
DMV_CATALOGS = "$SYSTEM.DBSCHEMA_CATALOGS"

SELECT_ALL_TEMPLATE = "SELECT * FROM {dmv}"

# DISCOVER_XML_METADATA returns the full TMSL/TMDL model definition. It is a
# restricted rowset, so it is queried via a Discover DBSCHEMA request with an
# ObjectExpansion restriction rather than as a plain DMV SELECT.
DISCOVER_XML_METADATA_REQUEST_TYPE = "DISCOVER_XML_METADATA"
OBJECT_EXPANSION_RESTRICTION = "ObjectExpansion"
OBJECT_EXPANSION_EXPAND_FULL = "ExpandFull"
DATABASE_ID_RESTRICTION = "DatabaseID"

# Column data-type family reported by TMSCHEMA_COLUMNS.ExplicitDataType /
# InferredDataType (Tabular Object Model DataType enumeration).
TOM_DATA_TYPE_STRING = 1
TOM_DATA_TYPE_INT64 = 6
TOM_DATA_TYPE_DOUBLE = 8
TOM_DATA_TYPE_DECIMAL = 10
TOM_DATA_TYPE_BOOLEAN = 11
TOM_DATA_TYPE_DATETIME = 9

# TMSCHEMA_COLUMNS.Type: 1 = data column, 2 = calculated, 3 = row-number.
COLUMN_TYPE_DATA = 1
COLUMN_TYPE_CALCULATED = 2
COLUMN_TYPE_ROW_NUMBER = 3

# TMSCHEMA_PARTITIONS.Type: 4 = M/Power Query, 7 = calculated (DAX), etc.
PARTITION_TYPE_QUERY = 4
PARTITION_TYPE_CALCULATED = 7

# --- Native-type labels ---------------------------------------------------

NATIVE_TYPE_MEASURE = "measure"
NATIVE_TYPE_CALCULATED_COLUMN = "calculated_column"

# --- View / schema metadata -----------------------------------------------

VIEW_LANGUAGE_M = "M"
VIEW_LANGUAGE_DAX = "DAX"
VIEW_LANGUAGE_TMSL = "TMSL"

# customProperties keys.
PROP_CATALOG = "catalog"
PROP_CULTURE = "culture"
PROP_IS_HIDDEN = "is_hidden"
PROP_ROLE_PREFIX = "role."
PROP_MEASURE_COUNT = "measure_count"
PROP_TABLE_COUNT = "table_count"

# --- Platform -------------------------------------------------------------

PLATFORM_AAS = "azure-analysis-services"
PLATFORM_POWERBI = "powerbi"

# --- Precompiled regex ----------------------------------------------------
# ``asazure://westeurope.asazure.windows.net/myserver`` or the https form; the
# region drives the token scope and cluster-resolve call, the trailing segment
# is the logical server name.
ASAZURE_ENDPOINT_RE: Pattern[str] = re.compile(
    r"^(?:asazure|https)://(?P<region>[^.]+)\.asazure\.windows\.net/(?P<server>[^/?#]+)",
    re.IGNORECASE,
)

# ``powerbi://api.powerbi.com/v1.0/myorg/<workspace>``
POWERBI_ENDPOINT_RE: Pattern[str] = re.compile(
    r"^powerbi://api\.powerbi\.com/v1\.0/myorg/(?P<workspace>[^/?#]+)",
    re.IGNORECASE,
)

# A DAX calc-dependency reference such as ``'Sales'[Amount]`` or ``Sales[Amount]``
# split into its table and object parts.
DAX_OBJECT_REFERENCE_RE: Pattern[str] = re.compile(
    r"'?(?P<table>[^'\[\]]+)'?\[(?P<object>[^\[\]]+)\]"
)

# Bearer tokens must never be logged in full; this matches the value after the
# scheme so it can be truncated in debug output.
BEARER_TOKEN_REDACT_RE: Pattern[str] = re.compile(
    r"(Bearer\s+)(?P<token>[A-Za-z0-9\-._~+/]+=*)", re.IGNORECASE
)
