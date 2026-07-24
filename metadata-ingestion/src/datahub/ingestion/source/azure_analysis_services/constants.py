import re
from enum import IntEnum
from typing import Pattern

# --- XMLA / SOAP protocol -------------------------------------------------

XMLA_NAMESPACE = "urn:schemas-microsoft-com:xml-analysis"
SOAP_ENVELOPE_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"

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

# --- Request-body templates -----------------------------------------------
# Kept as named templates rather than inline f-strings so the SOAP/XMLA wire
# format lives in one place (and out of the transport code).

XML_ELEMENT_TEMPLATE = "<{tag}>{value}</{tag}>"
PROPERTY_LIST_TEMPLATE = (
    "<Properties><PropertyList>{properties}</PropertyList></Properties>"
)
SOAP_ENVELOPE_TEMPLATE = (
    '<soap:Envelope xmlns:soap="{namespace}">'
    "<soap:Body>{body}</soap:Body></soap:Envelope>"
)
EXECUTE_REQUEST_TEMPLATE = (
    '<Execute xmlns="{namespace}">'
    "<Command><Statement>{statement}</Statement></Command>"
    "{properties}</Execute>"
)
DISCOVER_METADATA_REQUEST_TEMPLATE = (
    '<Discover xmlns="{namespace}">'
    "<RequestType>{request_type}</RequestType>"
    "<Restrictions><RestrictionList>"
    "<{database_id_tag}>{database_id}</{database_id_tag}>"
    "<{expansion_tag}>{expansion}</{expansion_tag}>"
    "</RestrictionList></Restrictions>"
    "{properties}</Discover>"
)

# --- Response element / rowset key names ----------------------------------
# Rowset elements are namespace-qualified and matched on their local name.

ELEMENT_ROOT = "root"
ELEMENT_ROW = "row"
ELEMENT_FAULT = "Fault"
ELEMENT_FAULT_STRING = "faultstring"
ELEMENT_FAULT_DESCRIPTION = "Description"
ELEMENT_METADATA = "METADATA"
ELEMENT_METADATA_ALT = "Metadata"

ROW_KEY_CATALOG_NAME = "CATALOG_NAME"
ROW_KEY_NAME = "Name"

# A rootless but fault-free 200 response is ambiguous (gateway HTML, redirect,
# empty <return/>); this many characters of the body are surfaced in the
# warning so operators can tell "no models" from "parse failed".
ROOTLESS_SNIPPET_LEN = 200

# --- Endpoints ------------------------------------------------------------

ASAZURE_CLUSTER_RESOLVE_URL = (
    "https://{region}.asazure.windows.net/webapi/clusterResolve"
)
ASAZURE_XMLA_URL = "https://{fqdn}/webapi/xmla"
ASAZURE_DEFAULT_SCOPE = "https://{region}.asazure.windows.net/.default"
ASAZURE_SERVER_NAME_KEY = "serverName"
ASAZURE_CLUSTER_FQDN_KEY = "clusterFQDN"

# Power BI Premium XMLA endpoint (used when connecting to a Premium workspace
# exposed as an AS-compatible endpoint).
POWERBI_XMLA_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# --- DMV / rowset queries -------------------------------------------------
# Tabular metadata DMVs. Selecting explicit columns keeps the rowset stable
# across engine versions (SELECT * ordering is not guaranteed).

DMV_MODEL = "$SYSTEM.TMSCHEMA_MODEL"
DMV_TABLES = "$SYSTEM.TMSCHEMA_TABLES"
DMV_COLUMNS = "$SYSTEM.TMSCHEMA_COLUMNS"
DMV_MEASURES = "$SYSTEM.TMSCHEMA_MEASURES"
DMV_PARTITIONS = "$SYSTEM.TMSCHEMA_PARTITIONS"
DMV_RELATIONSHIPS = "$SYSTEM.TMSCHEMA_RELATIONSHIPS"
DMV_ROLES = "$SYSTEM.TMSCHEMA_ROLES"
DMV_DATA_SOURCES = "$SYSTEM.TMSCHEMA_DATA_SOURCES"
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


class TomDataType(IntEnum):
    # TMSCHEMA_COLUMNS.ExplicitDataType / InferredDataType (Tabular Object Model
    # DataType enumeration).
    STRING = 1
    INT64 = 6
    DATETIME = 9
    DOUBLE = 8
    DECIMAL = 10
    BOOLEAN = 11


class ColumnType(IntEnum):
    # TMSCHEMA_COLUMNS.Type.
    DATA = 1
    CALCULATED = 2
    ROW_NUMBER = 3


class PartitionType(IntEnum):
    # TMSCHEMA_PARTITIONS.Type is the TOM PartitionSourceType enum. Verified
    # against a live Azure Analysis Services server: an M partition reports 4, a
    # DAX calculated-table partition reports 2.
    CALCULATED = 2
    QUERY = 4


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
