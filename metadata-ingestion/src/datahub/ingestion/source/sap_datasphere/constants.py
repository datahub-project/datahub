import re
from typing import Dict, Final

PLATFORM: Final[str] = "sap-datasphere"

# Synthetic connection name for assets that live in the Datasphere tenant's own
# managed HANA Cloud (Views, Analytic Models, Local Tables) rather than a
# federated remote source.
MANAGED_CONNECTION_KEY: Final[str] = "_managed"

# SAP-supported catalog service. The legacy /api/v1/dwc/catalog/ prefix is
# documented as deprecated in favour of this one.
CATALOG_BASE: Final[str] = "/api/v1/datasphere/consumption/catalog"

# SAP's catalog caps a page at 500 records; requesting more is silently capped.
DEFAULT_PAGE_SIZE: Final[int] = 500

# The connections endpoint is not documented to support pagination; warn past
# this many entries in a single space in case the API is silently truncating.
CONNECTIONS_TRUNCATION_THRESHOLD: Final[int] = 100

# How many of the slowest individual API calls to retain for outlier spotting.
SLOWEST_API_CALLS_RETAINED: Final[int] = 10

ACCEPT_JSON: Final[str] = "application/json"
ACCEPT_XML: Final[str] = "application/xml"
# Content type that makes the dwaas-core object endpoint return full CSN.
CSN_CONTENT_TYPE: Final[str] = "application/vnd.sap.datasphere.object.content+json"
FORM_URLENCODED: Final[str] = "application/x-www-form-urlencoded"

# Datasphere design-time object types (path segments under /dwaas-core/).
OBJECT_TYPE_VIEWS: Final[str] = "views"
OBJECT_TYPE_ANALYTIC_MODELS: Final[str] = "analyticmodels"
OBJECT_TYPE_LOCAL_TABLES: Final[str] = "localtables"
OBJECT_TYPE_REMOTE_TABLES: Final[str] = "remotetables"
OBJECT_TYPE_DATA_FLOWS: Final[str] = "dataflows"
OBJECT_TYPE_REPLICATION_FLOWS: Final[str] = "replicationflows"
OBJECT_TYPE_TRANSFORMATION_FLOWS: Final[str] = "transformationflows"
OBJECT_TYPE_TASK_CHAINS: Final[str] = "taskchains"

# The catalog's supportsAnalyticalQueries flag routes a CSN fetch to views vs
# analyticmodels, but it is not a reliable indicator of the design-time type, so
# a 404 on the primary route is retried under the sibling type. localtables has
# no sibling and is intentionally absent.
ALT_OBJECT_TYPE: Final[Dict[str, str]] = {
    OBJECT_TYPE_VIEWS: OBJECT_TYPE_ANALYTIC_MODELS,
    OBJECT_TYPE_ANALYTIC_MODELS: OBJECT_TYPE_VIEWS,
}

GRANT_REFRESH_TOKEN: Final[str] = "refresh_token"
GRANT_CLIENT_CREDENTIALS: Final[str] = "client_credentials"

# Reserved CDS/CQN pseudo-alias: {"ref": ["$projection", "<col>"]} references
# another OUTPUT column of the same query (a calculated column layered on a
# sibling projected column), not a column of a FROM source.
PROJECTION_ALIAS: Final[str] = "$projection"

# Stuffed into ColumnLineagePair.downstream_col when the CSN is structurally
# broken (MALFORMED) or an expression has refs but no derivable name (UNNAMED).
# The source layer routes these to report.column_lineage_unresolved instead of
# emitting a schemaField URN.
MALFORMED_COL_SENTINEL: Final[str] = "<malformed>"
UNNAMED_COL_SENTINEL: Final[str] = "<unnamed>"

# DataHub platform names are lowercase with hyphens/underscores; catches typos
# like "Snowflake" or "my snowflake" at config-load time.
PLATFORM_NAME_RE: Final["re.Pattern[str]"] = re.compile(r"^[a-z0-9_\-]+$")

# Derives the XSUAA token host from a standard SAP Datasphere tenant URL.
XSUAA_URL_RE: Final["re.Pattern[str]"] = re.compile(
    r"https://([^.]+)\.([^.]+)\.hcs\.cloud\.sap"
)

# A flow attributeMapping expression is the source column double-quoted, e.g.
# "\"ERDAT\"". Only a bare quoted identifier yields a column-lineage edge; a
# compound expression (function/arithmetic) is left as table-level lineage.
FLOW_COLUMN_EXPR_RE: Final["re.Pattern[str]"] = re.compile(r'^"([^"]+)"$')

# --- CDS-derived custom-property keys ---------------------------------------
# Emitted onto schema fields / datasets by the EDMX and CSN parsers, consumed by
# source.py for tagging and by tags.py for URN mapping. The producer and the
# consumers must agree on these exact strings, so they live in one place.
PROP_SAP_IS_DIMENSION: Final[str] = "sap_is_dimension"
PROP_SAP_IS_MEASURE: Final[str] = "sap_is_measure"
PROP_SAP_SEMANTIC: Final[str] = "sap_semantic"
PROP_SAP_CALENDAR_TYPE: Final[str] = "sap_calendar_type"
PROP_SAP_DIMENSION_TYPE: Final[str] = "sap_dimension_type"
PROP_SAP_VARIABLES: Final[str] = "sap_variables"
# Boolean flag value paired with the sap_is_* keys above.
PROP_VALUE_TRUE: Final[str] = "true"

# sap_semantic values.
SEMANTIC_CURRENCY: Final[str] = "currency"
SEMANTIC_UNIT: Final[str] = "unit"

# sap_calendar_type values — also the keys of SAP_CALENDAR_TAG_URNS in tags.py.
CALENDAR_DATE: Final[str] = "date"
CALENDAR_YEAR: Final[str] = "year"
CALENDAR_MONTH: Final[str] = "month"
CALENDAR_WEEK: Final[str] = "week"
CALENDAR_QUARTER: Final[str] = "quarter"
CALENDAR_YEARMONTH: Final[str] = "yearmonth"

# --- CSN / CQN structural keys (SAP Core Schema Notation grammar) ------------
# The vocabulary the lineage walker and schema builders read out of a CSN body.
CSN_KEY_QUERY: Final[str] = "query"
CSN_KEY_ELEMENTS: Final[str] = "elements"
CSN_KEY_BUSINESS_LAYER: Final[str] = "businessLayerDefinitions"
# Top-level CSN container mapping object technical-name → its definition.
CSN_KEY_DEFINITIONS: Final[str] = "definitions"
CSN_SELECT: Final[str] = "SELECT"
CSN_FROM: Final[str] = "from"
CSN_COLUMNS: Final[str] = "columns"
CSN_REF: Final[str] = "ref"
CSN_AS: Final[str] = "as"
CSN_JOIN: Final[str] = "join"
CSN_ARGS: Final[str] = "args"
CSN_FUNC: Final[str] = "func"
CSN_XPR: Final[str] = "xpr"
CSN_CASE: Final[str] = "case"
CSN_CAST: Final[str] = "cast"
CSN_TYPE: Final[str] = "type"
CSN_TYPE_ASSOCIATION: Final[str] = "cds.Association"
# A Composition is a containment association (parent-owns-child); for lineage it
# behaves exactly like an Association — it names a target entity.
CSN_TYPE_COMPOSITION: Final[str] = "cds.Composition"
# An association/composition element's target entity (the qualified or bare
# technical name of the entity it navigates to).
CSN_ASSOC_TARGET: Final[str] = "target"
# UNION / INTERSECT / EXCEPT set operation: query.SET.args is a list of SELECTs.
CSN_SET: Final[str] = "SET"
CSN_REMOTE_SOURCE: Final[str] = "@remote.source"
# SQL views store the modeler's raw SQL here; graphical views emit a CSN tree.
CSN_KEY_SQL_EDITOR_QUERY: Final[str] = "@DataWarehouse.sqlEditor.query"

# ViewProperties.viewLanguage values emitted for the two view shapes above.
VIEW_LANGUAGE_SQL: Final[str] = "SQL"
VIEW_LANGUAGE_CSN: Final[str] = "CSN"
# CSN element attribute keys read when building schema fields from `elements`.
CSN_ATTR_LENGTH: Final[str] = "length"
CSN_ATTR_PRECISION: Final[str] = "precision"
CSN_ATTR_SCALE: Final[str] = "scale"
CSN_ATTR_LABEL: Final[str] = "@EndUserText.label"

# --- Analytic-model businessLayerDefinitions keys ---------------------------
BLD_SOURCE_MODEL: Final[str] = "sourceModel"
BLD_FACT_SOURCES: Final[str] = "factSources"
BLD_DIMENSION_SOURCES: Final[str] = "dimensionSources"
BLD_MEASURES: Final[str] = "measures"
BLD_ATTRIBUTES: Final[str] = "attributes"
BLD_VARIABLES: Final[str] = "variables"
BLD_DATA_ENTITY: Final[str] = "dataEntity"
BLD_KEY: Final[str] = "key"

# --- Catalog asset fields (SAP consumption catalog API response) ------------
CATALOG_FIELD_NAME: Final[str] = "name"
CATALOG_FIELD_LABEL: Final[str] = "label"
CATALOG_FIELD_METADATA_URL: Final[str] = "assetRelationalMetadataUrl"
CATALOG_FIELD_HAS_PARAMETERS: Final[str] = "hasParameters"
# The connection record fields ("name"/"typeId") are intentionally NOT hoisted:
# they key the ConnectionRecord TypedDict, whose .get() only type-narrows with a
# string literal, so they stay inline at the call sites.

# Catalog asset field whose truthiness (unreliably) routes a CSN fetch to
# analyticmodels vs views; see ALT_OBJECT_TYPE for the 404 fallback. Also emitted
# verbatim as a dataset custom property.
CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES: Final[str] = "supportsAnalyticalQueries"

# --- Emitted dataset custom-property keys / values --------------------------
PROP_SPACE_NAME: Final[str] = "spaceName"
PROP_SAP_DATASPHERE_SPACE: Final[str] = "sap_datasphere_space"
PROP_SAP_DATASPHERE_ASSET: Final[str] = "sap_datasphere_asset"
PROP_EXPOSED_FOR_CONSUMPTION: Final[str] = "exposed_for_consumption"
PROP_LOCAL_TABLE: Final[str] = "local_table"
PROP_VALUE_FALSE: Final[str] = "false"

# schemaField URN prefix, used to recover a field's parent dataset URN.
SCHEMA_FIELD_URN_PREFIX: Final[str] = "urn:li:schemaField:("

# --- dwaas-core list-endpoint record field ---------------------------------
# Every /dwaas-core/api/v1/spaces/{space}/{type} list entry is {"technicalName": X}.
FIELD_TECHNICAL_NAME: Final[str] = "technicalName"

# --- Data-flow / transformation-flow payload grammar ------------------------
# A flow definition is {"<objectType>": {<name>: {contents, sources, targets}}}.
# The consumer/producer processes carry the qualified table refs + column maps.
FLOW_KEY_CONTENTS: Final[str] = "contents"
FLOW_KEY_PROCESSES: Final[str] = "processes"
FLOW_KEY_COMPONENT: Final[str] = "component"
FLOW_KEY_METADATA: Final[str] = "metadata"
FLOW_KEY_CONFIG: Final[str] = "config"
# A table.consumer feeds the graph (an input); a table.producer writes it (an
# output). Matched by suffix so view/table variants both classify correctly.
FLOW_COMPONENT_CONSUMER_SUFFIX: Final[str] = ".consumer"
FLOW_COMPONENT_PRODUCER_SUFFIX: Final[str] = ".producer"
# Process config keys naming the referenced Datasphere/remote object.
FLOW_CONFIG_DWC_ENTITY: Final[str] = "dwcEntity"
FLOW_CONFIG_QUALIFIED_NAME: Final[str] = "qualifiedName"
FLOW_CONFIG_HANA_CONNECTION: Final[str] = "hanaConnection"
FLOW_CONFIG_CONNECTION_ID: Final[str] = "connectionID"
# Sentinel connectionID meaning "the tenant's own managed HANA Cloud" (local).
FLOW_LOCAL_CONNECTION_ID: Final[str] = "$DWC"
# Column mapping: {"target": <out col>, "expression": "\"<in col>\""}.
FLOW_CONFIG_ATTR_MAPPINGS: Final[str] = "attributeMappings"
FLOW_ATTR_MAP_TARGET: Final[str] = "target"
FLOW_ATTR_MAP_EXPRESSION: Final[str] = "expression"

# --- Replication-flow payload grammar ---------------------------------------
RF_KEY_SOURCE_SYSTEM: Final[str] = "sourceSystem"
RF_KEY_TARGET_SYSTEM: Final[str] = "targetSystem"
RF_KEY_TASKS: Final[str] = "replicationTasks"
RF_SYSTEM_CONNECTION_ID: Final[str] = "connectionId"
RF_SYSTEM_CONNECTION_TYPE: Final[str] = "connectionType"
RF_SYSTEM_CONTAINER: Final[str] = "container"
RF_TASK_SOURCE_OBJECT: Final[str] = "sourceObject"
RF_TASK_TARGET_OBJECT: Final[str] = "targetObject"
RF_TASK_TRANSFORM: Final[str] = "transform"
RF_OBJECT_NAME: Final[str] = "name"

# --- Remote-table federation annotations (on the CSN entity) ----------------
# A remote table's CSN entity names its external origin: the source connection
# plus a 0x7f-delimited "<db>\x7f<schema>\x7f<table>" locator.
REMOTE_CONNECTION_KEY: Final[str] = "@DataWarehouse.remote.connection"
REMOTE_ENTITY_KEY: Final[str] = "@DataWarehouse.remote.entity"
REMOTE_ENTITY_DELIMITER: Final[str] = "\x7f"

# Kind discriminators seen at flow[<name>]["kind"]; used only for defensive
# validation / logging, not routing (the object_type path segment routes).
FLOW_KIND_DATAFLOW: Final[str] = "sap.dis.dataflow"
FLOW_KIND_REPLICATIONFLOW: Final[str] = "sap.dis.replicationflow"
