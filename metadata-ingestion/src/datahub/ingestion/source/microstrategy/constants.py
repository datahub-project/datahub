import re
from collections.abc import Sequence
from typing import Dict, Final, FrozenSet, Tuple

from datahub.metadata.urns import GlossaryTermUrn, TagUrn
from datahub.utilities.str_enum import StrEnum

MICROSTRATEGY_PLATFORM = "microstrategy"

# REST endpoint paths that are referenced in more than one place -- the request
# plus its response-shape/error check, or the re-auth guard -- are centralized
# so the two uses cannot drift apart. Single-use paths stay inline.
# Modeling-service header documented as the report instance id. Sending an
# executed instance is the vendor's suggested route to metric expressions that
# the static definition omits.
MSTR_MS_INSTANCE_HEADER = "X-MSTR-MS-Instance"
MSTR_AUTH_TOKEN_HEADER = "X-MSTR-AuthToken"
MSTR_PROJECT_ID_HEADER = "X-MSTR-ProjectID"
MSTR_HEADER_ACCEPT = "Accept"
MSTR_HEADER_CONTENT_TYPE = "Content-Type"
MSTR_CONTENT_TYPE_JSON = "application/json"
MSTR_HTTP_SCHEME_HTTPS = "https://"
MSTR_HTTP_SCHEME_HTTP = "http://"
MSTR_TEXT_ENCODING = "utf-8"
MSTR_IO_THREAD_NAME_PREFIX = "mstr-io"
MSTR_UNRESOLVED_FOLDER_LABEL = "unresolved"
DATAHUB_GRAPH_SESSION_ATTR = "_session"

MSTR_API_AUTH_PREFIX = "/api/auth/"
MSTR_API_AUTH_LOGIN = "/api/auth/login"
MSTR_API_AUTH_LOGOUT = "/api/auth/logout"
MSTR_API_PROJECTS = "/api/projects"
MSTR_API_OBJECT = "/api/objects/{object_id}"
MSTR_API_METADATA_SEARCHES = "/api/metadataSearches/results"
MSTR_API_SEARCHES = "/api/searches/results"
MSTR_API_FOLDERS_PREDEFINED = "/api/folders/preDefined"

# EnumDSSXMLFolderNames predefined folder type whose MicroStrategy-assigned label
# ("Shared Reports") differs from its raw metadata folder name ("Reports", nested
# under the project's "Public Objects" system folder). Resolved via
# MSTR_API_FOLDERS_PREDEFINED so the browse hierarchy matches what Strategy Web
# shows instead of the internal metadata name.
MSTR_FOLDER_TYPE_SHARED_REPORTS = 7
MSTR_PREDEFINED_FOLDER_LABELS: Dict[int, str] = {
    MSTR_FOLDER_TYPE_SHARED_REPORTS: "Shared Reports",
}
# System containers every object's ancestor chain passes through but Strategy
# Web never displays: the project root folder (named after the project, so it
# renders as a duplicate project level) and "Public Objects". Resolved by id
# via the same predefined-folders call and omitted from the browse hierarchy;
# children re-parent to the nearest kept ancestor.
MSTR_FOLDER_TYPE_PUBLIC_OBJECTS = 1
MSTR_FOLDER_TYPE_PROJECT_ROOT = 39
MSTR_PREDEFINED_HIDDEN_FOLDER_TYPES = frozenset(
    {MSTR_FOLDER_TYPE_PUBLIC_OBJECTS, MSTR_FOLDER_TYPE_PROJECT_ROOT}
)
# EnumDSSXMLFolderNames profile folder types. Both resolve to the LOGGED-IN
# principal's own folders: 19 is its profile folder (the per-user tree that
# holds "My Reports", "My Objects", "My Answers", ...), 20 is its "My Reports".
# The enum has no value for the "Profiles" system folder that holds every
# user's profile folder, so that root is resolved as the type-19 folder's
# parent (via GET /api/objects/{id}?type=8 ancestors); any object with that
# ancestor is personal, whichever user owns it.
MSTR_FOLDER_TYPE_PROFILE_OBJECTS = 19
MSTR_FOLDER_TYPE_PROFILE_REPORTS = 20
MSTR_PERSONAL_FOLDER_TYPES = (
    MSTR_FOLDER_TYPE_PROFILE_OBJECTS,
    MSTR_FOLDER_TYPE_PROFILE_REPORTS,
)
# EnumDSSXMLObjectTypes: Folder.
MSTR_OBJECT_TYPE_FOLDER = 8
# Fallback when the Profiles root id cannot be resolved (older server, no
# privilege): an ancestor folder whose name equals one of these
# (case-insensitive) marks an object as personal. The resolved label of the
# type-20 folder is added when available.
MSTR_PERSONAL_FOLDER_NAMES = frozenset({"profiles", "my reports"})

MSTR_LOGIN_MODE_STANDARD = 1
MSTR_LOGIN_MODE_GUEST = 8

MSTR_OBJECT_TYPE_DASHBOARD = 55
MSTR_OBJECT_TYPE_REPORT = 3
# Report Services documents and dossiers share object type 55 AND subtype
# 14081 (DssXmlSubTypeReportWritingDocument): search results carry the same
# type and subtype for both, and only viewMedia tells them apart (mstrio-py
# lists each kind by searching this subtype and filtering on view_media). The
# subtype still rules out the other type-55 subtypes (definition, theme, bot).
MSTR_OBJECT_SUBTYPE_DOCUMENT = "14081"
# EnumDSSXMLViewMedia flags that mark a type-55 object as a dossier: bits 30
# and 29 set, bits 31 and 27 clear (mstrio-py helper.is_dashboard). Anything
# else with the document subtype is a Report Services document.
MSTR_VIEW_MEDIA_DOSSIER_MASK = 0xE800_0000
MSTR_VIEW_MEDIA_DOSSIER_FLAGS = 0x6000_0000
# EnumDSSXMLObjectTypes: Metric, Consolidation. Used for project-wide metric
# search and to recognize a consolidation-type expression token. Metric is
# DssXmlTypeMetric = 4 (mstrio-py's ObjectTypes enum: METRIC = 4,
# AGG_METRIC = 7); searching type 7 returns no plain metrics at all.
MSTR_OBJECT_TYPE_METRIC = 4
# EnumDSSXMLObjectTypes: AggMetric. Unused; kept so 7 is not read as Metric.
MSTR_OBJECT_TYPE_AGG_METRIC = 7
MSTR_OBJECT_TYPE_CONSOLIDATION = 47

# Which REST endpoint supplied a report's derived metric definitions: the
# Modeling service (GET /api/model/reports/{id}, carries expressions) or the
# v2 report definition (GET /api/v2/reports/{id}, names derived metrics but
# omits their formulas). Recorded per field so a formula-less description can
# say which endpoint answered.
MSTR_DEFINITION_ENDPOINT_MODEL = "model"
MSTR_DEFINITION_ENDPOINT_V2 = "v2"
# Document/dossier-level derived metrics read from GET /api/model/documents/{id}.
MSTR_DEFINITION_ENDPOINT_MODEL_DOCUMENT = "model_document"
# The formula came from GET /api/model/metrics/{id} for a metric the report
# definition only named (an embedded, report-level derived metric).
MSTR_DEFINITION_ENDPOINT_METRIC_MODEL = "metric_model"

MEASURE_TAG_URN = "urn:li:tag:Measure"
DIMENSION_TAG_URN = "urn:li:tag:Dimension"
TEMPORAL_TAG_URN = "urn:li:tag:Temporal"
DERIVED_TAG_URN = "urn:li:tag:Derived"

MSTR_PROPERTY_PROJECT_ID = "microstrategyProjectId"
MSTR_PROPERTY_REPORT_ID = "microstrategyReportId"
MSTR_PROPERTY_DASHBOARD_COUNT = "microstrategyDashboardCount"
MSTR_PROPERTY_DASHBOARD_IDS = "microstrategyDashboardIds"
MSTR_PROPERTY_REPORT_PROMPT_COUNT = "microstrategyReportPromptCount"
MSTR_PROPERTY_REPORT_HAS_FILTER = "microstrategyReportHasFilter"
MSTR_PROPERTY_REPORT_SOURCE_ID = "microstrategyReportSourceId"
MSTR_PROPERTY_REPORT_SOURCE_NAME = "microstrategyReportSourceName"

# Stamped on emitted properties so a skip is trusted only if DataHub still has it.
MSTR_PROP_FINGERPRINT = "microstrategyFingerprint"
ENTITY_CHECK_BATCH_SIZE = 100
MSTR_UNCHANGED_OBJECTS_JOB_ID = "microstrategy_unchanged_objects"
# Config fields that change how a run reads, not what it emits.
FINGERPRINT_EXCLUDED_CONFIG_FIELDS: FrozenSet[str] = frozenset(
    {
        "stateful_ingestion",
        "skip_unchanged_objects",
        "cache_memory_entries",
        "progress_log_interval_dashboards",
        "progress_log_interval_seconds",
        "max_workers",
    }
)


class MicroStrategyReadOutcome(StrEnum):
    COMPLETE = "complete"
    # Read, but an API failure left part of its lineage out.
    INCOMPLETE = "incomplete"
    # Not read; its last read still stands.
    FAILED = "failed"


# Shared references rather than an object's own entities.
SHARED_REFERENCE_ENTITY_TYPES: FrozenSet[str] = frozenset(
    {TagUrn.ENTITY_TYPE, GlossaryTermUrn.ENTITY_TYPE}
)

# Sentinel MicroStrategy returns for "no object", which must never be emitted as
# a real object id.
MSTR_NULL_OBJECT_ID = "00000000000000000000000000000000"

# API response field-name fallbacks. MicroStrategy carries the same logical
# field under different keys across server versions, so every lookup tries an
# ordered list of aliases. Declaring each alias set once here keeps the model
# normalizers that share them from drifting apart.
MSTR_KEYS_ID = ("id", "objectId")
MSTR_KEYS_NAME = ("name", "title")
MSTR_KEYS_KEY_ID = ("key", "id")
MSTR_KEYS_DISPLAY_NAME = ("name", "title", "displayName")
MSTR_KEYS_OWNER = ("username", "name", "id")
MSTR_KEYS_PROJECT_ID = ("id", "projectId", "project_id")
MSTR_KEYS_PROJECT_NAME = ("name", "projectName")
MSTR_KEYS_DATASET_OBJECT_ID = ("id", "objectId", "datasetId")
MSTR_KEYS_DATASET_ID = ("id", "objectId", "datasetId", "dataSetId")
MSTR_KEYS_DATASOURCE_ID = ("id", "objectId", "datasourceId")
MSTR_KEYS_DRIVER_TYPE = ("driverType", "driver")
MSTR_KEYS_DATASOURCE_TYPE = ("datasourceType", "sourceType", "type")
MSTR_KEYS_DBMS_NAME = ("name", "type")
MSTR_KEYS_DATABASE_TYPE = ("type", "databaseType")
MSTR_KEYS_DATABASE_VERSION = ("version", "databaseVersion")
MSTR_KEYS_DATABASE_NAME = ("databaseName", "database", "catalog")
MSTR_KEYS_DATABASE_NAME_NESTED = ("name", "databaseName", "catalog")
MSTR_KEYS_SCHEMA_NAME = ("schemaName", "schema", "databaseSchema")
MSTR_KEYS_SCHEMA_NAME_NESTED = ("schema", "schemaName", "databaseSchema")
MSTR_KEYS_VISUALIZATION_KEY = (
    "key",
    "id",
    "objectId",
    "visualizationKey",
    "nodeKey",
    "definitionKey",
)
MSTR_KEYS_VISUALIZATION_TYPE = ("type", "visualizationType")
MSTR_KEYS_SOURCE_OBJECT = (
    "dataSource",
    "datasource",
    "source",
    "sourceObject",
    "cube",
    "dataset",
)
MSTR_KEYS_SOURCE_OBJECT_ID = (
    "id",
    "objectId",
    "sourceId",
    "dataSourceId",
    "datasetId",
    "cubeId",
)
MSTR_KEYS_SOURCE_ID = (
    "sourceId",
    "dataSourceId",
    "datasourceId",
    "datasetId",
    "cubeId",
)
MSTR_KEYS_SOURCE_NAME = (
    "sourceName",
    "dataSourceName",
    "datasourceName",
    "datasetName",
    "cubeName",
)
MSTR_KEYS_DATASOURCE_REFERENCE = (
    "sourceWarehouse",
    "warehouse",
    "sourceDatasource",
    "sourceDataSource",
    "dataSource",
    "datasource",
)
MSTR_KEYS_LIST_CONTAINERS = ("items", "objects", "prompts")
MSTR_KEYS_FOLDER_PATH = ("path", "name")

# Lowercased container keys whose children are dataset references.
MSTR_DATASET_CONTAINER_KEYS = frozenset(
    {
        "dataset",
        "datasets",
        "datasetid",
        "datasetids",
        "datasetkey",
        "datasources",
        "datasource",
    }
)
# Lowercased parent keys / object types that mark metric & attribute object ids.
# templateMetrics is a container key in dossier definitions, not an object type.
MSTR_OBJECT_ID_PARENT_KEYS = frozenset(
    {"metric", "metrics", "attribute", "attributes", "templatemetrics"}
)
MSTR_OBJECT_TYPES = frozenset({"metric", "attribute"})
# Lowercased grid unit types used when walking a runtime grid definition: the
# metrics container column, the metric elements inside it, and attribute units.
MSTR_GRID_TEMPLATE_METRICS_TYPE = "templatemetrics"
MSTR_GRID_METRIC_ELEMENT_TYPE = "metric"
MSTR_GRID_ATTRIBUTE_TYPE = "attribute"
# Grid axes in the order Strategy renders their headers (row attributes on the
# left, then column units, then page-by); a compound grid's columnSets follow.
MSTR_GRID_AXES = ("rows", "columns", "pageBy")
MSTR_GRID_COLUMN_SETS_KEY = "columnSets"

# Pre-compiled regexes. Matched case-insensitively; declared once so the
# hot-path normalizers reuse a single compiled pattern.
MSTR_DATASET_KEY_RE = re.compile("dataset", re.IGNORECASE)


def _compile_connection_param_re(*param_names: str) -> re.Pattern[str]:
    alternatives = "|".join(re.escape(name) for name in param_names)
    # Delimiters cover ODBC-style (;KEY=value) and JDBC URL query-string style
    # (?db=value&schema=value) connection strings.
    return re.compile(
        rf"(?:^|[;,&?\s])(?:{alternatives})\s*=\s*([^;&,\s}}]+)",
        re.IGNORECASE,
    )


MSTR_DATABASE_PARAM_RE = _compile_connection_param_re(
    "DATABASE", "databaseName", "db", "catalog"
)
MSTR_SCHEMA_PARAM_RE = _compile_connection_param_re(
    "schema", "currentSchema", "CURRENT_SCHEMA", "searchpath", "search_path"
)

# Generic BI vocabulary excluded from the name-token overlap heuristic used to
# infer visualization -> dataset lineage. Only genuinely non-discriminating
# words belong here; business terms (e.g. "SALES") must NOT be added because
# they legitimately distinguish datasets on real tenants.
MSTR_LINEAGE_STOP_WORDS = frozenset(
    {
        "AND",
        "DASHBOARD",
        "DATA",
        "DATASET",
        "REPORT",
        "TOTAL",
        "VISUALIZATION",
    }
)

# MicroStrategy SQL views append non-SQL commentary after the final pass
# ("[Analytical engine calculation steps: ...]", "with parameters: 1") and
# include DROP statements for its volatile tables. None of these carry lineage,
# so they are skipped instead of counted as parse failures. The "with
# parameters" match requires the trailing colon so a legitimate CTE named
# "parameters" is never skipped.
MSTR_LINEAGE_IRRELEVANT_STATEMENT_RE = re.compile(
    r"^\s*(drop\s|\[|with\s+parameters\s*:)",
    re.IGNORECASE,
)
# Captures the target name of a CREATE ... TABLE statement, tolerating any
# modifiers between CREATE and TABLE (VOLATILE, MULTISET, SET, GLOBAL TEMPORARY,
# TEMP, ...). Group 1 is the (optionally dotted/quoted) created table name; its
# leaf identifies the intermediate temp table to collapse for SQL-view lineage.
MSTR_CREATE_TABLE_NAME_RE = re.compile(
    r"^\s*create\b[\s\S]*?\btable\s+(?:if\s+not\s+exists\s+)?([A-Za-z0-9_$#.\"]+)",
    re.IGNORECASE,
)
# Matches a statement that returns rows (bare SELECT or a CTE-led SELECT). Only
# these can define a SQL-view dataset's output columns.
MSTR_SELECT_STATEMENT_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
# Non-emitted sentinel output table. Wrapping a SQL view's final SELECT as
# `CREATE TABLE <sentinel> AS <select>` gives the parser a concrete downstream,
# so it surfaces the final projection's column lineage (temp tables collapsed).
MSTR_SQL_VIEW_OUTPUT_SENTINEL = "__datahub_mstr_sql_view_output__"

# Alphanumeric run used to tokenize object names for the lineage overlap
# heuristic.
MSTR_NAME_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
# Collapses non-alphanumeric runs to a single underscore when normalizing a
# datasource source-type into a comparable key.
MSTR_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
# SQL identifier (column/table token) extractor for expression parsing.
MSTR_SQL_IDENTIFIER_RE = re.compile(r"[A-Za-z_][\w$#]*")
# Metric formulas reference sibling catalog objects as `{Object Name}` tokens
# in the Modeling API's expression text, e.g. `({Net Amt} - {Net Amt LY}) /
# Abs({Net Amt LY})`, and as `[Object Name]` in Strategy Web's editor syntax,
# e.g. `([Net Amt] / [Plan Amt]) - 1`; report-level derived metric formulas
# can surface in either form depending on the endpoint that returned them.
MSTR_METRIC_REFERENCE_RE = re.compile(r"\{([^{}]+)\}|\[([^\[\]]+)\]")
# Object type / subtype words that identify an embedded expression-bearing
# node as a metric definition, and words that rule one out (filters and
# thresholds also carry expression-like text in report definitions).
MSTR_METRIC_DEFINITION_TYPE_WORDS = ("metric",)
MSTR_NON_METRIC_DEFINITION_TYPE_WORDS = (
    "attribute",
    "consolidation",
    "fact",
    "filter",
    "prompt",
    "qualification",
    "threshold",
)
# A 32-char hex string is MicroStrategy's canonical object-id form.
MSTR_HEX_OBJECT_ID_RE = re.compile(r"[0-9A-Fa-f]{32}")
# Collapses internal whitespace runs to a single space.
MSTR_WHITESPACE_RE = re.compile(r"\s+")
# Collapses repeated dots when normalizing a qualified table name.
MSTR_DOT_COLLAPSE_RE = re.compile(r"\.+")

# Maps normalized MicroStrategy datasource/database type tokens (see
# _normalize_source_type) to DataHub platform names. Keyed by the normalized
# form so both "SQL Server" and "sql_server" resolve to "mssql".
MSTR_SOURCE_TYPE_TO_DATAHUB_PLATFORM = {
    "athena": "athena",
    "big_query": "bigquery",
    "bigquery": "bigquery",
    "db2": "db2",
    "google_bigquery": "bigquery",
    "microsoft_sql_server": "mssql",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgre_sql": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "red_shift": "redshift",
    "redshift": "redshift",
    "snow_flake": "snowflake",
    "snowflake": "snowflake",
    "sql_server": "mssql",
    "sqlserver": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
}

# Number of parts a warehouse table's fully-qualified name carries on each
# platform, so model-derived table urns match the native connector's. Most
# warehouses use database.schema.table (3); MySQL/MariaDB use database.table (2)
# because they have no schema layer. Platforms absent here default to 3.
MSTR_PLATFORM_QUALIFIED_NAME_PARTS = {
    "mysql": 2,
    "mariadb": 2,
}
MSTR_DEFAULT_QUALIFIED_NAME_PARTS = 3

# Keywords and function names excluded when extracting column identifiers from
# model expression text (e.g. `SUM(QTY_SOLD * UNIT_PRICE)` must yield only the
# column names, never the function tokens).
MSTR_SQL_EXPRESSION_KEYWORDS = frozenset(
    {
        "abs",
        "and",
        "as",
        "avg",
        "between",
        "by",
        "case",
        "cast",
        "ceil",
        "ceiling",
        "coalesce",
        "concat",
        "count",
        "cross",
        "cume_dist",
        "delete",
        "dense_rank",
        "distinct",
        "else",
        "end",
        "exp",
        "first",
        "first_value",
        "floor",
        "full",
        "greatest",
        "group",
        "having",
        "if",
        "iif",
        "ifnull",
        "in",
        "inner",
        "into",
        "is",
        "isnull",
        "join",
        "lag",
        "last",
        "last_value",
        "lead",
        "least",
        "left",
        "length",
        "ln",
        "log",
        "lower",
        "lpad",
        "ltrim",
        "max",
        "median",
        "min",
        "mod",
        "not",
        "ntile",
        "null",
        "nullif",
        "nvl",
        "on",
        "or",
        "order",
        "outer",
        "over",
        "partition",
        "percentile",
        "percentile_cont",
        "percentile_disc",
        "power",
        "rank",
        "replace",
        "right",
        "round",
        "row_number",
        "rpad",
        "rtrim",
        "select",
        "set",
        "sign",
        "sqrt",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "stdev",
        "substr",
        "substring",
        "sum",
        "then",
        "trim",
        "trunc",
        "update",
        "upper",
        "var",
        "var_pop",
        "var_samp",
        "variance",
        "when",
        "where",
        "with",
    }
)

# --- Platform Analytics usage cube ------------------------------------------
# Object names in the shipped Platform Analytics aggregate cube. GUIDs are
# stable across shipped versions in practice but undocumented as a contract, so
# everything is resolved by name from the live cube definition.
MSTR_USAGE_DATE_ATTRIBUTE_NAME = "Date"
MSTR_USAGE_PROJECT_ATTRIBUTE_NAME = "Project"
MSTR_USAGE_OBJECT_ATTRIBUTE_NAME = "Object"
MSTR_USAGE_USER_ATTRIBUTE_NAME = "User"
MSTR_USAGE_EXECUTIONS_METRIC_NAMES = ("Num Executions", "Count Actions")
MSTR_USAGE_GUID_FORM_NAME = "GUID"
MSTR_USAGE_NAME_FORM_NAME = "Name"
MSTR_USAGE_ID_FORM_NAME = "ID"
MSTR_USAGE_USER_FORM_NAMES = ("Login", "Name")

# Intelligent cube (776) and super cube (779) subtypes. Cubes share object type
# 3 with reports; only the subtype tells them apart. Used to skip plain reports
# in the usage-cube quick search and to decide whether a dataset object can be
# opened in Library (reports can, cubes cannot).
MSTR_CUBE_SUBTYPES = frozenset({"776", "779"})
MSTR_USAGE_CUBE_SUBTYPES = MSTR_CUBE_SUBTYPES
# Grid report subtype (EnumDSSXMLObjectSubTypes DssXmlSubTypeReportGrid). A
# Report Services document's datasets are grid reports, and only a report has
# a report SQL view, so the document SQL-lineage path opens report instances
# for datasets of this subtype alone; cubes and other subtypes are skipped and
# counted. The only subtype observed as a document dataset so far.
MSTR_OBJECT_SUBTYPE_REPORT_GRID = "768"

# Date form values are rendered strings whose format follows the Intelligence
# Server locale; month-first is the shipped default.
MSTR_USAGE_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p")

# Timestamp formats returned by object metadata (creation/modification dates).
# MicroStrategy renders a trailing 'Z' that strptime cannot parse as a zone, so
# callers normalize it to +0000 before matching against these.
MSTR_OBJECT_TIMESTAMP_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
)

# Structured properties that place a dossier visualization chart in its
# chapter and page (emit_chapter_page_structured_properties). Each entry is
# the property's qualified name (the id of its urn:li:structuredProperty:
# URN), display name, DataHub data type and description. The definitions are
# emitted in this order, which is the order the UI lists them in: chapter
# above page, then the page's position within its chapter.
MSTR_STRUCTURED_PROPERTY_CHAPTER = "io.acryl.microstrategy.chapter"
MSTR_STRUCTURED_PROPERTY_PAGE = "io.acryl.microstrategy.page"
MSTR_STRUCTURED_PROPERTY_PAGE_POSITION = "io.acryl.microstrategy.pagePosition"
MSTR_CHAPTER_PAGE_STRUCTURED_PROPERTIES: Tuple[Tuple[str, str, str, str], ...] = (
    (
        MSTR_STRUCTURED_PROPERTY_CHAPTER,
        "MicroStrategy Chapter",
        "string",
        "Name of the dossier chapter the visualization sits in.",
    ),
    (
        MSTR_STRUCTURED_PROPERTY_PAGE,
        "MicroStrategy Page",
        "string",
        "Name of the dossier page the visualization sits on.",
    ),
    (
        MSTR_STRUCTURED_PROPERTY_PAGE_POSITION,
        "MicroStrategy Page Position",
        "number",
        "1-based position of the visualization's page within its chapter.",
    ),
)

MSTR_MODEL_TABLE_FIELDS = "information,physicalTable,attributes,facts"
# Requested only for a project with several warehouses.
MSTR_MODEL_TABLE_DATASOURCE_FIELD = "primaryDataSource"
MSTR_UNKNOWN_DATASOURCE_TYPE = "unknown"
MSTR_DATASOURCE_MAPPING_HINT = (
    "Set datasource_platform_mapping, keyed by connection or datasource name, "
    "for datasources of a type the connector does not recognize."
)


# per_report attaches usage to the report's dataset; otherwise only dashboards and charts.
class UsageTargetKind(StrEnum):
    DASHBOARD = "dashboard"
    CHART = "chart"
    DATASET = "dataset"


class MicroStrategyDatasetIdentity(StrEnum):
    PER_DASHBOARD = "per_dashboard"
    PER_REPORT = "per_report"


class MicroStrategyPhase(StrEnum):
    DASHBOARD = "dashboard"
    DASHBOARD_SEARCH = "dashboard_search"
    SQL_PARSE = "sql_parse"
    WAREHOUSE_FIELD_LOOKUP = "warehouse_field_lookup"
    USAGE = "usage"
    SEMANTIC_MODEL = "semantic_model"
    SOURCE_WAREHOUSES = "source_warehouses"
    FOLDER_RESOLUTION = "folder_resolution"
    DATASET_OBJECT_LOOKUP = "dataset_object_lookup"
    REPORT_DERIVED_METRICS = "report_derived_metrics"
    DOCUMENT_DERIVED_METRICS = "document_derived_metrics"
    REPORT_DERIVED_METRIC_DEFINITIONS = "report_derived_metric_definitions"
    REPORT_MODEL_INSTANCE = "report_model_instance"
    REPORTS = "reports"
    DASHBOARD_DEFINITION = "dashboard_definition"
    MODEL_DOCUMENT = "model_document"
    VISUALIZATION_BINDINGS = "visualization_bindings"
    REPORT_DEFINITION = "report_definition"
    MODEL_TABLES = "model_tables"
    MODEL_LINEAGE_INDEX = "model_lineage_index"
    DASHBOARD_DEPENDENCIES = "dashboard_dependencies"
    METRIC_EXPRESSIONS = "metric_expressions"
    METRIC_MODEL_FETCH = "metric_model_fetch"
    VISUALIZATION_DETAILS = "visualization_details"
    WAREHOUSE_LINEAGE = "warehouse_lineage"
    DOCUMENT_WAREHOUSE_LINEAGE = "document_warehouse_lineage"
    REPORT_SQL_FETCH = "report_sql_fetch"
    DASHBOARD_INSTANCE = "dashboard_instance"
    INSTANCE_CLEANUP = "instance_cleanup"


class MicroStrategyCache(StrEnum):
    SQL_PARSE = "sql_parse"
    REPORT_SQL = "report_sql"
    REPORT_OBJECT = "report_object"
    REPORT_DERIVED_METRIC = "report_derived_metric"
    METRIC_MODEL = "metric_model"
    REPORT_DEFINITION = "report_definition"


class MicroStrategyLineageLookup(StrEnum):
    WAREHOUSE_CONTEXT = "warehouse_context"
    MODEL_TABLES = "model_tables"
    MODEL_LINEAGE_INDEX = "model_lineage_index"


# Isolated metric model 500s are expected; a long run of them means the
# Modeling service is down and every further call would fail too.
MSTR_METRIC_MODEL_CONSECUTIVE_FAILURE_LIMIT = 20

# Metric search and /api/model/tables are slow on large projects.
MSTR_MODEL_API_MIN_TIMEOUT_SECONDS = 120

MSTR_TELEMETRY_NO_PHASE = "(no phase)"
MSTR_TELEMETRY_ERROR_STATUS = "error"

MSTR_DASHBOARD_SECONDS_BUCKET_BOUNDS: Final[Sequence[int]] = (
    10,
    30,
    60,
    120,
    300,
    600,
    1800,
)

MSTR_SQLGLOT_LOGGER = "sqlglot"
# One warning per generated statement; they fill the executor's bounded log.
MSTR_SQLGLOT_SUPPRESSED_PREFIXES: Final[Sequence[str]] = ("Unsupported property",)

MSTR_ENDPOINT_ID_PLACEHOLDER = "/{id}"
# 32-hex object ids and UUIDs, so one endpoint is one row.
MSTR_ENDPOINT_OBJECT_ID_SEGMENT_RE = re.compile(r"/[0-9A-Fa-f]{32}(?=/|$)")
MSTR_ENDPOINT_UUID_SEGMENT_RE = re.compile(
    r"/[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}(?=/|$)"
)
# Not 32-hex object ids, so a literal match would make one row per visualization.
# The replacement keeps group 1, the segment kind.
MSTR_ENDPOINT_DOSSIER_KEY_SEGMENT_RE = re.compile(
    r"/(chapters|pages|visualizations)/[^/]+"
)
MSTR_ENDPOINT_KEY_REPLACEMENT = r"/\1/{key}"
