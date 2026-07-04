import re

MICROSTRATEGY_PLATFORM = "microstrategy"

# REST endpoint paths that are referenced in more than one place -- the request
# plus its response-shape/error check, or the re-auth guard -- are centralized
# so the two uses cannot drift apart. Single-use paths stay inline.
MSTR_API_AUTH_PREFIX = "/api/auth/"
MSTR_API_AUTH_LOGIN = "/api/auth/login"
MSTR_API_AUTH_LOGOUT = "/api/auth/logout"
MSTR_API_PROJECTS = "/api/projects"
MSTR_API_OBJECT = "/api/objects/{object_id}"
MSTR_API_METADATA_SEARCHES = "/api/metadataSearches/results"
MSTR_API_SEARCHES = "/api/searches/results"

MSTR_LOGIN_MODE_STANDARD = 1
MSTR_LOGIN_MODE_GUEST = 8

MSTR_OBJECT_TYPE_DASHBOARD = 55
MSTR_OBJECT_TYPE_REPORT = 3
# Documents share object type 55 with dossiers; the subtype distinguishes them.
MSTR_OBJECT_SUBTYPE_DOCUMENT = "14081"

MEASURE_TAG_URN = "urn:li:tag:Measure"
DIMENSION_TAG_URN = "urn:li:tag:Dimension"
TEMPORAL_TAG_URN = "urn:li:tag:Temporal"

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
