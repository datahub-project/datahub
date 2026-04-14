"""
MicroStrategy API constants.
"""

# Platform name
PLATFORM_NAME = "microstrategy"

# IServer REST errors (JSON body: iServerCode)
ISERVER_PROJECT_UNAVAILABLE = -2147209151  # project not loaded  → fail fast
ISERVER_PROJECT_UNAVAILABLE_DETAIL = (
    "Project unavailable or not loaded on IServer (idle/unloaded). "
    "Try another project or load the project on the server; "
    "or set include_unloaded_projects: true only if you intend to ingest idle projects."
)
ISERVER_CUBE_NOT_PUBLISHED = -2147072488  # cube not in memory  → definition only
ISERVER_DYNAMIC_SOURCING_CUBE = -2147212800  # attr form cache cube → definition only

# Search API object types (/api/searches/results type=...)
SEARCH_OBJECT_TYPE_CUBE = 776
SEARCH_OBJECT_TYPE_WAREHOUSE_TABLE = 53

# Dashboard/dossier subtype codes confirmed via live API testing
SUBTYPE_LEGACY_DOCUMENT = 14081  # → GET /api/documents/{id}/definition
SUBTYPE_MODERN_DOSSIER = 14336  # → GET /api/v2/dossiers/{id}/definition
SUBTYPE_SKIP = frozenset({14082, 14087, 14088})  # themes, agent templates — no content
