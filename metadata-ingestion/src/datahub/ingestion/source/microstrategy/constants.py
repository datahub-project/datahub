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
