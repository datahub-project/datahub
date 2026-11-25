import re
from typing import Final

QVD_EXTENSION = ".qvd"
# Every Qlik Cloud space's connection to its own data files.
DATA_FILES_CONNECTION = "datafiles"
# Lineage graph QRIs of Qlik dataset tables, `<dataset secureQri>#<table>`.
# An analytics app is a sense resource in the lineage graph, a script app a
# dataprep resource.
QLIK_APP_QRI_RE = re.compile(r"^qri:app:(?:sense|dataprep)://([^#]+)")
# Entities per DataHub request when checking that a skipped app's entities exist.
ENTITY_CHECK_BATCH_SIZE = 100
# A table Qlik renamed because its name was taken, such as `Orders-1`.
DUPLICATE_TABLE_NAME_RE = re.compile(r"^(?P<name>.+)-\d+$")

# Why the QVD writes of an app with an unresolved STORE are not all known.
UNRESOLVED_STORE = "a STORE path could not be resolved"
BINARY_TABLES_UNKNOWN: Final[str] = (
    "The tables the app's BINARY load brings in are not known: "
)
SCRIPT_TABLE_LINEAGE_SKIPPED: Final[str] = (
    "Upstream lineage for this script table is skipped."
)
UNQUALIFIED_TABLES_SKIPPED: Final[str] = (
    "Lineage to these tables is skipped. Set `database` and "
    "`default_schema` for their connection in "
    "`data_connection_to_platform_instance`."
)

CHECKPOINT_LOAD_FAILED_TITLE: Final[str] = (
    "Unable to load the last checkpoint of a Qlik app"
)
DATAHUB_LINEAGE_READ_FAILED_TITLE: Final[str] = (
    "Unable to read Qlik dataset lineage from DataHub"
)
APP_PARTLY_READ_TITLE: Final[str] = "Unable to fully read Qlik app"

APPS_READ_WITHOUT_DATA_TITLE: Final[str] = "Qlik apps read without their data"
ENGINE_REFUSED_CONNECTIONS_TITLE: Final[str] = "Qlik Engine refused connections"
COLUMN_LINEAGE_READ_FAILED_TITLE: Final[str] = "Unable to read Qlik column lineage"
UNSEEN_SPACES_TITLE: Final[str] = "Qlik spaces the API key can't see"
INCLUDE_READ_FAILED_TITLE: Final[str] = "Unable to read included Qlik scripts"
UNRESOLVED_FILE_CONNECTIONS_TITLE: Final[str] = (
    "Qlik file connections have no known storage"
)
DATASET_LINEAGE_READ_FAILED_TITLE: Final[str] = "Unable to read Qlik dataset lineage"
UPSTREAM_GRAPH_READ_FAILED_TITLE: Final[str] = (
    "Unable to read Qlik upstream lineage graph"
)
CHANGE_CHECK_FAILED_TITLE: Final[str] = "Unable to check whether Qlik apps changed"
OWNER_LOOKUP_FAILED_TITLE: Final[str] = "Unable to look up Qlik owners"
QLIK_READ_FAILED_TITLE: Final[str] = "Unable to read from Qlik"
CHECKPOINT_TOO_LARGE_TITLE: Final[str] = "Qlik app checkpoint is too large to save"
