import re
from typing import Final

QVD_EXTENSION: Final = ".qvd"
# Every Qlik Cloud space's connection to its own data files.
DATA_FILES_CONNECTION: Final = "datafiles"
# An analytics app is a sense resource in the lineage graph, a script app a
# dataprep resource.
QLIK_APP_QRI_RE: Final = re.compile(r"^qri:app:(?:sense|dataprep)://([^#]+)")
# Entities or aspects per DataHub request.
ENTITY_CHECK_BATCH_SIZE: Final = 100
# A table Qlik renamed because its name was taken, such as `Orders-1`.
DUPLICATE_TABLE_NAME_RE: Final = re.compile(r"^(?P<name>.+)-\d+$")

BINARY_TABLES_UNKNOWN: Final = (
    "The tables the app's BINARY load brings in are not known: "
)
SCRIPT_TABLE_LINEAGE_SKIPPED: Final = (
    "Upstream lineage for this script table is skipped."
)
UNQUALIFIED_TABLES_SKIPPED: Final = (
    "Lineage to these tables is skipped. Set `database` and "
    "`default_schema` for their connection in "
    "`data_connection_to_platform_instance`."
)

CHECKPOINT_LOAD_FAILED_TITLE: Final = "Unable to load the last checkpoint of a Qlik app"
DATAHUB_LINEAGE_READ_FAILED_TITLE: Final = (
    "Unable to read Qlik dataset lineage from DataHub"
)
APP_PARTLY_READ_TITLE: Final = "Unable to fully read Qlik app"

APPS_READ_WITHOUT_DATA_TITLE: Final = "Qlik apps read without their data"
ENGINE_REFUSED_CONNECTIONS_TITLE: Final = "Qlik Engine refused connections"
COLUMN_LINEAGE_READ_FAILED_TITLE: Final = "Unable to read Qlik column lineage"
UNSEEN_SPACES_TITLE: Final = "Qlik spaces the API key can't see"
INCLUDE_READ_FAILED_TITLE: Final = "Unable to read included Qlik scripts"
UNRESOLVED_FILE_CONNECTIONS_TITLE: Final = "Qlik file connections have no known storage"
SHARES_NAMED_FROM_CONNECTIONS_TITLE: Final = (
    "Windows shares named from their Qlik connections"
)
DATASET_LINEAGE_READ_FAILED_TITLE: Final = "Unable to read Qlik dataset lineage"
UPSTREAM_GRAPH_READ_FAILED_TITLE: Final = "Unable to read Qlik upstream lineage graph"
CHANGE_CHECK_FAILED_TITLE: Final = "Unable to check whether Qlik apps changed"
OWNER_LOOKUP_FAILED_TITLE: Final = "Unable to look up Qlik owners"
QLIK_READ_FAILED_TITLE: Final = "Unable to read from Qlik"
CHECKPOINT_TOO_LARGE_TITLE: Final = "Qlik app checkpoint is too large to save"
