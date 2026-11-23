from datahub.utilities.str_enum import StrEnum

# General is Grafana's virtual root folder; no folder container exists for it.
# Grafana sends folderId 0 as an int; the models' coerce_numbers_to_str makes it "0".
GRAFANA_GENERAL_FOLDER_ID = "0"

# Grafana-specific ingestion stages
GRAFANA_BASIC_EXTRACTION = "Grafana Basic Dashboard Extraction"
GRAFANA_FOLDER_EXTRACTION = "Grafana Folder Extraction"
GRAFANA_DASHBOARD_EXTRACTION = "Grafana Dashboard Extraction"
GRAFANA_PANEL_EXTRACTION = "Grafana Panel Extraction"

HTTP_PERMISSION_STATUS_CODES = frozenset({401, 403})


class FolderPlacement(StrEnum):
    """Where a dashboard with a folderId ends up in one ingestion run."""

    PLACED = "placed"  # folder container emitted this run
    GENERAL = "general"  # Grafana's root folder; no container exists
    FILTERED = "filtered"  # folder excluded by folder_pattern
    NOT_INGESTED = "not_ingested"  # folder not returned by /api/folders


_LATER_PAGES_NOT_PLACED = (
    "dashboards in folders on this or later pages will not be placed in a folder"
)
FOLDER_FETCH_ERROR_TITLE = "Folder Fetch Error"
FOLDER_FETCH_ERROR_MESSAGE = (
    f"Failed to fetch folders on page; {_LATER_PAGES_NOT_PLACED}"
)
FOLDER_UNEXPECTED_RESPONSE_MESSAGE = (
    "Grafana /api/folders returned a page that is not a list; "
    f"{_LATER_PAGES_NOT_PLACED}"
)
FOLDER_PARSE_ERROR_TITLE = "Folder Parse Error"
FOLDER_PARSE_ERROR_MESSAGE = (
    "Skipped a folder Grafana returned in an unexpected shape; its dashboards "
    "are not placed in a folder on this run"
)
FOLDERS_UNPARSEABLE_TITLE = "Folders could not be parsed"
FOLDERS_UNPARSEABLE_MESSAGE = (
    f"Some folders could not be read and were skipped (see {FOLDER_PARSE_ERROR_TITLE}). "
    "So that their folder containers are not soft-deleted, this run removes no stale "
    "metadata of any kind, and later runs do the same until Grafana returns those "
    "folders with an id. Fix the folders in Grafana; setting "
    "stateful_ingestion.remove_stale_metadata to false stops these failures but "
    "also stops all stale-metadata removal."
)

DASHBOARD_FOLDER_NOT_INGESTED_TITLE = "Dashboard folder not ingested"
DASHBOARD_FOLDER_NOT_INGESTED_MESSAGE = (
    "The dashboard's folder was not ingested, so the dashboard was not placed in "
    "a folder. The folder may be nested (only top-level folders are listed by "
    "/api/folders), on a folder page that failed to load, or skipped because it "
    f"could not be parsed; check for {FOLDER_FETCH_ERROR_TITLE} or "
    f"{FOLDER_PARSE_ERROR_TITLE} entries in this report."
)
DASHBOARD_WITHOUT_FOLDER_ID_TITLE = "Dashboard has no folderId"
DASHBOARD_WITHOUT_FOLDER_ID_MESSAGE = (
    "Grafana returned no meta.folderId for the dashboard, so it was not placed in "
    "a folder. If this affects every dashboard, the Grafana API may have stopped "
    "sending folderId."
)
