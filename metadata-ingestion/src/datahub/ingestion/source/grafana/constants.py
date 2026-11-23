# General is Grafana's virtual root folder; no folder container exists for it.
# Grafana sends folderId 0 as an int; the models' coerce_numbers_to_str makes it "0".
GRAFANA_GENERAL_FOLDER_ID = "0"

# Grafana-specific ingestion stages
GRAFANA_BASIC_EXTRACTION = "Grafana Basic Dashboard Extraction"
GRAFANA_FOLDER_EXTRACTION = "Grafana Folder Extraction"
GRAFANA_DASHBOARD_EXTRACTION = "Grafana Dashboard Extraction"
GRAFANA_PANEL_EXTRACTION = "Grafana Panel Extraction"

HTTP_PERMISSION_STATUS_CODES = (401, 403)

FOLDER_FETCH_ERROR_TITLE = "Folder Fetch Error"
FOLDER_FETCH_ERROR_MESSAGE = (
    "Failed to fetch folders on page; dashboards in folders on this or later "
    "pages will not be placed in a folder"
)
FOLDER_UNEXPECTED_RESPONSE_MESSAGE = (
    "Grafana /api/folders returned a page that is not a list; dashboards in "
    "folders on this or later pages will not be placed in a folder"
)
FOLDER_PARSE_ERROR_TITLE = "Folder Parse Error"
FOLDER_PARSE_ERROR_MESSAGE = (
    "Skipped a folder Grafana returned in an unexpected shape; its dashboards "
    "will not be placed in a folder"
)

DASHBOARD_FOLDER_NOT_INGESTED_TITLE = "Dashboard folder not ingested"
DASHBOARD_FOLDER_NOT_INGESTED_MESSAGE = (
    "The dashboard's folder was not ingested, so the dashboard was not placed in "
    "a folder. The folder may be nested (only top-level folders are listed by "
    "/api/folders), on a folder page that failed to load, or skipped because it "
    "could not be parsed; check for Folder Fetch Error or Folder Parse Error "
    "entries in this report."
)
