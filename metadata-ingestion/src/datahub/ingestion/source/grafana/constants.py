# General is Grafana's virtual root folder; no folder container exists for it.
GRAFANA_GENERAL_FOLDER_ID = "0"

# Grafana-specific ingestion stages
GRAFANA_BASIC_EXTRACTION = "Grafana Basic Dashboard Extraction"
GRAFANA_FOLDER_EXTRACTION = "Grafana Folder Extraction"
GRAFANA_DASHBOARD_EXTRACTION = "Grafana Dashboard Extraction"
GRAFANA_PANEL_EXTRACTION = "Grafana Panel Extraction"

DASHBOARD_FOLDER_NOT_INGESTED_TITLE = "Dashboard folder not ingested"
DASHBOARD_FOLDER_NOT_INGESTED_MESSAGE = (
    "The dashboard's folder was not returned by /api/folders (for example a "
    "nested folder, or a folder page that failed to load), so the dashboard was "
    "not placed in a folder."
)
