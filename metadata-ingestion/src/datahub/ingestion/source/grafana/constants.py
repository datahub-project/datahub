DASHBOARD_PARSE_ERROR_TITLE = "Dashboard Parse Error"
DASHBOARD_PARSE_ERROR_MESSAGE = (
    "Skipped a dashboard Grafana returned in an unexpected shape"
)
SEARCH_HIT_WITHOUT_UID_TITLE = "Dashboard Search Result Skipped"
SEARCH_HIT_WITHOUT_UID_MESSAGE = (
    "Skipped a dashboard search result that has no uid or is not an object"
)
REPEATED_PAGE_TITLE = "Paging Stopped"
REPEATED_PAGE_MESSAGE = (
    "Grafana returned a page with nothing new, so paging stopped; the API may "
    "be ignoring the page parameter"
)
BASIC_MODE_RESPONSE_ERROR_TITLE = "Dashboard Search Error"
BASIC_MODE_RESPONSE_ERROR_MESSAGE = (
    "Grafana /api/search did not return a list of dashboards in basic mode"
)
BASIC_MODE_ITEM_SKIPPED_TITLE = "Dashboard Skipped"
BASIC_MODE_ITEM_SKIPPED_MESSAGE = (
    "Skipped a dashboard search result missing uid, title or url in basic mode"
)
PANEL_SKIPPED_TITLE = "Panel Skipped"
PANEL_SKIPPED_MESSAGE = (
    "Skipped a panel with missing or invalid fields; it was not ingested as a chart"
)
MAX_REPORT_CONTEXT_CHARS = 200
