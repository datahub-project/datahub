from enum import Enum


class CreatedFrom(Enum):
    REPORT = "Report"
    DATASET = "Dataset"
    VISUALIZATION = "Visualization"
    UNKNOWN = "Unknown"


class RelationshipDirection(Enum):
    INCOMING = "INCOMING"
    OUTGOING = "OUTGOING"


class Constant:
    """
    keys used in powerbi plugin
    """

    DATASET = "DATASET"
    REPORTS = "REPORTS"
    REPORT = "REPORT"
    REPORT_DATASOURCES = "REPORT_DATASOURCES"
    TYPE_REPORT = "Report"
    DATASOURCE = "DATASOURCE"
    DATASET_DATASOURCES = "DATASET_DATASOURCES"
    DatasetId = "DatasetId"
    ReportId = "ReportId"
    PowerBiReportId = "ReportId"
    Dataset_URN = "DatasetURN"
    DASHBOARD_ID = "powerbi.linkedin.com/dashboards/{}"
    DASHBOARD = "dashboard"
    DATASETS = "DATASETS"
    DATASET_ID = "powerbi.linkedin.com/datasets/{}"
    DATASET_PROPERTIES = "datasetProperties"
    SUBSCRIPTION = "SUBSCRIPTION"
    SYSTEM = "SYSTEM"
    CATALOG_ITEM = "CATALOG_ITEM"
    EXCEL_WORKBOOK = "EXCEL_WORKBOOK"
    EXTENSIONS = "EXTENSIONS"
    FAVORITE_ITEM = "FAVORITE_ITEM"
    FOLDERS = "FOLDERS"
    KPIS = "KPIS"
    LINKED_REPORTS = "LINKED_REPORTS"
    LINKED_REPORT = "LINKED_REPORT"
    ME = "ME"
    MOBILE_REPORTS = "MOBILE_REPORTS"
    MOBILE_REPORT = "MOBILE_REPORT"
    POWERBI_REPORTS = "POWERBI_REPORTS"
    POWERBI_REPORT = "POWERBI_REPORT"
    POWERBI_REPORT_DATASOURCES = "POWERBI_REPORT_DATASOURCES"
    TYPE_POWERBI_REPORT = "PowerBIReport"
    RESOURCE = "RESOURCE"
    SESSION = "SESSION"
    SYSTEM_POLICIES = "SYSTEM_POLICIES"
    DATASET_KEY = "datasetKey"
    BROWSERPATH = "browsePaths"
    DATAPLATFORM_INSTANCE = "dataPlatformInstance"
    STATUS = "status"
    VALUE = "value"
    ID = "ID"
    DASHBOARD_INFO = "dashboardInfo"
    DASHBOARD_KEY = "dashboardKey"
    CORP_USER = "corpuser"
    CORP_USER_INFO = "corpUserInfo"
    OWNERSHIP = "ownership"
    CORP_USER_KEY = "corpUserKey"


API_ENDPOINTS = {
    Constant.CATALOG_ITEM: "{PBIRS_BASE_URL}/CatalogItems({CATALOG_ID})",
    Constant.DATASETS: "{PBIRS_BASE_URL}/Datasets",
    Constant.DATASET: "{PBIRS_BASE_URL}/Datasets({DATASET_ID})",
    Constant.DATASET_DATASOURCES: "{PBIRS_BASE_URL}/Datasets({DATASET_ID})/DataSources",
    Constant.DATASOURCE: "{PBIRS_BASE_URL}/DataSources({DATASOURCE_ID})",
    Constant.EXCEL_WORKBOOK: "{PBIRS_BASE_URL}/ExcelWorkbooks({EXCEL_WORKBOOK_ID})",
    Constant.EXTENSIONS: "{PBIRS_BASE_URL}/Extensions",
    Constant.FAVORITE_ITEM: "{PBIRS_BASE_URL}/FavoriteItems({FAVORITE_ITEM_ID})",
    Constant.FOLDERS: "{PBIRS_BASE_URL}/Folders({FOLDER_ID})",
    Constant.KPIS: "{PBIRS_BASE_URL}/Kpis({KPI_ID})",
    Constant.LINKED_REPORTS: "{PBIRS_BASE_URL}/LinkedReports",
    Constant.LINKED_REPORT: "{PBIRS_BASE_URL}/LinkedReports({LINKED_REPORT_ID})",
    Constant.ME: "{PBIRS_BASE_URLL}/Me",
    Constant.MOBILE_REPORTS: "{PBIRS_BASE_URL}/MobileReports",
    Constant.MOBILE_REPORT: "{PBIRS_BASE_URL}/MobileReports({MOBILE_REPORT_ID})",
    Constant.POWERBI_REPORTS: "{PBIRS_BASE_URL}/PowerBiReports",
    Constant.POWERBI_REPORT: "{PBIRS_BASE_URL}/PowerBiReports({POWERBI_REPORT_ID})",
    Constant.POWERBI_REPORT_DATASOURCES: "{PBIRS_BASE_URL}/PowerBiReports({ID})/DataSources",
    Constant.REPORTS: "{PBIRS_BASE_URL}/Reports",
    Constant.REPORT: "{PBIRS_BASE_URL}/Reports({REPORT_ID})",
    Constant.REPORT_DATASOURCES: "{PBIRS_BASE_URL}/Reports({ID})/DataSources",
    Constant.RESOURCE: "{PBIRS_BASE_URL}/Resources({RESOURCE_GET})",
    Constant.SESSION: "{PBIRS_BASE_URL}/Session",
    Constant.SUBSCRIPTION: "{PBIRS_BASE_URL}/Subscriptions({SUBSCRIPTION_ID})",
    Constant.SYSTEM: "{PBIRS_BASE_URL}/System",
    Constant.SYSTEM_POLICIES: "{PBIRS_BASE_URL}/System/Policies",
}
