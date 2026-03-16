"""
MicroStrategy API constants.

Centralizes API endpoints, field names, and platform constants.
"""

# Platform name
PLATFORM_NAME = "microstrategy"


# API Endpoints
class Endpoints:
    """MicroStrategy REST API endpoints."""

    # Authentication
    AUTH_LOGIN = "/api/auth/login"
    AUTH_LOGOUT = "/api/auth/logout"
    SESSIONS = "/api/sessions"

    # Projects
    PROJECTS = "/api/v2/projects"
    PROJECT = "/api/v2/projects/{project_id}"

    # Folders
    FOLDERS = "/api/v2/projects/{project_id}/folders"
    FOLDER = "/api/v2/folders/{folder_id}"

    # Dashboards (Dossiers)
    DASHBOARDS = "/api/v2/projects/{project_id}/dashboards"
    DOSSIERS = "/api/v2/projects/{project_id}/dossiers"  # Legacy
    DASHBOARD_DEFINITION = "/api/v2/dossiers/{dashboard_id}/definition"

    # Reports
    REPORTS = "/api/v2/projects/{project_id}/reports"
    REPORT = "/api/v2/reports/{report_id}"

    # Cubes
    CUBES = "/api/v2/projects/{project_id}/cubes"
    CUBE = "/api/v2/cubes/{cube_id}"
    CUBE_SCHEMA = "/api/v2/cubes/{cube_id}/schema"

    # Datasets
    DATASETS = "/api/v2/projects/{project_id}/datasets"
    DATASET = "/api/v2/datasets/{dataset_id}"

    # Users
    USERS = "/api/v2/users"
    USER = "/api/v2/users/{user_id}"


# Field Names
class Fields:
    """Common field names in API responses."""

    # Common fields
    ID = "id"
    NAME = "name"
    DESCRIPTION = "description"
    TYPE = "type"
    OWNER = "owner"
    CREATED_DATE = "createdDate"
    MODIFIED_DATE = "modifiedDate"
    CREATED_BY = "createdBy"
    MODIFIED_BY = "modifiedBy"

    # Project fields
    PROJECT_ID = "projectId"

    # Folder fields
    PARENT_ID = "parentId"
    FOLDER_ID = "folderId"

    # Dashboard fields
    CHAPTERS = "chapters"
    VISUALIZATIONS = "visualizations"
    PAGES = "pages"

    # Cube/Dataset fields
    ATTRIBUTES = "attributes"
    METRICS = "metrics"
    TABLES = "tables"
    COLUMNS = "columns"

    # User fields
    USERNAME = "username"
    EMAIL = "email"
    FULL_NAME = "fullName"

    # Response wrappers
    VALUE = "value"
    DATA = "data"
    ITEMS = "items"


# Login Modes
class LoginMode:
    """MicroStrategy login modes."""

    STANDARD = 1  # Username/password authentication
    ANONYMOUS = 8  # Guest/anonymous access
    LDAP = 16  # LDAP authentication


# Object Types
class ObjectType:
    """MicroStrategy object type identifiers."""

    PROJECT = 32  # Project
    FOLDER = 8  # Folder
    REPORT_DEFINITION = 3  # Report
    DOCUMENT_DEFINITION = 55  # Dashboard/Dossier
    INTELLIGENT_CUBE = 76  # Intelligent Cube
    ATTRIBUTE = 12  # Attribute
    METRIC = 4  # Metric
    TABLE = 47  # Table
    COLUMN = 26  # Column


# Subtype Mappings
class SubTypes:
    """DataHub subtype mappings."""

    # Container subtypes
    PROJECT = "BIContainerSubTypes.PROJECT"
    FOLDER = "BIContainerSubTypes.LOOKER_FOLDER"

    # Dashboard subtypes
    DASHBOARD = "dashboard"
    DOSSIER = "dossier"  # Legacy name

    # Chart subtypes
    REPORT = "report"

    # Dataset subtypes
    CUBE = "cube"
    DATASET = "dataset"


# Default Values
class Defaults:
    """Default configuration values."""

    TIMEOUT_SECONDS = 30
    MAX_RETRIES = 3
    PAGE_SIZE = 1000
    SESSION_TIMEOUT_MINUTES = 30
    REFRESH_THRESHOLD_MINUTES = 5
