import re
from dataclasses import dataclass, field
from typing import Optional, Pattern

from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.utilities.str_enum import StrEnum

# ========================================
# 1. Google API Configuration
# ========================================

# Google API scopes
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.activity.readonly",
]

# Google Sheets URL pattern
# Pre-compiled regex patterns for performance
# Each pattern documents its capture groups for clarity

# Google Sheets URL pattern
# Group 1: Spreadsheet ID (the unique identifier in the URL)
GOOGLE_SHEETS_URL_PATTERN = re.compile(
    r"https://docs\.google\.com/spreadsheets/d/([^/]+)"
)

# Google Drive API fields to request
DRIVE_API_FIELDS = "nextPageToken, files(id, name, description, owners, createdTime, modifiedTime, webViewLink, parents, shared, permissions, labelInfo, driveId)"

# Google Drive API query parameters
DRIVE_QUERY_SHEETS_MIMETYPE = "mimeType='application/vnd.google-apps.spreadsheet'"
DRIVE_SPACES_DEFAULT = "drive"
DRIVE_SPACES_ALL = "drive,appDataFolder,photos"

# ========================================
# Platform Identifiers
# ========================================

PLATFORM_NAME = "googlesheets"
PLATFORM_DISPLAY_NAME = "Google Sheets"
PLATFORM_ID = "google-sheets"

# ========================================
# Subtypes
# ========================================

SUBTYPE_GOOGLE_SHEETS = DatasetSubTypes.GOOGLE_SHEETS
SUBTYPE_GOOGLE_SHEETS_NAMED_RANGE = DatasetSubTypes.GOOGLE_SHEETS_NAMED_RANGE
SUBTYPE_GOOGLE_SHEETS_TAB = DatasetSubTypes.GOOGLE_SHEETS_TAB

SUBTYPE_FOLDER = DatasetContainerSubTypes.FOLDER
SUBTYPE_GOOGLE_SHEETS_SPREADSHEET = DatasetContainerSubTypes.GOOGLE_SHEETS_SPREADSHEET
SUBTYPE_PLATFORM = DatasetContainerSubTypes.PLATFORM

# ========================================
# Google Sheets API Field Names
# ========================================

FIELD_ID = "id"
FIELD_NAME = "name"
FIELD_DESCRIPTION = "description"
FIELD_PATH = "path"  # Full path in Drive (constructed by source)
FIELD_OWNERS = "owners"
FIELD_CREATED_TIME = "createdTime"
FIELD_MODIFIED_TIME = "modifiedTime"
FIELD_WEB_VIEW_LINK = "webViewLink"
FIELD_PARENTS = "parents"
FIELD_SHARED = "shared"
FIELD_PERMISSIONS = "permissions"
FIELD_DRIVE_ID = "driveId"
FIELD_NEXT_PAGE_TOKEN = "nextPageToken"
FIELD_FILES = "files"

FIELD_SPREADSHEET = "spreadsheet"
FIELD_PROPERTIES = "properties"
FIELD_TITLE = "title"
FIELD_SHEETS = "sheets"
FIELD_SHEET_ID = "sheetId"
FIELD_VALUES = "values"
FIELD_NAMED_RANGES = "namedRanges"

FIELD_EMAIL_ADDRESS = "emailAddress"
FIELD_ROLE = "role"
FIELD_TYPE = "type"

PERMISSION_TYPE_USER = "user"
PERMISSION_TYPE_GROUP = "group"
PERMISSION_TYPE_DOMAIN = "domain"
PERMISSION_TYPE_ANYONE = "anyone"

# Default values
DEFAULT_SHEET_NAME = "Sheet1"
DEFAULT_SHEET_TITLE = "Unknown"

# ========================================
# Google Sheets Formulas
# ========================================

# Formula names for lineage extraction
FORMULA_IMPORTRANGE = "IMPORTRANGE"
FORMULA_QUERY = "QUERY"
FORMULA_FILTER = "FILTER"

# Formula regex patterns
# IMPORTRANGE formula patterns (pre-compiled for performance)
# IMPORTRANGE_URL_PATTERN: extracts just the spreadsheet URL/ID
#   Group 1: Spreadsheet URL or ID (from first parameter)
IMPORTRANGE_URL_PATTERN = re.compile(
    r'IMPORTRANGE\s*\(\s*["\']([^"\']+)["\']', re.IGNORECASE
)

# IMPORTRANGE_FULL_PATTERN: extracts complete reference with range
#   Group 1: Sheet name (before the '!')
#   Group 2: Range (after the '!', e.g., "A1:B10")
IMPORTRANGE_FULL_PATTERN = re.compile(
    r'IMPORTRANGE\s*\(\s*["\'][^"\']+["\'],\s*["\']([^!]+)!([^"\']+)["\']',
    re.IGNORECASE,
)

# ========================================
# DataHub URN Prefixes
# ========================================

# URN prefixes
URN_PREFIX_GLOSSARY_TERM = "urn:li:glossaryTerm:"
URN_PREFIX_CORP_USER = "urn:li:corpuser:"

# Default actor
DEFAULT_ACTOR_URN = "urn:li:corpuser:datahub"

# ========================================
# Custom Properties
# ========================================

# Custom property key prefixes
CUSTOM_PROPERTY_NAMED_RANGE_PREFIX = "namedRange_"

# Custom property keys
CUSTOM_PROPERTY_CREATED_TIME = "createdTime"
CUSTOM_PROPERTY_MODIFIED_TIME = "modifiedTime"
CUSTOM_PROPERTY_WEB_VIEW_LINK = "webViewLink"
CUSTOM_PROPERTY_SHARED = "shared"

# ========================================
# Type Inference
# ========================================

# Type inference configuration
TYPE_INFERENCE_SAMPLE_SIZE = 50  # Number of rows to fetch for schema inference
DATE_CONFIDENCE_THRESHOLD = 0.8
MAX_DISTINCT_VALUES_FOR_FREQUENCIES = 50
MAX_SAMPLE_VALUES_COUNT = 10
ARRAY_DETECTION_SAMPLE_SIZE = 10  # Number of values to check for array type detection

# Logging and display configuration
SQL_PREVIEW_MAX_LENGTH = 100  # Maximum length for SQL query previews in logs

# ========================================
# Lineage
# ========================================

# Transform operation names
TRANSFORM_OPERATION_IMPORTRANGE = "IMPORTRANGE"

# ========================================
# Permission Roles
# ========================================

# Google Drive permission roles
PERMISSION_ROLE_OWNER = "owner"
PERMISSION_ROLE_ORGANIZER = "organizer"
PERMISSION_ROLE_FILE_ORGANIZER = "fileOrganizer"
PERMISSION_ROLE_WRITER = "writer"
PERMISSION_ROLE_COMMENTER = "commenter"
PERMISSION_ROLE_READER = "reader"

# Permission types to skip
PERMISSION_TYPE_ANYONE = "anyone"
PERMISSION_TYPE_DOMAIN = "domain"

# Type inference constants
BOOLEAN_VALUES = ("true", "false", "yes", "no", "1", "0")
DATE_CONFIDENCE_THRESHOLD = 0.8  # 80% of values must look like dates

# Date regex patterns for type inference (pre-compiled)
DATE_PATTERNS = [
    re.compile(
        r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$"
    ),  # Basic date (flexible year/day order)
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),  # ISO 8601 format
    re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"),  # MM/DD/YYYY or DD/MM/YYYY
    re.compile(r"^\d{4}/\d{2}/\d{2}$"),  # YYYY/MM/DD
    re.compile(r"^\w{3}\s+\d{1,2},?\s+\d{4}$"),  # Month DD, YYYY (e.g., "Jan 15, 2024")
]

# ========================================
# 10. Database Lineage Patterns
# ========================================

# BigQuery patterns
# Database reference patterns (pre-compiled for performance)
# Each pattern documents its capture groups

# BigQuery patterns
# BIGQUERY_DIRECT_PATTERN: Matches project.dataset.table format
#   Group 1: Project ID
#   Group 2: Dataset ID
#   Group 3: Table name
BIGQUERY_DIRECT_PATTERN = re.compile(
    r"(?:project[:.])?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)

# BIGQUERY_JDBC_PATTERN: Matches jdbc:bigquery://host:port/dataset.table
#   Group 1: Project ID (from host)
#   Group 2: Dataset ID
#   Group 3: Table name
BIGQUERY_JDBC_PATTERN = re.compile(
    r"jdbc:bigquery://([^:;/]+)[:/]([^:;/]+)\.([^:;/\s)]+)", re.IGNORECASE
)

# Snowflake patterns
# SNOWFLAKE_DIRECT_PATTERN: Matches database.schema.table format
#   Group 1: Database name
#   Group 2: Schema name
#   Group 3: Table name
SNOWFLAKE_DIRECT_PATTERN = re.compile(
    r'(?:snowflake[:\.])?["\']?([a-zA-Z0-9_-]+)["\']?\.["\'\ ]?([a-zA-Z0-9_-]+)["\']?\.["\'\ ]?([a-zA-Z0-9_-]+)["\']?',
    re.IGNORECASE,
)

# SNOWFLAKE_JDBC_PATTERN: Matches jdbc:snowflake://host/database?schema=schema_name
#   Group 1: Host (not used for lineage)
#   Group 2: Database name
#   Group 3: Schema name
SNOWFLAKE_JDBC_PATTERN = re.compile(
    r"jdbc:snowflake://([^/]+)/([^?]+)\?.*schema=([^&]+)", re.IGNORECASE
)
SNOWFLAKE_KEYWORDS = ["snowflake", "jdbc:snowflake"]

# PostgreSQL and MySQL JDBC patterns
# POSTGRES_MYSQL_JDBC_PATTERN: Matches jdbc:postgresql://host/database or jdbc:mysql://host/database
#   Group 1: Platform type ("postgresql" or "mysql")
#   Group 2: Database name
POSTGRES_MYSQL_JDBC_PATTERN = re.compile(
    r"jdbc:(postgresql|mysql)://[^/]+/([a-zA-Z0-9_-]+)", re.IGNORECASE
)

# Redshift JDBC pattern
# REDSHIFT_JDBC_PATTERN: Matches jdbc:redshift://host/database
#   Group 1: Database name
REDSHIFT_JDBC_PATTERN = re.compile(
    r"jdbc:redshift://[^/]+/([a-zA-Z0-9_-]+)", re.IGNORECASE
)

# SQL query pattern for extracting table names
# SQL_TABLE_PATTERN: Matches FROM or JOIN clauses
#   Group 1: Table name (may include schema, e.g., "schema.table")
SQL_TABLE_PATTERN = re.compile(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_.-]+)", re.IGNORECASE)

# SQL_SELECT_PATTERN: Matches SELECT...FROM statements in formulas
# Used to extract complete SQL queries from formula cells
SQL_SELECT_PATTERN = re.compile(
    r"(?:SELECT\s+.+?\s+FROM\s+[^\"\'\)]+)", re.IGNORECASE | re.DOTALL
)

# JDBC_DATABASE_PARAM_PATTERN: Extracts database= parameter from JDBC URLs
#   Group 1: Database name
# Example: jdbc:snowflake://host.snowflakecomputing.com/?database=MY_DB&schema=PUBLIC
JDBC_DATABASE_PARAM_PATTERN = re.compile(r"database=([^&;]+)", re.IGNORECASE)

# JDBC_DATABASE_PATH_PATTERN: Extracts database from JDBC URL path
#   Group 1: Database name
# Example: jdbc:bigquery://host/my_database?params=values
JDBC_DATABASE_PATH_PATTERN = re.compile(r"jdbc:[^:]+://[^/]+/([^?;]+)")

# Alternative sheet ID pattern using \w shorthand (matches 25+ chars)
# Used for lenient sheet ID validation
SHEET_ID_LENIENT_PATTERN = re.compile(r"^[-\w]{25,}$")


# ========================================
# Platform Configuration for Database Lineage
# ========================================


@dataclass
class PlatformConfig:
    """Configuration for a supported database platform."""

    platform_id: str  # DataHub platform name (e.g., "bigquery", "snowflake")
    default_schema: Optional[str]  # Default schema name (e.g., "public" for Postgres)
    jdbc_prefix: str  # JDBC URL prefix (e.g., "jdbc:bigquery://")
    supports_schemas: bool  # Whether platform uses schemas (MySQL doesn't)
    direct_patterns: list[Pattern] = field(default_factory=list)
    jdbc_patterns: list[Pattern] = field(default_factory=list)


# Platform configurations for database lineage detection
SUPPORTED_PLATFORMS = {
    "bigquery": PlatformConfig(
        platform_id="bigquery",
        default_schema=None,  # BigQuery uses dataset, not schema
        jdbc_prefix="jdbc:bigquery://",
        supports_schemas=False,
        direct_patterns=[BIGQUERY_DIRECT_PATTERN],
        jdbc_patterns=[BIGQUERY_JDBC_PATTERN],
    ),
    "snowflake": PlatformConfig(
        platform_id="snowflake",
        default_schema=None,  # Schema must be specified in Snowflake
        jdbc_prefix="jdbc:snowflake://",
        supports_schemas=True,
        direct_patterns=[SNOWFLAKE_DIRECT_PATTERN],
        jdbc_patterns=[SNOWFLAKE_JDBC_PATTERN],
    ),
    "postgres": PlatformConfig(
        platform_id="postgres",
        default_schema="public",
        jdbc_prefix="jdbc:postgresql://",
        supports_schemas=True,
        jdbc_patterns=[POSTGRES_MYSQL_JDBC_PATTERN],
    ),
    "mysql": PlatformConfig(
        platform_id="mysql",
        default_schema=None,  # MySQL doesn't use schemas
        jdbc_prefix="jdbc:mysql://",
        supports_schemas=False,
        jdbc_patterns=[POSTGRES_MYSQL_JDBC_PATTERN],
    ),
    "redshift": PlatformConfig(
        platform_id="redshift",
        default_schema="public",
        jdbc_prefix="jdbc:redshift://",
        supports_schemas=True,
        jdbc_patterns=[REDSHIFT_JDBC_PATTERN],
    ),
}


# ========================================
# 11. Google Sheets API Default Values
# ========================================

# Sheet types from Google Sheets API
SHEET_TYPE_GRID = "GRID"
SHEET_TYPE_OBJECT = "OBJECT"
SHEET_TYPE_DATA_SOURCE = "DATA_SOURCE"

# Value range dimensions from Google Sheets API
DIMENSION_ROWS = "ROWS"
DIMENSION_COLUMNS = "COLUMNS"

# Default sheet name when not specified
DEFAULT_SHEET_NAME = "Sheet1"

# ========================================
# 12. Profiling and Analysis Defaults
# ========================================

# Maximum number of distinct values to show frequencies for
MAX_DISTINCT_VALUES_FOR_FREQUENCIES = 50

# Default maximum rows to profile when limit not specified
DEFAULT_PROFILING_MAX_ROWS = 10000

# Maximum distinct value count threshold for showing frequencies
MAX_UNIQUE_COUNT_FOR_FREQUENCIES = 50

# ========================================
# 13. Pre-compiled Regex Patterns
# ========================================

# Sheet ID validation (at least 10 alphanumeric/dash/underscore chars)
SHEET_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{10,}$")

# A1 notation patterns for ranges and cells
# Matches: A1:B10, A:B, A1:B, etc.
A1_RANGE_PATTERN = re.compile(r"([A-Za-z]+)(\d+)?:([A-Za-z]+)(\d+)?")

# Matches single cell: A1, B2, etc.
SINGLE_CELL_PATTERN = re.compile(r"[A-Za-z]+\d+")

# Extract column letter from cell reference: A1 -> A
CELL_COLUMN_PATTERN = re.compile(r"([A-Za-z]+)\d+")

# Pure number detection (for distinguishing from dates): 123, 123.45
PURE_NUMBER_PATTERN = re.compile(r"^-?\d+\.?\d*$")  # Supports negative numbers


# ========================================
# 14. Data Type Enums
# ========================================


class NativeDataType(StrEnum):
    """Native data types for Google Sheets columns."""

    STRING = "STRING"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    ARRAY = "ARRAY"
