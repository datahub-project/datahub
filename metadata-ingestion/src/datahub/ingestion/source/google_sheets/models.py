from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from datahub.ingestion.source.google_sheets.constants import (
    DIMENSION_ROWS,
    SHEET_TYPE_GRID,
)

# ========================================
# Google Sheets API Response Models
# ========================================


class ExtendedValue(BaseModel):
    """Represents the value entered by a user in a cell (from Google Sheets API)."""

    stringValue: Optional[str] = None
    numberValue: Optional[float] = None
    booleanValue: Optional[bool] = None
    formulaValue: Optional[str] = None
    errorValue: Optional[Dict[str, Any]] = None


class CellValue(BaseModel):
    """Represents a cell value in a Google Sheet."""

    formattedValue: Optional[str] = None
    userEnteredValue: Optional[ExtendedValue] = None
    effectiveValue: Optional[ExtendedValue] = None
    formulaValue: Optional[str] = None


class RowData(BaseModel):
    """Represents a row of cells in a Google Sheet."""

    values: List[CellValue] = Field(default_factory=list)


class GridData(BaseModel):
    """Represents grid data for a sheet."""

    rowData: List[RowData] = Field(default_factory=list)
    startRow: Optional[int] = None
    startColumn: Optional[int] = None


class SheetProperties(BaseModel):
    """Properties of a sheet within a spreadsheet."""

    sheetId: int
    title: str
    index: int
    sheetType: str = SHEET_TYPE_GRID  # Already imported from constants
    gridProperties: Optional[Dict[str, Any]] = None


class Sheet(BaseModel):
    """Represents a sheet/tab in a spreadsheet."""

    properties: SheetProperties
    data: List[GridData] = Field(default_factory=list)


class SpreadsheetProperties(BaseModel):
    """Properties of the entire spreadsheet."""

    title: str
    locale: Optional[str] = None
    autoRecalc: Optional[str] = None
    timeZone: Optional[str] = None
    defaultFormat: Optional[Dict[str, Any]] = None


class GridRange(BaseModel):
    """Represents a range in a grid (sheet) from Google Sheets API."""

    sheetId: int = 0
    startRowIndex: int = 0
    endRowIndex: int = 0
    startColumnIndex: int = 0
    endColumnIndex: int = 0


class NamedRange(BaseModel):
    """Represents a named range in a spreadsheet."""

    namedRangeId: Optional[str] = None
    name: str
    range: GridRange


class Spreadsheet(BaseModel):
    """Complete spreadsheet metadata from Google Sheets API."""

    spreadsheetId: str
    properties: SpreadsheetProperties
    sheets: List[Sheet] = Field(default_factory=list)
    namedRanges: List[NamedRange] = Field(default_factory=list)
    spreadsheetUrl: Optional[str] = None
    developerMetadata: List[Dict[str, Any]] = Field(default_factory=list)


class ValuesResponse(BaseModel):
    """Response from Sheets values.get() API."""

    range: str
    majorDimension: str = DIMENSION_ROWS  # Already imported from constants
    values: List[List[Any]] = Field(default_factory=list)


# ========================================
# Google Drive API Response Models
# ========================================


class DriveUser(BaseModel):
    """Represents a user in Google Drive."""

    displayName: Optional[str] = None
    emailAddress: Optional[str] = None
    permissionId: Optional[str] = None
    photoLink: Optional[str] = None


class DrivePermission(BaseModel):
    """Represents a permission on a Drive file."""

    id: str
    type: str  # user, group, domain, anyone
    role: str  # owner, organizer, fileOrganizer, writer, commenter, reader
    emailAddress: Optional[str] = None
    displayName: Optional[str] = None
    domain: Optional[str] = None
    deleted: bool = False


class DriveFileMetadata(BaseModel):
    """Minimal file metadata for path resolution from Drive API."""

    id: Optional[str] = None
    name: str
    parents: List[str] = Field(default_factory=list)


class DriveFile(BaseModel):
    """Represents a file in Google Drive (used for spreadsheet metadata).

    This model captures fields from the Google Drive API v3 used for metadata extraction.
    See: https://developers.google.com/drive/api/v3/reference/files
    """

    id: str
    name: str
    mimeType: str
    description: str = ""
    path: str = ""
    createdTime: str = ""
    modifiedTime: str = ""
    owners: List[DriveUser] = Field(default_factory=list)
    shared: bool = False
    permissions: List[DrivePermission] = Field(default_factory=list)
    webViewLink: str = ""
    sharingUser: Optional[DriveUser] = None
    lastModifyingUser: Optional[DriveUser] = None
    parents: List[str] = Field(default_factory=list)
    labelInfo: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DriveFile":
        """Create DriveFile from API response dictionary."""
        return cls.model_validate(data)


class DriveFilesListResponse(BaseModel):
    """Response from Drive files.list() API."""

    files: List[DriveFile] = Field(default_factory=list)
    nextPageToken: Optional[str] = None
    incompleteSearch: bool = False


# ========================================
# Internal Data Models
# ========================================


class FormulaLocation(BaseModel):
    """Location of a formula within a sheet."""

    row: int
    column: int
    header: str


class FormulaExtractionResult(BaseModel):
    """Result from extracting formulas from a sheet."""

    formulas: List[str] = Field(default_factory=list)
    locations: List[FormulaLocation] = Field(default_factory=list)


# ========================================
# Drive Activity API Response Models
# ========================================


class DriveActivityKnownUser(BaseModel):
    """Known user from Drive Activity API."""

    personName: str


class DriveActivityUser(BaseModel):
    """User information from Drive Activity API."""

    knownUser: Optional[DriveActivityKnownUser] = None


class DriveActivityActor(BaseModel):
    """Actor who performed an action in Drive Activity API."""

    user: Optional[DriveActivityUser] = None


class DriveActivityViewAction(BaseModel):
    """View action details from Drive Activity API."""

    viewCount: Optional[int] = None


class DriveActivityActionDetail(BaseModel):
    """Primary action detail from Drive Activity API."""

    view: Optional[DriveActivityViewAction] = None


class DriveActivity(BaseModel):
    """Individual activity record from Drive Activity API."""

    primaryActionDetail: Optional[DriveActivityActionDetail] = None
    actors: List[DriveActivityActor] = Field(default_factory=list)
    timestamp: Optional[str] = None


class DriveActivityResponse(BaseModel):
    """Response from Drive Activity API query."""

    activities: List[DriveActivity] = Field(default_factory=list)


# ========================================
# Usage Statistics
# ========================================


class SheetUsageStatistics(BaseModel):
    """Usage statistics for a Google Sheet from Drive Activity API."""

    view_count: int = 0
    unique_user_count: int = 0
    last_viewed_at: Optional[str] = None


class SheetData(BaseModel):
    """Complete data for a spreadsheet including Drive, Sheets API data, usage stats, and named ranges."""

    spreadsheet: Spreadsheet
    usage_stats: Optional["SheetUsageStatistics"] = None
    named_ranges: List["NamedRangeInfo"] = Field(default_factory=list)


class JdbcParseResult(BaseModel):
    """Result from parsing a JDBC URL and extracting SQL query."""

    platform: Optional[str] = None
    database: Optional[str] = None
    sql_query: Optional[str] = None


class SheetPathResult(BaseModel):
    """Result from extracting folder path information for a Google Sheet."""

    full_path: str
    folder_path_parts: List[str] = Field(default_factory=list)


class DatabaseReference(BaseModel):
    """Reference to an external database table from a formula."""

    platform: str
    table_identifier: str

    @staticmethod
    def create_bigquery(
        project_id: str, dataset_id: str, table_id: str
    ) -> "DatabaseReference":
        """Create BigQuery reference: project.dataset.table (3 parts, no schema)."""
        return DatabaseReference(
            platform="bigquery",
            table_identifier=f"{project_id}.{dataset_id}.{table_id}",
        )

    @staticmethod
    def create_snowflake(
        database: str, schema: str, table: Optional[str] = None
    ) -> "DatabaseReference":
        """Create Snowflake reference: database.schema.table (3 parts with schema).

        If table is None, creates a schema-level reference (database.schema).
        """
        if table:
            table_identifier = f"{database}.{schema}.{table}"
        else:
            table_identifier = f"{database}.{schema}"

        return DatabaseReference(
            platform="snowflake",
            table_identifier=table_identifier,
        )

    @staticmethod
    def create_postgres(database: str, schema: str, table: str) -> "DatabaseReference":
        """Create PostgreSQL reference: database.schema.table (3 parts with schema)."""
        return DatabaseReference(
            platform="postgres",
            table_identifier=f"{database}.{schema}.{table}",
        )

    @staticmethod
    def create_mysql(database: str, table: str) -> "DatabaseReference":
        """Create MySQL reference: database.table (2 parts, no schema)."""
        return DatabaseReference(
            platform="mysql",
            table_identifier=f"{database}.{table}",
        )

    @staticmethod
    def create_redshift(database: str, schema: str, table: str) -> "DatabaseReference":
        """Create Redshift reference: database.schema.table (3 parts with schema)."""
        return DatabaseReference(
            platform="redshift",
            table_identifier=f"{database}.{schema}.{table}",
        )


class ImportRangeReference(BaseModel):
    """Reference to another Google Sheet via IMPORTRANGE formula."""

    source_sheet_id: str
    source_sheet_name: str
    source_range: str
    formula: str
    location: FormulaLocation


class InferredType(BaseModel):
    """Result of type inference for a column."""

    data_type: Any
    native_type: str


class LabelFieldValue(BaseModel):
    """Represents a field value in a Google Drive label.

    Drive labels support multiple field types (text, selection, date, integer, user).
    We only extract text and selection values as tags.
    """

    text: List[str] = Field(default_factory=list)
    selection: List[str] = Field(default_factory=list)


class DriveLabel(BaseModel):
    """Represents a Google Drive label applied to a file.

    See: https://developers.google.com/drive/api/v3/reference/files#labelInfo
    """

    id: str
    fields: Dict[str, LabelFieldValue] = Field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DriveLabel":
        """Create a DriveLabel from API response dictionary.

        Handles parsing nested field values into LabelFieldValue objects.
        """
        label_id = data.get("id", "")
        raw_fields = data.get("fields", {})

        parsed_fields: Dict[str, LabelFieldValue] = {}
        for field_id, field_value in raw_fields.items():
            if isinstance(field_value, dict):
                parsed_fields[field_id] = LabelFieldValue.model_validate(field_value)

        return cls(id=label_id, fields=parsed_fields)


class DriveLabelInfo(BaseModel):
    """Represents the labelInfo structure from Google Drive API v3.

    Contains all labels applied to a Drive file.
    See: https://developers.google.com/drive/api/v3/reference/files#labelInfo
    """

    labels: List[DriveLabel] = Field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DriveLabelInfo":
        """Create DriveLabelInfo from API response dictionary."""
        raw_labels = data.get("labels", [])
        parsed_labels = [DriveLabel.from_dict(label) for label in raw_labels]
        return cls(labels=parsed_labels)


class SheetPath(BaseModel):
    """Folder path information for a Google Sheet."""

    full_path: str
    folder_components: List[str]


class UsageStats(BaseModel):
    """Usage statistics for a Google Sheet."""

    view_count: int = 0
    last_viewed: Optional[str] = None
    unique_viewers: int = 0


class NamedRangeInfo(BaseModel):
    """Information about a named range (simplified for internal use)."""

    name: str
    range_notation: str


class SheetMetadataResponse(BaseModel):
    """Complete metadata for a spreadsheet from combined APIs.

    This model combines data from:
    - Google Sheets API (spreadsheet structure, data)
    - Google Drive API (file metadata, permissions)
    - Drive Activity API (usage statistics)
    - Named range processing
    """

    spreadsheet: Spreadsheet
    drive_file: DriveFile
    usage_stats: Optional[SheetUsageStatistics] = None
    named_ranges: List[NamedRangeInfo] = Field(default_factory=list)


class DriveFolderMetadata(BaseModel):
    """Metadata for a Google Drive folder."""

    id: str
    name: str
    parents: List[str] = Field(default_factory=list)
    mimeType: str = ""


# ========================================
# Helper Functions
# ========================================


def parse_spreadsheet(data: Dict[str, Any]) -> Spreadsheet:
    """Parse raw API response into Spreadsheet model.

    Args:
        data: Raw dictionary from Sheets API spreadsheets.get()

    Returns:
        Validated Spreadsheet model
    """
    return Spreadsheet.model_validate(data)


def parse_drive_file(data: Dict[str, Any]) -> DriveFile:
    """Parse raw API response into DriveFile model.

    Args:
        data: Raw dictionary from Drive API files.get()

    Returns:
        Validated DriveFile model
    """
    return DriveFile.model_validate(data)


def parse_values_response(data: Dict[str, Any]) -> ValuesResponse:
    """Parse raw API response into ValuesResponse model.

    Args:
        data: Raw dictionary from Sheets API values.get()

    Returns:
        Validated ValuesResponse model
    """
    return ValuesResponse.model_validate(data)


def parse_drive_files_list(data: Dict[str, Any]) -> DriveFilesListResponse:
    """Parse raw API response into DriveFilesListResponse model.

    Args:
        data: Raw dictionary from Drive API files.list()

    Returns:
        Validated DriveFilesListResponse model
    """
    return DriveFilesListResponse.model_validate(data)
