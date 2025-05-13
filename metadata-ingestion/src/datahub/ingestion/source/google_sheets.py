import os
import re
import time
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import Field, validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    HistogramClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    QuantileClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
    ValueFrequencyClass,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict


class GoogleSheetsProfilerConfig(ConfigModel):
    enabled: bool = Field(
        default=False, description="Whether profiling should be done."
    )

    max_rows: int = Field(
        default=10000, description="Maximum number of rows to profile per sheet."
    )

    include_field_null_count: bool = Field(
        default=True,
        description="Whether to profile for the number of nulls for each column.",
    )

    include_field_min_value: bool = Field(
        default=True,
        description="Whether to profile for the min value of numeric columns.",
    )

    include_field_max_value: bool = Field(
        default=True,
        description="Whether to profile for the max value of numeric columns.",
    )

    include_field_mean_value: bool = Field(
        default=True,
        description="Whether to profile for the mean value of numeric columns.",
    )

    include_field_median_value: bool = Field(
        default=True,
        description="Whether to profile for the median value of numeric columns.",
    )

    include_field_stddev_value: bool = Field(
        default=True,
        description="Whether to profile for the standard deviation of numeric columns.",
    )

    include_field_quantiles: bool = Field(
        default=True,
        description="Whether to profile for the quantiles of numeric columns.",
    )

    include_field_distinct_value_frequencies: bool = Field(
        default=True, description="Whether to profile for distinct value frequencies."
    )

    include_field_histogram: bool = Field(
        default=True,
        description="Whether to profile for the histogram for numeric fields.",
    )

    include_field_sample_values: bool = Field(
        default=True,
        description="Whether to profile for the sample values for all columns.",
    )

    max_number_of_fields_to_profile: Optional[int] = Field(
        default=None,
        description="A positive integer that specifies the maximum number of columns to profile for any sheet. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
    )


class GoogleSheetsSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    # Google API authentication
    credentials: str = Field(
        description="Path to the Google service account credentials JSON file."
    )

    # Configuration options
    folder_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Patterns to filter Google Drive folders to scan for sheets.",
    )

    sheet_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Patterns to filter Google Sheets by name.",
    )

    sheets_as_datasets: bool = Field(
        default=False,
        description="If True, each sheet within a Google Sheets document will be treated as a separate dataset. The Google Sheets document itself will be represented as a container. If False, each Google Sheets document will be treated as a dataset, with sheets as fields within that dataset.",
    )

    extract_usage_stats: bool = Field(
        default=True, description="Whether to extract usage statistics for sheets."
    )

    profiling: GoogleSheetsProfilerConfig = Field(
        default=GoogleSheetsProfilerConfig(),
        description="Profiling configuration.",
    )

    extract_lineage_from_formulas: bool = Field(
        default=True,
        description="Whether to extract lineage information from sheet formulas. This includes connections to other Google Sheets and external data sources.",
    )

    enable_cross_platform_lineage: bool = Field(
        default=True,
        description="Whether to extract cross-platform lineage (e.g., connections to BigQuery, etc.). Only applicable if extract_lineage_from_formulas is True.",
    )

    extract_column_level_lineage: bool = Field(
        default=True,
        description="Whether to extract column-level lineage from Google Sheets formulas.",
    )

    scan_shared_drives: bool = Field(
        default=False,
        description="Whether to scan sheets in shared drives in addition to 'My Drive'.",
    )

    max_retries: int = Field(
        default=3, description="Number of retries for failed API requests."
    )

    retry_delay: int = Field(default=1, description="Delay in seconds between retries.")

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("credentials")
    def credentials_exist(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"Credentials file {v} does not exist")
        return v


class GoogleSheetsContainerKey(ContainerKey):
    key: str


@dataclass
class GoogleSheetsSourceReport(
    StaleEntityRemovalSourceReport,
    IngestionStageReport,
):
    sheets_scanned: int = 0
    sheets_dropped: List[str] = dataclass_field(default_factory=list)
    formulas_processed: int = 0
    lineage_edges_found: int = 0
    fine_grained_edges_found: int = 0
    sheets_profiled: int = 0

    # Timers
    scan_sheets_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    extract_schema_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    extract_lineage_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    profile_sheet_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)
    usage_stats_timer: Dict[str, float] = dataclass_field(default_factory=TopKDict)

    def report_sheet_scanned(self) -> None:
        self.sheets_scanned += 1

    def report_sheet_dropped(self, sheet: str) -> None:
        self.sheets_dropped.append(sheet)

    def report_formulas_processed(self, count: int) -> None:
        self.formulas_processed += count

    def report_lineage_edge(self) -> None:
        self.lineage_edges_found += 1

    def report_fine_grained_edge(self) -> None:
        self.fine_grained_edges_found += 1

    def report_sheet_profiled(self) -> None:
        self.sheets_profiled += 1


@platform_name("Google Sheets", id="google-sheets")
@config_class(GoogleSheetsSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Extracts schema from Google Sheets")
@capability(
    SourceCapability.DATA_PROFILING, "Can profile Google Sheets data", supported=True
)
@capability(
    SourceCapability.USAGE_STATS,
    "Can extract usage statistics from Google Sheets",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Can extract lineage between Google Sheets and other platforms",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Can extract column-level lineage from Google Sheets formulas",
    supported=True,
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is turned on.",
)
class GoogleSheetsSource(StatefulIngestionSourceBase):
    """Extract metadata from Google Sheets"""

    platform = "googlesheets"
    report: GoogleSheetsSourceReport

    def __init__(self, config: GoogleSheetsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = GoogleSheetsSourceReport()

        # Setup Google API clients
        self.creds = service_account.Credentials.from_service_account_file(
            config.credentials,
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.activity.readonly",
            ],
        )
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.sheets_service = build("sheets", "v4", credentials=self.creds)
        try:
            self.drive_activity_service = build(
                "driveactivity", "v2", credentials=self.creds
            )
        except Exception as e:
            self.report.report_failure(
                message="Unable to initialize drive activity service:", exc=e
            )
            self.drive_activity_service = None

        # Keep track of processed sheets
        self.processed_sheets: Set[str] = set()

    def get_workunit_processors(self):
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def close(self):
        # Clean up any resources
        pass

    def _get_sheets_from_drive(self) -> List[Dict[str, Any]]:
        """Get all Google Sheets from Drive that match the configured patterns."""
        with PerfTimer() as timer:
            sheets = []
            page_token = None

            # Query parameters
            query = "mimeType='application/vnd.google-apps.spreadsheet'"
            spaces = "drive"
            if self.config.scan_shared_drives:
                spaces = "drive,appDataFolder,photos"

            while True:
                try:
                    # Query for Google Sheets files
                    response = (
                        self.drive_service.files()
                        .list(
                            q=query,
                            spaces=spaces,
                            fields="nextPageToken, files(id, name, description, owners, createdTime, modifiedTime, webViewLink, parents, shared, permissions)",
                            pageToken=page_token,
                        )
                        .execute()
                    )

                    for sheet in response.get("files", []):
                        # Check if the sheet matches the configured patterns
                        if self.config.sheet_patterns.allowed(sheet["name"]):
                            # Get additional metadata
                            sheet["path"] = self._get_sheet_path(sheet["id"])
                            sheets.append(sheet)
                            self.report.report_sheet_scanned()
                        else:
                            self.report.report_sheet_dropped(sheet["name"])

                    page_token = response.get("nextPageToken", None)
                    if page_token is None:
                        break

                except Exception as e:
                    self.report.report_warning(
                        message=f"Error fetching sheets from Google Drive. Retrying {self.config.max_retries} times",
                        exc=e,
                    )
                    # Implement retry logic
                    if self.config.max_retries > 0:
                        self.config.max_retries -= 1
                        time.sleep(self.config.retry_delay)
                        continue
                    else:
                        break

            self.report.scan_sheets_timer["total"] = timer.elapsed_seconds()
            return sheets

    def _get_sheet_path(self, sheet_id: str) -> str:
        """Get the folder path for a Google Sheet."""
        try:
            # Get file metadata to get parent folder
            file_metadata = (
                self.drive_service.files()
                .get(fileId=sheet_id, fields="parents,name")
                .execute()
            )

            parent_id = file_metadata.get("parents", [None])[0]
            if not parent_id:
                return f"/{file_metadata.get('name', sheet_id)}"

            # Recursively build path
            path_parts: List[str] = []
            while parent_id:
                try:
                    folder = (
                        self.drive_service.files()
                        .get(fileId=parent_id, fields="id,name,parents")
                        .execute()
                    )

                    path_parts.insert(0, folder.get("name", parent_id))
                    parent_id = folder.get("parents", [None])[0]
                except Exception as e:
                    self.report.report_warning(
                        message="Error getting folder info", exc=e
                    )
                    break

            # Add sheet name to path
            path_parts.append(file_metadata.get("name", sheet_id))

            return "/" + "/".join(path_parts)
        except Exception as e:
            self.report.report_warning(
                "Error getting sheet path", context=sheet_id, exc=e
            )
            return f"/{sheet_id}"

    def _get_sheet_metadata(self, sheet_id: str) -> Dict[str, Any]:
        """Get detailed metadata for a specific Google Sheet."""
        try:
            # Get sheet properties
            spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            )

            # Get usage statistics if enabled
            usage_stats = None
            if self.config.extract_usage_stats and self.drive_activity_service:
                with PerfTimer() as timer:
                    usage_stats = self._get_sheet_usage_stats(sheet_id)
                    self.report.usage_stats_timer[sheet_id] = timer.elapsed_seconds()

            return {"spreadsheet": spreadsheet, "usage_stats": usage_stats}
        except Exception as e:
            self.report.report_failure(
                message="Error fetching sheet metadata for", context=sheet_id, exc=e
            )
            return {"spreadsheet": {"properties": {"title": sheet_id}}}

    def _get_sheet_usage_stats(self, sheet_id: str) -> Dict[str, Any]:
        """Get usage statistics for a Google Sheet using Drive Activity API."""
        if not self.drive_activity_service:
            return {"viewCount": 0, "uniqueUserCount": 0}

        try:
            # Set time filter to last 30 days
            time_filter = {
                "startTime": (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"
            }

            # Query for activities on this file
            response = (
                self.drive_activity_service.activity()
                .query(
                    body={
                        "itemName": f"items/{sheet_id}",
                        "filter": f"time >= {time_filter['startTime']}",
                    }
                )
                .execute()
            )

            # Count view events and track unique users
            view_count = 0
            unique_users = set()

            for activity in response.get("activities", []):
                actions = activity.get("primaryActionDetail", {})
                actors = activity.get("actors", [])

                if "view" in actions and actors:
                    view_count += 1

                    # Extract user information
                    for actor in actors:
                        if "user" in actor:
                            user_info = actor.get("user", {})
                            user_id = user_info.get("knownUser", {}).get("personName")
                            if user_id:
                                unique_users.add(user_id)

            return {
                "viewCount": view_count,
                "uniqueUserCount": len(unique_users),
                "lastViewedAt": response.get("activities", [{}])[0].get("timestamp")
                if response.get("activities")
                else None,
            }
        except Exception as e:
            self.report.report_failure(
                message="Error fetching usage stats for sheet", context=sheet_id, exc=e
            )
            return {"viewCount": 0, "uniqueUserCount": 0}

    def _get_sheet_schema(
        self, sheet_id: str, sheet_data: Dict[str, Any]
    ) -> SchemaMetadataClass:
        """Extract schema from Google Sheet."""
        with PerfTimer() as timer:
            fields = []

            # Get all sheets in the spreadsheet
            sheets = sheet_data["spreadsheet"].get("sheets", [])

            for sheet in sheets:
                sheet_name = sheet.get("properties", {}).get("title", "Sheet1")

                # Get the first row as header and sample data to infer types
                range_name = f"{sheet_name}!1:50"  # Get header + sample rows
                try:
                    result = (
                        self.sheets_service.spreadsheets()
                        .values()
                        .get(spreadsheetId=sheet_id, range=range_name)
                        .execute()
                    )

                    rows = result.get("values", [])
                    if not rows:
                        continue

                    headers = rows[0]
                    data_rows = rows[1:] if len(rows) > 1 else []

                    # Create schema fields for each column
                    for i, header in enumerate(headers):
                        # Skip empty headers
                        if not header:
                            continue

                        field_name = f"{sheet_name}.{header}"
                        data_type, native_type = self._infer_type(data_rows, i)

                        fields.append(
                            SchemaFieldClass(
                                fieldPath=field_name,
                                type=SchemaFieldDataTypeClass(type=data_type),
                                nativeDataType=native_type,
                                description=f"Column {header} from sheet {sheet_name}",
                            )
                        )
                except Exception as e:
                    self.report.report_failure(
                        message="Error extracting schema from sheet",
                        context=sheet_name,
                        exc=e,
                    )

            schema_metadata = SchemaMetadataClass(
                schemaName=sheet_data["spreadsheet"]
                .get("properties", {})
                .get("title", "Unknown"),
                platform=make_data_platform_urn(self.platform),
                version=0,
                fields=fields,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
            )

            self.report.extract_schema_timer[sheet_id] = timer.elapsed_seconds()
            return schema_metadata

    def _infer_type(self, data_rows: List[List], column_index: int) -> Tuple[Any, str]:
        """Infer the data type of a column based on sample data."""
        # Basic type inference logic
        if not data_rows or len(data_rows[0]) <= column_index:
            return StringTypeClass(), "STRING"

        non_empty_values = [
            row[column_index]
            for row in data_rows
            if column_index < len(row) and row[column_index]
        ]

        if not non_empty_values:
            return StringTypeClass(), "STRING"

        # Check if all values are numeric
        if all(self._is_number(val) for val in non_empty_values):
            return NumberTypeClass(), "NUMBER"

        # Check if all values are boolean
        if all(val.lower() in ("true", "false") for val in non_empty_values):
            return BooleanTypeClass(), "BOOLEAN"

        # Check if all values might be dates
        date_pattern = re.compile(r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$")
        if all(date_pattern.match(str(val)) for val in non_empty_values):
            return DateTypeClass(), "DATE"

        # Check if potentially an array (has commas)
        if all("," in str(val) for val in non_empty_values):
            return ArrayTypeClass(), "ARRAY"

        # Default to string
        return StringTypeClass(), "STRING"

    def _is_number(self, s: str) -> bool:
        """Check if a string is a number."""
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            return False

    def _get_lineage(
        self, sheet_id: str, sheet_data: Dict[str, Any]
    ) -> Optional[UpstreamLineageClass]:
        """Extract lineage information from Google Sheet formulas and external data sources."""
        if not self.config.extract_lineage_from_formulas:
            return None

        with PerfTimer() as timer:
            upstream_datasets = []
            fine_grained_lineages = []

            # Get all sheets in the spreadsheet
            sheets = sheet_data["spreadsheet"].get("sheets", [])

            for sheet in sheets:
                sheet_name = sheet.get("properties", {}).get("title", "Sheet1")

                # Get all formulas in the sheet
                try:
                    formulas, formula_locations = self._get_sheet_formulas(
                        sheet_id, sheet_name
                    )
                    self.report.report_formulas_processed(len(formulas))

                    # Process formulas to extract lineage
                    for formula_idx, formula in enumerate(formulas):
                        # 1. Check for IMPORTRANGE formulas that link to other Google Sheets
                        import_range_matches = list(
                            re.finditer(
                                r'IMPORTRANGE\s*\(\s*["\']([^"\']+)["\']',
                                formula,
                                re.IGNORECASE,
                            )
                        )

                        for match in import_range_matches:
                            source_sheet_reference = match.group(1)
                            source_sheet_id = self._extract_sheet_id_from_reference(
                                source_sheet_reference
                            )

                            if source_sheet_id:
                                # Add sheet-to-sheet lineage
                                upstream_datasets.append(
                                    UpstreamClass(
                                        dataset=builder.make_dataset_urn_with_platform_instance(
                                            self.platform,
                                            source_sheet_id,
                                            self.config.platform_instance,
                                            self.config.env,
                                        ),
                                        type=DatasetLineageTypeClass.TRANSFORMED,
                                    )
                                )
                                self.report.report_lineage_edge()

                                # Add fine-grained lineage if we can determine column references
                                if (
                                    self.config.extract_column_level_lineage
                                    and formula_idx < len(formula_locations)
                                ):
                                    fine_grained_lineage = (
                                        self._extract_importrange_fine_grained_lineage(
                                            formula,
                                            sheet_id,
                                            source_sheet_id,
                                            sheet_name,
                                            formula_locations[formula_idx],
                                        )
                                    )
                                    if fine_grained_lineage:
                                        fine_grained_lineages.append(
                                            fine_grained_lineage
                                        )
                                        self.report.report_fine_grained_edge()

                        # 2. Check for cross-platform lineage with BigQuery
                        if self.config.enable_cross_platform_lineage:
                            # Check for BigQuery connections
                            bigquery_refs = self._extract_bigquery_references(formula)
                            for ref in bigquery_refs:
                                upstream_datasets.append(
                                    UpstreamClass(
                                        dataset=builder.make_dataset_urn_with_platform_instance(
                                            "bigquery",
                                            ref,
                                            None,  # Platform instance for BigQuery
                                            self.config.env,
                                        ),
                                        type=DatasetLineageTypeClass.TRANSFORMED,
                                    )
                                )
                                self.report.report_lineage_edge()
                except Exception as e:
                    self.report.report_warning(
                        "Error extracting formulas from sheet",
                        context=sheet_name,
                        exc=e,
                    )

            lineage = None
            if upstream_datasets or fine_grained_lineages:
                lineage = UpstreamLineageClass(
                    upstreams=upstream_datasets,
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                )

            self.report.extract_lineage_timer[sheet_id] = timer.elapsed_seconds()
            return lineage

    def _extract_sheet_id_from_reference(self, reference: str) -> Optional[str]:
        """Extract Google Sheet ID from a reference (URL or ID)."""
        # If it's a URL, extract the sheet ID
        url_match = re.search(
            r"https://docs\.google\.com/spreadsheets/d/([^/]+)", reference
        )
        if url_match:
            return url_match.group(1)

        # If it's already an ID (alphanumeric with hyphens)
        if re.match(r"^[-\w]{25,}$", reference):
            return reference

        return None

    def _extract_bigquery_references(self, formula: str) -> List[str]:
        """Extract BigQuery table references from a formula."""
        bigquery_refs = []

        # Look for common BigQuery connectors or formulas
        # 1. Check for direct BigQuery references
        bq_matches = re.finditer(
            r'project[:.]([^.]+)\.([^.]+)\.([^\'"\s)]+)', formula, re.IGNORECASE
        )

        for match in bq_matches:
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            bigquery_refs.append(f"{project_id}.{dataset_id}.{table_id}")

        # 2. Check for JDBC connections to BigQuery
        jdbc_matches = re.finditer(
            r"jdbc:bigquery://([^:;]+):([^:;]+)\.([^:;]+)", formula, re.IGNORECASE
        )

        for match in jdbc_matches:
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            bigquery_refs.append(f"{project_id}.{dataset_id}.{table_id}")

        return bigquery_refs

    def _get_sheet_formulas(
        self, sheet_id: str, sheet_name: str
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        """Get all formulas from a specific sheet."""
        formulas = []
        formula_locations = []

        # Get sheet data with formulas
        result = (
            self.sheets_service.spreadsheets()
            .get(spreadsheetId=sheet_id, ranges=[sheet_name], includeGridData=True)
            .execute()
        )

        # Extract formulas from the grid data
        if "sheets" in result and result["sheets"]:
            sheet_data = result["sheets"][0]
            if "data" in sheet_data and sheet_data["data"]:
                grid_data = sheet_data["data"][0]

                # Extract header row to map column indices to names
                headers = []
                if (
                    "rowData" in grid_data
                    and grid_data["rowData"]
                    and len(grid_data["rowData"]) > 0
                ):
                    first_row = grid_data["rowData"][0]
                    if "values" in first_row:
                        for cell in first_row["values"]:
                            if "formattedValue" in cell:
                                headers.append(cell["formattedValue"])

                # Now extract formulas
                if "rowData" in grid_data:
                    for row_idx, row in enumerate(grid_data["rowData"]):
                        if "values" not in row:
                            continue

                        for col_idx, cell in enumerate(row["values"]):
                            if (
                                "userEnteredValue" in cell
                                and "formulaValue" in cell["userEnteredValue"]
                            ):
                                formula = cell["userEnteredValue"]["formulaValue"]
                                formulas.append(formula)

                                # Store formula location info for column lineage
                                location = {
                                    "row": row_idx,
                                    "column": col_idx,
                                    "header": headers[col_idx]
                                    if col_idx < len(headers)
                                    else f"Column{col_idx}",
                                }
                                formula_locations.append(location)

        return formulas, formula_locations

    def _extract_importrange_fine_grained_lineage(
        self,
        formula: str,
        sheet_id: str,
        source_sheet_id: str,
        sheet_name: str,
        formula_location: Dict[str, Any],
    ) -> Optional[FineGrainedLineageClass]:
        """Extract fine-grained lineage from IMPORTRANGE formula."""
        # Extract source sheet name and range
        range_match = re.search(
            r'IMPORTRANGE\s*\(\s*["\'][^"\']+["\'],\s*["\']([^!]+)!([^"\']+)["\']',
            formula,
            re.IGNORECASE,
        )

        if not range_match:
            return None

        source_sheet_name = range_match.group(1)
        source_range = range_match.group(2)

        # Parse the range to determine columns
        # A simple A1:B10 style range parser
        columns = self._parse_range_columns(source_range)

        if not columns:
            return None

        # Create fine-grained lineage
        downstream_column = formula_location["header"]

        # Create upstreams for each source column
        upstreams = [
            builder.make_schema_field_urn(
                parent_urn=builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    source_sheet_id,
                    self.config.platform_instance,
                    self.config.env,
                ),
                field_path=f"{source_sheet_name}.{column}",
            )
            for column in columns
        ]

        if not upstreams:
            return None

        return FineGrainedLineageClass(
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[
                builder.make_schema_field_urn(
                    parent_urn=builder.make_dataset_urn_with_platform_instance(
                        self.platform,
                        sheet_id,
                        self.config.platform_instance,
                        self.config.env,
                    ),
                    field_path=f"{sheet_name}.{downstream_column}",
                )
            ],
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=upstreams,
            transformOperation="IMPORTRANGE",
        )

    def _parse_range_columns(self, range_str: str) -> List[str]:
        """Parse column names from a range string like A1:B10."""
        # Extract column letters from range
        columns = []

        # Handle simple ranges like A1:B10
        simple_range_match = re.match(r"([A-Za-z]+)(\d+)?:([A-Za-z]+)(\d+)?", range_str)
        if simple_range_match:
            start_col = simple_range_match.group(1)
            end_col = simple_range_match.group(3)

            # Convert column letters to indices
            start_idx = self._column_letter_to_index(start_col)
            end_idx = self._column_letter_to_index(end_col)

            # Generate column names
            for idx in range(start_idx, end_idx + 1):
                columns.append(self._index_to_column_letter(idx))

        # Handle single cell reference like A1
        elif re.match(r"[A-Za-z]+\d+", range_str):
            col_match = re.match(r"([A-Za-z]+)\d+", range_str)
            if col_match:
                columns.append(col_match.group(1))

        return columns

    def _column_letter_to_index(self, column: str) -> int:
        """Convert column letter (A, B, AA, etc.) to index (0, 1, 26, etc.)."""
        index = 0
        for char in column:
            index = index * 26 + (ord(char.upper()) - ord("A") + 1)
        return index - 1

    def _index_to_column_letter(self, index: int) -> str:
        """Convert column index to letter."""
        if index < 0:
            return ""

        column = ""
        index += 1  # 1-based for Excel-style columns

        while index > 0:
            remainder = (index - 1) % 26
            column = chr(ord("A") + remainder) + column
            index = (index - 1) // 26

        return column

    def _profile_column(
        self, df: pd.DataFrame, col: str, sheet_name: str, row_count: int
    ) -> DatasetFieldProfileClass:
        """Profile a single column in a sheet"""
        field_path = f"{sheet_name}.{col}"
        field_profile = DatasetFieldProfileClass(fieldPath=field_path)

        # Convert numeric columns
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            is_numeric = True
        except (ValueError, TypeError):  # Fix for bare except
            is_numeric = False

        # Basic statistics
        non_null_count = df[col].count()
        null_count = row_count - non_null_count

        if self.config.profiling.include_field_null_count:
            field_profile.nullCount = null_count
            field_profile.nullProportion = (
                null_count / row_count if row_count > 0 else 0
            )

        # Unique value stats
        unique_count = df[col].nunique()
        field_profile.uniqueCount = unique_count
        field_profile.uniqueProportion = (
            unique_count / non_null_count if non_null_count > 0 else 0
        )

        # Numeric column stats
        if is_numeric and non_null_count > 0:
            self._add_numeric_column_stats(field_profile, df, col, unique_count)

        return field_profile

    def _add_numeric_column_stats(
        self,
        field_profile: DatasetFieldProfileClass,
        df: pd.DataFrame,
        col: str,
        unique_count: int,
    ) -> None:
        """Add numeric statistics to a column profile"""
        # Min, max, mean, median, stddev
        if self.config.profiling.include_field_min_value:
            field_profile.min = str(df[col].min())

        if self.config.profiling.include_field_max_value:
            field_profile.max = str(df[col].max())

        if self.config.profiling.include_field_mean_value:
            field_profile.mean = str(df[col].mean())

        if self.config.profiling.include_field_median_value:
            field_profile.median = str(df[col].median())

        if self.config.profiling.include_field_stddev_value:
            field_profile.stdev = str(df[col].std())

        # Quantiles
        if self.config.profiling.include_field_quantiles:
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
            quantile_values = df[col].quantile(quantiles)
            field_profile.quantiles = [
                QuantileClass(quantile=str(q), value=str(quantile_values[q]))
                for q in quantiles
            ]

        # Histogram
        if self.config.profiling.include_field_histogram:
            try:
                hist, bin_edges = np.histogram(df[col].dropna(), bins=20)
                field_profile.histogram = HistogramClass(
                    [str(bin_edges[i]) for i in range(len(bin_edges) - 1)],
                    [float(count) for count in hist],
                )
            except Exception as e:
                self.report.report_warning(
                    message="Error generating histogram", context=col, exc=e
                )

        # Value frequencies for low-cardinality columns
        if (
            self.config.profiling.include_field_distinct_value_frequencies
            and unique_count <= 50
        ):
            value_counts = df[col].value_counts().head(50)
            field_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=str(value), frequency=int(count))
                for value, count in value_counts.items()
            ]

        # Sample values
        if self.config.profiling.include_field_sample_values:
            non_null_count = df[col].count()
            sample_size = min(10, non_null_count)
            if sample_size > 0:
                field_profile.sampleValues = [
                    str(val) for val in df[col].dropna().sample(sample_size).tolist()
                ]

    def _profile_sheet(
        self, sheet_id: str, sheet_data: Dict[str, Any]
    ) -> Optional[DatasetProfileClass]:
        """Profile the data in a Google Sheet if profiling is enabled."""
        if not self.config.profiling.enabled:
            return None

        with PerfTimer() as timer:
            profile = DatasetProfileClass(timestampMillis=int(time.time() * 1000))
            sheets = sheet_data["spreadsheet"].get("sheets", [])

            total_row_count = 0
            field_profiles = []

            # Process each sheet in the spreadsheet
            for sheet in sheets:
                sheet_name = sheet.get("properties", {}).get("title", "Sheet1")

                # Get data from the sheet (up to max_rows)
                range_name = f"{sheet_name}!A1:Z{self.config.profiling.max_rows + 1}"
                try:
                    result = (
                        self.sheets_service.spreadsheets()
                        .values()
                        .get(spreadsheetId=sheet_id, range=range_name)
                        .execute()
                    )

                    rows = result.get("values", [])
                    if not rows:
                        continue

                    headers = rows[0]
                    data_rows = rows[1:]

                    # Convert to pandas DataFrame for easier profiling
                    df = pd.DataFrame(data_rows, columns=headers)
                    row_count = len(df)
                    total_row_count += row_count

                    # Apply max number of fields to profile if configured
                    columns_to_profile = self._get_columns_to_profile(df)

                    # Profile each column
                    for col in columns_to_profile:
                        field_profile = self._profile_column(
                            df, col, sheet_name, row_count
                        )
                        field_profiles.append(field_profile)

                except Exception as e:
                    self.report.report_warning(
                        message="Error profiling sheet", context=sheet_name, exc=e
                    )

            profile.rowCount = total_row_count
            profile.columnCount = len(field_profiles)
            profile.fieldProfiles = field_profiles

            self.report.report_sheet_profiled()
            self.report.profile_sheet_timer[sheet_id] = timer.elapsed_seconds()
            return profile

    def _get_columns_to_profile(self, df: pd.DataFrame) -> List[str]:
        """Get the list of columns to profile based on configuration"""
        columns_to_profile = list(df.columns)
        if (
            self.config.profiling.max_number_of_fields_to_profile is not None
            and len(columns_to_profile)
            > self.config.profiling.max_number_of_fields_to_profile
        ):
            columns_to_profile = columns_to_profile[
                : self.config.profiling.max_number_of_fields_to_profile
            ]
        return columns_to_profile

    def _get_container_urn(self, path_part: str) -> str:
        """Generate a container URN for a path part."""
        container_key = GoogleSheetsContainerKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            key=path_part,
        )
        return container_key.as_urn()

    def _create_platform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create a DataPlatformInstance aspect MCP for the specified dataset."""
        if not self.config.platform_instance:
            return None

        platform_instance = DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=builder.make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            ),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=platform_instance
        )

    def _create_dataset_properties_mcps(
        self, sheet: Dict[str, Any], dataset_urn: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create a DatasetProperties aspect MCP for the specified sheet."""
        custom_properties = {
            "createdTime": sheet.get("createdTime", ""),
            "modifiedTime": sheet.get("modifiedTime", ""),
            "webViewLink": sheet.get("webViewLink", ""),
        }

        # Add sharing info
        if "shared" in sheet:
            custom_properties["shared"] = str(sheet["shared"])

        dataset_properties = DatasetPropertiesClass(
            name=sheet.get("name", ""),
            description=sheet.get("description", ""),
            customProperties=custom_properties,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        )

    def _create_ownership_mcps(
        self, sheet: Dict[str, Any], dataset_urn: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create an Ownership aspect MCP for the specified sheet if owners are available."""
        if "owners" not in sheet or not sheet["owners"]:
            return

        owner = sheet["owners"][0]
        if "emailAddress" not in owner:
            return

        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(owner.get("emailAddress")),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )

        yield MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=ownership)

    def _create_status_aspect(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Create a Status aspect MCP."""
        status = StatusClass(removed=False)
        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=status)

    def _create_container_aspect(
        self, sheet: Dict[str, Any], dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create a Container aspect to associate the dataset with its parent container."""
        sheet_path = sheet.get("path", "").lstrip("/")
        path_segments = [segment for segment in sheet_path.split("/") if segment]

        if not path_segments:
            # If no path segments, associate with the root platform container
            container_urn = self._get_container_urn(self.platform)
        else:
            # Associate with the deepest container
            container_urn = self._get_container_urn(path_segments[-1])

        container_aspect = ContainerClass(container=container_urn)

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=container_aspect
        )

    def _create_container_mcps(
        self, path_parts: List[str]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create container entities for each path part and yield MCPs."""
        for i, path_part in enumerate(path_parts):
            container_urn = self._get_container_urn(path_part)

            # Create container properties
            container_properties = ContainerPropertiesClass(
                name=path_part, description=f"Google Drive {path_part}"
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=container_urn, aspect=container_properties
            )

            # Add status aspect
            status = StatusClass(removed=False)
            yield MetadataChangeProposalWrapper(entityUrn=container_urn, aspect=status)

            # Add subtype
            subtype = SubTypesClass(typeNames=["Folder"])
            yield MetadataChangeProposalWrapper(entityUrn=container_urn, aspect=subtype)

            # If not the root, add parent container link
            if i > 0:
                parent_urn = self._get_container_urn(path_parts[i - 1])
                container_parent = ContainerClass(container=parent_urn)
                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn, aspect=container_parent
                )

    def _create_browse_paths_aspect(
        self, sheet: Dict[str, Any], dataset_urn: str
    ) -> MetadataChangeProposalWrapper:
        """Create a BrowsePathsV2 aspect MCP with proper container references."""
        # Get the sheet path and split it into segments
        sheet_path = sheet.get("path", "").lstrip("/")
        path_segments = [segment for segment in sheet_path.split("/") if segment]

        # Create entries for each path segment
        browse_entries: List[BrowsePathEntryClass] = []

        # Add entries for each path segment with container URNs
        for segment in path_segments:
            browse_entries.append(
                BrowsePathEntryClass(id=segment, urn=self._get_container_urn(segment))
            )

        browse_paths = BrowsePathsV2Class(path=browse_entries)

        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=browse_paths)

    def _create_subtype_aspect(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Create a SubTypes aspect MCP."""
        subtype = SubTypesClass(typeNames=["Google Sheet"])
        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=subtype)

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Generate workunits for each Google Sheet."""
        sheets = self._get_sheets_from_drive()

        # Track containers we've already created to avoid duplicates
        created_containers = set()

        for sheet in sheets:
            sheet_id = sheet["id"]

            # Skip already processed sheets
            if sheet_id in self.processed_sheets:
                continue

            self.processed_sheets.add(sheet_id)

            # Get detailed metadata
            sheet_data = self._get_sheet_metadata(sheet_id)

            # Create dataset urn
            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                self.platform, sheet_id, self.config.platform_instance, self.config.env
            )

            # Create containers for path parts if needed
            sheet_path = sheet.get("path", "").lstrip("/")
            path_segments = [segment for segment in sheet_path.split("/") if segment]

            # Always create the platform container if it doesn't exist
            if self.platform not in created_containers:
                platform_container_urn = self._get_container_urn(self.platform)
                platform_container_props = ContainerPropertiesClass(
                    name=self.platform, description=f"{self.platform} Root Container"
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_props
                ).as_workunit()

                platform_container_subtype = SubTypesClass(typeNames=["Platform"])
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_subtype
                ).as_workunit()

                created_containers.add(self.platform)

            # Create other container entities
            for _i, segment in enumerate(path_segments):
                if segment not in created_containers:
                    # Create container MCPs
                    for mcp in self._create_container_mcps([segment]):
                        yield mcp.as_workunit()

                    created_containers.add(segment)

            # Emit dataset entities and aspects

            # Add DataPlatformInstance aspect if configured
            platform_instance_mcp = self._create_platform_instance_aspect(dataset_urn)
            if platform_instance_mcp:
                yield platform_instance_mcp.as_workunit()

            # Add DatasetProperties aspect
            yield from auto_workunit(
                self._create_dataset_properties_mcps(sheet, dataset_urn)
            )

            # Add Ownership aspect if available
            yield from auto_workunit(self._create_ownership_mcps(sheet, dataset_urn))

            # Add schema metadata
            schema_metadata = self._get_sheet_schema(sheet_id, sheet_data)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=schema_metadata
            ).as_workunit()

            # Add browse paths
            browse_paths_mcp = self._create_browse_paths_aspect(sheet, dataset_urn)
            yield browse_paths_mcp.as_workunit()

            # Add container aspect to associate dataset with its container
            container_mcp = self._create_container_aspect(sheet, dataset_urn)
            if container_mcp:
                yield container_mcp.as_workunit()

            # Add subtype
            subtype_mcp = self._create_subtype_aspect(dataset_urn)
            yield subtype_mcp.as_workunit()

            # Add status
            status_mcp = self._create_status_aspect(dataset_urn)
            yield status_mcp.as_workunit()

            # Emit usage statistics if available
            if sheet_data.get("usage_stats") and (
                sheet_data["usage_stats"].get("viewCount", 0) > 0
                or sheet_data["usage_stats"].get("uniqueUserCount", 0) > 0
            ):
                usage_stats = DatasetUsageStatisticsClass(
                    timestampMillis=int(time.time() * 1000),
                    uniqueUserCount=sheet_data["usage_stats"].get("uniqueUserCount", 0),
                    # We could also create DatasetUserUsageCounts for each user if needed
                    userCounts=[]
                    if sheet_data["usage_stats"].get("uniqueUserCount", 0) > 0
                    else None,
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=usage_stats
                ).as_workunit()

            # Emit lineage if available
            lineage = self._get_lineage(sheet_id, sheet_data)
            if lineage:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=lineage
                ).as_workunit()

            # Emit profile if enabled
            if self.config.profiling.enabled:
                profile = self._profile_sheet(sheet_id, sheet_data)
                if profile:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=profile
                    ).as_workunit()

    def get_report(self) -> GoogleSheetsSourceReport:
        return self.report
