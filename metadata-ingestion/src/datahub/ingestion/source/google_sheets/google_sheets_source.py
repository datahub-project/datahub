import logging
import time
from datetime import datetime, timedelta
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

import datahub.emitter.mce_builder as builder
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
from datahub.ingestion.source.google_sheets.config import (
    GoogleSheetsCheckpointState,
    GoogleSheetsSourceConfig,
)
from datahub.ingestion.source.google_sheets.constants import (
    A1_RANGE_PATTERN,
    BIGQUERY_DIRECT_PATTERN,
    BIGQUERY_JDBC_PATTERN,
    CELL_COLUMN_PATTERN,
    DATE_PATTERNS,
    DRIVE_API_FIELDS,
    DRIVE_QUERY_SHEETS_MIMETYPE,
    DRIVE_SPACES_ALL,
    DRIVE_SPACES_DEFAULT,
    FIELD_CREATED_TIME,
    FIELD_DRIVE_ID,
    FIELD_ID,
    FIELD_MODIFIED_TIME,
    FIELD_NAME,
    FIELD_NEXT_PAGE_TOKEN,
    FIELD_PARENTS,
    FIELD_PATH,
    FIELD_SHARED,
    FIELD_SHEET_ID,
    FIELD_SHEETS,
    FIELD_WEB_VIEW_LINK,
    GOOGLE_SCOPES,
    GOOGLE_SHEETS_URL_PATTERN,
    IMPORTRANGE_FULL_PATTERN,
    IMPORTRANGE_URL_PATTERN,
    PERMISSION_ROLE_COMMENTER,
    PERMISSION_ROLE_OWNER,
    PERMISSION_ROLE_READER,
    PERMISSION_ROLE_WRITER,
    POSTGRES_MYSQL_JDBC_PATTERN,
    REDSHIFT_JDBC_PATTERN,
    SHEET_ID_LENIENT_PATTERN,
    SINGLE_CELL_PATTERN,
    SNOWFLAKE_DIRECT_PATTERN,
    SNOWFLAKE_JDBC_PATTERN,
    SQL_PREVIEW_MAX_LENGTH,
    SQL_TABLE_PATTERN,
    SUBTYPE_GOOGLE_SHEETS,
    SUBTYPE_GOOGLE_SHEETS_SPREADSHEET,
    SUBTYPE_GOOGLE_SHEETS_TAB,
    SUBTYPE_PLATFORM,
    TRANSFORM_OPERATION_IMPORTRANGE,
    TYPE_INFERENCE_SAMPLE_SIZE,
)
from datahub.ingestion.source.google_sheets.drive_api import DriveAPIClient
from datahub.ingestion.source.google_sheets.header_detection import HeaderDetector
from datahub.ingestion.source.google_sheets.jdbc_parser import JDBCParser
from datahub.ingestion.source.google_sheets.metadata_builder import MetadataBuilder
from datahub.ingestion.source.google_sheets.models import (
    DatabaseReference,
    DriveActivityResponse,
    DriveFile,
    DriveFileMetadata,
    FormulaExtractionResult,
    FormulaLocation,
    SheetData,
    SheetPathResult,
    SheetUsageStatistics,
    Spreadsheet,
    SpreadsheetProperties,
    parse_spreadsheet,
    parse_values_response,
)
from datahub.ingestion.source.google_sheets.report import GoogleSheetsSourceReport
from datahub.ingestion.source.google_sheets.utils import (
    extract_tags_from_drive_metadata,
    get_named_ranges,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
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
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ValueFrequencyClass,
)
from datahub.sdk.dataset import Dataset
from datahub.sql_parsing.sql_parsing_aggregator import (
    SqlParsingAggregator,
)
from datahub.utilities.perf_timer import PerfTimer


class GoogleSheetsContainerKey(ContainerKey):
    key: str


logger = logging.getLogger(__name__)


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

        self.creds = service_account.Credentials.from_service_account_file(
            config.credentials,
            scopes=GOOGLE_SCOPES,
        )
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.sheets_service = build(FIELD_SHEETS, "v4", credentials=self.creds)
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

        # Rate limiting
        self.last_request_time: Optional[float] = None
        self.min_request_interval = (
            1.0 / self.config.requests_per_second
            if self.config.requests_per_second
            else 0.0
        )

        self.checkpoint_state = GoogleSheetsCheckpointState()
        if self.config.stateful_ingestion and self.config.enable_incremental_ingestion:
            last_checkpoint = self.get_last_checkpoint(  # type: ignore[attr-defined]
                GoogleSheetsCheckpointState.__name__, GoogleSheetsCheckpointState
            )
            if last_checkpoint:
                self.checkpoint_state = last_checkpoint.state

        self._sql_aggregators: Dict[str, Optional[SqlParsingAggregator]] = {}
        self._registered_urns: Set[str] = set()

        self.drive_client = DriveAPIClient(
            self.drive_service, self.config, self.report, self.checkpoint_state
        )
        self.header_detector = HeaderDetector(
            header_detection_mode=self.config.header_detection_mode,
            header_row_index=self.config.header_row_index,
            skip_empty_leading_rows=self.config.skip_empty_leading_rows,
        )
        self.metadata_builder = MetadataBuilder(self.platform, self.config, self.ctx)
        self.jdbc_parser = JDBCParser()

    def get_workunit_processors(self):
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def close(self):
        pass

    def _get_sql_aggregator_for_platform(
        self, platform: str, platform_instance: Optional[str] = None
    ) -> Optional[SqlParsingAggregator]:
        """Get or create SQL parsing aggregator for a specific platform."""
        if not platform:
            logger.debug("No platform specified, skipping SQL aggregator creation")
            return None

        if not self.ctx.graph:
            logger.debug("No DataHub graph available, SQL parsing disabled")
            return None

        if not self.config.parse_sql_for_lineage:
            return None

        cache_key = f"{platform}:{platform_instance or ''}"
        if cache_key in self._sql_aggregators:
            return self._sql_aggregators[cache_key]

        try:
            logger.info(
                f"Creating SQL parsing aggregator for platform: {platform} "
                f"(instance: {platform_instance})"
            )

            self._sql_aggregators[cache_key] = SqlParsingAggregator(
                platform=platform,
                platform_instance=platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
                eager_graph_load=False,
                generate_lineage=True,
                generate_queries=False,
                generate_usage_statistics=False,
                generate_operations=False,
            )
            return self._sql_aggregators[cache_key]

        except (ValueError, TypeError, KeyError) as e:
            logger.info(f"SQL aggregator skipped for {platform}: {e}")
            self._sql_aggregators[cache_key] = None
            return None

    def _extract_and_register_database_lineage(
        self,
        formula: str,
        dataset_urn: str,
    ) -> None:
        """Extract database references from formula and register SQL with aggregator."""
        jdbc_urls = JDBCParser.extract_jdbc_urls(formula)

        for jdbc_url in jdbc_urls:
            result = JDBCParser.parse_jdbc_url_and_sql(jdbc_url, formula)

            if not result.sql_query or not result.platform or not result.database:
                logger.debug(
                    f"Could not extract SQL query from formula with platform={result.platform}, database={result.database}"
                )
                continue

            aggregator = self._get_sql_aggregator_for_platform(result.platform)

            if not aggregator:
                logger.debug(
                    f"No SQL aggregator available for platform {result.platform}, skipping SQL lineage"
                )
                continue

            try:
                default_schema = None
                if result.platform in ["postgres", "redshift"]:
                    default_schema = "public"

                aggregator.add_view_definition(
                    view_urn=dataset_urn,
                    view_definition=result.sql_query,
                    default_db=result.database,
                    default_schema=default_schema,
                )

                self.report.num_sql_queries_parsed += 1
                logger.debug(
                    f"Registered SQL query for {dataset_urn} on platform {result.platform}: {result.sql_query[:SQL_PREVIEW_MAX_LENGTH]}..."
                )

            except Exception as e:
                logger.warning(
                    f"Failed to parse SQL for {dataset_urn} on {result.platform}: {e}. "
                    f"SQL preview: {result.sql_query[:SQL_PREVIEW_MAX_LENGTH]}..."
                )
                self.report.num_sql_queries_failed += 1

    def _should_skip_sheet_not_modified(
        self, sheet_name: str, modified_time: str
    ) -> bool:
        """Check if sheet should be skipped because it hasn't been modified since last run."""
        if not self.config.enable_incremental_ingestion:
            return False

        if not self.checkpoint_state.last_run_time or not modified_time:
            return False

        if modified_time <= self.checkpoint_state.last_run_time:
            self.report.report_sheet_dropped(f"{sheet_name} (not modified)")
            return True

        return False

    def _process_sheet_from_drive(self, sheet: Dict[str, Any]) -> Optional[DriveFile]:
        """Process a single sheet from Drive API response. Returns the sheet if it passes all filters, None otherwise."""
        sheet_id = sheet[FIELD_ID]
        sheet_name = sheet[FIELD_NAME]
        modified_time = sheet.get(FIELD_MODIFIED_TIME, "")

        if self._should_skip_sheet_not_modified(sheet_name, modified_time):
            return None

        if self.config.scan_shared_drives and self.config.shared_drive_patterns:
            drive_id = sheet.get(FIELD_DRIVE_ID)
            if drive_id:
                allowed_drives = self.drive_client.get_allowed_shared_drives()
                if allowed_drives and drive_id not in allowed_drives:
                    self.report.report_sheet_dropped(
                        f"{sheet_name} (shared drive filtered)"
                    )
                    return None

        path_result = self.drive_client.get_sheet_path(sheet_id)
        sheet[FIELD_PATH] = path_result.full_path

        folder_path_str = (
            "/".join(path_result.folder_path_parts)
            if path_result.folder_path_parts
            else "/"
        )
        if not self.config.folder_patterns.allowed(folder_path_str):
            self.report.report_sheet_dropped(f"{sheet_name} (folder filtered)")
            return None

        if not self.config.sheet_patterns.allowed(sheet_name):
            self.report.report_sheet_dropped(sheet_name)
            return None

        self.report.report_sheet_scanned()

        if self.config.enable_incremental_ingestion and modified_time:
            self.checkpoint_state.sheet_modified_times[sheet_id] = modified_time

        return DriveFile.from_dict(sheet)

    def _fetch_drive_sheets_page(
        self, query: str, spaces: str, page_token: Optional[str]
    ) -> Dict[str, Any]:
        """Fetch a single page of sheets from Drive API with rate limiting."""
        self.drive_client._rate_limit()

        return (
            self.drive_service.files()
            .list(
                q=query,
                spaces=spaces,
                fields=DRIVE_API_FIELDS,
                pageToken=page_token,
            )
            .execute()
        )

    def _get_sheets_from_drive(self) -> List[DriveFile]:
        """Get all Google Sheets from Drive that match the configured patterns."""
        with PerfTimer() as timer:
            sheets: List[DriveFile] = []
            page_token = None

            query = DRIVE_QUERY_SHEETS_MIMETYPE
            spaces = (
                DRIVE_SPACES_ALL
                if self.config.scan_shared_drives
                else DRIVE_SPACES_DEFAULT
            )

            retry_count = 0
            max_retries = self.config.max_retries

            while True:
                try:
                    response = self._fetch_drive_sheets_page(query, spaces, page_token)

                    for sheet_data in response.get("files", []):
                        processed_sheet = self.drive_client._process_sheet_from_drive(
                            sheet_data
                        )
                        if processed_sheet:
                            sheets.append(processed_sheet)

                    page_token = response.get(FIELD_NEXT_PAGE_TOKEN)
                    if not page_token:
                        break

                    retry_count = 0

                except Exception as e:
                    self.report.report_warning(
                        message=f"Error fetching sheets from Google Drive (attempt {retry_count + 1}/{max_retries + 1})",
                        exc=e,
                    )

                    if retry_count < max_retries:
                        retry_count += 1
                        time.sleep(self.config.retry_delay)
                        continue
                    else:
                        break

            self.report.scan_sheets_timer["total"] = timer.elapsed_seconds()
            return sheets

    def _get_sheet_path(self, sheet_id: str) -> SheetPathResult:
        """Get the folder path for a Google Sheet and return both full path and folder path components."""
        try:
            file_metadata_dict = (
                self.drive_service.files()
                .get(fileId=sheet_id, fields=f"{FIELD_PARENTS},{FIELD_NAME}")
                .execute()
            )

            file_metadata = DriveFileMetadata.model_validate(file_metadata_dict)

            parent_id = file_metadata.parents[0] if file_metadata.parents else None
            if not parent_id:
                return SheetPathResult(
                    full_path=f"/{file_metadata.name}",
                    folder_path_parts=[],
                )

            folder_path_parts: List[str] = []
            while parent_id:
                try:
                    folder_dict = (
                        self.drive_service.files()
                        .get(
                            fileId=parent_id,
                            fields=f"{FIELD_ID},{FIELD_NAME},{FIELD_PARENTS}",
                        )
                        .execute()
                    )

                    folder = DriveFileMetadata.model_validate(folder_dict)
                    folder_path_parts.insert(0, folder.name)
                    parent_id = folder.parents[0] if folder.parents else None
                except Exception as e:
                    self.report.report_warning(
                        message="Error getting folder info", exc=e
                    )
                    break

            full_path_parts = folder_path_parts + [file_metadata.name]
            full_path = "/" + "/".join(full_path_parts)

            return SheetPathResult(
                full_path=full_path, folder_path_parts=folder_path_parts
            )
        except Exception as e:
            self.report.report_warning(
                "Error getting sheet path", context=sheet_id, exc=e
            )
            return SheetPathResult(full_path=f"/{sheet_id}", folder_path_parts=[])

    def _get_sheet_metadata(self, sheet_id: str) -> SheetData:
        """Get detailed metadata for a specific Google Sheet.

        Returns SheetData model with spreadsheet, usage_stats, and named_ranges.
        """
        try:
            self.drive_client._rate_limit()

            raw_spreadsheet = (
                self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            )

            spreadsheet = parse_spreadsheet(raw_spreadsheet)

            usage_stats = None
            if self.config.extract_usage_stats and self.drive_activity_service:
                with PerfTimer() as timer:
                    usage_stats = self._get_sheet_usage_stats(sheet_id)
                    self.report.usage_stats_timer[sheet_id] = timer.elapsed_seconds()

            named_ranges = []
            if self.config.extract_named_ranges:
                named_ranges = get_named_ranges(spreadsheet)

            return SheetData(
                spreadsheet=spreadsheet,
                usage_stats=usage_stats,
                named_ranges=named_ranges,
            )
        except Exception as e:
            self.report.report_failure(
                message="Error fetching sheet metadata for", context=sheet_id, exc=e
            )

            return SheetData(
                spreadsheet=Spreadsheet(
                    spreadsheetId=sheet_id,
                    properties=SpreadsheetProperties(title=sheet_id),
                    sheets=[],
                ),
                usage_stats=None,
                named_ranges=[],
            )

    def _get_sheet_usage_stats(self, sheet_id: str) -> SheetUsageStatistics:
        """Get usage statistics for a Google Sheet using Drive Activity API."""
        if not self.drive_activity_service:
            return SheetUsageStatistics(view_count=0, unique_user_count=0)

        try:
            start_time = (
                datetime.utcnow()
                - timedelta(days=self.config.usage_stats_lookback_days)
            ).isoformat() + "Z"

            response_dict = (
                self.drive_activity_service.activity()
                .query(
                    body={
                        "itemName": f"items/{sheet_id}",
                        "filter": f"time >= {start_time}",
                    }
                )
                .execute()
            )

            response = DriveActivityResponse.model_validate(response_dict)

            view_count = 0
            unique_users: Set[str] = set()

            for activity in response.activities:
                if activity.primaryActionDetail and activity.primaryActionDetail.view:
                    if activity.actors:
                        view_count += 1

                        for actor in activity.actors:
                            if (
                                actor.user
                                and actor.user.knownUser
                                and actor.user.knownUser.personName
                            ):
                                unique_users.add(actor.user.knownUser.personName)

            return SheetUsageStatistics(
                view_count=view_count,
                unique_user_count=len(unique_users),
                last_viewed_at=response.activities[0].timestamp
                if response.activities
                else None,
            )
        except Exception as e:
            self.report.report_failure(
                message="Error fetching usage stats for sheet", context=sheet_id, exc=e
            )
            return SheetUsageStatistics(view_count=0, unique_user_count=0)

    def _get_sheet_schema(
        self,
        sheet_id: str,
        sheet_data: SheetData,
        sheet_name: Optional[str] = None,
    ) -> SchemaMetadataClass:
        """Extract schema from Google Sheet.

        Args:
            sheet_id: The spreadsheet ID
            sheet_data: The spreadsheet metadata
            sheet_name: If provided, only extract schema for this specific sheet (for sheets_as_datasets mode)
        """
        with PerfTimer() as timer:
            fields = []

            sheets = sheet_data.spreadsheet.sheets

            if sheet_name:
                sheets = [s for s in sheets if s.properties.title == sheet_name]

            for sheet in sheets:
                current_sheet_name = sheet.properties.title

                range_name = f"{current_sheet_name}!1:{TYPE_INFERENCE_SAMPLE_SIZE}"
                try:
                    result = (
                        self.sheets_service.spreadsheets()
                        .values()
                        .get(spreadsheetId=sheet_id, range=range_name)
                        .execute()
                    )

                    values_response = parse_values_response(result)
                    if not values_response.values:
                        continue

                    header_row_idx = self.header_detector.find_header_row(
                        values_response.values
                    )

                    if header_row_idx == -1:
                        headers = [
                            self.header_detector.generate_column_letter_name(i)
                            for i in range(
                                len(values_response.values[0])
                                if values_response.values
                                else 0
                            )
                        ]
                        data_rows = values_response.values
                    else:
                        headers = values_response.values[header_row_idx]
                        data_rows = (
                            values_response.values[header_row_idx + 1 :]
                            if header_row_idx + 1 < len(values_response.values)
                            else []
                        )

                    for i, header in enumerate(headers):
                        if not header:
                            header = HeaderDetector.generate_column_letter_name(i)

                        if self.config.sheets_as_datasets:
                            field_name = header
                        else:
                            field_name = f"{current_sheet_name}.{header}"

                        data_type, native_type = self._infer_type(data_rows, i)

                        fields.append(
                            SchemaFieldClass(
                                fieldPath=field_name,
                                type=SchemaFieldDataTypeClass(type=data_type),
                                nativeDataType=native_type,
                            )
                        )
                except Exception as e:
                    self.report.report_failure(
                        message="Error extracting schema from sheet",
                        context=current_sheet_name,
                        exc=e,
                    )

            schema_name = (
                sheet_name if sheet_name else sheet_data.spreadsheet.properties.title
            )
            schema_metadata = SchemaMetadataClass(
                schemaName=schema_name,
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
        if not data_rows or len(data_rows[0]) <= column_index:
            return StringTypeClass(), "STRING"

        non_empty_values = [
            row[column_index]
            for row in data_rows
            if column_index < len(row) and row[column_index]
        ]

        if not non_empty_values:
            return StringTypeClass(), "STRING"

        if all(self._is_number(val) for val in non_empty_values):
            return NumberTypeClass(), "NUMBER"

        if all(
            str(val).lower() in ("true", "false", "yes", "no", "1", "0")
            for val in non_empty_values
        ):
            return BooleanTypeClass(), "BOOLEAN"

        date_count = 0
        for val in non_empty_values:
            if self._is_date(str(val)):
                date_count += 1

        if date_count / len(non_empty_values) > 0.8:
            return DateTypeClass(), "DATE"

        if all("," in str(val) for val in non_empty_values):
            return ArrayTypeClass(), "ARRAY"

        return StringTypeClass(), "STRING"

    def _is_date(self, s: str) -> bool:
        """Check if a string represents a date using various patterns."""
        try:
            from dateutil import parser

            parser.parse(s, fuzzy=False)
            return True
        except (ValueError, TypeError, parser.ParserError):
            # Fall back to pattern matching for common date formats
            return any(pattern.match(s) for pattern in DATE_PATTERNS)

    def _is_number(self, s: str) -> bool:
        """Check if a string is a number."""
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            return False

    def _get_lineage(
        self,
        sheet_id: str,
        sheet_data: SheetData,
        sheet_name: Optional[str] = None,
    ) -> Optional[UpstreamLineageClass]:
        """Extract lineage information from Google Sheet formulas and external data sources.

        Args:
            sheet_id: The spreadsheet ID
            sheet_data: Complete spreadsheet data from API
            sheet_name: Optional sheet tab name to filter lineage extraction to a specific tab.
                       If None, extracts lineage from all sheets (spreadsheet mode).
        """
        if not self.config.extract_lineage_from_formulas:
            return None

        with PerfTimer() as timer:
            upstream_datasets = []
            fine_grained_lineages = []

            sheets = sheet_data.spreadsheet.sheets

            if sheet_name:
                sheets = [s for s in sheets if s.properties.title == sheet_name]

            for sheet in sheets:
                sheet_name = sheet.properties.title

                try:
                    result = self._get_sheet_formulas(sheet_id, sheet_name)
                    self.report.report_formulas_processed(len(result.formulas))

                    for formula_idx, formula in enumerate(result.formulas):
                        import_range_matches = list(
                            IMPORTRANGE_URL_PATTERN.finditer(formula)
                        )

                        for match in import_range_matches:
                            source_sheet_reference = match.group(1)
                            source_sheet_id = self._extract_sheet_id_from_reference(
                                source_sheet_reference
                            )

                            if source_sheet_id:
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

                                if (
                                    self.config.extract_column_level_lineage
                                    and formula_idx < len(result.locations)
                                ):
                                    fine_grained_lineage = (
                                        self._extract_importrange_fine_grained_lineage(
                                            formula,
                                            sheet_id,
                                            source_sheet_id,
                                            sheet_name,
                                            result.locations[formula_idx],
                                        )
                                    )
                                    if fine_grained_lineage:
                                        fine_grained_lineages.append(
                                            fine_grained_lineage
                                        )
                                        self.report.report_fine_grained_edge()

                        if self.config.enable_cross_platform_lineage:
                            if self.config.parse_sql_for_lineage:
                                current_dataset_urn = (
                                    builder.make_dataset_urn_with_platform_instance(
                                        self.platform,
                                        sheet_id
                                        if not self.config.sheets_as_datasets
                                        else f"{sheet_id}.{sheet_name}",
                                        self.config.platform_instance,
                                        self.config.env,
                                    )
                                )
                                self._extract_and_register_database_lineage(
                                    formula=formula,
                                    dataset_urn=current_dataset_urn,
                                )

                            db_refs = self._extract_bigquery_references(formula)
                            for db_ref in db_refs:
                                upstream_datasets.append(
                                    UpstreamClass(
                                        dataset=builder.make_dataset_urn_with_platform_instance(
                                            db_ref.platform,
                                            db_ref.table_identifier,
                                            None,  # Platform instance
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
        url_match = GOOGLE_SHEETS_URL_PATTERN.search(reference)
        if url_match:
            return url_match.group(1)

        if SHEET_ID_LENIENT_PATTERN.match(reference):
            return reference

        return None

    def _extract_bigquery_references(self, formula: str) -> List[DatabaseReference]:
        """Extract database table references from formulas.

        Returns:
            List of DatabaseReference objects with platform and table_identifier
        """
        db_refs: List[DatabaseReference] = []

        # BigQuery references (using pre-compiled patterns)
        for match in BIGQUERY_DIRECT_PATTERN.finditer(formula):
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            db_refs.append(
                DatabaseReference.create_bigquery(project_id, dataset_id, table_id)
            )

        for match in BIGQUERY_JDBC_PATTERN.finditer(formula):
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            db_refs.append(
                DatabaseReference.create_bigquery(project_id, dataset_id, table_id)
            )

        # Snowflake references (using pre-compiled patterns)
        sf_matches = SNOWFLAKE_DIRECT_PATTERN.finditer(formula)
        if "snowflake" in formula.lower() or "jdbc:snowflake" in formula.lower():
            for match in sf_matches:
                database = match.group(1)
                schema = match.group(2)
                table = match.group(3)
                db_refs.append(
                    DatabaseReference.create_snowflake(database, schema, table)
                )

        # JDBC Snowflake connections (using pre-compiled pattern)
        for match in SNOWFLAKE_JDBC_PATTERN.finditer(formula):
            database = match.group(2)
            schema = match.group(3)
            db_refs.append(DatabaseReference.create_snowflake(database, schema, None))

        # Postgres/MySQL JDBC connections (using pre-compiled pattern)
        for match in POSTGRES_MYSQL_JDBC_PATTERN.finditer(formula):
            platform_name = match.group(1).lower()
            database = match.group(2)
            table_match = SQL_TABLE_PATTERN.search(formula)
            if table_match:
                table = table_match.group(1)
                if platform_name == "postgresql":
                    # Postgres uses database.schema.table, assume public schema
                    db_refs.append(
                        DatabaseReference.create_postgres(database, "public", table)
                    )
                else:
                    # MySQL uses database.table (no schema)
                    db_refs.append(DatabaseReference.create_mysql(database, table))

        # Redshift JDBC connections (using pre-compiled pattern)
        for match in REDSHIFT_JDBC_PATTERN.finditer(formula):
            database = match.group(1)
            table_match = SQL_TABLE_PATTERN.search(formula)
            if table_match:
                table = table_match.group(1)
                db_refs.append(
                    DatabaseReference.create_redshift(database, "public", table)
                )

        return db_refs

    def _get_sheet_formulas(
        self, sheet_id: str, sheet_name: str
    ) -> FormulaExtractionResult:
        """Get all formulas from a specific sheet."""
        formulas = []
        formula_locations: List[FormulaLocation] = []

        result = (
            self.sheets_service.spreadsheets()
            .get(spreadsheetId=sheet_id, ranges=[sheet_name], includeGridData=True)
            .execute()
        )

        if FIELD_SHEETS in result and result[FIELD_SHEETS]:
            sheet_data = result[FIELD_SHEETS][0]
            if "data" in sheet_data and sheet_data["data"]:
                grid_data = sheet_data["data"][0]

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
                                location = FormulaLocation(
                                    row=row_idx,
                                    column=col_idx,
                                    header=headers[col_idx]
                                    if col_idx < len(headers)
                                    else f"Column{col_idx}",
                                )
                                formula_locations.append(location)

        return FormulaExtractionResult(formulas=formulas, locations=formula_locations)

    def _extract_importrange_fine_grained_lineage(
        self,
        formula: str,
        sheet_id: str,
        source_sheet_id: str,
        sheet_name: str,
        formula_location: FormulaLocation,
    ) -> Optional[FineGrainedLineageClass]:
        """Extract fine-grained lineage from IMPORTRANGE formula."""
        range_match = IMPORTRANGE_FULL_PATTERN.search(formula)

        if not range_match:
            return None

        source_sheet_name = range_match.group(1)
        source_range = range_match.group(2)

        columns = self._parse_range_columns(source_range)

        if not columns:
            return None

        downstream_column = formula_location.header

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
            transformOperation=TRANSFORM_OPERATION_IMPORTRANGE,
        )

    def _parse_range_columns(self, range_str: str) -> List[str]:
        """Parse column names from a range string like A1:B10."""
        columns = []

        simple_range_match = A1_RANGE_PATTERN.match(range_str)
        if simple_range_match:
            start_col = simple_range_match.group(1)
            end_col = simple_range_match.group(3)

            start_idx = self._column_letter_to_index(start_col)
            end_idx = self._column_letter_to_index(end_col)

            for idx in range(start_idx, end_idx + 1):
                columns.append(self._index_to_column_letter(idx))

        elif SINGLE_CELL_PATTERN.match(range_str):
            col_match = CELL_COLUMN_PATTERN.match(range_str)
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

        try:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            is_numeric = True
        except (ValueError, TypeError):  # Fix for bare except
            is_numeric = False

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

        if self.config.profiling.include_field_quantiles:
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
            quantile_values = df[col].quantile(quantiles)
            field_profile.quantiles = [
                QuantileClass(quantile=str(q), value=str(quantile_values[q]))
                for q in quantiles
            ]

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

        if self.config.profiling.include_field_sample_values:
            non_null_count = df[col].count()
            sample_size = min(10, non_null_count)
            if sample_size > 0:
                field_profile.sampleValues = [
                    str(val) for val in df[col].dropna().sample(sample_size).tolist()
                ]

    def _profile_sheet(
        self, sheet_id: str, sheet_data: SheetData
    ) -> Optional[DatasetProfileClass]:
        """Profile the data in a Google Sheet if profiling is enabled."""
        if not self.config.profiling.enabled:
            return None

        with PerfTimer() as timer:
            profile = DatasetProfileClass(timestampMillis=int(time.time() * 1000))
            sheets = sheet_data.spreadsheet.sheets

            total_row_count = 0
            field_profiles = []

            for sheet in sheets:
                sheet_name = sheet.properties.title

                max_rows = self.config.profiling.limit or 10000
                range_name = f"{sheet_name}!1:{max_rows + 1}"
                try:
                    result = (
                        self.sheets_service.spreadsheets()
                        .values()
                        .get(spreadsheetId=sheet_id, range=range_name)
                        .execute()
                    )

                    values_response = parse_values_response(result)
                    if not values_response.values:
                        continue

                    rows = values_response.values

                    headers = rows[0]
                    data_rows = rows[1:]

                    df = pd.DataFrame(data_rows, columns=headers)
                    row_count = len(df)
                    total_row_count += row_count

                    columns_to_profile = self._get_columns_to_profile(df)

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

    def _profile_sheet_tab(
        self, sheet_id: str, sheet_data: SheetData, sheet_name: str
    ) -> Optional[DatasetProfileClass]:
        """Profile a specific sheet tab in sheets_as_datasets mode."""
        if not self.config.profiling.enabled:
            return None

        with PerfTimer() as timer:
            profile = DatasetProfileClass(timestampMillis=int(time.time() * 1000))
            field_profiles = []

            max_rows = self.config.profiling.limit or 10000
            range_name = f"{sheet_name}!1:{max_rows + 1}"
            try:
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=sheet_id, range=range_name)
                    .execute()
                )

                values_response = parse_values_response(result)
                if not values_response.values:
                    return None

                rows = values_response.values

                headers = rows[0]
                data_rows = rows[1:]

                df = pd.DataFrame(data_rows, columns=headers)
                row_count = len(df)

                columns_to_profile = self._get_columns_to_profile(df)

                for col in columns_to_profile:
                    field_profile = self._profile_column(
                        df,
                        col,
                        sheet_name if not self.config.sheets_as_datasets else "",
                        row_count,
                    )
                    # Fix field path for sheets_as_datasets mode
                    if self.config.sheets_as_datasets:
                        field_profile.fieldPath = col
                    field_profiles.append(field_profile)

                profile.rowCount = row_count
                profile.columnCount = len(field_profiles)
                profile.fieldProfiles = field_profiles

                self.report.report_sheet_profiled()
                self.report.profile_sheet_timer[f"{sheet_id}/{sheet_name}"] = (
                    timer.elapsed_seconds()
                )
                return profile

            except Exception as e:
                self.report.report_warning(
                    message="Error profiling sheet tab", context=sheet_name, exc=e
                )
                return None

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

    def _extract_tags_from_drive_metadata(
        self, sheet_file: DriveFile
    ) -> List[TagAssociationClass]:
        """Extract tags from Google Drive file labels and custom properties.

        Parses the labelInfo field from Drive API v3 to extract applied labels.
        See: https://developers.google.com/drive/api/v3/reference/files#labelInfo

        Note: Metadata like sharing status and owners are captured elsewhere:
        - Sharing status: Added to customProperties
        - Owners: Added as OwnershipClass aspects

        Args:
            sheet_file: Drive file metadata (Pydantic model)

        Returns:
            List of tag associations from Drive labels
        """
        return extract_tags_from_drive_metadata(sheet_file, self.config)

    def _create_dataset_with_sdk(
        self,
        dataset_id: str,
        sheet_file: DriveFile,
        schema_fields: Optional[List[Tuple[str, str, str]]] = None,
        upstream_lineage: Optional[List[str]] = None,
    ) -> Dataset:
        """Create a Dataset using SDK V2 builders.

        This method demonstrates the SDK V2 approach for creating datasets
        with type-safe builders and fluent APIs.

        Args:
            dataset_id: The unique identifier for the dataset
            sheet_file: The DriveFile model with sheet metadata
            schema_fields: Optional list of (name, type, description) tuples
            upstream_lineage: Optional list of upstream dataset URNs

        Returns:
            Dataset entity configured with all metadata
        """
        dataset = Dataset(
            platform=self.platform,
            name=dataset_id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            description=sheet_file.description,
            display_name=sheet_file.name,
        )

        custom_properties = {
            FIELD_CREATED_TIME: sheet_file.createdTime,
            FIELD_MODIFIED_TIME: sheet_file.modifiedTime,
            FIELD_WEB_VIEW_LINK: sheet_file.webViewLink,
        }

        sheet_dict = sheet_file.model_dump()
        if FIELD_SHARED in sheet_dict:
            custom_properties[FIELD_SHARED] = str(sheet_dict[FIELD_SHARED])

        dataset.set_custom_properties(custom_properties)

        owners = []
        if sheet_file.owners:
            owners.extend(
                [
                    owner.emailAddress
                    for owner in sheet_file.owners
                    if owner.emailAddress
                ]
            )
        if sheet_file.permissions:
            for permission in sheet_file.permissions:
                if (
                    permission.emailAddress
                    and permission.role == PERMISSION_ROLE_WRITER
                ):
                    owners.append(permission.emailAddress)

        if owners:
            owner_urns: Sequence[str] = [
                builder.make_user_urn(email) for email in owners
            ]
            dataset.set_owners(cast(Any, owner_urns))

        tags_list = self._extract_tags_from_drive_metadata(sheet_file)
        if tags_list:
            tag_names = [tag.tag.split(":")[-1] for tag in tags_list]
            dataset.set_tags(tag_names)

        if schema_fields:
            dataset._set_schema(schema_fields)

        if upstream_lineage:
            dataset.set_upstreams(cast(Any, upstream_lineage))

        dataset.set_subtype(SUBTYPE_GOOGLE_SHEETS)

        return dataset

    def _create_dataset_properties_mcps(
        self,
        sheet_file: DriveFile,
        dataset_urn: str,
        sheet_data: Optional[SheetData] = None,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create a DatasetProperties aspect MCP for the specified sheet."""
        custom_properties = {
            FIELD_CREATED_TIME: sheet_file.createdTime,
            FIELD_MODIFIED_TIME: sheet_file.modifiedTime,
            FIELD_WEB_VIEW_LINK: sheet_file.webViewLink,
        }

        sheet_dict = sheet_file.model_dump()
        if FIELD_SHARED in sheet_dict:
            custom_properties[FIELD_SHARED] = str(sheet_dict[FIELD_SHARED])

        if sheet_data and sheet_data.named_ranges:
            for idx, named_range in enumerate(sheet_data.named_ranges):
                custom_properties[f"namedRange_{idx + 1}"] = (
                    f"{named_range.name} = {named_range.range_notation}"
                )

        dataset_properties = DatasetPropertiesClass(
            name=sheet_file.name,
            description=sheet_file.description,
            customProperties=custom_properties,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        )

    def _create_ownership_mcps(
        self, sheet_file: DriveFile, dataset_urn: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create an Ownership aspect MCP for the specified sheet if owners/permissions are available."""
        owners_list = []

        if sheet_file.owners:
            for owner in sheet_file.owners:
                if owner.emailAddress:
                    owners_list.append(
                        OwnerClass(
                            owner=builder.make_user_urn(owner.emailAddress),
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    )

        if sheet_file.permissions:
            for permission in sheet_file.permissions:
                if not permission.emailAddress:
                    continue

                email = permission.emailAddress
                role = permission.role

                if role == PERMISSION_ROLE_OWNER:
                    continue

                # Map role to ownership type
                if role == PERMISSION_ROLE_WRITER:
                    ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
                elif role in (PERMISSION_ROLE_COMMENTER, PERMISSION_ROLE_READER):
                    continue
                else:
                    # Unknown role, use a generic type
                    ownership_type = OwnershipTypeClass.TECHNICAL_OWNER

                owners_list.append(
                    OwnerClass(
                        owner=builder.make_user_urn(email),
                        type=ownership_type,
                    )
                )

        if not owners_list:
            return

        ownership = OwnershipClass(owners=owners_list)
        yield MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=ownership)

    def _create_status_aspect(self, dataset_urn: str) -> MetadataChangeProposalWrapper:
        """Create a Status aspect MCP."""
        status = StatusClass(removed=False)
        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=status)

    def _generate_sheet_datasets_workunits(
        self,
        sheets: List[DriveFile],
        created_containers: Set[str],
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Generate workunits treating individual sheets (tabs) as separate datasets.

        In this mode:
        - Each sheet tab becomes a dataset
        - The spreadsheet becomes a container
        """
        for sheet_file in sheets:
            spreadsheet_id = sheet_file.id

            if spreadsheet_id in self.processed_sheets:
                continue

            self.processed_sheets.add(spreadsheet_id)

            sheet_data = self._get_sheet_metadata(spreadsheet_id)

            spreadsheet_container_urn = self.metadata_builder.get_container_urn(
                f"spreadsheet_{spreadsheet_id}"
            )

            if spreadsheet_container_urn not in created_containers:
                container_props = ContainerPropertiesClass(
                    name=sheet_file.name or spreadsheet_id,
                    description=sheet_file.description,
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=spreadsheet_container_urn, aspect=container_props
                ).as_workunit()

                container_subtype = SubTypesClass(
                    typeNames=[SUBTYPE_GOOGLE_SHEETS_SPREADSHEET]
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=spreadsheet_container_urn, aspect=container_subtype
                ).as_workunit()

                # Link to parent folder container if available
                sheet_path = sheet_file.path.lstrip("/")
                path_segments = [
                    segment for segment in sheet_path.split("/") if segment
                ][:-1]  # Exclude filename
                if path_segments:
                    parent_container_urn = self.metadata_builder.get_container_urn(
                        path_segments[-1]
                    )
                    parent_container = ContainerClass(container=parent_container_urn)
                    yield MetadataChangeProposalWrapper(
                        entityUrn=spreadsheet_container_urn, aspect=parent_container
                    ).as_workunit()

                created_containers.add(spreadsheet_container_urn)

            # Now process each sheet tab as a separate dataset
            sheets_tabs = sheet_data.spreadsheet.sheets

            for sheet_tab in sheets_tabs:
                sheet_tab_name = sheet_tab.properties.title
                sheet_tab_id = sheet_tab.properties.sheetId

                dataset_id = f"{spreadsheet_id}/{sheet_tab_name}"
                dataset_urn = builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_id,
                    self.config.platform_instance,
                    self.config.env,
                )

                platform_instance_mcp = (
                    self.metadata_builder.create_platform_instance_aspect(dataset_urn)
                )
                if platform_instance_mcp:
                    yield platform_instance_mcp.as_workunit()

                custom_properties = {
                    "spreadsheetId": spreadsheet_id,
                    FIELD_SHEET_ID: str(sheet_tab_id),
                    "sheetName": sheet_tab_name,
                    FIELD_CREATED_TIME: sheet_file.createdTime,
                    FIELD_MODIFIED_TIME: sheet_file.modifiedTime,
                    FIELD_WEB_VIEW_LINK: sheet_file.webViewLink,
                }

                dataset_properties = DatasetPropertiesClass(
                    name=sheet_tab_name,
                    description=sheet_file.description,
                    customProperties=custom_properties,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=dataset_properties
                ).as_workunit()

                yield from auto_workunit(
                    self._create_ownership_mcps(sheet_file, dataset_urn)
                )

                schema_metadata = self._get_sheet_schema(
                    spreadsheet_id, sheet_data, sheet_tab_name
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=schema_metadata
                ).as_workunit()

                tags = self._extract_tags_from_drive_metadata(sheet_file)
                if tags:
                    global_tags = GlobalTagsClass(tags=tags)
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=global_tags
                    ).as_workunit()

                container_aspect = ContainerClass(container=spreadsheet_container_urn)
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=container_aspect
                ).as_workunit()

                subtype = SubTypesClass(typeNames=[SUBTYPE_GOOGLE_SHEETS_TAB])
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=subtype
                ).as_workunit()

                status = StatusClass(removed=False)
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=status
                ).as_workunit()

                browse_paths_mcp = self.metadata_builder.create_browse_paths_aspect(
                    sheet_file, dataset_urn
                )
                yield browse_paths_mcp.as_workunit()

                # Emit tab-specific lineage
                if self.config.extract_lineage_from_formulas:
                    lineage = self._get_lineage(
                        spreadsheet_id, sheet_data, sheet_name=sheet_tab_name
                    )
                    if lineage:
                        yield MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn, aspect=lineage
                        ).as_workunit()

                # Note: Usage stats are at spreadsheet level, not sheet tab level
                # We emit the same usage stats for each tab for consistency
                if self.config.extract_usage_stats:
                    usage_stats = self._get_sheet_usage_stats(spreadsheet_id)
                    if usage_stats and usage_stats.view_count > 0:
                        dataset_usage_stats = DatasetUsageStatisticsClass(
                            timestampMillis=int(time.time() * 1000),
                            eventGranularity=None,
                            uniqueUserCount=usage_stats.unique_user_count,
                        )
                        yield MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn, aspect=dataset_usage_stats
                        ).as_workunit()

                # Emit profile if enabled (profile individual sheet)
                if self.config.profiling.enabled:
                    profile = self._profile_sheet_tab(
                        spreadsheet_id, sheet_data, sheet_tab_name
                    )
                    if profile:
                        yield MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn, aspect=profile
                        ).as_workunit()

    def _generate_spreadsheet_datasets_workunits(
        self,
        sheets: List[DriveFile],
        created_containers: Set[str],
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Generate workunits treating entire spreadsheets as datasets (original behavior).

        In this mode:
        - Each spreadsheet is a dataset
        - Sheets are represented as fields within that dataset
        """
        for sheet_file in sheets:
            sheet_id = sheet_file.id

            if sheet_id in self.processed_sheets:
                continue

            self.processed_sheets.add(sheet_id)

            sheet_data = self._get_sheet_metadata(sheet_id)

            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                self.platform, sheet_id, self.config.platform_instance, self.config.env
            )

            sheet_path = sheet_file.path.lstrip("/")
            path_segments = [segment for segment in sheet_path.split("/") if segment]

            if self.platform not in created_containers:
                platform_container_urn = self.metadata_builder.get_container_urn(
                    self.platform
                )
                platform_container_props = ContainerPropertiesClass(
                    name=self.platform, description=f"{self.platform} Root Container"
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_props
                ).as_workunit()

                platform_container_subtype = SubTypesClass(typeNames=[SUBTYPE_PLATFORM])
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_subtype
                ).as_workunit()

                created_containers.add(self.platform)

            for _i, segment in enumerate(path_segments):
                if segment not in created_containers:
                    for mcp in self.metadata_builder.create_container_mcps([segment]):
                        yield mcp.as_workunit()

                    created_containers.add(segment)

            platform_instance_mcp = (
                self.metadata_builder.create_platform_instance_aspect(dataset_urn)
            )
            if platform_instance_mcp:
                yield platform_instance_mcp.as_workunit()

            yield from auto_workunit(
                self._create_dataset_properties_mcps(
                    sheet_file, dataset_urn, sheet_data
                )
            )

            yield from auto_workunit(
                self._create_ownership_mcps(sheet_file, dataset_urn)
            )

            schema_metadata = self._get_sheet_schema(sheet_id, sheet_data, None)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=schema_metadata
            ).as_workunit()

            tags = self._extract_tags_from_drive_metadata(sheet_file)
            if tags:
                global_tags = GlobalTagsClass(tags=tags)
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=global_tags
                ).as_workunit()

            browse_paths_mcp = self.metadata_builder.create_browse_paths_aspect(
                sheet_file, dataset_urn
            )
            yield browse_paths_mcp.as_workunit()

            container_mcp = self.metadata_builder.create_container_aspect(
                sheet_file, dataset_urn
            )
            if container_mcp:
                yield container_mcp.as_workunit()

            subtype = SubTypesClass(typeNames=[SUBTYPE_GOOGLE_SHEETS])
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=subtype
            ).as_workunit()

            status_mcp = self._create_status_aspect(dataset_urn)
            yield status_mcp.as_workunit()

            # Emit usage statistics if available
            if sheet_data.usage_stats and isinstance(
                sheet_data.usage_stats, SheetUsageStatistics
            ):
                usage_stats_data = sheet_data.usage_stats
                if (
                    usage_stats_data.view_count > 0
                    or usage_stats_data.unique_user_count > 0
                ):
                    dataset_usage_stats = DatasetUsageStatisticsClass(
                        timestampMillis=int(time.time() * 1000),
                        uniqueUserCount=usage_stats_data.unique_user_count,
                        userCounts=[]
                        if usage_stats_data.unique_user_count > 0
                        else None,
                    )
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=dataset_usage_stats
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

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        """Generate workunits for each Google Sheet."""
        sheets = self.drive_client.get_sheets_from_drive()

        # Track containers we've already created to avoid duplicates
        created_containers = set()

        for sheet_file in sheets:
            sheet_path = sheet_file.path.lstrip("/")
            path_segments = [segment for segment in sheet_path.split("/") if segment][
                :-1
            ]  # Exclude filename

            if self.platform not in created_containers:
                platform_container_urn = self.metadata_builder.get_container_urn(
                    self.platform
                )
                platform_container_props = ContainerPropertiesClass(
                    name=self.platform, description=f"{self.platform} Root Container"
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_props
                ).as_workunit()

                platform_container_subtype = SubTypesClass(typeNames=[SUBTYPE_PLATFORM])
                yield MetadataChangeProposalWrapper(
                    entityUrn=platform_container_urn, aspect=platform_container_subtype
                ).as_workunit()

                created_containers.add(self.platform)

            for _i, segment in enumerate(path_segments):
                if segment not in created_containers:
                    for mcp in self.metadata_builder.create_container_mcps([segment]):
                        yield mcp.as_workunit()
                    created_containers.add(segment)

        # Dispatch to the appropriate method based on sheets_as_datasets config
        if self.config.sheets_as_datasets:
            yield from self._generate_sheet_datasets_workunits(
                sheets, created_containers
            )
        else:
            yield from self._generate_spreadsheet_datasets_workunits(
                sheets, created_containers
            )

        yield from self._generate_sql_aggregator_workunits()

        self._save_checkpoint_state()

    def _generate_sql_aggregator_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate workunits from SQL parsing aggregators."""
        if not self._sql_aggregators:
            return

        active_aggregators = {
            platform: aggregator
            for platform, aggregator in self._sql_aggregators.items()
            if aggregator is not None
        }

        if not active_aggregators:
            return

        logger.info(
            f"Generating lineage from {len(active_aggregators)} SQL aggregator(s)"
        )
        for platform_key, aggregator in active_aggregators.items():
            logger.info(f"Generating lineage from {platform_key} SQL aggregator")
            try:
                for mcp in aggregator.gen_metadata():
                    yield mcp.as_workunit()
            except Exception as e:
                logger.warning(
                    f"Failed to generate metadata from {platform_key} SQL aggregator: {e}",
                    exc_info=True,
                )
            finally:
                try:
                    aggregator.close()
                except Exception as e:
                    logger.debug(f"Error closing {platform_key} SQL aggregator: {e}")

    def get_report(self) -> GoogleSheetsSourceReport:
        return self.report

    def _save_checkpoint_state(self) -> None:
        """Save the checkpoint state for incremental ingestion."""
        if not self.config.enable_incremental_ingestion:
            return

        self.checkpoint_state.last_run_time = datetime.utcnow().isoformat() + "Z"

        if self.config.stateful_ingestion:
            cur_checkpoint = self.get_current_checkpoint(  # type: ignore[attr-defined]
                GoogleSheetsCheckpointState.__name__
            )
            if cur_checkpoint is not None:
                cur_checkpoint.state = self.checkpoint_state
