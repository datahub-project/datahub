"""Google Drive API client for fetching and filtering sheets."""

import logging
import time
from typing import Any, Dict, List, Optional, Set

from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig
from datahub.ingestion.source.google_sheets.constants import (
    DRIVE_API_FIELDS,
    DRIVE_QUERY_SHEETS_MIMETYPE,
    DRIVE_SPACES_ALL,
    DRIVE_SPACES_DEFAULT,
    FIELD_DRIVE_ID,
    FIELD_ID,
    FIELD_MODIFIED_TIME,
    FIELD_NAME,
    FIELD_NEXT_PAGE_TOKEN,
    FIELD_PATH,
)
from datahub.ingestion.source.google_sheets.models import DriveFile, SheetPathResult
from datahub.ingestion.source.google_sheets.report import GoogleSheetsSourceReport
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


class DriveAPIClient:
    """Client for interacting with Google Drive API to fetch and filter sheets."""

    def __init__(
        self,
        drive_service: Any,
        config: GoogleSheetsSourceConfig,
        report: GoogleSheetsSourceReport,
        checkpoint_state: CheckpointStateBase,
    ):
        self.drive_service = drive_service
        self.config = config
        self.report = report
        self.checkpoint_state = checkpoint_state

        self.min_request_interval = (
            1.0 / config.requests_per_second if config.requests_per_second else 0
        )
        self.last_request_time: Optional[float] = None
        self._allowed_drive_ids: Optional[Set[str]] = None

    def get_sheets_from_drive(self) -> List[DriveFile]:
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
                        processed_sheet = self._process_sheet_from_drive(sheet_data)
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

    def _fetch_drive_sheets_page(
        self, query: str, spaces: str, page_token: Optional[str]
    ) -> Dict[str, Any]:
        """Fetch a single page of sheets from Drive API with rate limiting."""
        self._rate_limit()

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
                allowed_drives = self.get_allowed_shared_drives()
                if allowed_drives and drive_id not in allowed_drives:
                    self.report.report_sheet_dropped(
                        f"{sheet_name} (shared drive filtered)"
                    )
                    return None

        path_result = self.get_sheet_path(sheet_id)
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

    def get_allowed_shared_drives(self) -> Set[str]:
        """Get set of allowed Shared Drive IDs based on shared_drive_patterns config."""
        if self._allowed_drive_ids is not None:
            return self._allowed_drive_ids

        self._allowed_drive_ids = set()

        if not self.config.scan_shared_drives or not self.config.shared_drive_patterns:
            return self._allowed_drive_ids

        try:
            page_token = None
            while True:
                response = (
                    self.drive_service.drives()
                    .list(
                        pageSize=100,
                        pageToken=page_token,
                        fields="nextPageToken, drives(id, name)",
                    )
                    .execute()
                )

                for drive in response.get("drives", []):
                    drive_name = drive.get("name", "")
                    drive_id = drive.get("id", "")

                    if self.config.shared_drive_patterns.allowed(drive_name):
                        self._allowed_drive_ids.add(drive_id)
                        logger.info(
                            f"Including Shared Drive: {drive_name} (ID: {drive_id})"
                        )
                    else:
                        logger.debug(
                            f"Excluding Shared Drive: {drive_name} (filtered by pattern)"
                        )

                page_token = response.get(FIELD_NEXT_PAGE_TOKEN)
                if not page_token:
                    break

        except Exception as e:
            logger.warning(
                f"Error listing shared drives: {e}. All shared drives will be scanned."
            )
            self._allowed_drive_ids = set()

        return self._allowed_drive_ids

    def get_sheet_path(self, sheet_id: str) -> SheetPathResult:
        """Get the folder path for a Google Sheet and return both full path and folder path components."""
        try:
            file_metadata = (
                self.drive_service.files()
                .get(fileId=sheet_id, fields="parents,name")
                .execute()
            )

            parent_id = file_metadata.get("parents", [None])[0]
            if not parent_id:
                return SheetPathResult(
                    full_path=f"/{file_metadata.get('name', sheet_id)}",
                    folder_path_parts=[],
                )

            folder_path_parts: List[str] = []
            while parent_id:
                try:
                    folder = (
                        self.drive_service.files()
                        .get(fileId=parent_id, fields="id,name,parents")
                        .execute()
                    )

                    folder_path_parts.insert(0, folder.get("name", parent_id))
                    parent_id = folder.get("parents", [None])[0]
                except Exception as e:
                    self.report.report_warning(
                        message="Error getting folder info", exc=e
                    )
                    break

            full_path_parts = folder_path_parts + [file_metadata.get("name", sheet_id)]
            full_path = "/" + "/".join(full_path_parts)

            return SheetPathResult(
                full_path=full_path, folder_path_parts=folder_path_parts
            )
        except Exception as e:
            self.report.report_warning(
                message="Error getting sheet path", context=sheet_id, exc=e
            )
            return SheetPathResult(full_path=f"/{sheet_id}", folder_path_parts=[])

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

    def _rate_limit(self) -> None:
        """Enforce rate limiting if configured."""
        if self.min_request_interval > 0 and self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_request_interval:
                time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
