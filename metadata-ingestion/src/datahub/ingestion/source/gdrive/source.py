import logging
from typing import Dict, Iterable, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

try:
    from datahub.ingestion.source.gdrive.config import GoogleDriveConfig
except ImportError:
    # Fallback for when running validation standalone
    from config import GoogleDriveConfig

logger = logging.getLogger(__name__)


@platform_name("Google Drive")
@config_class(GoogleDriveConfig)
@support_status(SupportStatus.INCUBATING)
class GoogleDriveSource(Source):
    """
    Google Drive DataHub source connector

    This connector extracts metadata from Google Drive files and folders
    and converts them to DataHub entities.
    """

    def __init__(self, config: GoogleDriveConfig, ctx: PipelineContext):
        """Initialize the Google Drive source"""
        super().__init__(ctx)
        self.config = config
        self.service = None
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GoogleDriveSource":
        """Factory method to create GoogleDriveSource"""
        config = GoogleDriveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units from Google Drive files

        This is the main method that DataHub calls to get metadata.
        Returns an iterator of MetadataWorkUnit objects.
        """
        if not self.service:
            self.authenticate()

        try:
            # Process root folder contents
            folders_to_process = ["root"]  # Start with root folder
            if self.config.root_folder_id:
                folders_to_process = [self.config.root_folder_id]

            for folder_id in folders_to_process:
                yield from self._process_folder(folder_id, depth=0)

            # Also process files shared with the service account
            yield from self._process_shared_files()

        except Exception as e:
            logger.error(f"Error getting work units: {e}")
            self.report.report_failure("general", f"Failed to get work units: {e}")
            raise

    def authenticate(self) -> None:
        """Authenticate with Google Drive API using service account credentials"""
        try:
            if self.config.credentials_path:
                # Use service account credentials file
                credentials = Credentials.from_service_account_file(
                    self.config.credentials_path,
                    scopes=["https://www.googleapis.com/auth/drive.readonly"],
                )
                # Build the service
                self.service = build("drive", "v3", credentials=credentials)
                logger.info(
                    "Successfully authenticated with Google Drive API using service account"
                )
            else:
                raise ValueError(
                    "credentials_path must be provided (Google Drive API does not support API key authentication)"
                )

        except Exception as e:
            logger.error(f"Failed to authenticate with Google Drive: {e}")
            self.report.report_failure("authentication", f"Failed to authenticate: {e}")
            raise

    def _process_folder(
        self, folder_id: str, depth: int = 0
    ) -> Iterable[MetadataWorkUnit]:
        """Process a Google Drive folder and its contents"""
        if depth >= self.config.max_recursion_depth:
            logger.warning(
                f"Max recursion depth {self.config.max_recursion_depth} reached"
            )
            return

        try:
            # Query for files in the folder
            query_parts = [f"'{folder_id}' in parents"]

            if not self.config.include_trashed:
                query_parts.append("trashed=false")

            query = " and ".join(query_parts)

            # Get files with pagination
            page_token = None
            while True:
                results = (
                    self.service.files()
                    .list(
                        q=query,
                        pageSize=1000,  # Max page size
                        fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, owners, parents, shared, webViewLink)",
                        pageToken=page_token,
                        includeItemsFromAllDrives=self.config.include_shared_drives,
                        supportsAllDrives=self.config.include_shared_drives,
                    )
                    .execute()
                )

                files = results.get("files", [])

                for file_info in files:
                    # Process individual file
                    file_workunit = self._process_file(file_info)
                    if file_workunit:
                        yield file_workunit

                    # If it's a folder, recurse into it
                    if (
                        file_info.get("mimeType")
                        == "application/vnd.google-apps.folder"
                    ):
                        yield from self._process_folder(file_info["id"], depth + 1)

                # Check if there are more pages
                page_token = results.get("nextPageToken")
                if not page_token:
                    break

        except HttpError as e:
            logger.error(f"HTTP error processing folder {folder_id}: {e}")
            self.report.report_failure(
                "folder_processing", f"HTTP error in folder {folder_id}: {e}"
            )
            raise
        except Exception as e:
            logger.error(f"Error processing folder {folder_id}: {e}")
            self.report.report_failure(
                "folder_processing", f"Error in folder {folder_id}: {e}"
            )
            raise

    def _process_file(self, file_info: Dict) -> Optional[MetadataWorkUnit]:
        """Process an individual Google Drive file"""
        try:
            file_name = file_info.get("name", "Unknown")
            file_id = file_info.get("id")
            mime_type = file_info.get("mimeType", "unknown")

            # Process folders as container datasets
            if mime_type == "application/vnd.google-apps.folder":
                logger.debug(f"Processing folder as dataset: {file_name}")
                return self._create_folder_workunit(file_info)

            # Check file size limit
            file_size = int(file_info.get("size", 0))
            if file_size > self.config.max_file_size_mb * 1024 * 1024:
                logger.info(f"Skipping large file {file_name} ({file_size} bytes)")
                return None

            # Apply inclusion/exclusion patterns
            if self.config.include_files:
                if not self.config.include_files.allowed(file_name):
                    logger.debug(f"Skipping file {file_name} due to patterns")
                    return None

            # Create a simple metadata work unit for now
            # In a real implementation, you would create proper DataHub entities
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.ingestion.api.workunit import MetadataWorkUnit
            from datahub.metadata.schema_classes import DatasetPropertiesClass

            # Create basic file metadata
            dataset_properties = DatasetPropertiesClass(
                name=file_name,
                description=f"Google Drive file: {file_name}",
                customProperties={
                    "file_id": file_id,
                    "mime_type": mime_type,
                    "size": str(file_size),
                    "created_time": file_info.get("createdTime", ""),
                    "modified_time": file_info.get("modifiedTime", ""),
                    "web_view_link": file_info.get("webViewLink", ""),
                },
            )

            # Create work unit
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:gdrive,{file_id},PROD)",
                aspectName="datasetProperties",
                aspect=dataset_properties,
            )

            wu = MetadataWorkUnit(id=f"gdrive-{file_id}", mcp=mcp)

            logger.debug(f"Processed file: {file_name}")
            self.report.report_workunit(wu)
            return wu

        except Exception as e:
            logger.error(f"Error processing file {file_info}: {e}")
            self.report.report_failure("file_processing", f"Error processing file: {e}")
            return None

    def _create_folder_workunit(self, folder_info: Dict) -> Optional[MetadataWorkUnit]:
        """Create a work unit for a Google Drive folder as a container dataset"""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.ingestion.api.workunit import MetadataWorkUnit
            from datahub.metadata.schema_classes import DatasetPropertiesClass

            folder_name = folder_info.get("name", "Unknown Folder")
            folder_id = folder_info.get("id")

            # Create folder metadata
            dataset_properties = DatasetPropertiesClass(
                name=folder_name,
                description=f"Google Drive folder: {folder_name}",
                customProperties={
                    "platform": "Google Drive",
                    "folder_id": folder_id,
                    "mime_type": folder_info.get("mimeType", ""),
                    "created_time": folder_info.get("createdTime", ""),
                    "modified_time": folder_info.get("modifiedTime", ""),
                    "web_view_link": folder_info.get("webViewLink", ""),
                    "type": "folder",
                },
            )

            # Create work unit for folder
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:gdrive,{folder_id},PROD)",
                aspectName="datasetProperties",
                aspect=dataset_properties,
            )

            wu = MetadataWorkUnit(id=f"gdrive-folder-{folder_id}", mcp=mcp)

            logger.debug(f"Processed folder: {folder_name}")
            self.report.report_workunit(wu)
            return wu

        except Exception as e:
            logger.error(f"Error processing folder {folder_info}: {e}")
            self.report.report_failure(
                "folder_processing", f"Error processing folder: {e}"
            )
            return None

    def _process_shared_files(self) -> Iterable[MetadataWorkUnit]:
        """Process files that have been shared with the service account"""
        try:
            logger.info("Processing files shared with service account...")

            # Get files shared with this account
            results = (
                self.service.files()
                .list(
                    q="sharedWithMe=true and trashed=false",
                    pageSize=1000,
                    fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, owners, shared, webViewLink, sharedWithMeTime)",
                )
                .execute()
            )

            files = results.get("files", [])
            logger.info(f"Found {len(files)} shared files")

            for file_info in files:
                # Process shared file
                file_workunit = self._process_file(file_info)
                if file_workunit:
                    yield file_workunit

                # If it's a shared folder, process its contents too
                if file_info.get("mimeType") == "application/vnd.google-apps.folder":
                    logger.info(f"Processing shared folder: {file_info.get('name')}")
                    yield from self._process_folder(file_info["id"], depth=1)

        except Exception as e:
            logger.error(f"Error processing shared files: {e}")
            self.report.report_failure(
                "shared_files", f"Error processing shared files: {e}"
            )

    def get_report(self) -> SourceReport:
        """Return a report of the ingestion process"""
        return self.report
