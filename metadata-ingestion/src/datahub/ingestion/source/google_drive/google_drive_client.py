import json
import logging
from collections import deque
from typing import Deque, Iterator, List, Optional, Set, Union

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from markdownify import markdownify
from pydantic import ValidationError

from datahub.ingestion.source.google_drive.constants import (
    ABOUT_FIELDS,
    API_ABOUT_GET,
    API_DRIVES_GET,
    API_DRIVES_LIST,
    API_FILES_EXPORT,
    API_FILES_GET,
    API_FILES_LIST,
    API_MAX_RETRIES,
    CHILDREN_QUERY_TEMPLATE,
    DRIVE_API_NAME,
    DRIVE_API_VERSION,
    DRIVE_FIELDS,
    DRIVE_ID_RE,
    DRIVE_SCOPES,
    DRIVES_LIST_FIELDS,
    DRIVES_PAGE_SIZE,
    EXPORT_HTML,
    EXPORT_MARKDOWN,
    EXPORT_TOO_LARGE_REASON,
    FAILURE_EXPORT_FAILED,
    FAILURE_NOT_FOUND,
    FAILURE_TOO_LARGE,
    FILE_FIELDS,
    FILE_WITH_PARENTS_FIELDS,
    FOLDER_PATH_SEPARATOR,
    HTTP_NOT_FOUND,
    LIST_FIELDS,
    LIST_PAGE_SIZE,
    MARKDOWNIFY_HEADING_STYLE,
    MIME_DOCUMENT,
    MIME_FOLDER,
    ORDER_BY,
    SHARED_DRIVE_URL_TEMPLATE,
)
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_report import (
    GoogleDriveSourceReport,
)
from datahub.ingestion.source.google_drive.google_drive_selection import (
    FOLDER_PATTERN,
    document_title_verdict,
    subfolder_verdict,
)
from datahub.ingestion.source.google_drive.models import (
    DocumentRef,
    DriveFile,
    DriveTree,
    ExportResult,
    FolderNode,
    SharedDrive,
)

logger = logging.getLogger(__name__)


class NotAFolderError(ValueError):
    """A configured root resolved to a file rather than a folder or shared drive."""


def http_error_reason(error: HttpError) -> str:
    """Return the first `errors[].reason` from a Drive API error body, or ''."""
    try:
        content = error.content
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        body = json.loads(content)
        return str(body["error"]["errors"][0]["reason"])
    except (ValueError, KeyError, IndexError, TypeError):
        return ""


def http_error_status(error: HttpError) -> int:
    return int(getattr(error.resp, "status", 0) or 0)


def describe_http_error(error: HttpError) -> str:
    reason = http_error_reason(error)
    status = f"HTTP {http_error_status(error)}"
    return f"{status} {reason}" if reason else status


def build_drive_service(config: GoogleDriveSourceConfig) -> Resource:
    # Credentials are injected programmatically; never via os.environ or a key file
    # on disk, so the key is not exposed to other code in the process.
    if config.credential is not None:
        credentials = service_account.Credentials.from_service_account_info(
            config.credential.to_dict(), scopes=DRIVE_SCOPES
        )
        if config.delegated_user:
            credentials = credentials.with_subject(config.delegated_user)
    else:
        credentials, _ = google.auth.default(scopes=DRIVE_SCOPES)
    return build(
        DRIVE_API_NAME,
        DRIVE_API_VERSION,
        credentials=credentials,
        cache_discovery=False,
    )


class GoogleDriveClient:
    def __init__(
        self,
        config: GoogleDriveSourceConfig,
        report: GoogleDriveSourceReport,
        service: Optional[Resource] = None,
    ) -> None:
        self.config = config
        self.report = report
        self._service = service if service is not None else build_drive_service(config)

    def get_authenticated_email(self) -> Optional[str]:
        self.report.report_api_call(API_ABOUT_GET)
        about = (
            self._service.about()
            .get(fields=ABOUT_FIELDS)
            .execute(num_retries=API_MAX_RETRIES)
        )
        user = about.get("user") or {}
        return user.get("emailAddress")

    def get_root(self, root_id: str) -> FolderNode:
        """Resolve a configured root (folder or shared drive) into a FolderNode.

        Raises HttpError when the root is not accessible and NotAFolderError when the ID
        refers to something other than a folder.
        """
        try:
            self.report.report_api_call(API_FILES_GET)
            raw = (
                self._service.files()
                .get(fileId=root_id, fields=FILE_FIELDS, supportsAllDrives=True)
                .execute(num_retries=API_MAX_RETRIES)
            )
        except HttpError as e:
            if http_error_status(e) != HTTP_NOT_FOUND:
                raise
            # Some shared drive IDs are not addressable through files.get.
            return self._get_shared_drive_root(root_id)

        root = DriveFile.model_validate(raw)
        if not root.is_folder:
            raise NotAFolderError(
                f"Root {root_id} is a '{root.mime_type}', not a folder or shared drive"
            )
        title = root.name
        if root.drive_id == root.id:
            # files.get on a shared drive's root reports a generic name; the drive
            # name lives on the drives resource.
            title = self._get_shared_drive_name(root_id) or title
        return FolderNode(
            id=root.id,
            title=title,
            parent_id=None,
            path=title,
            drive_id=root.drive_id,
            web_view_link=root.web_view_link,
            created_time=root.created_time,
            modified_time=root.modified_time,
            version=root.version,
        )

    def _get_shared_drive(self, drive_id: str) -> SharedDrive:
        self.report.report_api_call(API_DRIVES_GET)
        raw = (
            self._service.drives()
            .get(driveId=drive_id, fields=DRIVE_FIELDS)
            .execute(num_retries=API_MAX_RETRIES)
        )
        return SharedDrive.model_validate(raw)

    def get_file(self, file_id: str) -> DriveFile:
        """One file with its parents and trash state. Raises HttpError when the
        account cannot see it."""
        if not DRIVE_ID_RE.match(file_id):
            raise ValueError(f"Refusing to fetch unexpected file id '{file_id}'")
        self.report.report_api_call(API_FILES_GET)
        raw = (
            self._service.files()
            .get(
                fileId=file_id, fields=FILE_WITH_PARENTS_FIELDS, supportsAllDrives=True
            )
            .execute(num_retries=API_MAX_RETRIES)
        )
        return DriveFile.model_validate(raw)

    def iter_shared_drives(self) -> Iterator[SharedDrive]:
        """Shared drives the authenticated account is a member of."""
        page_token: Optional[str] = None
        while True:
            self.report.report_api_call(API_DRIVES_LIST)
            response = (
                self._service.drives()
                .list(
                    fields=DRIVES_LIST_FIELDS,
                    pageSize=DRIVES_PAGE_SIZE,
                    pageToken=page_token,
                )
                .execute(num_retries=API_MAX_RETRIES)
            )
            for raw in response.get("drives", []):
                yield SharedDrive.model_validate(raw)
            page_token = response.get("nextPageToken")
            if not page_token:
                return

    def close(self) -> None:
        self._service.close()

    def _get_shared_drive_name(self, drive_id: str) -> Optional[str]:
        return self._get_shared_drive(drive_id).name

    def _get_shared_drive_root(self, drive_id: str) -> FolderNode:
        drive = self._get_shared_drive(drive_id)
        title = drive.name or drive_id
        return FolderNode(
            id=drive_id,
            title=title,
            parent_id=None,
            path=title,
            drive_id=drive_id,
            web_view_link=SHARED_DRIVE_URL_TEMPLATE.format(drive_id=drive_id),
            created_time=drive.created_time,
            # The drives resource has no modified time; reuse the creation time so
            # the folder document's timestamp stays stable across runs.
            modified_time=drive.created_time,
        )

    def list_children(
        self, folder_id: str, unparseable_document_ids: List[str]
    ) -> Iterator[DriveFile]:
        # IDs are interpolated into the Drive query language, so refuse anything that
        # is not a plain Drive ID rather than risk altering the query.
        if not DRIVE_ID_RE.match(folder_id):
            raise ValueError(f"Refusing to query unexpected folder id '{folder_id}'")

        query = CHILDREN_QUERY_TEMPLATE.format(folder_id=folder_id)
        page_token: Optional[str] = None
        while True:
            self.report.report_api_call(API_FILES_LIST)
            response = (
                self._service.files()
                .list(
                    q=query,
                    fields=LIST_FIELDS,
                    pageSize=LIST_PAGE_SIZE,
                    pageToken=page_token,
                    orderBy=ORDER_BY,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute(num_retries=API_MAX_RETRIES)
            )
            for raw in response.get("files", []):
                try:
                    yield DriveFile.model_validate(raw)
                except ValidationError as e:
                    context = f"folder_id={folder_id}, file_id={raw.get('id')}"
                    if raw.get("mimeType") in (MIME_FOLDER, None):
                        # Skipping a folder skips its whole subtree, which stale
                        # removal would then soft-delete; failing suppresses that.
                        self.report.failure(
                            title="Unparseable Drive folder",
                            message="A Drive folder could not be parsed; its contents "
                            "were not ingested and stale-entity removal is skipped.",
                            context=context,
                            exc=e,
                        )
                    elif raw.get("mimeType") == MIME_DOCUMENT and raw.get("id"):
                        self.report.documents_unparseable += 1
                        unparseable_document_ids.append(raw["id"])
                        self.report.warning(
                            title="Unparseable Google Doc",
                            message="A Google Doc could not be parsed and was skipped; "
                            "the copy already in DataHub, if any, was kept.",
                            context=context,
                            exc=e,
                        )
                    else:
                        self.report.warning(
                            title="Unparseable Drive file",
                            message="A Drive item could not be parsed and was skipped.",
                            context=context,
                            exc=e,
                        )
            page_token = response.get("nextPageToken")
            if not page_token:
                return

    def walk(self, roots: List[FolderNode]) -> DriveTree:
        tree = DriveTree()
        queue: Deque[FolderNode] = deque()
        for root in roots:
            if root.id not in tree.folders:
                tree.folders[root.id] = root
                queue.append(root)
        visited: Set[str] = set(tree.folders)
        seen_documents: Set[str] = set()

        while queue:
            folder = queue.popleft()
            try:
                children = list(
                    self.list_children(folder.id, tree.unparseable_document_ids)
                )
            except Exception as e:
                # A partial listing would make stale-entity removal soft-delete every
                # document under this folder, so this is a failure (which suppresses
                # stale removal for the run) rather than a warning.
                self.report.failure(
                    title="Failed to list Drive folder",
                    message="A folder could not be listed. Documents under it were not "
                    "ingested and stale-entity removal is skipped for this run.",
                    context=f"folder_id={folder.id}, path={folder.path}",
                    exc=e,
                )
                continue

            for child in children:
                if child.is_shortcut:
                    self.report.shortcuts_skipped += 1
                elif child.is_folder:
                    self._visit_folder(child, folder, tree, queue, visited)
                elif child.is_document:
                    # A document listed under several folders is counted once.
                    if child.id in seen_documents:
                        continue
                    self.report.documents_discovered += 1
                    if not document_title_verdict(self.config, child.name).included:
                        self.report.documents_filtered += 1
                        continue
                    seen_documents.add(child.id)
                    tree.documents.append(DocumentRef(file=child, folder_id=folder.id))
        return tree

    def _visit_folder(
        self,
        child: DriveFile,
        parent: FolderNode,
        tree: DriveTree,
        queue: Deque[FolderNode],
        visited: Set[str],
    ) -> None:
        self.report.folders_discovered += 1
        if child.id in visited:
            return
        path = parent.path + FOLDER_PATH_SEPARATOR + child.name
        verdict = subfolder_verdict(self.config, path)
        if not verdict.included:
            if verdict.excluded_by == FOLDER_PATTERN:
                self.report.folders_filtered += 1
            return
        visited.add(child.id)
        node = FolderNode(
            id=child.id,
            title=child.name,
            parent_id=parent.id,
            path=path,
            drive_id=child.drive_id,
            web_view_link=child.web_view_link,
            created_time=child.created_time,
            modified_time=child.modified_time,
            version=child.version,
        )
        tree.folders[child.id] = node
        queue.append(node)

    def export_markdown(self, file_id: str) -> ExportResult:
        try:
            markdown = self._export(file_id, EXPORT_MARKDOWN)
            if markdown.strip():
                return ExportResult(
                    text=markdown, used_html_fallback=False, failure_reason=None
                )
        except HttpError as e:
            if http_error_status(e) == HTTP_NOT_FOUND:
                return ExportResult(
                    text=None,
                    used_html_fallback=False,
                    failure_reason=FAILURE_NOT_FOUND,
                    error=describe_http_error(e),
                )
            if http_error_reason(e) == EXPORT_TOO_LARGE_REASON:
                # HTML is larger than markdown, so falling back cannot succeed.
                return ExportResult(
                    text=None,
                    used_html_fallback=False,
                    failure_reason=FAILURE_TOO_LARGE,
                    error=describe_http_error(e),
                )
            logger.debug("Markdown export failed for %s: %s", file_id, e)

        try:
            html = self._export(file_id, EXPORT_HTML)
        except HttpError as e:
            return ExportResult(
                text=None,
                used_html_fallback=True,
                failure_reason=FAILURE_EXPORT_FAILED,
                error=describe_http_error(e),
            )

        return ExportResult(
            text=markdownify(html, heading_style=MARKDOWNIFY_HEADING_STYLE),
            used_html_fallback=True,
            failure_reason=None,
        )

    def _export(self, file_id: str, mime_type: str) -> str:
        self.report.report_api_call(API_FILES_EXPORT)
        content: Union[bytes, str] = (
            self._service.files()
            .export(fileId=file_id, mimeType=mime_type)
            .execute(num_retries=API_MAX_RETRIES)
        )
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="replace")
        return content
