"""In-memory stand-in for the googleapiclient Drive v3 resource, plus a synthetic
fixture tree shared by the unit and golden tests.

All IDs, names and emails are synthetic.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import httplib2
from googleapiclient.errors import HttpError

MIME_FOLDER = "application/vnd.google-apps.folder"
MIME_DOC = "application/vnd.google-apps.document"
MIME_SHORTCUT = "application/vnd.google-apps.shortcut"
MIME_SHEET = "application/vnd.google-apps.spreadsheet"
EXPORT_MD = "text/markdown"
EXPORT_HTML = "text/html"

_PARENT_RE = re.compile(r"'([A-Za-z0-9_-]+)' in parents")
_FILES_MASK_RE = re.compile(r"files\((.*)\)")
_MIME_CLAUSE_RE = re.compile(r"mimeType='([^']+)'")
_NOT_TRASHED = "trashed=false"


def mask_fields(raw: Dict[str, Any], fields: str) -> Dict[str, Any]:
    """Keep only the top-level keys a Drive `fields` mask asks for, as the real API
    does, so a field dropped from the connector's mask is caught by tests."""
    depth = 0
    names: List[str] = []
    current = ""
    for char in fields:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            names.append(current)
            current = ""
            continue
        if depth == 0 and char != ")":
            current += char
    names.append(current)
    wanted = {name.strip() for name in names}
    return {key: value for key, value in raw.items() if key in wanted}


CREDENTIAL = {
    "project_id": "test-project",
    "private_key_id": "key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
    "client_email": "ingest@test-project.iam.gserviceaccount.com",
    "client_id": "123",
}

ExportValue = Union[bytes, Exception]


def http_error(status: int, reason: str = "") -> HttpError:
    body = {"error": {"code": status, "errors": [{"reason": reason}]}}
    return HttpError(
        httplib2.Response({"status": status}), json.dumps(body).encode("utf-8")
    )


def drive_file(
    file_id: str,
    name: str,
    mime_type: str,
    parent: Optional[str],
    version: str = "1",
    owners: Optional[List[str]] = None,
    drive_id: Optional[str] = None,
    size: Optional[str] = None,
) -> Dict[str, Any]:
    raw: Dict[str, Any] = {
        "id": file_id,
        "name": name,
        "mimeType": mime_type,
        "parents": [parent] if parent else [],
        "version": version,
        "createdTime": "2026-01-02T03:04:05.000Z",
        "modifiedTime": "2026-02-03T04:05:06.000Z",
        "webViewLink": f"https://docs.example.com/{file_id}",
        "owners": [{"displayName": o, "emailAddress": o} for o in (owners or [])],
        "lastModifyingUser": {"emailAddress": "editor@example.com"},
    }
    if drive_id:
        raw["driveId"] = drive_id
    if size:
        raw["size"] = size
    return raw


class _Request:
    def __init__(self, fn: Callable[[], Any]) -> None:
        self._fn = fn

    def execute(self, num_retries: int = 0) -> Any:
        return self._fn()


@dataclass
class FakeDriveService:
    """Mimics the subset of the Drive v3 resource used by GoogleDriveClient.

    `files_by_id` holds every item, `children` maps a folder ID to its child IDs,
    and `exports` maps (file_id, mime_type) to bytes or an HttpError. Listings are
    paginated with `page_size` so pagination is always exercised.
    """

    files_by_id: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    children: Dict[str, List[str]] = field(default_factory=dict)
    exports: Dict[Tuple[str, str], ExportValue] = field(default_factory=dict)
    shared_drives: Dict[str, str] = field(default_factory=dict)
    list_errors: Dict[str, HttpError] = field(default_factory=dict)
    page_size: int = 2
    calls: List[Tuple[str, str]] = field(default_factory=list)

    def add(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        self.files_by_id[raw["id"]] = raw
        for parent in raw.get("parents", []):
            self.children.setdefault(parent, []).append(raw["id"])
        return raw

    def files(self) -> "_FakeFiles":
        return _FakeFiles(self)

    def drives(self) -> "_FakeDrives":
        return _FakeDrives(self)

    def about(self) -> "_FakeAbout":
        return _FakeAbout(self)

    def calls_for(self, method: str) -> List[str]:
        return [target for name, target in self.calls if name == method]


class _FakeFiles:
    def __init__(self, svc: FakeDriveService) -> None:
        self._svc = svc

    def list(
        self, q: str, fields: str, pageToken: Optional[str] = None, **_: Any
    ) -> _Request:
        match = _PARENT_RE.search(q)
        assert match, q
        folder_id = match.group(1)
        files_mask = _FILES_MASK_RE.search(fields)
        assert files_mask, fields
        # Apply the query's filters as Drive does, so a filter dropped from the
        # connector's query lets the fixture's trashed and non-Doc items through.
        mime_types = set(_MIME_CLAUSE_RE.findall(q))
        hide_trashed = _NOT_TRASHED in q

        def run() -> Dict[str, Any]:
            self._svc.calls.append(("files.list", folder_id))
            if folder_id in self._svc.list_errors:
                raise self._svc.list_errors[folder_id]
            child_ids = [
                c
                for c in self._svc.children.get(folder_id, [])
                if not (hide_trashed and self._svc.files_by_id[c].get("trashed"))
                and (
                    not mime_types or self._svc.files_by_id[c]["mimeType"] in mime_types
                )
            ]
            start = int(pageToken or 0)
            end = start + self._svc.page_size
            page = [
                mask_fields(self._svc.files_by_id[c], files_mask.group(1))
                for c in child_ids[start:end]
            ]
            response: Dict[str, Any] = {"files": page}
            if end < len(child_ids):
                response["nextPageToken"] = str(end)
            return response

        return _Request(run)

    def get(self, fileId: str, fields: str, **_: Any) -> _Request:
        def run() -> Dict[str, Any]:
            self._svc.calls.append(("files.get", fileId))
            if fileId not in self._svc.files_by_id:
                raise http_error(404, "notFound")
            return mask_fields(self._svc.files_by_id[fileId], fields)

        return _Request(run)

    def export(self, fileId: str, mimeType: str) -> _Request:
        def run() -> bytes:
            self._svc.calls.append(("files.export", f"{fileId}:{mimeType}"))
            value = self._svc.exports.get((fileId, mimeType))
            if value is None:
                raise http_error(404, "notFound")
            if isinstance(value, Exception):
                raise value
            return value

        return _Request(run)


class _FakeDrives:
    def __init__(self, svc: FakeDriveService) -> None:
        self._svc = svc

    def get(self, driveId: str, **_: Any) -> _Request:
        def run() -> Dict[str, Any]:
            self._svc.calls.append(("drives.get", driveId))
            if driveId not in self._svc.shared_drives:
                raise http_error(404, "notFound")
            return {
                "id": driveId,
                "name": self._svc.shared_drives[driveId],
                "createdTime": "2025-12-01T00:00:00.000Z",
            }

        return _Request(run)


class _FakeAbout:
    def __init__(self, svc: FakeDriveService) -> None:
        self._svc = svc

    def get(self, **_: Any) -> _Request:
        return _Request(lambda: {"user": {"emailAddress": "ingest@example.com"}})


# Synthetic fixture tree (mirrors the live test folder's structure):
#
# Root A (rootAAAAAAAA)
# ├── Doc A1                         owners: alice
# ├── Copy of Doc A1                 (document_pattern deny)
# ├── Shortcut to A1                 (shortcut)
# ├── Budget                         (sheet; filtered out by the query)
# ├── Doc Trashed                    (in the trash; filtered out by the query)
# ├── Doc Empty                      (empty markdown export)
# ├── Eng/
# │   ├── Doc Runbook                owners: alice, bob, alice (dedup)
# │   ├── Archive/Doc Old            (folder_pattern deny)
# │   └── Specs/
# │       ├── Doc Spec Images        (data-URI images)
# │       └── Doc Spec Html          (markdown 403 -> HTML fallback)
# Shared Drive B (sharedBBBBBBB)
# ├── Doc B1                         (no owners)
# ├── Doc Huge                       (exportSizeLimitExceeded)
# └── Team/Doc B2
ROOT_A = "rootAAAAAAAA"
ENG = "folderEngEng01"
ARCHIVE = "folderArchive01"
SPECS = "folderSpecs001"
SHARED_B = "sharedBBBBBBB"
TEAM = "folderTeam0001"

DOC_A1 = "docA1xxxxxxxx"
DOC_COPY = "docCopyxxxxxx"
SHORTCUT = "shortcutxxxxx"
SHEET = "sheetxxxxxxxx"
DOC_EMPTY = "docEmptyxxxxx"
DOC_RUNBOOK = "docRunbookxxx"
DOC_OLD = "docOldxxxxxxx"
DOC_IMAGES = "docImagesxxxx"
DOC_HTML = "docHtmlxxxxxx"
DOC_B1 = "docB1xxxxxxxx"
DOC_HUGE = "docHugexxxxxx"
DOC_B2 = "docB2xxxxxxxx"
DOC_TRASHED = "docTrashedxxx"

IMAGES_MARKDOWN = (
    "# Spec with images\n\nIntro.\n\n![Blue diagram](data:image/png;base64,AAAA)\n\n"
    "Between.\n\n![][image1]\n\n\n\n\nEnd.\n\n"
    "[image1]: <data:image/png;base64,BBBB>\n"
)


def build_fixture_service() -> FakeDriveService:
    svc = FakeDriveService()
    svc.add(drive_file(ROOT_A, "Root A", MIME_FOLDER, None))
    svc.add(
        drive_file(DOC_A1, "Doc A1", MIME_DOC, ROOT_A, owners=["alice@example.com"])
    )
    svc.add(drive_file(DOC_COPY, "Copy of Doc A1", MIME_DOC, ROOT_A))
    svc.add(drive_file(SHORTCUT, "Shortcut to A1", MIME_SHORTCUT, ROOT_A))
    svc.add(drive_file(SHEET, "Budget", MIME_SHEET, ROOT_A))
    svc.add(drive_file(DOC_EMPTY, "Doc Empty", MIME_DOC, ROOT_A))
    trashed = drive_file(DOC_TRASHED, "Doc Trashed", MIME_DOC, ROOT_A)
    trashed["trashed"] = True
    svc.add(trashed)
    svc.add(drive_file(ENG, "Eng", MIME_FOLDER, ROOT_A))
    svc.add(
        drive_file(
            DOC_RUNBOOK,
            "Doc Runbook",
            MIME_DOC,
            ENG,
            version="7",
            size="8290",
            owners=["alice@example.com", "bob@example.com", "alice@example.com"],
        )
    )
    svc.add(drive_file(ARCHIVE, "Archive", MIME_FOLDER, ENG))
    svc.add(drive_file(DOC_OLD, "Doc Old", MIME_DOC, ARCHIVE))
    svc.add(drive_file(SPECS, "Specs", MIME_FOLDER, ENG))
    svc.add(drive_file(DOC_IMAGES, "Doc Spec Images", MIME_DOC, SPECS))
    svc.add(drive_file(DOC_HTML, "Doc Spec Html", MIME_DOC, SPECS))

    svc.add(drive_file(SHARED_B, "Drive", MIME_FOLDER, None, drive_id=SHARED_B))
    svc.shared_drives[SHARED_B] = "Shared Drive B"
    svc.add(drive_file(DOC_B1, "Doc B1", MIME_DOC, SHARED_B, drive_id=SHARED_B))
    svc.add(drive_file(DOC_HUGE, "Doc Huge", MIME_DOC, SHARED_B, drive_id=SHARED_B))
    svc.add(drive_file(TEAM, "Team", MIME_FOLDER, SHARED_B, drive_id=SHARED_B))
    svc.add(drive_file(DOC_B2, "Doc B2", MIME_DOC, TEAM, drive_id=SHARED_B))

    md = {
        DOC_A1: "# Doc A1\n\nHello from **A1**.\n",
        DOC_COPY: "# Copy\n",
        DOC_EMPTY: "\n",
        DOC_RUNBOOK: "# Runbook\n\n1. Check the dashboard.\n2. Page the secondary.\n",
        DOC_OLD: "# Old\n",
        DOC_TRASHED: "# Trashed\n",
        DOC_IMAGES: IMAGES_MARKDOWN,
        DOC_B1: "# B1\n\nShared drive document.\n",
        DOC_B2: "# B2\n\nNested shared drive document.\n",
    }
    for file_id, text in md.items():
        svc.exports[(file_id, EXPORT_MD)] = text.encode("utf-8")
    svc.exports[(DOC_EMPTY, EXPORT_HTML)] = b"<html><body></body></html>"
    svc.exports[(DOC_HTML, EXPORT_MD)] = http_error(403, "cannotExportFile")
    svc.exports[(DOC_HTML, EXPORT_HTML)] = (
        b"<html><body><h1>Html Spec</h1><p>Converted via <b>fallback</b>.</p>"
        b"</body></html>"
    )
    svc.exports[(DOC_HUGE, EXPORT_MD)] = http_error(403, "exportSizeLimitExceeded")
    return svc
