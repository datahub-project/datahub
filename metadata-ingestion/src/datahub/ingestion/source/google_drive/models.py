from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.google_drive.constants import (
    MIME_DOCUMENT,
    MIME_FOLDER,
    MIME_SHORTCUT,
)
from datahub.metadata.schema_classes import DocumentInfoClass


class DriveUser(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    email_address: Optional[str] = Field(default=None, alias="emailAddress")


class DriveFile(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str
    name: str
    mime_type: str = Field(alias="mimeType")
    drive_id: Optional[str] = Field(default=None, alias="driveId")
    # Drive returns the version as a decimal string (int64 in JSON).
    version: Optional[str] = None
    # Storage size in bytes as a decimal string. For native Docs it does not track
    # the exported text length, so it is reported but never used for filtering.
    size: Optional[str] = None
    created_time: Optional[datetime] = Field(default=None, alias="createdTime")
    modified_time: Optional[datetime] = Field(default=None, alias="modifiedTime")
    web_view_link: Optional[str] = Field(default=None, alias="webViewLink")
    owners: List[DriveUser] = Field(default_factory=list)
    last_modifying_user: Optional[DriveUser] = Field(
        default=None, alias="lastModifyingUser"
    )

    @property
    def is_folder(self) -> bool:
        return self.mime_type == MIME_FOLDER

    @property
    def is_document(self) -> bool:
        return self.mime_type == MIME_DOCUMENT

    @property
    def is_shortcut(self) -> bool:
        return self.mime_type == MIME_SHORTCUT


class SharedDrive(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str
    name: Optional[str] = None
    created_time: Optional[datetime] = Field(default=None, alias="createdTime")


@dataclass
class FolderNode:
    id: str
    title: str
    # None for configured roots.
    parent_id: Optional[str]
    path: str
    drive_id: Optional[str]
    web_view_link: Optional[str]
    # Taken from Drive so folder documents keep stable timestamps across runs.
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    # Unset for shared drives resolved through drives.get, which have no version.
    version: Optional[str] = None

    @property
    def is_root(self) -> bool:
        return self.parent_id is None


@dataclass
class DocumentRef:
    file: DriveFile
    folder_id: str


@dataclass
class DriveTree:
    # Insertion order is BFS order, so parents always precede their children.
    folders: Dict[str, FolderNode] = field(default_factory=dict)
    documents: List[DocumentRef] = field(default_factory=list)
    unparseable_document_ids: List[str] = field(default_factory=list)


@dataclass
class ExportResult:
    text: Optional[str]
    used_html_fallback: bool
    failure_reason: Optional[str]
    # Short description of the last API error, surfaced in the run report.
    error: Optional[str] = None


@dataclass
class StoredDocument:
    """What DataHub holds for a document from an earlier run."""

    info: Optional[DocumentInfoClass]
    removed: bool
    # Owners this connector added earlier, identified by their attribution source.
    connector_owners: List[str] = field(default_factory=list)


@dataclass
class UnchangedCheck:
    unchanged: bool
    soft_deleted: bool


@dataclass
class EmbedResult:
    workunits: List[MetadataWorkUnit]
    succeeded: bool
    # The max_documents error, re-raised after the document itself is emitted.
    limit_error: Optional[RuntimeError] = None
