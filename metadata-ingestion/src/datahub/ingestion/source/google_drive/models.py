from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.google_drive.constants import (
    MIME_DOCUMENT,
    MIME_FOLDER,
    MIME_SHORTCUT,
)


class DriveUser(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    display_name: Optional[str] = Field(default=None, alias="displayName")
    email_address: Optional[str] = Field(default=None, alias="emailAddress")


class DriveFile(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str
    name: str
    mime_type: str = Field(alias="mimeType")
    parents: List[str] = Field(default_factory=list)
    drive_id: Optional[str] = Field(default=None, alias="driveId")
    # Drive returns the version as a decimal string (int64 in JSON).
    version: Optional[str] = None
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


@dataclass
class FolderNode:
    id: str
    title: str
    # None for configured roots.
    parent_id: Optional[str]
    root_id: str
    path: str
    depth: int
    drive_id: Optional[str]
    web_view_link: Optional[str]

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


@dataclass
class ExportResult:
    text: Optional[str]
    used_html_fallback: bool
    failure_reason: Optional[str]


@dataclass
class UnchangedCheck:
    unchanged: bool
    soft_deleted: bool
