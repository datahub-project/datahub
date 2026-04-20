from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.str_enum import StrEnum

IdmcObjectType = Literal["Project", "Folder", "TASKFLOW", "DTEMPLATE"]


class ExportJobState(StrEnum):
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_api_value(cls, raw: Optional[str]) -> "ExportJobState":
        if not raw:
            return cls.UNKNOWN
        try:
            return cls(raw)
        except ValueError:
            return cls.UNKNOWN


@dataclass
class IdmcObject:
    id: str
    name: str
    path: str
    object_type: str
    description: Optional[str] = None
    updated_by: Optional[str] = None
    created_by: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    @classmethod
    def from_flat(cls, data: Dict[str, Any], fallback_type: str) -> "IdmcObject":
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            path=data.get("path", ""),
            object_type=data.get("documentType") or fallback_type,
            description=data.get("description") or None,
            updated_by=data.get("lastUpdatedBy") or data.get("updatedBy") or None,
            created_by=data.get("createdBy") or None,
            create_time=data.get("createdTime") or data.get("createTime") or None,
            update_time=data.get("lastUpdatedTime") or data.get("updateTime") or None,
        )

    @classmethod
    def from_properties(cls, data: Dict[str, Any], fallback_type: str) -> "IdmcObject":
        props = {
            p.get("name", ""): p.get("value")
            for p in data.get("properties", [])
            if isinstance(p, dict)
        }
        return cls(
            id=props.get("id") or data.get("id", ""),
            name=props.get("name") or "",
            path=props.get("path") or "",
            object_type=props.get("documentType") or fallback_type,
            description=props.get("description") or None,
            updated_by=props.get("lastUpdatedBy") or None,
            created_by=props.get("createdBy") or None,
            create_time=props.get("createdTime") or None,
            update_time=props.get("lastUpdatedTime") or None,
        )


@dataclass
class IdmcMapping:
    v2_id: str
    name: str
    asset_frs_guid: str  # v3 GUID used for cross-referencing
    description: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    document_type: Optional[str] = None
    valid: bool = True
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    references: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "IdmcMapping":
        return cls(
            v2_id=data.get("id", ""),
            name=data.get("name", ""),
            asset_frs_guid=data.get("assetFrsGuid", ""),
            description=data.get("description") or None,
            created_by=data.get("createdBy") or None,
            updated_by=data.get("updatedBy") or None,
            create_time=data.get("createTime") or None,
            update_time=data.get("updateTime") or None,
            document_type=data.get("documentType") or None,
            valid=data.get("valid", True),
            parameters=data.get("parameters", []),
            references=data.get("references", []),
        )


@dataclass
class IdmcConnection:
    id: str
    name: str
    conn_type: str  # connParams["Connection Type"] — the platform signal
    base_type: str = ""
    federated_id: str = ""
    host: str = ""
    database: str = ""
    schema: str = ""

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "IdmcConnection":
        conn_params = data.get("connParams", {})
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            conn_type=conn_params.get("Connection Type", ""),
            base_type=data.get("type", ""),
            federated_id=data.get("federatedId", ""),
            host=conn_params.get("Host", data.get("host", "")),
            database=conn_params.get("Database", data.get("database", "")),
            schema=conn_params.get("Schema", data.get("schema", "")),
        )


@dataclass
class IdmcMappingTask:
    v2_id: str
    name: str
    description: Optional[str] = None
    mapping_id: str = ""
    mapping_name: str = ""
    connection_id: str = ""
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "IdmcMappingTask":
        return cls(
            v2_id=data.get("id", ""),
            name=data.get("name", ""),
            description=data.get("description") or None,
            mapping_id=data.get("mappingId", ""),
            mapping_name=data.get("mappingName", ""),
            connection_id=data.get("connectionId", ""),
            created_by=data.get("createdBy") or None,
            updated_by=data.get("updatedBy") or None,
            create_time=data.get("createTime") or None,
            update_time=data.get("updateTime") or None,
        )


@dataclass
class LineageTable:
    table_name: str
    schema_name: str = ""
    connection_federated_id: str = ""
    transformation_name: str = ""


@dataclass
class MappingLineageInfo:
    mapping_id: str
    mapping_name: str
    source_tables: List[LineageTable] = field(default_factory=list)
    target_tables: List[LineageTable] = field(default_factory=list)


@dataclass
class ExportJobStatus:
    job_id: str
    state: ExportJobState
    message: str = ""


@dataclass
class InformaticaSourceReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    folders_scanned: int = 0
    taskflows_scanned: int = 0
    mappings_scanned: int = 0
    mapping_tasks_scanned: int = 0
    lineage_edges_emitted: int = 0

    projects_filtered: int = 0
    folders_filtered: int = 0
    taskflows_filtered: int = 0
    mappings_filtered: int = 0

    connections_resolved: int = 0
    connections_unresolved: LossyList[str] = field(default_factory=LossyList)

    export_jobs_submitted: int = 0
    export_jobs_failed: LossyList[str] = field(default_factory=LossyList)

    objects_failed: LossyList[str] = field(default_factory=LossyList)

    api_call_count: int = 0

    def report_api_call(self) -> None:
        self.api_call_count += 1

    def report_object_failed(self, object_name: str, error: str) -> None:
        self.objects_failed.append(f"{object_name}: {error}")

    def report_connection_unresolved(
        self, connection_id: str, connection_name: str, reason: str
    ) -> None:
        self.connections_unresolved.append(
            f"{connection_name} ({connection_id}): {reason}"
        )

    def report_export_failed(self, job_id: str, error: str) -> None:
        self.export_jobs_failed.append(f"{job_id}: {error}")


class InformaticaLoginError(Exception):
    """Raised when authentication to IDMC fails non-transiently (bad credentials, missing session)."""


class InformaticaApiError(Exception):
    """Raised when IDMC returns a non-recoverable error response."""
