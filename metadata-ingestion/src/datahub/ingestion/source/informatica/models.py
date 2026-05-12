import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, NewType, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

# IDMC keys mappings by two different ids: ``mapping.id`` from the v2 API
# and ``assetFrsGuid`` from v3. They look identical (both str) but mixing
# them up silently drops lineage. NewType catches it at type-check time.
V2Id = NewType("V2Id", str)
V3Guid = NewType("V3Guid", str)

IdmcObjectType = Literal[
    "Project",
    "Folder",
    "TASKFLOW",
    "DTEMPLATE",
    "MAPPING",
    "MAPPLET",
    "DMAPPLET",
    "MTT",
]


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


# ``extra="allow"`` tolerates new IDMC fields across API upgrades without failing ingestion.
_IDMC_MODEL_CONFIG = ConfigDict(extra="allow", arbitrary_types_allowed=True)


class IdmcObject(BaseModel):
    """Parsed representation of one row from ``/public/core/v3/objects``."""

    model_config = _IDMC_MODEL_CONFIG

    id: str
    name: str
    path: str
    object_type: str
    description: Optional[str] = None
    updated_by: Optional[str] = None
    created_by: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    tags: List[str] = Field(default_factory=list)

    @field_validator("create_time", "update_time", mode="before")
    @classmethod
    def _parse_times(cls, v: Union[str, datetime, None]) -> Optional[datetime]:
        return _parse_optional_datetime(v)

    @field_validator("id")
    @classmethod
    def _warn_on_empty_id(cls, v: str) -> str:
        # Warn, don't raise: IDMC occasionally returns empty-id rows for
        # malformed entries. Raising would abort parsing the whole page;
        # downstream filters skip empty-id objects from emission.
        if not v:
            logger.warning(
                "IdmcObject constructed with an empty id; "
                "it will be filtered out of emission."
            )
        return v

    def is_bundle(self) -> bool:
        # Bundles are marketplace entries we never want to ingest. IDMC
        # returns their path in a few different shapes (with/without the
        # ``/Explore`` prefix), so match any path segment.
        if self.updated_by == "bundle-license-notifier":
            return True
        if self.name == "Add-On Bundles":
            return True
        return any(seg == "Add-On Bundles" for seg in self.path.split("/") if seg)

    @classmethod
    def from_flat(cls, data: Dict[str, Any], fallback_type: str) -> "IdmcObject":
        path_val = _extract_path(data)
        return cls(
            id=data.get("id", ""),
            name=data.get("name", "") or _name_from_path(path_val),
            path=path_val,
            object_type=data.get("documentType") or fallback_type,
            description=data.get("description") or None,
            updated_by=data.get("lastUpdatedBy") or data.get("updatedBy") or None,
            created_by=data.get("createdBy") or None,
            create_time=data.get("createdTime") or data.get("createTime") or None,
            update_time=data.get("lastUpdatedTime") or data.get("updateTime") or None,
            tags=_parse_tags(data.get("tags")),
        )

    @classmethod
    def from_properties(cls, data: Dict[str, Any], fallback_type: str) -> "IdmcObject":
        props = {
            p.get("name", ""): p.get("value")
            for p in data.get("properties", [])
            if isinstance(p, dict)
        }
        path_val = _extract_path(props) or _extract_path(data)
        name_val = (
            props.get("name") or data.get("name", "") or _name_from_path(path_val)
        )
        return cls(
            id=props.get("id") or data.get("id", ""),
            name=name_val,
            path=path_val,
            object_type=props.get("documentType") or fallback_type,
            description=props.get("description") or None,
            updated_by=props.get("lastUpdatedBy") or None,
            created_by=props.get("createdBy") or None,
            create_time=props.get("createdTime") or None,
            update_time=props.get("lastUpdatedTime") or None,
            tags=_parse_tags(props.get("tags") or data.get("tags")),
        )


def _name_from_path(path: str) -> str:
    parts = [p for p in path.strip("/").split("/") if p]
    return parts[-1] if parts else ""


def _extract_path(data: Dict[str, Any]) -> str:
    # TASKFLOW and a few other types put the folder under ``location`` /
    # ``folderPath`` / ``parentPath`` instead of ``path``. Combine with
    # ``name`` to reconstruct when the canonical field is missing.
    direct = data.get("path")
    if direct:
        return str(direct)
    for alt in ("fullPath", "objectPath"):
        alt_val = data.get(alt)
        if alt_val:
            return str(alt_val)
    parent = (
        data.get("location") or data.get("folderPath") or data.get("parentPath") or ""
    )
    name = data.get("name", "")
    if parent and name:
        parent_str = str(parent).rstrip("/")
        return f"{parent_str}/{name}"
    return str(parent) if parent else ""


def _parse_tags(raw: Optional[List[Union[str, Dict[str, str]]]]) -> List[str]:
    # IDMC returns tags either as strings or dicts with a "name" key.
    if not raw:
        return []
    out: List[str] = []
    for item in raw:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            name = item.get("name") or item.get("tagName")
            if name:
                out.append(name)
    return out


def _parse_optional_datetime(v: Union[str, datetime, None]) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except (ValueError, TypeError):
        logger.debug("Could not parse datetime value: %r", v)
        return None


class IdmcMapping(BaseModel):
    """v2 mapping metadata (``/api/v2/mapping``).

    Canonical cross-reference key between v2 and v3 APIs is
    :attr:`asset_frs_guid` (the v3 GUID); :attr:`v2_id` is only meaningful
    against the v2 REST API and should not be used as a lookup key into any
    v3-sourced structure. Transposed lookups silently return ``None``.
    """

    model_config = _IDMC_MODEL_CONFIG

    v2_id: V2Id
    name: str
    asset_frs_guid: V3Guid
    description: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    document_type: Optional[str] = None
    valid: bool = True
    parameters: List[Dict[str, object]] = Field(default_factory=list)
    references: List[Dict[str, object]] = Field(default_factory=list)

    @field_validator("create_time", "update_time", mode="before")
    @classmethod
    def _parse_times(cls, v: Union[str, datetime, None]) -> Optional[datetime]:
        return _parse_optional_datetime(v)

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "IdmcMapping":
        return cls(
            v2_id=V2Id(data.get("id", "")),
            name=data.get("name", ""),
            asset_frs_guid=V3Guid(data.get("assetFrsGuid", "")),
            description=data.get("description") or None,
            updated_by=data.get("updatedBy") or None,
            created_by=data.get("createdBy") or None,
            create_time=data.get("createTime") or None,
            update_time=data.get("updateTime") or None,
            document_type=data.get("documentType") or None,
            valid=data.get("valid", True),
            parameters=data.get("parameters", []),
            references=data.get("references", []),
        )


class IdmcConnection(BaseModel):
    """v2 connection metadata (``/api/v2/connection``).

    Two lookup keys exist: :attr:`id` (the v2 REST id, used by config
    overrides) and :attr:`federated_id` (the cross-org federated id, used by
    lineage). They must not be transposed — each is keyed into its own
    dict on the source; ``federated_id`` is ``None`` (not empty string) when
    IDMC omits it so callers can tell missing from present-but-blank.
    """

    model_config = _IDMC_MODEL_CONFIG

    id: str
    name: str
    conn_type: str  # connParams["Connection Type"] — the platform signal
    base_type: str = ""
    federated_id: Optional[str] = None
    host: str = ""
    database: str = ""
    db_schema: str = ""  # named ``db_schema`` to avoid shadowing BaseModel.schema

    @field_validator("federated_id", mode="before")
    @classmethod
    def _normalize_federated_id(cls, v: Optional[str]) -> Optional[str]:
        # ``""`` would collide with other blank-federated-id connections in
        # the lineage lookup dict; enforce the None/non-empty invariant.
        return None if v == "" else v

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> "IdmcConnection":
        conn_params = data.get("connParams", {})
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            conn_type=conn_params.get("Connection Type", ""),
            base_type=data.get("type", ""),
            federated_id=data.get("federatedId") or None,
            host=conn_params.get("Host", data.get("host", "")),
            database=conn_params.get("Database", data.get("database", "")),
            db_schema=conn_params.get("Schema", data.get("schema", "")),
        )


class IdmcMappingTask(BaseModel):
    """Mapping-task metadata from v3 MTT or v2 ``/api/v2/mttask/{id}``.

    ``mapping_id`` is the *v2* id of the referenced Mapping. To look up
    the mapping in v3 GUID-keyed state, translate via the v2 index first.
    ``None`` means the v3 MTT listing has been parsed but the v2-detail
    enrichment has not yet populated the reference (or the v2 call
    failed) — distinct from "enriched, but the reference is empty".
    """

    model_config = _IDMC_MODEL_CONFIG

    v2_id: V2Id
    name: str
    path: str = ""
    description: Optional[str] = None
    mapping_id: Optional[V2Id] = None
    mapping_name: str = ""
    connection_id: str = ""
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    tags: List[str] = Field(default_factory=list)

    @field_validator("create_time", "update_time", mode="before")
    @classmethod
    def _parse_times(cls, v: Union[str, datetime, None]) -> Optional[datetime]:
        return _parse_optional_datetime(v)

    @classmethod
    def from_idmc_object(cls, obj: IdmcObject) -> "IdmcMappingTask":
        return cls(
            v2_id=V2Id(obj.id),
            name=obj.name,
            path=obj.path,
            description=obj.description,
            created_by=obj.created_by,
            updated_by=obj.updated_by,
            create_time=obj.create_time,
            update_time=obj.update_time,
            tags=list(obj.tags),
        )

    def merge_v2_details(self, data: Dict[str, Any]) -> None:
        # v3 MTT listing omits the mapping reference; the v2 detail
        # endpoint carries it. Empty values don't clobber existing fields.
        new_mapping_id = data.get("mappingId")
        if new_mapping_id:
            self.mapping_id = V2Id(new_mapping_id)
        self.mapping_name = data.get("mappingName") or self.mapping_name
        self.connection_id = data.get("connectionId") or self.connection_id


class LineageTable(BaseModel):
    """One source-or-target table entry parsed out of the IDMC export."""

    model_config = _IDMC_MODEL_CONFIG

    table_name: str
    schema_name: str = ""
    connection_federated_id: str = ""
    transformation_name: str = ""


class MappingLineageInfo(BaseModel):
    """Aggregated source/target tables for a single mapping.

    ``mapping_id`` is the v3 GUID (the submitted export-job object id), not
    the v2 ``mapping.id``. Downstream code joins this against
    ``IdmcMapping.asset_frs_guid``.
    """

    model_config = _IDMC_MODEL_CONFIG

    mapping_id: V3Guid
    mapping_name: str
    source_tables: List[LineageTable] = Field(default_factory=list)
    target_tables: List[LineageTable] = Field(default_factory=list)


class ExportJobStatus(BaseModel):
    """Status poll response for a submitted v3 export job."""

    model_config = _IDMC_MODEL_CONFIG

    job_id: str
    state: ExportJobState
    message: str = ""


# Closed set — anything else is normalized to ``"unknown"``.
TaskflowStepType = Literal[
    "data",
    "command",
    "decision",
    "assignment",
    "notification",
    "subtaskflow",
    "unknown",
]


class TaskflowStep(BaseModel):
    """One step within an IDMC Taskflow's DAG.

    Only ``data`` steps carry a ``task_ref_*`` referencing a runnable task.
    ``predecessor_step_ids`` is empty for the entry point.
    """

    model_config = _IDMC_MODEL_CONFIG

    step_id: str
    step_name: str
    step_type: TaskflowStepType
    task_type: Optional[str] = None  # e.g. "MTT", "DATA_INGESTION"
    task_ref_id: Optional[str] = None
    task_ref_name: Optional[str] = None
    predecessor_step_ids: List[str] = Field(default_factory=list)


class TaskflowDefinition(BaseModel):
    """The step-by-step structure of an IDMC Taskflow."""

    model_config = _IDMC_MODEL_CONFIG

    taskflow_id: str
    taskflow_name: str
    steps: List[TaskflowStep] = Field(default_factory=list)


# Must remain a dataclass — StaleEntityRemovalSourceReport is a dataclass.
@dataclass
class InformaticaSourceReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    folders_scanned: int = 0
    taskflows_scanned: int = 0
    taskflows_with_steps: int = 0
    mappings_scanned: int = 0
    mapping_tasks_scanned: int = 0
    lineage_edges_emitted: int = 0

    projects_filtered: int = 0
    folders_filtered: int = 0
    taskflows_filtered: int = 0
    mapping_tasks_filtered: int = 0

    connections_resolved: int = 0
    connections_unresolved: LossyList[str] = field(default_factory=LossyList)

    export_jobs_submitted: int = 0
    export_jobs_failed: LossyList[str] = field(default_factory=LossyList)

    objects_failed: LossyList[str] = field(default_factory=LossyList)

    api_call_count: int = 0

    # Raw counts before filtering, for diagnosing "where did my assets go?".
    raw_objects_by_type: Dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    # Drop counts keyed by "<reason>:<type>", e.g. "bundle:DTEMPLATE".
    filtered_by_reason: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sample_paths_by_type: LossyDict[str, LossyList[str]] = field(
        default_factory=LossyDict
    )
    api_call_log: LossyList[str] = field(default_factory=LossyList)
    api_errors: LossyList[str] = field(default_factory=LossyList)

    def report_api_call(self) -> None:
        self.api_call_count += 1

    def report_api_response(
        self,
        method: str,
        url: str,
        status: int,
        item_count: Optional[int] = None,
        extra: str = "",
    ) -> None:
        # Full URL (with query string) disambiguates endpoints like
        # ``q=type=='MAPPING'`` vs ``DTEMPLATE``.
        count_str = f" items={item_count}" if item_count is not None else ""
        extra_str = f" {extra}" if extra else ""
        self.api_call_log.append(f"{method} {url} → {status}{count_str}{extra_str}")

    def report_raw_object(self, object_type: str, path: str) -> None:
        self.raw_objects_by_type[object_type] = (
            self.raw_objects_by_type.get(object_type, 0) + 1
        )
        if object_type not in self.sample_paths_by_type:
            self.sample_paths_by_type[object_type] = LossyList()
        self.sample_paths_by_type[object_type].append(path or "<empty path>")

    def report_filtered(self, reason: str, object_type: str, detail: str = "") -> None:
        key = f"{reason}:{object_type}"
        self.filtered_by_reason[key] = self.filtered_by_reason.get(key, 0) + 1
        if detail and len(self.api_errors) < 50:
            self.api_errors.append(f"{key} {detail}")

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
