from dataclasses import dataclass, field
from typing import Any, Dict, List

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class IdmcObject:
    """A generic IDMC object returned by the v3 objects API."""

    id: str
    name: str
    path: str
    object_type: str
    description: str = ""
    updated_by: str = ""
    created_by: str = ""
    create_time: str = ""
    update_time: str = ""
    tags: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IdmcMapping:
    """Mapping metadata from the v2 mapping API."""

    v2_id: str
    name: str
    asset_frs_guid: str  # v3 GUID, used for cross-referencing
    description: str = ""
    created_by: str = ""
    updated_by: str = ""
    create_time: str = ""
    update_time: str = ""
    document_type: str = ""
    valid: bool = True
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    references: List[Dict[str, Any]] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IdmcConnection:
    """Connection metadata from the v2 connection API."""

    id: str
    name: str
    conn_type: str  # connParams["Connection Type"] — the actual platform signal
    base_type: str = ""
    federated_id: str = ""
    host: str = ""
    database: str = ""
    schema: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_api_response(data: Dict[str, Any]) -> "IdmcConnection":
        conn_params = data.get("connParams", {})
        return IdmcConnection(
            id=data.get("id", ""),
            name=data.get("name", ""),
            conn_type=conn_params.get("Connection Type", ""),
            base_type=data.get("type", ""),
            federated_id=data.get("federatedId", ""),
            host=conn_params.get("Host", data.get("host", "")),
            database=conn_params.get("Database", data.get("database", "")),
            schema=conn_params.get("Schema", data.get("schema", "")),
            raw=data,
        )


@dataclass
class IdmcMappingTask:
    """Mapping task metadata from the v2 mttask API."""

    v2_id: str
    name: str
    description: str = ""
    mapping_id: str = ""
    mapping_name: str = ""
    connection_id: str = ""
    created_by: str = ""
    updated_by: str = ""
    create_time: str = ""
    update_time: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingLineageInfo:
    """Lineage extracted from a mapping's @3.bin export."""

    mapping_id: str
    mapping_name: str
    source_tables: List["LineageTable"] = field(default_factory=list)
    target_tables: List["LineageTable"] = field(default_factory=list)


@dataclass
class LineageTable:
    """A source or target table referenced in a mapping transformation."""

    table_name: str
    schema_name: str = ""
    connection_federated_id: str = ""
    transformation_name: str = ""


@dataclass
class ExportJobStatus:
    """Status of a v3 export job."""

    job_id: str
    state: str  # IN_PROGRESS, SUCCESSFUL, FAILED
    message: str = ""


@dataclass
class InformaticaSourceReport(StaleEntityRemovalSourceReport):
    """Report for Informatica Cloud ingestion."""

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
