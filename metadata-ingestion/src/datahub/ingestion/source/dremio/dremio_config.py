from typing import Optional, Dict, List, Type, Set
import certifi

from datahub.configuration.common import ConfigModel, AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalHandler, \
    StatefulStaleMetadataRemovalConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import FabricTypeClass

from pydantic import Field, validator


class DremioSourceMapping(ConfigModel):
    platform: str
    platform_name: str
    platform_instance: Optional[str] = None
    dremio_source_type: Optional[str] = None
    env: Optional[str] = FabricTypeClass.PROD
    rootPath: Optional[str] = None
    databaseName: Optional[str] = None


class DremioSourceConfig(ConfigModel, StatefulIngestionConfigBase):

    # Dremio Connection Details
    hostname: Optional[str] = Field(
        default=None,
        description="Hostname or IP Address of the Dremio server",
    )

    port: Optional[int] = Field(
        default=9047,
        description="Port of the Dremio REST API",
    )

    username: Optional[str] = Field(
        default=None,
        description="Dremio username",
    )

    authentication_method: Optional[str] = Field(
        default="password",
        description="Authentication method: 'password' or 'PAT' (Personal Access Token)",
    )

    password: Optional[str] = Field(
        default=None,
        description="Dremio password or Personal Access Token",
    )

    tls: Optional[bool] = Field(
        default=True,
        description="Whether the Dremio REST API port is encrypted",
    )

    disable_certificate_verification: Optional[bool] = Field(
        default=False,
        description="Disable TLS certificate verification",
    )

    path_to_certificates: str = Field(
        default=certifi.where(),
        description="Path to SSL certificates",
    )

    # Dremio Cloud specific configs
    is_dremio_cloud: Optional[bool] = Field(
        default=False,
        description="Whether this is a Dremio Cloud instance",
    )
    dremio_cloud_region: Optional[str] = Field(
        default=None,
        description="Dremio Cloud region ('US' or 'EMEA')",
    )

    # DataHub Environment details
    env: str = Field(
        default="PROD",
        description="Environment to use in namespace when constructing URNs.",
    )
    platform_instance: Optional[str] = Field(
        default="",
        description="Platform instance for the source.",
    )

    source_mappings: Optional[List[DremioSourceMapping]] = Field(
        default=None,
        description="Mappings from Dremio sources to DataHub platforms and datasets.",
    )

    # Entity Filters
    collect_pds: bool = Field(
        default=False,
        description="Whether to collect physical datasets",
    )

    collect_system_tables: bool = Field(
        default=False,
        description="Whether to collect Dremio system tables",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter",
    )

    # Advanced Configs
    max_workers: Optional[int] = Field(
        default=20,
        description="Maximum number of worker threads for parallel processing.",
    )

    include_query_lineage: bool = Field(
        default=False,
        description="Whether to include query-based lineage information.",
    )

    include_table_rename_lineage: bool = Field(
        default=True,
        description="Whether to include table rename lineage",
    )

    include_copy_lineage: bool = Field(
        default=True,
        description="Whether to include copy lineage",
    )


    @validator("authentication_method")
    def validate_auth_method(cls, v):
        allowed_methods = ["password", "PAT"]
        if v not in allowed_methods:
            raise ValueError(
                f"authentication_method must be one of {allowed_methods}",
            )
        return v

    @validator("password")
    def validate_password(cls, v, values):
        if values.get("authentication_method") == "PAT" and not v:
            raise ValueError(
                "Password (Personal Access Token) is required when using PAT authentication",
            )
        return v


class DremioState(GenericCheckpointState):
    """
    Serialized state for Dremio ingestion runs.
    """
    urn_to_timestamp: Dict[str, float] = {}

    def add_urn_timestamp(
            self,
            urn: str,
            timestamp: float,
    ) -> None:
        self.urn_to_timestamp[urn] = timestamp

    def get_urn_timestamp(
            self,
            urn: str,
    ) -> Optional[float]:
        return self.urn_to_timestamp.get(urn)


class DremioCheckpointState(GenericCheckpointState):
    """
    Checkpoint state for Dremio ingestion runs.
    """
    dremio_datasets: Set[str] = set()
    dremio_views: Set[str] = set()
    dremio_jobs: Set[str] = set()
    last_updated_timestamps: Dict[str, float] = {}
    scanned_urns: List[str] = []

    def add_dremio_dataset(
            self,
            dataset_urn: str,
    ) -> None:
        self.dremio_datasets.add(dataset_urn)

    def add_dremio_view(
            self,
            view_urn: str,
    ) -> None:
        self.dremio_views.add(view_urn)

    def add_dremio_job(self, job_urn: str) -> None:
        self.dremio_jobs.add(job_urn)

    def add_last_updated_timestamp(
            self,
            urn: str,
            timestamp: float,
    ) -> None:
        self.last_updated_timestamps[urn] = timestamp

    def get_last_updated_timestamp(
            self,
            urn: str,
    ) -> Optional[float]:
        return self.last_updated_timestamps.get(urn)

    def is_dremio_dataset_seen(self, dataset_urn: str) -> bool:
        return dataset_urn in self.dremio_datasets

    def is_dremio_view_seen(self, view_urn: str) -> bool:
        return view_urn in self.dremio_views

    def is_dremio_job_seen(self, job_urn: str) -> bool:
        return job_urn in self.dremio_jobs

    def clear_dremio_state(self) -> None:
        self.dremio_datasets.clear()
        self.dremio_views.clear()
        self.dremio_jobs.clear()
        self.last_updated_timestamps.clear()

    def get_all_entity_urns(self) -> Set[str]:
        return self.dremio_datasets.union(
            self.dremio_views
        ).union(
            self.dremio_jobs
        )

    def convert_to_checkpoint(self) -> Dict[str, bool]:
        checkpoint = {}
        for urn in self.get_all_entity_urns():
            checkpoint[urn] = True
        return checkpoint

    def update_state(self, mcp: MetadataChangeProposalWrapper) -> None:
        urn = mcp.entityUrn
        if urn.startswith("urn:li:dataset"):
            if mcp.aspectName == "subTypes":
                subtypes = mcp.aspect.get("typeNames", [])
                if "View" in subtypes:
                    self.add_dremio_view(urn)
                else:
                    self.add_dremio_dataset(urn)
            else:
                # If we can't determine if it's a view, default to dataset
                self.add_dremio_dataset(urn)
        elif urn.startswith("urn:li:dataJob"):
            self.add_dremio_job(urn)

        # Update the last updated timestamp
        if hasattr(mcp, 'systemMetadata') and mcp.systemMetadata and mcp.systemMetadata.lastObserved:
            self.add_last_updated_timestamp(urn, mcp.systemMetadata.lastObserved)
        else:
            # If systemMetadata is not available in MCP, you might want to use
            # the current timestamp or handle this case differently
            from time import time
            self.add_last_updated_timestamp(urn, time())

    def add_scanned_urn(self, urn: str) -> None:
        self.scanned_urns.append(urn)

    def get_urns(self) -> List[str]:
        return self.scanned_urns

class DremioStaleEntityRemovalHandler(StaleEntityRemovalHandler):
    """
    Manages the deletion of stale entities during Dremio ingestion runs.
    """

    def __init__(
            self,
            source: StatefulIngestionSourceBase,
            config: StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
            state_type_class: Type[DremioCheckpointState],
            pipeline_name: Optional[str],
            run_id: str,
    ):
        super().__init__(
            source=source,
            config=config,
            state_type_class=state_type_class,
            pipeline_name=pipeline_name,
            run_id=run_id,
        )
        self.source = source





