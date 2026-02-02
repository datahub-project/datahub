"""Metadata construction utilities for Google Sheets connector."""

import logging
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import make_dataplatform_instance_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig
from datahub.ingestion.source.google_sheets.constants import (
    PERMISSION_ROLE_OWNER,
    PERMISSION_ROLE_WRITER,
    PERMISSION_TYPE_USER,
    SUBTYPE_FOLDER,
)
from datahub.ingestion.source.google_sheets.models import DriveFile
from datahub.ingestion.source.google_sheets.utils import (
    extract_tags_from_drive_metadata,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetLineageTypeClass
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


class GoogleSheetsContainerKey(ContainerKey):
    """Container key for Google Sheets folder hierarchy."""

    key: str


class MetadataBuilder:
    """Builds DataHub metadata objects (MCPs, containers, etc.) for Google Sheets."""

    def __init__(
        self,
        platform: str,
        config: GoogleSheetsSourceConfig,
        ctx: PipelineContext,
    ):
        self.platform = platform
        self.config = config
        self.ctx = ctx

    def get_container_urn(self, path_part: str) -> str:
        """Generate a container URN for a path part."""
        container_key = GoogleSheetsContainerKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            key=path_part,
        )
        return container_key.as_urn()

    def create_platform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create a DataPlatformInstance aspect MCP for the specified dataset."""
        if not self.config.platform_instance:
            return None

        platform_instance = DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            ),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=platform_instance
        )

    def create_container_aspect(
        self, sheet_file: DriveFile, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create a Container aspect to associate the dataset with its parent container."""
        sheet_path = sheet_file.path.lstrip("/")
        path_segments = [segment for segment in sheet_path.split("/") if segment]

        if not path_segments:
            container_urn = self.get_container_urn(self.platform)
        else:
            container_urn = self.get_container_urn(path_segments[-1])

        container_aspect = ContainerClass(container=container_urn)

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=container_aspect
        )

    def create_container_mcps(
        self, path_parts: List[str]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Create container entities for each path part and yield MCPs."""
        for i, path_part in enumerate(path_parts):
            container_urn = self.get_container_urn(path_part)

            container_properties = ContainerPropertiesClass(
                name=path_part, description=f"Google Drive {path_part}"
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=container_urn, aspect=container_properties
            )

            status = StatusClass(removed=False)
            yield MetadataChangeProposalWrapper(entityUrn=container_urn, aspect=status)

            subtype = SubTypesClass(typeNames=[SUBTYPE_FOLDER])
            yield MetadataChangeProposalWrapper(entityUrn=container_urn, aspect=subtype)

            if i > 0:
                parent_urn = self.get_container_urn(path_parts[i - 1])
                container_parent = ContainerClass(container=parent_urn)
                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn, aspect=container_parent
                )

    def create_browse_paths_aspect(
        self, sheet_file: DriveFile, dataset_urn: str
    ) -> MetadataChangeProposalWrapper:
        """Create a BrowsePathsV2 aspect MCP with proper container references."""
        sheet_path = sheet_file.path.lstrip("/")
        path_segments = [segment for segment in sheet_path.split("/") if segment]

        browse_entries: List[BrowsePathEntryClass] = []

        for segment in path_segments:
            browse_entries.append(
                BrowsePathEntryClass(id=segment, urn=self.get_container_urn(segment))
            )

        browse_paths = BrowsePathsV2Class(path=browse_entries)

        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=browse_paths)

    def create_ownership_aspect(
        self, sheet_file: DriveFile, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create an Ownership aspect from Drive file owners and permissions."""
        owners_list: List[OwnerClass] = []

        if sheet_file.owners:
            for owner in sheet_file.owners:
                email = owner.get("emailAddress")
                if email:
                    owner_urn = builder.make_user_urn(email)
                    owners_list.append(
                        OwnerClass(owner=owner_urn, type=OwnershipTypeClass.DATAOWNER)
                    )

        if sheet_file.permissions:
            for permission in sheet_file.permissions:
                if not permission.emailAddress:
                    continue

                role = permission.role
                perm_type = permission.type

                if role == PERMISSION_ROLE_OWNER:
                    continue

                if perm_type == PERMISSION_TYPE_USER and role == PERMISSION_ROLE_WRITER:
                    owner_urn = builder.make_user_urn(permission.emailAddress)
                    owners_list.append(
                        OwnerClass(
                            owner=owner_urn, type=OwnershipTypeClass.TECHNICAL_OWNER
                        )
                    )
                else:
                    continue

        if not owners_list:
            return None

        ownership = OwnershipClass(owners=owners_list)
        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=ownership)

    def create_tags_aspect(
        self, sheet_file: DriveFile, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create tags aspect from Drive labels."""
        tags_list = extract_tags_from_drive_metadata(sheet_file, self.config)
        if not tags_list:
            return None

        from datahub.metadata.schema_classes import GlobalTagsClass

        tags = GlobalTagsClass(tags=tags_list)
        return MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=tags)

    def create_upstream_lineage(
        self, upstream_urns: List[str]
    ) -> Optional[UpstreamLineageClass]:
        """Create upstream lineage from a list of dataset URNs."""
        if not upstream_urns:
            return None

        upstreams = [
            UpstreamClass(
                dataset=urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for urn in upstream_urns
        ]

        return UpstreamLineageClass(upstreams=upstreams)

    def build_dataset_urn(self, dataset_id: str) -> str:
        """Build a dataset URN for Google Sheets."""
        return DatasetUrn(
            platform=self.platform,
            name=dataset_id,
            env=self.config.env,
        ).urn()

    def extract_owner_emails(self, sheet_file: DriveFile) -> List[str]:
        """Extract owner email addresses from Drive file metadata."""
        owner_emails = []

        if sheet_file.owners:
            for owner in sheet_file.owners:
                email = owner.get("emailAddress")
                if email:
                    owner_emails.append(email)

        return owner_emails
