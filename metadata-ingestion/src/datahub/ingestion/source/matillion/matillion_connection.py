import logging
from typing import Iterable, Set

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_container_urn,
    make_data_platform_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.matillion.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.constants import MATILLION_PLATFORM
from datahub.ingestion.source.matillion.models import (
    MatillionConnection,
    MatillionProject,
)
from datahub.ingestion.source.matillion.urn_builder import MatillionUrnBuilder
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


class MatillionConnectionHandler:
    def __init__(
        self,
        config: MatillionSourceConfig,
        report: MatillionSourceReport,
        urn_builder: MatillionUrnBuilder,
    ):
        self.config = config
        self.report = report
        self.urn_builder = urn_builder
        self._connections_emitted: Set[str] = set()
        self._connections_containers_emitted: Set[str] = set()

    def _get_connections_container_urn(self, project: MatillionProject) -> str:
        guid = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "container_type": "connections",
            }
        )
        return make_container_urn(guid)

    def _emit_connections_container(
        self, project: MatillionProject, project_container_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        connections_container_urn = self._get_connections_container_urn(project)

        if connections_container_urn in self._connections_containers_emitted:
            return

        container_properties = ContainerPropertiesClass(
            name="Connections",
            description=f"All connections in project {project.name}",
            customProperties={
                "project_id": project.id,
                "platform": MATILLION_PLATFORM,
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=connections_container_urn,
            aspect=container_properties,
        ).as_workunit()

        container = ContainerClass(container=project_container_urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=connections_container_urn,
            aspect=container,
        ).as_workunit()

        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(id=make_data_platform_urn(MATILLION_PLATFORM)),
                BrowsePathEntryClass(id=project_container_urn),
                BrowsePathEntryClass(id=connections_container_urn),
            ]
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=connections_container_urn,
            aspect=browse_path,
        ).as_workunit()

        sub_types = SubTypesClass(typeNames=["Connections"])
        yield MetadataChangeProposalWrapper(
            entityUrn=connections_container_urn,
            aspect=sub_types,
        ).as_workunit()

        self._connections_containers_emitted.add(connections_container_urn)
        self.report.report_containers_emitted()
        logger.debug(f"Emitted Connections container for project: {project.name}")

    def emit_connection_dataset(
        self, connection: MatillionConnection, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        if not connection.name:
            logger.debug(f"Skipping connection {connection.id} - no name")
            return

        connection_urn = self.urn_builder.make_connection_dataset_urn(connection)
        if not connection_urn:
            logger.debug(f"Could not create URN for connection {connection.name}")
            return

        if connection_urn in self._connections_emitted:
            return

        custom_properties = {
            "connection_id": connection.id,
            "connection_type": connection.connection_type or "unknown",
            "source": "matillion",
        }

        if connection.project_id:
            custom_properties["project_id"] = connection.project_id
        if connection.environment_id:
            custom_properties["environment_id"] = connection.environment_id

        dataset_properties = DatasetPropertiesClass(
            name=connection.name,
            description=connection.description,
            customProperties=custom_properties,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=connection_urn,
            aspect=dataset_properties,
        ).as_workunit()

        status = StatusClass(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=connection_urn,
            aspect=status,
        ).as_workunit()

        sub_types = SubTypesClass(typeNames=[DatasetSubTypes.CONNECTION])
        yield MetadataChangeProposalWrapper(
            entityUrn=connection_urn,
            aspect=sub_types,
        ).as_workunit()

        if self.config.extract_projects_to_containers:
            project_container_urn = self.urn_builder.make_project_container_urn(project)
            connections_container_urn = self._get_connections_container_urn(project)

            yield from self._emit_connections_container(project, project_container_urn)

            container_aspect = ContainerClass(container=connections_container_urn)
            yield MetadataChangeProposalWrapper(
                entityUrn=connection_urn,
                aspect=container_aspect,
            ).as_workunit()

            browse_path = BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=make_data_platform_urn(MATILLION_PLATFORM)),
                    BrowsePathEntryClass(id=project_container_urn),
                    BrowsePathEntryClass(id=connections_container_urn),
                    BrowsePathEntryClass(id=connection_urn),
                ]
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=connection_urn,
                aspect=browse_path,
            ).as_workunit()

        self._connections_emitted.add(connection_urn)
        self.report.report_connection_datasets_emitted()
        logger.debug(f"Emitted connection dataset: {connection.name}")
