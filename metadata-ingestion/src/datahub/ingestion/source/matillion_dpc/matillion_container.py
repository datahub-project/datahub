import logging
from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_container_urn,
    make_data_platform_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    MATILLION_PLATFORM,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionProject,
)
from datahub.ingestion.source.matillion_dpc.urn_builder import MatillionUrnBuilder
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


class MatillionContainerHandler:
    def __init__(
        self,
        config: MatillionSourceConfig,
        report: MatillionSourceReport,
        urn_builder: MatillionUrnBuilder,
    ):
        self.config = config
        self.report = report
        self.urn_builder = urn_builder
        self._containers_emitted: set[str] = set()
        self._projects_cache: Dict[str, MatillionProject] = {}

    def register_project(self, project: MatillionProject) -> None:
        self._projects_cache[project.id] = project

    def get_project(self, project_id: str) -> Optional[MatillionProject]:
        return self._projects_cache.get(project_id)

    def get_project_container_urn(self, project: MatillionProject) -> str:
        guid = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
            }
        )
        return make_container_urn(guid)

    def get_environment_container_urn(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> str:
        guid = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "environment_name": environment.name,
            }
        )
        return make_container_urn(guid)

    def emit_project_container(
        self, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_project_container_urn(project)

        if container_urn in self._containers_emitted:
            return

        container_properties = ContainerPropertiesClass(
            name=project.name,
            description=project.description,
            customProperties={
                "project_id": project.id,
                "platform": MATILLION_PLATFORM,
            },
        )

        container = ContainerClass(container=container_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=container_properties,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=container,
        ).as_workunit()

        platform_urn = make_data_platform_urn(MATILLION_PLATFORM)
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(id=platform_urn, urn=platform_urn),
                BrowsePathEntryClass(id=container_urn, urn=container_urn),
            ]
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=browse_path,
        ).as_workunit()

        sub_types = SubTypesClass(
            typeNames=[DatasetContainerSubTypes.MATILLION_PROJECT]
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=sub_types,
        ).as_workunit()

        self._containers_emitted.add(container_urn)
        self.report.report_containers_emitted()
        logger.debug(f"Emitted project container: {project.name}")

    def emit_environment_container(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        project_container_urn = self.get_project_container_urn(project)

        if project_container_urn not in self._containers_emitted:
            yield from self.emit_project_container(project)

        environment_urn = self.get_environment_container_urn(environment, project)

        if environment_urn in self._containers_emitted:
            return

        container_properties = ContainerPropertiesClass(
            name=environment.name,
            customProperties={
                "environment_name": environment.name,
                "project_id": project.id,
                "platform": MATILLION_PLATFORM,
            },
        )

        container = ContainerClass(container=project_container_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=environment_urn,
            aspect=container_properties,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=environment_urn,
            aspect=container,
        ).as_workunit()

        platform_urn = make_data_platform_urn(MATILLION_PLATFORM)
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(id=platform_urn, urn=platform_urn),
                BrowsePathEntryClass(
                    id=project_container_urn, urn=project_container_urn
                ),
                BrowsePathEntryClass(id=environment_urn, urn=environment_urn),
            ]
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=environment_urn,
            aspect=browse_path,
        ).as_workunit()

        sub_types = SubTypesClass(
            typeNames=[DatasetContainerSubTypes.MATILLION_ENVIRONMENT]
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=environment_urn,
            aspect=sub_types,
        ).as_workunit()

        self._containers_emitted.add(environment_urn)
        self.report.report_containers_emitted()
        logger.debug(
            f"Emitted environment container: {environment.name} in project {project.name}"
        )

    def add_pipeline_to_container(
        self,
        pipeline_urn: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if environment:
            container_urn = self.get_environment_container_urn(environment, project)
        else:
            container_urn = self.get_project_container_urn(project)

        container = ContainerClass(container=container_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_urn,
            aspect=container,
        ).as_workunit()

        platform_urn = make_data_platform_urn(MATILLION_PLATFORM)
        project_urn = self.get_project_container_urn(project)
        path_elements = [
            BrowsePathEntryClass(id=platform_urn, urn=platform_urn),
            BrowsePathEntryClass(id=project_urn, urn=project_urn),
        ]

        if environment:
            env_urn = self.get_environment_container_urn(environment, project)
            path_elements.append(BrowsePathEntryClass(id=env_urn, urn=env_urn))

        path_elements.append(BrowsePathEntryClass(id=pipeline_urn, urn=pipeline_urn))

        browse_path = BrowsePathsV2Class(path=path_elements)

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_urn,
            aspect=browse_path,
        ).as_workunit()
