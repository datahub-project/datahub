import logging
from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
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
)

logger = logging.getLogger(__name__)


class MatillionProjectKey(ContainerKey):
    """Container key for Matillion DPC projects."""

    project_id: str


class MatillionEnvironmentKey(MatillionProjectKey):
    """Container key for Matillion DPC environments (inherits from ProjectKey for hierarchy)."""

    environment_name: str


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

    def _get_project_key(self, project: MatillionProject) -> MatillionProjectKey:
        return MatillionProjectKey(
            platform=MATILLION_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_id=project.id,
        )

    def _get_environment_key(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> MatillionEnvironmentKey:
        return MatillionEnvironmentKey(
            platform=MATILLION_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_id=project.id,
            environment_name=environment.name,
        )

    def get_project_container_urn(self, project: MatillionProject) -> str:
        return self._get_project_key(project).as_urn()

    def get_environment_container_urn(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> str:
        return self._get_environment_key(environment, project).as_urn()

    def emit_project_container(
        self, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        container_urn = self.get_project_container_urn(project)

        if container_urn in self._containers_emitted:
            return

        yield from gen_containers(
            container_key=self._get_project_key(project),
            name=project.name,
            description=project.description,
            sub_types=[DatasetContainerSubTypes.MATILLION_PROJECT],
            extra_properties={
                "project_id": project.id,
            },
        )

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

        yield from gen_containers(
            container_key=self._get_environment_key(environment, project),
            name=environment.name,
            sub_types=[DatasetContainerSubTypes.MATILLION_ENVIRONMENT],
            parent_container_key=self._get_project_key(project),
            extra_properties={
                "environment_name": environment.name,
            },
        )

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
