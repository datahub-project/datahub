from typing import Dict, Iterable, List, Optional

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
from datahub.ingestion.source.matillion_dpc.matillion_utils import (
    MatillionUrnBuilder,
    build_project_url,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionProject,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
)


class MatillionProjectKey(ContainerKey):
    project_id: str


class MatillionEnvironmentKey(MatillionProjectKey):
    environment_name: str


class MatillionFolderKey(MatillionEnvironmentKey):
    # folder_path is cumulative from the environment root so each nested level
    # resolves to a distinct container.
    folder_path: str


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

    def _get_folder_key(
        self,
        project: MatillionProject,
        environment: MatillionEnvironment,
        folder_path: str,
    ) -> MatillionFolderKey:
        return MatillionFolderKey(
            platform=MATILLION_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_id=project.id,
            environment_name=environment.name,
            folder_path=folder_path,
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
        # The project is the only level gated by extract_projects_to_containers;
        # environments and folders are always materialized.
        if not self.config.extract_projects_to_containers:
            return

        container_urn = self.get_project_container_urn(project)
        if container_urn in self._containers_emitted:
            return

        yield from gen_containers(
            container_key=self._get_project_key(project),
            name=project.name,
            description=project.description,
            sub_types=[DatasetContainerSubTypes.MATILLION_PROJECT],
            external_url=build_project_url(
                self.config.api_config.console_url, project.id
            ),
            extra_properties={"project_id": project.id},
        )
        self._containers_emitted.add(container_urn)
        self.report.report_containers_emitted()

    def emit_environment_container(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> Iterable[MetadataWorkUnit]:
        parent_key: Optional[ContainerKey] = None
        if self.config.extract_projects_to_containers:
            yield from self.emit_project_container(project)
            parent_key = self._get_project_key(project)

        environment_urn = self.get_environment_container_urn(environment, project)
        if environment_urn in self._containers_emitted:
            return

        yield from gen_containers(
            container_key=self._get_environment_key(environment, project),
            name=environment.name,
            sub_types=[DatasetContainerSubTypes.MATILLION_ENVIRONMENT],
            parent_container_key=parent_key,
            extra_properties={"environment_name": environment.name},
        )
        self._containers_emitted.add(environment_urn)
        self.report.report_containers_emitted()

    def _emit_folder_containers(
        self,
        project: MatillionProject,
        environment: MatillionEnvironment,
        folder_segments: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        parent_key: ContainerKey = self._get_environment_key(environment, project)
        cumulative: List[str] = []
        for segment in folder_segments:
            cumulative.append(segment)
            folder_key = self._get_folder_key(
                project, environment, "/".join(cumulative)
            )
            folder_urn = folder_key.as_urn()
            if folder_urn not in self._containers_emitted:
                yield from gen_containers(
                    container_key=folder_key,
                    name=segment,
                    sub_types=[DatasetContainerSubTypes.MATILLION_FOLDER],
                    parent_container_key=parent_key,
                )
                self._containers_emitted.add(folder_urn)
                self.report.report_containers_emitted()
            parent_key = folder_key

    def get_deepest_container_urn(
        self,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
        folder_segments: List[str],
    ) -> Optional[str]:
        if environment and folder_segments:
            return self._get_folder_key(
                project, environment, "/".join(folder_segments)
            ).as_urn()
        if environment:
            return self.get_environment_container_urn(environment, project)
        if self.config.extract_projects_to_containers:
            return self.get_project_container_urn(project)
        return None

    def _browse_path_ancestors(
        self,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment],
        folder_segments: List[str],
    ) -> List[BrowsePathEntryClass]:
        entries: List[BrowsePathEntryClass] = []
        if self.config.extract_projects_to_containers:
            project_urn = self.get_project_container_urn(project)
            entries.append(BrowsePathEntryClass(id=project_urn, urn=project_urn))

        if not environment:
            return entries

        env_urn = self.get_environment_container_urn(environment, project)
        entries.append(BrowsePathEntryClass(id=env_urn, urn=env_urn))

        cumulative: List[str] = []
        for segment in folder_segments:
            cumulative.append(segment)
            folder_urn = self._get_folder_key(
                project, environment, "/".join(cumulative)
            ).as_urn()
            entries.append(BrowsePathEntryClass(id=folder_urn, urn=folder_urn))

        return entries

    def add_pipeline_to_container(
        self,
        pipeline_urn: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment] = None,
        folder_segments: Optional[List[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        folder_segments = folder_segments or []
        if environment:
            yield from self.emit_environment_container(environment, project)
            yield from self._emit_folder_containers(
                project, environment, folder_segments
            )
        else:
            yield from self.emit_project_container(project)

        container_urn = self.get_deepest_container_urn(
            project, environment, folder_segments
        )
        if container_urn is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=pipeline_urn,
                aspect=ContainerClass(container=container_urn),
            ).as_workunit()

        path_elements = self._browse_path_ancestors(
            project, environment, folder_segments
        )
        if path_elements:
            yield MetadataChangeProposalWrapper(
                entityUrn=pipeline_urn,
                aspect=BrowsePathsV2Class(path=path_elements),
            ).as_workunit()

    def add_data_job_to_container(
        self,
        data_job_urn: str,
        pipeline_urn: str,
        project: MatillionProject,
        environment: Optional[MatillionEnvironment] = None,
        folder_segments: Optional[List[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        # Components nest in their pipeline's container and browse under the pipeline.
        folder_segments = folder_segments or []
        container_urn = self.get_deepest_container_urn(
            project, environment, folder_segments
        )
        if container_urn is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job_urn,
                aspect=ContainerClass(container=container_urn),
            ).as_workunit()

        path_elements = self._browse_path_ancestors(
            project, environment, folder_segments
        )
        path_elements.append(BrowsePathEntryClass(id=pipeline_urn, urn=pipeline_urn))
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job_urn,
            aspect=BrowsePathsV2Class(path=path_elements),
        ).as_workunit()
