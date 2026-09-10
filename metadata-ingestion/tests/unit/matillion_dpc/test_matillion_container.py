from typing import List, Optional

import pytest
from pydantic import SecretStr

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionProject,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ContainerClass,
    SubTypesClass,
)

PROJECT = MatillionProject(id="proj-123", name="Test Project")
ENVIRONMENT = MatillionEnvironment(name="prod", default_agent_id="agent-1")
PIPELINE_URN = "urn:li:dataFlow:(matillion,flow-guid,PROD)"
JOB_URN = "urn:li:dataJob:(urn:li:dataFlow:(matillion,flow-guid,PROD),job-guid)"


@pytest.fixture
def handler() -> MatillionContainerHandler:
    config = MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            client_id=SecretStr("id"),
            client_secret=SecretStr("secret"),
            custom_base_url="http://test.com",
        ),
        platform_instance="test-instance",
    )
    report = MatillionSourceReport()
    return MatillionContainerHandler(config, report)


def _aspect_for(
    workunits: List[MetadataWorkUnit], urn: str, aspect_type: type
) -> Optional[object]:
    for wu in workunits:
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalWrapper)
            and mcp.entityUrn == urn
            and isinstance(mcp.aspect, aspect_type)
        ):
            return mcp.aspect
    return None


def _container_entity_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    urns = []
    for wu in workunits:
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalWrapper)
            and mcp.entityUrn is not None
            and mcp.entityUrn.startswith("urn:li:container:")
            and mcp.entityUrn not in urns
        ):
            urns.append(mcp.entityUrn)
    return urns


def _folder_container_urns(workunits: List[MetadataWorkUnit]) -> List[str]:
    urns = []
    for wu in workunits:
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalWrapper)
            and isinstance(mcp.aspect, SubTypesClass)
            and DatasetContainerSubTypes.MATILLION_FOLDER in mcp.aspect.typeNames
            and mcp.entityUrn not in urns
        ):
            assert mcp.entityUrn is not None
            urns.append(mcp.entityUrn)
    return urns


def test_pipeline_nests_under_deepest_folder(
    handler: MatillionContainerHandler,
) -> None:
    folders = ["ingest", "staging", "orders"]
    workunits = list(
        handler.add_pipeline_to_container(
            PIPELINE_URN, PROJECT, environment=ENVIRONMENT, folder_segments=folders
        )
    )

    deepest = handler.get_deepest_container_urn(PROJECT, ENVIRONMENT, folders)
    container = _aspect_for(workunits, PIPELINE_URN, ContainerClass)
    assert isinstance(container, ContainerClass)
    assert container.container == deepest

    # One container per folder segment, plus the project and environment.
    assert len(_folder_container_urns(workunits)) == len(folders)
    assert len(_container_entity_urns(workunits)) == len(folders) + 2

    browse = _aspect_for(workunits, PIPELINE_URN, BrowsePathsV2Class)
    assert isinstance(browse, BrowsePathsV2Class)
    path_urns = [entry.urn for entry in browse.path]
    # project -> env -> 3 folders, ancestors only (pipeline is never in its own path).
    assert len(browse.path) == 5
    assert PIPELINE_URN not in path_urns
    assert path_urns[-1] == deepest


def test_pipeline_without_folders_nests_under_environment(
    handler: MatillionContainerHandler,
) -> None:
    workunits = list(
        handler.add_pipeline_to_container(
            PIPELINE_URN, PROJECT, environment=ENVIRONMENT, folder_segments=[]
        )
    )

    env_urn = handler.get_environment_container_urn(ENVIRONMENT, PROJECT)
    container = _aspect_for(workunits, PIPELINE_URN, ContainerClass)
    assert isinstance(container, ContainerClass)
    assert container.container == env_urn
    assert _folder_container_urns(workunits) == []


def test_folders_emitted_without_project_when_projects_disabled(
    handler: MatillionContainerHandler,
) -> None:
    # extract_projects_to_containers gates only the project: environment and folders
    # are still materialized, and the pipeline nests under its deepest folder.
    handler.config.extract_projects_to_containers = False
    folders = ["ingest", "staging"]
    workunits = list(
        handler.add_pipeline_to_container(
            PIPELINE_URN, PROJECT, environment=ENVIRONMENT, folder_segments=folders
        )
    )

    project_urn = handler.get_project_container_urn(PROJECT)
    assert project_urn not in _container_entity_urns(workunits)
    assert len(_folder_container_urns(workunits)) == len(folders)

    browse = _aspect_for(workunits, PIPELINE_URN, BrowsePathsV2Class)
    assert isinstance(browse, BrowsePathsV2Class)
    path_urns = [entry.urn for entry in browse.path]
    # env -> 2 folders, no project node at the top.
    assert project_urn not in path_urns
    assert path_urns[0] == handler.get_environment_container_urn(ENVIRONMENT, PROJECT)
    assert len(browse.path) == 3


def test_folder_containers_not_re_emitted(
    handler: MatillionContainerHandler,
) -> None:
    folders = ["ingest", "staging"]
    list(
        handler.add_pipeline_to_container(
            PIPELINE_URN, PROJECT, environment=ENVIRONMENT, folder_segments=folders
        )
    )
    second = list(
        handler.add_pipeline_to_container(
            "urn:li:dataFlow:(matillion,flow-guid-2,PROD)",
            PROJECT,
            environment=ENVIRONMENT,
            folder_segments=folders,
        )
    )
    assert _container_entity_urns(second) == []


def test_data_job_shares_folder_and_browses_under_pipeline(
    handler: MatillionContainerHandler,
) -> None:
    folders = ["ingest", "staging", "orders"]
    workunits = list(
        handler.add_data_job_to_container(
            JOB_URN,
            PIPELINE_URN,
            PROJECT,
            environment=ENVIRONMENT,
            folder_segments=folders,
        )
    )

    deepest = handler.get_deepest_container_urn(PROJECT, ENVIRONMENT, folders)
    container = _aspect_for(workunits, JOB_URN, ContainerClass)
    assert isinstance(container, ContainerClass)
    assert container.container == deepest

    browse = _aspect_for(workunits, JOB_URN, BrowsePathsV2Class)
    assert isinstance(browse, BrowsePathsV2Class)
    path_urns = [entry.urn for entry in browse.path]
    # project -> env -> 3 folders -> pipeline (job excluded from its own path).
    assert path_urns[-1] == PIPELINE_URN
    assert JOB_URN not in path_urns
    assert deepest in path_urns
