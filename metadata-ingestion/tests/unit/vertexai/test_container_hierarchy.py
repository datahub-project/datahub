"""Test container hierarchy generation for Vertex AI"""

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.ingestion.source.vertexai.vertexai_constants import (
    ResourceCategory,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
)

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
    )


def test_project_container_generation(source: VertexAISource) -> None:
    """Test that project container is generated"""
    workunits = list(source._gen_project_workunits())

    # Should generate workunits for:
    # 1. Project container (ContainerProperties, Status, DataPlatformInstance, SubTypes)
    # 2. 7 resource category containers, each with (Container, ContainerProperties, Status, DataPlatformInstance, SubTypes)
    # Total: 4 (project) + 7*5 (categories) = 39 workunits
    assert len(workunits) >= 35, f"Expected at least 35 workunits, got {len(workunits)}"

    # Verify project container aspects
    project_urn = source._get_project_container().as_urn()
    project_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.entityUrn == project_urn
    ]

    assert any(
        isinstance(aspect, ContainerPropertiesClass) for aspect in project_aspects
    ), "Project container should have ContainerProperties"
    assert any(isinstance(aspect, SubTypesClass) for aspect in project_aspects), (
        "Project container should have SubTypes"
    )


def test_resource_category_containers_generation(source: VertexAISource) -> None:
    """Test that all resource category containers are generated with correct hierarchy"""
    workunits = list(source._generate_resource_category_containers())

    categories = [
        ResourceCategory.MODELS,
        ResourceCategory.TRAINING_JOBS,
        ResourceCategory.DATASETS,
        ResourceCategory.ENDPOINTS,
        ResourceCategory.PIPELINES,
        ResourceCategory.EXPERIMENTS,
        ResourceCategory.EVALUATIONS,
    ]

    # Each category should generate 5 workunits:
    # Container (parent link), ContainerProperties, Status, DataPlatformInstance, SubTypes
    expected_wu_count = len(categories) * 5
    assert len(workunits) == expected_wu_count, (
        f"Expected {expected_wu_count} workunits for {len(categories)} categories, got {len(workunits)}"
    )

    # Verify each category container
    for category in categories:
        category_key = source._get_resource_category_container(category)
        category_urn = category_key.as_urn()

        category_wus = [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn == category_urn
        ]
        assert len(category_wus) > 0, f"No workunits found for category '{category}'"

        aspects = [
            wu.metadata.aspect
            for wu in category_wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]

        # Check for parent container link
        container_aspects = [a for a in aspects if isinstance(a, ContainerClass)]
        assert len(container_aspects) == 1, (
            f"Category '{category}' should have exactly one ContainerClass aspect"
        )

        parent_urn = container_aspects[0].container
        project_urn = source._get_project_container().as_urn()
        assert parent_urn == project_urn, (
            f"Category '{category}' should be linked to project container"
        )

        # Check for container properties
        props_aspects = [a for a in aspects if isinstance(a, ContainerPropertiesClass)]
        assert len(props_aspects) == 1, (
            f"Category '{category}' should have ContainerProperties"
        )
        assert props_aspects[0].name == category, (
            f"Category container name should be '{category}', got '{props_aspects[0].name}'"
        )

        # Check for subtypes
        subtype_aspects = [a for a in aspects if isinstance(a, SubTypesClass)]
        assert len(subtype_aspects) == 1, f"Category '{category}' should have SubTypes"
        assert DatasetContainerSubTypes.FOLDER in subtype_aspects[0].typeNames, (
            f"Category '{category}' should have FOLDER subtype"
        )


def test_resource_category_containers_have_unique_urns(source: VertexAISource) -> None:
    """Test that each resource category container has a unique URN"""
    workunits = list(source._generate_resource_category_containers())

    urns = [
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.entityUrn is not None
    ]
    unique_entity_urns = set(urns)

    # We should have exactly 7 unique entity URNs (one per category)
    # Each entity has multiple aspects, so total workunits > unique URNs
    categories_count = 7
    assert len(unique_entity_urns) == categories_count, (
        f"Expected {categories_count} unique container URNs, got {len(unique_entity_urns)}"
    )

    # Verify all URNs are containers
    for urn in unique_entity_urns:
        assert urn.startswith("urn:li:container:"), (
            f"Expected container URN, got: {urn}"
        )
