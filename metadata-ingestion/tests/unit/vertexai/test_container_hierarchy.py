import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource

PROJECT_ID = "acryl-poc"
REGION = "us-west2"


@pytest.fixture
def source() -> VertexAISource:
    return VertexAISource(
        ctx=PipelineContext(run_id="vertexai-source-test"),
        config=VertexAIConfig(project_id=PROJECT_ID, region=REGION),
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
