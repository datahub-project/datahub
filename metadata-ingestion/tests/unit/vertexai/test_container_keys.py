from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_constants import ResourceCategory
from datahub.ingestion.source.vertexai.vertexai_models import (
    VertexAIResourceCategoryKey,
)


def test_vertex_ai_resource_category_key_unique_guids():
    """Test that each resource category generates a unique GUID"""
    project_id = "test-project"
    platform = "vertexai"

    categories = [
        ResourceCategory.MODELS,
        ResourceCategory.TRAINING_JOBS,
        ResourceCategory.DATASETS,
        ResourceCategory.ENDPOINTS,
        ResourceCategory.PIPELINES,
        ResourceCategory.EVALUATIONS,
    ]

    guids = set()
    urns = set()

    for category in categories:
        key = VertexAIResourceCategoryKey(
            project_id=project_id, platform=platform, category=category
        )
        guid = key.guid()
        urn = key.as_urn()

        assert guid not in guids, f"Duplicate GUID for category '{category}': {guid}"
        assert urn not in urns, f"Duplicate URN for category '{category}': {urn}"

        guids.add(guid)
        urns.add(urn)

    # Verify we have the expected number of unique GUIDs
    assert len(guids) == len(categories), (
        f"Expected {len(categories)} unique GUIDs, got {len(guids)}"
    )
    assert len(urns) == len(categories), (
        f"Expected {len(categories)} unique URNs, got {len(urns)}"
    )


def test_vertex_ai_resource_category_key_different_from_project():
    """Test that resource category keys are different from project key"""
    project_id = "test-project"
    platform = "vertexai"

    project_key = ProjectIdKey(project_id=project_id, platform=platform)
    project_guid = project_key.guid()
    project_urn = project_key.as_urn()

    models_key = VertexAIResourceCategoryKey(
        project_id=project_id, platform=platform, category=ResourceCategory.MODELS
    )
    models_guid = models_key.guid()
    models_urn = models_key.as_urn()

    assert models_guid != project_guid, (
        "Resource category GUID should be different from project GUID"
    )
    assert models_urn != project_urn, (
        "Resource category URN should be different from project URN"
    )
