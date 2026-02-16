from typing import Dict, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_models import (
    VertexAIResourceCategoryKey,
)


def get_actor_from_labels(labels: Optional[Dict[str, str]]) -> Optional[str]:
    """Extract actor URN from resource labels (created_by, creator, owner)."""
    if not labels:
        return None

    actor_keys = ["created_by", "creator", "owner"]
    for key in actor_keys:
        if key in labels and labels[key]:
            return builder.make_user_urn(labels[key])

    return None


def get_project_container(
    project_id: str, platform: str, platform_instance: Optional[str], env: str
) -> ProjectIdKey:
    return ProjectIdKey(
        project_id=project_id,
        platform=platform,
        instance=platform_instance,
        env=env,
    )


def get_resource_category_container(
    project_id: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    category: str,
) -> VertexAIResourceCategoryKey:
    return VertexAIResourceCategoryKey(
        project_id=project_id,
        platform=platform,
        instance=platform_instance,
        env=env,
        category=category,
    )
