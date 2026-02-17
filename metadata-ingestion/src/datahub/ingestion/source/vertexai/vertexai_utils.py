import logging
from typing import Dict, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PROGRESS_LOG_INTERVAL,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    VertexAIResourceCategoryKey,
)

logger = logging.getLogger(__name__)


def log_progress(
    current: int,
    total: Optional[int],
    item_type: str,
    interval: int = PROGRESS_LOG_INTERVAL,
) -> None:
    if current % interval == 0:
        logger.info(f"Processed {current} {item_type} from Vertex AI")
    if total and current == total:
        logger.info(f"Finished processing {total} {item_type} from Vertex AI")


def get_actor_from_labels(labels: Optional[Dict[str, str]]) -> Optional[str]:
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
