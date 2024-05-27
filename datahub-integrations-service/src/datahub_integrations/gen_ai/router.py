import fastapi
import pydantic
from datahub.metadata.urns import QueryUrn, Urn

from datahub_integrations.gen_ai.entity_context import generate_context
from datahub_integrations.gen_ai.suggest_description import generate_desc
from datahub_integrations.gen_ai.suggest_query_description import (
    generate_query_desc,
    get_query_context,
)

router = fastapi.APIRouter()


class SuggestedDescription(pydantic.BaseModel):
    """A suggested entity description."""

    entity_description: str = pydantic.Field(
        description="The suggested description of the entity."
    )


@router.get("/suggest_description")
def suggest_description(entity_urn: str) -> SuggestedDescription:
    """Generate an entity description."""

    urn = Urn.from_string(entity_urn)

    if isinstance(urn, QueryUrn):
        query_context = get_query_context(entity_urn)
        desc = generate_query_desc(query_context)

    else:
        entity_context = generate_context(entity_urn)
        desc = generate_desc(entity_context, use_flattery=True)

    return SuggestedDescription(
        entity_description=desc,
    )
