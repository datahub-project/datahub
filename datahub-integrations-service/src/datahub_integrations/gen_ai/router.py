import fastapi
import pydantic
from datahub.metadata.urns import DatasetUrn, QueryUrn, Urn

from datahub_integrations.app import graph as uncached_graph
from datahub_integrations.gen_ai.cached_graph import make_cached_graph
from datahub_integrations.gen_ai.description_v2 import (
    generate_column_descriptions_for_urn,
    parse_llm_output,
)
from datahub_integrations.gen_ai.entity_context import generate_context
from datahub_integrations.gen_ai.suggest_description import generate_desc
from datahub_integrations.gen_ai.suggest_query_description import (
    generate_query_desc,
    get_query_context,
)

router = fastapi.APIRouter()
_LEGACY_DESCRIPTION_SUGGESTION_ENABLED = False

graph = make_cached_graph(uncached_graph)


class SuggestedDescription(pydantic.BaseModel):
    """A suggested entity description."""

    entity_description: str = pydantic.Field(
        description="The suggested description of the entity."
    )

    column_descriptions: dict[str, str] = pydantic.Field(
        description="The suggested descriptions of the columns."
    )


@router.get("/suggest_description", response_model=SuggestedDescription)
def suggest_description(entity_urn: str) -> SuggestedDescription:
    """Generate an entity description."""

    urn = Urn.from_string(entity_urn)

    if isinstance(urn, QueryUrn):
        query_context = get_query_context(entity_urn)
        desc = generate_query_desc(query_context)

        return SuggestedDescription(
            entity_description=desc,
            column_descriptions={},
        )

    elif _LEGACY_DESCRIPTION_SUGGESTION_ENABLED:
        entity_context = generate_context(entity_urn)
        desc = generate_desc(entity_context, use_flattery=True)

        return SuggestedDescription(
            entity_description=desc,
            column_descriptions={},
        )
    elif isinstance(urn, DatasetUrn):
        raw_column_descriptions, _ = generate_column_descriptions_for_urn(
            graph_client=graph, urn=str(urn)
        )
        table_description, column_descriptions = parse_llm_output(
            raw_column_descriptions
        )
        if column_descriptions is None:
            raise ValueError(
                "Failed to parse structured output from raw output: "
                f"{raw_column_descriptions}"
            )

        return SuggestedDescription(
            entity_description=table_description or "",
            column_descriptions=column_descriptions,
        )

    else:
        raise ValueError(f"Unsupported entity type: {urn}")
