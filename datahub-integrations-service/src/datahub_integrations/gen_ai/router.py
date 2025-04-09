import functools
from datetime import timedelta
from typing import Annotated, List

import fastapi
import pydantic
import tenacity
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, QueryUrn, Urn
from loguru import logger

from datahub_integrations.gen_ai.cached_graph import make_cached_graph
from datahub_integrations.gen_ai.description_v2 import (
    ShellEntityError,
    generate_entity_descriptions_for_urn,
)
from datahub_integrations.gen_ai.suggest_query_description import (
    generate_query_desc,
    get_query_context,
)
from datahub_integrations.gen_ai.term_suggestion_v2 import (
    TERM_SUGGESTION_CONFIDENCE_THRESHOLD,
    TermSuggestionBundle,
    get_term_recommendations,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import (
    GlossaryInfo,
    GlossaryUniverseConfig,
    fetch_glossary_info,
)

_METADATA_CACHE_TTL_SEC = timedelta(minutes=15).total_seconds()

router = fastapi.APIRouter()


@functools.cache
def cached_graph() -> DataHubGraph:
    # Using FastAPI dependency injection to provide the cached graph. This keeps
    # the API endpoint methods generic. It also lets us call them directly as well,
    # which is useful for scripts and the action runner.

    # Wrapping it with the functools.cache decorator ensures that we only
    # have one instance of the cached graph, so that it actually caches.

    # By putting the import inside the function, we can avoid requiring the graph
    # for cases where this file is used outside of the FastAPI app.
    from datahub_integrations.app import graph as uncached_graph

    return make_cached_graph(uncached_graph, ttl_sec=_METADATA_CACHE_TTL_SEC)


class SuggestedDescription(pydantic.BaseModel):
    """A suggested entity description."""

    entity_description: str = pydantic.Field(
        description="The suggested description of the entity."
    )

    column_descriptions: dict[str, str] = pydantic.Field(
        description="The suggested descriptions of the columns."
    )


class DescriptionV2ParsingError(Exception):
    pass


@tenacity.retry(
    stop=tenacity.stop_after_attempt(2),
    retry=tenacity.retry_if_exception_type(DescriptionV2ParsingError),
)
def _description_v2(graph: DataHubGraph, urn: DatasetUrn) -> SuggestedDescription:
    result = generate_entity_descriptions_for_urn(graph_client=graph, urn=str(urn))
    if result.column_descriptions is None:
        raise DescriptionV2ParsingError(
            f"Failed to parse structured output from raw output: {result.raw_llm_output}"
        )

    return SuggestedDescription(
        entity_description=result.table_description or "",
        column_descriptions=result.column_descriptions,
    )


@router.get("/suggest_description", response_model=SuggestedDescription)
def suggest_description(
    graph: Annotated[DataHubGraph, fastapi.Depends(cached_graph)],
    entity_urn: str,
) -> SuggestedDescription:
    """Generate an entity description."""

    urn = Urn.from_string(entity_urn)

    if isinstance(urn, QueryUrn):
        query_context = get_query_context(graph, entity_urn)
        desc = generate_query_desc(query_context)

        return SuggestedDescription(
            entity_description=desc,
            column_descriptions={},
        )

    elif isinstance(urn, DatasetUrn):
        return _description_v2(graph, urn)

    else:
        raise ValueError(f"Unsupported entity type: {urn}")


class SuggestedTerm(pydantic.BaseModel):
    urn: str

    # A confidence score for this suggestion, from 1-10 with 10 being the highest confidence.
    confidence_score: float


class SuggestedTerms(pydantic.BaseModel):
    table_terms: list[SuggestedTerm] | None
    column_terms: dict[str, list[SuggestedTerm]] | None


def _extract_recommendations(recs: list[TermSuggestionBundle]) -> list[SuggestedTerm]:
    terms = []
    for rec in recs:
        if rec.is_fake:
            continue
        if rec.confidence_score < TERM_SUGGESTION_CONFIDENCE_THRESHOLD:
            continue
        terms.append(
            SuggestedTerm(
                urn=rec.urn,
                confidence_score=rec.confidence_score,
            )
        )

    return terms


def _suggest_terms(
    graph: DataHubGraph, entity_urn: str, glossary_info: GlossaryInfo
) -> SuggestedTerms:
    table_terms, column_terms, _raw_llm_response = get_term_recommendations(
        table_urn=entity_urn, graph_client=graph, glossary_info=glossary_info
    )
    if table_terms is None and column_terms is None:
        logger.debug(f"No terms returned for {entity_urn}: {_raw_llm_response}")
    else:
        logger.debug(f"table_terms: {table_terms}")
        logger.debug(f"column_terms: {column_terms}")

    return SuggestedTerms(
        table_terms=_extract_recommendations(table_terms) if table_terms else None,
        column_terms=(
            {
                column_name: _extract_recommendations(single_column_terms)
                for column_name, single_column_terms in column_terms.items()
            }
            if column_terms
            else None
        ),
    )


@router.post("/suggest_terms", response_model=SuggestedTerms)
def suggest_terms(
    graph: Annotated[DataHubGraph, fastapi.Depends(cached_graph)],
    entity_urn: str,
    universe_config: GlossaryUniverseConfig,
) -> SuggestedTerms:
    glossary_info = fetch_glossary_info(graph_client=graph, universe=universe_config)

    return _suggest_terms(graph, entity_urn, glossary_info)


@router.post("/suggest_terms_batch", response_model=dict[str, SuggestedTerms])
def suggest_terms_batch(
    graph: Annotated[DataHubGraph, fastapi.Depends(cached_graph)],
    entity_urns: List[str],
    universe_config: GlossaryUniverseConfig,
) -> dict[str, SuggestedTerms]:
    glossary_info = fetch_glossary_info(graph_client=graph, universe=universe_config)

    last_exception: Exception | None = None
    results = {}
    for entity_urn in entity_urns:
        try:
            results[entity_urn] = _suggest_terms(graph, entity_urn, glossary_info)
        except ShellEntityError as e:
            last_exception = last_exception or e
            logger.debug(f"Skipping shell entity: {e}")
        except Exception as e:
            last_exception = e
            logger.exception(f"Failed to suggest terms for {entity_urn}: {e}")

    if entity_urns and not results and last_exception:
        # If all of the calls fail, reraise one of the exceptions so we don't just
        # silently fail.
        raise last_exception

    return results
