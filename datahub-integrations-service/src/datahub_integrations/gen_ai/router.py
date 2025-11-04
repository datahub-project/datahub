import functools
from datetime import datetime, timedelta, timezone
from typing import Annotated, Dict, List, Optional

import fastapi
import pydantic
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, QueryUrn, Urn
from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.gen_ai.cached_graph import make_cached_graph
from datahub_integrations.gen_ai.description_context import (
    ShellEntityError,
)
from datahub_integrations.gen_ai.description_v3 import (
    generate_entity_descriptions_for_urn,
)
from datahub_integrations.gen_ai.embeddings import create_embeddings, get_model_info
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
from datahub_integrations.telemetry.ai_docs_events import (
    InferDocsApiRequestEvent,
    InferDocsApiResponseEvent,
)
from datahub_integrations.telemetry.telemetry import track_saas_event

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

    column_descriptions: Dict[str, str] = pydantic.Field(
        description="The suggested descriptions of the columns."
    )


@router.get("/suggest_description", response_model=SuggestedDescription)
def suggest_description(
    graph: Annotated[DataHubGraph, fastapi.Depends(cached_graph)],
    entity_urn: str,
    user_urn: Optional[str] = None,
) -> SuggestedDescription:
    """Generate an entity description."""

    urn = Urn.from_string(entity_urn)

    track_saas_event(
        InferDocsApiRequestEvent(
            entity_urn=entity_urn,
            user_urn=user_urn,
            entity_type=QueryUrn.ENTITY_TYPE
            if isinstance(urn, QueryUrn)
            else DatasetUrn.ENTITY_TYPE,
        )
    )

    if isinstance(urn, QueryUrn):
        with PerfTimer() as timer:
            query_context = get_query_context(graph, entity_urn)
            desc = generate_query_desc(graph, query_context)

            track_saas_event(
                InferDocsApiResponseEvent(
                    entity_urn=entity_urn,
                    user_urn=user_urn,
                    entity_type=QueryUrn.ENTITY_TYPE,
                    response_time_ms=timer.elapsed_seconds() * 1000,
                    has_entity_description=desc is not None and len(desc) > 0,
                    entity_description=desc,
                )
            )

        return SuggestedDescription(
            entity_description=desc,
            column_descriptions={},
        )

    elif isinstance(urn, DatasetUrn):
        with PerfTimer() as timer:
            result = generate_entity_descriptions_for_urn(
                graph_client=graph, urn=str(urn)
            )

            track_saas_event(
                InferDocsApiResponseEvent(
                    entity_urn=entity_urn,
                    user_urn=user_urn,
                    entity_type=DatasetUrn.ENTITY_TYPE,
                    response_time_ms=timer.elapsed_seconds() * 1000,
                    has_entity_description=result.table_description is not None
                    and len(result.table_description) > 0,
                    has_column_descriptions=result.column_descriptions is not None
                    and len(result.column_descriptions) > 0,
                    error_msg=result.failure_reason,
                    entity_description=result.table_description,
                    num_columns=result.num_columns,
                    num_columns_with_description=result.num_columns_with_description,
                    metadata_extraction_time_ms=result.metadata_extraction_time_ms,
                )
            )

            return SuggestedDescription(
                entity_description=result.table_description or "",
                column_descriptions=result.column_descriptions or {},
            )

    else:
        raise ValueError(f"Unsupported entity type: {urn}")


class SuggestedTerm(pydantic.BaseModel):
    urn: str

    # A confidence score for this suggestion, from 1-10 with 10 being the highest confidence.
    confidence_score: float


class SuggestedTerms(pydantic.BaseModel):
    table_terms: list[SuggestedTerm] | None = None
    column_terms: dict[str, list[SuggestedTerm]] | None = None


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
    graph: DataHubGraph,
    entity_urn: str,
    glossary_info: GlossaryInfo,
    custom_instructions: str | None = None,
) -> SuggestedTerms:
    table_terms, column_terms, _raw_llm_response = get_term_recommendations(
        table_urn=entity_urn,
        graph_client=graph,
        glossary_info=glossary_info,
        custom_instructions=custom_instructions,
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


def _suggest_terms_batch(
    graph: DataHubGraph,
    entity_urns: List[str],
    glossary_info: GlossaryInfo,
    custom_instructions: str | None = None,
) -> dict[str, SuggestedTerms]:
    """Internal helper that processes multiple URNs with pre-fetched glossary info.

    This avoids fetching glossary info multiple times when processing URNs individually.
    """
    last_exception: Exception | None = None
    results = {}
    for entity_urn in entity_urns:
        try:
            results[entity_urn] = _suggest_terms(
                graph, entity_urn, glossary_info, custom_instructions
            )
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


@router.post("/suggest_terms_batch", response_model=dict[str, SuggestedTerms])
def suggest_terms_batch(
    graph: Annotated[DataHubGraph, fastapi.Depends(cached_graph)],
    entity_urns: List[str],
    universe_config: GlossaryUniverseConfig,
) -> dict[str, SuggestedTerms]:
    glossary_info = fetch_glossary_info(graph_client=graph, universe=universe_config)
    return _suggest_terms_batch(graph, entity_urns, glossary_info)


class QueryEmbeddingRequest(pydantic.BaseModel):
    """Request model for generating query embeddings."""

    text: str = pydantic.Field(
        description="The query text to generate embeddings for",
        min_length=1,
        max_length=131072,  # 128k chars - models will truncate as needed
    )
    model: Optional[str] = pydantic.Field(
        default=None,
        description="The embedding model to use (e.g., 'bedrock:cohere.embed-english-v3'). "
        "If not provided, uses the default configured model.",
    )


class QueryEmbeddingResponse(pydantic.BaseModel):
    """Response model for query embeddings."""

    provider: str = pydantic.Field(
        description="The provider used (e.g., 'bedrock', 'openai')"
    )
    model: str = pydantic.Field(
        description="The model identifier used for embedding generation"
    )
    embedding: List[float] = pydantic.Field(
        description="The normalized embedding vector"
    )
    dimensionality: int = pydantic.Field(
        description="The dimension of the embedding vector"
    )
    truncated: Optional[bool] = pydantic.Field(
        default=None,
        description="Whether the input text was truncated by the model. "
        "None if truncation status is unknown.",
    )
    created_at: str = pydantic.Field(
        description="ISO 8601 timestamp of when the embedding was generated"
    )


@router.post("/embeddings/query", response_model=QueryEmbeddingResponse)
def embed_query(
    request: QueryEmbeddingRequest,
) -> QueryEmbeddingResponse:
    """
    Generate embeddings for a single query text.

    This endpoint is designed to be called by Java's EmbeddingProvider for semantic search.
    The embeddings are always L2-normalized for cosine similarity scoring.

    Uses Bedrock with Cohere embedding models (or other supported Bedrock models).
    Raises an exception if Bedrock is unavailable or authentication fails.
    """

    logger.debug(f"Generating embeddings for query text of length {len(request.text)}")

    # Determine model (use default if not specified)
    model = request.model or "cohere.embed-english-v3"
    provider = "bedrock"  # Hardcoded to bedrock

    # Create Bedrock embeddings with model from request
    embeddings = create_embeddings(
        provider=provider,
        model_id=model,
    )

    # Generate embedding
    embedding = embeddings.embed_query(request.text)

    # Check for truncation based on model's max tokens
    model_info = get_model_info(model)
    max_chars = (
        model_info.get("max_tokens", 512) * 4
    )  # Rough char estimate (4 chars per token)
    truncated = len(request.text) > max_chars
    dimensionality = len(embedding)

    return QueryEmbeddingResponse(
        provider=provider,
        model=model,
        embedding=embedding,
        dimensionality=dimensionality,
        truncated=truncated,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
