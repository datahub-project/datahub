"""
Search routes - Proxy to DataHub search GraphQL and explain REST APIs.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict

import httpx
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

# Add backend directory to path
backend_dir = Path(__file__).parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# Import from parent directory for datahub SDK
parent_dir = Path(__file__).parent.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from api.dependencies import get_datahub_graph
from api.models import (
    ExplainRequest,
    ExplainResponse,
    RankingAnalysisRequest,
    RankingAnalysisResponse,
    SearchConfigResponse,
    SearchRequest,
    SearchResponse,
)

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore

try:
    from datahub_integrations.gen_ai.llm.bedrock import BedrockLLMWrapper
    from datahub_integrations.observability.metrics_constants import AIModule
except ImportError:
    BedrockLLMWrapper = None  # type: ignore
    AIModule = None  # type: ignore

router = APIRouter(prefix="/api/search", tags=["search"])

# GraphQL query matching production UI
SEARCH_QUERY = """
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          platform {
            name
          }
          properties {
            name
            description
          }
        }
        ... on Dashboard {
          dashboardId
          properties {
            name
            description
          }
        }
        ... on Chart {
          chartId
          properties {
            name
            description
          }
        }
        ... on DataJob {
          jobId
          properties {
            name
            description
          }
        }
        ... on DataFlow {
          flowId
          properties {
            name
            description
          }
        }
      }
      matchedFields {
        name
        value
      }
      score
    }
  }
}
"""


@router.post("", response_model=SearchResponse)
async def search(
    request: SearchRequest, graph: DataHubGraph = Depends(get_datahub_graph)
) -> SearchResponse:
    """
    Execute search query against DataHub.

    This endpoint proxies the request to DataHub's searchAcrossEntities GraphQL query,
    using the same query structure as the production UI.
    """
    try:
        # Build GraphQL variables matching production format
        variables = {
            "input": {
                "types": request.types,
                "query": request.query,
                "start": request.start,
                "count": request.count,
                "filters": [],
                "searchFlags": {
                    "getSuggestions": False,
                    "includeStructuredPropertyFacets": False,
                },
            }
        }

        logger.info(
            f"Executing search query: {request.query} (start={request.start}, count={request.count})"
        )

        # Execute GraphQL query
        result = graph.execute_graphql(SEARCH_QUERY, variables)

        logger.debug(f"GraphQL response keys: {result.keys()}")

        if "errors" in result:
            logger.error(f"GraphQL errors: {result['errors']}")
            raise HTTPException(
                status_code=500, detail=f"GraphQL error: {result['errors']}"
            )

        # Extract search results
        # Note: execute_graphql returns the data directly, not wrapped in a "data" key
        search_data = result.get("searchAcrossEntities", {})
        logger.info(
            f"Search returned {search_data.get('total', 0)} total results, "
            f"returning {len(search_data.get('searchResults', []))} results"
        )

        if search_data.get("total", 0) == 0:
            logger.warning(
                f"No results found for query '{request.query}' - this may indicate the DataHub instance has no matching entities"
            )

        # Transform to response model
        search_results = []
        for item in search_data.get("searchResults", []):
            entity_data = item.get("entity", {})

            # Extract entity properties based on type
            properties = {}
            if "properties" in entity_data:
                properties = entity_data["properties"]
            if "platform" in entity_data:
                properties["platform"] = entity_data["platform"]["name"]

            # Build entity
            entity = {
                "urn": entity_data.get("urn", ""),
                "type": entity_data.get("type", ""),
                "name": _extract_entity_name(entity_data),
                "properties": properties,
            }

            # Build matched fields
            matched_fields = [
                {"name": f["name"], "value": f["value"]}
                for f in item.get("matchedFields", [])
            ]

            search_results.append(
                {
                    "entity": entity,
                    "matchedFields": matched_fields,
                    "score": item.get("score"),
                }
            )

        return SearchResponse(
            start=search_data.get("start", 0),
            count=search_data.get("count", 0),
            total=search_data.get("total", 0),
            searchResults=search_results,
        )

    except Exception as e:
        logger.exception(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/explain", response_model=ExplainResponse)
async def explain(
    request: ExplainRequest, graph: DataHubGraph = Depends(get_datahub_graph)
) -> ExplainResponse:
    """
    Get explain information for a specific search result.

    This endpoint calls DataHub's explain REST API to get scoring details.
    """
    try:
        # Get GMS URL from graph client config
        gms_url = graph.config.server
        token = graph.config.token

        # Build explain endpoint URL
        explain_url = f"{gms_url}/openapi/operations/elasticSearch/explainSearchQuery"

        # Build query parameters
        params = {
            "query": request.query,
            "documentId": request.documentId,
            "entityName": request.entityName,
        }

        # Make HTTP request to explain endpoint
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                explain_url, params=params, headers=headers, timeout=30.0
            )
            response.raise_for_status()

            data = response.json()

            # Extract matched status from explanation object
            explanation = data.get("explanation", {})
            matched = explanation.get("match", False)

            return ExplainResponse(
                index=data.get("index", ""),
                documentId=data.get("id", request.documentId),
                matched=matched,
                score=explanation.get("value"),
                explanation=explanation,
            )

    except httpx.HTTPError as e:
        logger.exception(f"Explain API call failed: {e}")
        raise HTTPException(status_code=500, detail=f"Explain API error: {str(e)}")
    except Exception as e:
        logger.exception(f"Explain failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _extract_entity_name(entity_data: Dict[str, Any]) -> str:
    """Extract entity name from various entity types."""
    if "name" in entity_data:
        return entity_data["name"]
    if "properties" in entity_data and "name" in entity_data["properties"]:
        return entity_data["properties"]["name"]
    if "dashboardId" in entity_data:
        return entity_data["dashboardId"]
    if "chartId" in entity_data:
        return entity_data["chartId"]
    if "jobId" in entity_data:
        return entity_data["jobId"]
    if "flowId" in entity_data:
        return entity_data["flowId"]
    return entity_data.get("urn", "Unknown")


@router.post("/analyze-ranking", response_model=RankingAnalysisResponse)
async def analyze_ranking(
    request: RankingAnalysisRequest, graph: DataHubGraph = Depends(get_datahub_graph)
) -> RankingAnalysisResponse:
    """
    Analyze search ranking using AI.

    Uses Bedrock Claude to explain why selected results ranked as they did,
    based on their scores, positions, and explain data.
    Includes actual DataHub search configuration for precise analysis.
    """
    try:
        if BedrockLLMWrapper is None or AIModule is None:
            raise HTTPException(
                status_code=500,
                detail="Bedrock LLM integration not available",
            )

        # Initialize Bedrock LLM
        llm = BedrockLLMWrapper(
            model_name="anthropic.claude-3-5-sonnet-20240620-v1:0",
            read_timeout=60,
            connect_timeout=60,
        )

        # Fetch search configuration
        search_config = await get_search_config(graph)

        # Build context for Claude with detailed explanations
        results_context = []
        for item in request.results:
            # Parse detailed explanation tree
            detailed_explain = _parse_detailed_explanation(item.explanation)

            result_info = {
                "position": item.position + 1,  # 1-indexed for readability
                "name": item.name,
                "type": item.type,
                "score": item.score,
                "matched": item.matched,
                "explanation_tree": detailed_explain,
            }
            results_context.append(result_info)

        # System prompt with search configuration
        system = [
            {
                "text": "You are a DataHub search relevance expert analyzing Elasticsearch ranking results."
            },
            {
                "text": "You have access to the ACTUAL search configuration including field boost weights and scoring algorithm."
            },
            {
                "text": "Use the provided configuration to give PRECISE explanations of why results ranked as they did."
            },
            {
                "text": "Reference specific field boosts, BM25 scores, and matched fields from the explanation tree."
            },
        ]

        # Build comparative summary
        result_names = [f"Result {i['position']}: {i['name']}" for i in results_context]

        # User prompt with search config and detailed context
        user_prompt = f"""Search Query: "{request.query}"

I have selected {len(results_context)} results to analyze their ranking:
{", ".join(result_names)}

DataHub Search Configuration:
{search_config.config_notes}

Field Boost Weights:
{json.dumps(search_config.field_weights, indent=2)}

Selected Results with Scoring Details:
{json.dumps(results_context, indent=2)}

IMPORTANT: I need a COMPARATIVE ANALYSIS across ALL {len(results_context)} selected results.

Provide your analysis in this structure:

## Ranking Overview
- List all {len(results_context)} results with their positions and scores
- Highlight the score differences between consecutive results

## Field Match Comparison
For each result, create a table showing:
- Which fields matched the query "{request.query}"
- The boost weight for each matched field
- The estimated contribution to the total score

## Ranking Explanation
Compare the results SIDE-BY-SIDE and explain:
1. WHY Result at position X scored higher than Result at position Y
2. Which specific field matches or boost factors caused the ranking difference
3. Use the explanation_tree to identify the exact scoring components

## Key Differences
Summarize the CRITICAL DIFFERENCES that explain the ranking order:
- Did one result have more high-value field matches (name, urn)?
- Did BM25 field length normalization favor one result?
- Were there exact keyword matches vs partial/delimited matches?

Be technical, comparative, and precise. Focus on DIFFERENCES between results, not just individual analysis.

FORMAT YOUR RESPONSE IN CLEAN MARKDOWN:
- Use ## for section headers
- Use tables for field comparisons
- Use **bold** for emphasis
- Use bullet points for lists
- Use code blocks for technical details if needed"""

        messages = [{"role": "user", "content": [{"text": user_prompt}]}]

        logger.info(
            f"Analyzing ranking for query: {request.query} with {len(request.results)} results"
        )

        # Call Bedrock
        response = llm.converse(
            system=system,
            messages=messages,
            ai_module=AIModule.CHAT,
            inferenceConfig={
                "temperature": 0.3,  # More deterministic for analysis
                "maxTokens": 4000,
            },
        )

        # Extract response
        analysis_text = response["output"]["message"]["content"][0]["text"]
        token_usage = response.get("usage", {})

        logger.info(f"Analysis complete. Tokens: {token_usage}")

        return RankingAnalysisResponse(
            analysis=analysis_text,
            model="anthropic.claude-3-5-sonnet-20240620-v1:0",
            tokens=token_usage,
        )

    except Exception as e:
        logger.exception(f"Ranking analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _summarize_explanation(explanation: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key scoring details from Elasticsearch explanation."""
    if not explanation:
        return {}

    return {
        "value": explanation.get("value"),
        "description": explanation.get("description", "")[
            :200
        ],  # Truncate long descriptions
        "match": explanation.get("match", False),
        "details_count": len(explanation.get("details", [])),
    }


def _parse_detailed_explanation(
    explanation: Dict[str, Any], depth: int = 0, max_depth: int = 3
) -> str:
    """
    Recursively parse Elasticsearch explanation into human-readable text.

    Extracts field matches, boost factors, and scoring components.
    """
    if not explanation or depth > max_depth:
        return ""

    lines = []
    description = explanation.get("description", "")
    value = explanation.get("value", 0)

    indent = "  " * depth
    lines.append(f"{indent}{description} = {value:.4f}")

    # Recursively process details
    for detail in explanation.get("details", []):
        sub_explanation = _parse_detailed_explanation(detail, depth + 1, max_depth)
        if sub_explanation:
            lines.append(sub_explanation)

    return "\n".join(lines)


@router.get("/config", response_model=SearchConfigResponse)
async def get_search_config(
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> SearchConfigResponse:
    """
    Get DataHub search configuration including field weights and boosting factors.

    This provides insight into how DataHub ranks search results.
    """
    try:
        # Standard DataHub field weights (from SearchableAnnotation.boostScore)
        field_weights = {
            "urn": 10.0,  # URN exact match
            "urn.delimited": 4.0,  # URN partial match (10.0 * 0.4 urnFactor)
            "name": 10.0,  # Name exact match (KEYWORD type)
            "name.delimited": 4.0,  # Name partial match (10.0 * 0.4)
            "name.ngram": 1.0,  # Name word-gram match
            "description": 1.0,  # Description match (TEXT type)
            "tags": 5.0,  # Tag matches
            "glossaryTerms": 5.0,  # Glossary term matches
            "fieldPaths": 1.0,  # Schema field paths
            "fieldDescriptions": 0.5,  # Schema field descriptions
        }

        config_notes = """
DataHub Search Ranking Configuration:

1. Field Boost Scores:
   - URN (exact): 10.0 - Highest priority for exact URN matches
   - URN (partial): 4.0 - Partial URN matches (delimited)
   - Name (exact): 10.0 - Exact name matches on keyword field
   - Name (partial): 4.0 - Partial name matches (delimited)
   - Name (n-gram): 1.0 - Word-gram matches for fuzzy matching
   - Tags: 5.0 - Matches on assigned tags
   - Glossary Terms: 5.0 - Matches on glossary terms
   - Description: 1.0 - Text matches in descriptions
   - Field Paths: 1.0 - Schema field name matches
   - Field Descriptions: 0.5 - Schema field description matches

2. Query Structure:
   - Bool query with should clauses for each searchable field
   - Exact matches (keyword fields) get higher boost
   - Partial matches (delimited subfields) get 0.4x factor
   - Text matches analyzed with standard analyzer
   - Case-insensitive matching (lowercase normalization)

3. Scoring Algorithm:
   - Elasticsearch BM25 (default)
   - Sum of field-level scores with boosts applied
   - Field length normalization applied
   - Multiple field matches combine additively

4. Special Behavior:
   - URN matches are prioritized highest (boost 10.0)
   - Name matches almost as high as URN (boost 10.0)
   - Structured metadata (tags, terms) > unstructured (descriptions)
   - Exact keyword matches score higher than analyzed text matches
"""

        return SearchConfigResponse(
            field_weights=field_weights, config_notes=config_notes
        )

    except Exception as e:
        logger.exception(f"Failed to get search config: {e}")
        raise HTTPException(status_code=500, detail=str(e))
