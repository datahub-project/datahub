"""
Search routes - Proxy to DataHub search GraphQL and explain REST APIs.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    SearchConfigurationModel,
    SearchRequest,
    SearchResponse,
    SignalConfigModel,
    SignalNormalizationModel,
    Stage1ConfigurationModel,
    Stage1PresetModel,
    Stage2ConfigurationModel,
    Stage2PresetModel,
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

# GraphQL query matching production UI - extended with ranking-relevant fields
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
            customProperties {
              key
              value
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
                properties {
                  name
                }
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                properties {
                  name
                }
              }
            }
          }
          domain {
            domain {
              urn
              properties {
                name
              }
            }
          }
          subTypes {
            typeNames
          }
          browsePathV2 {
            path {
              name
            }
          }
        }
        ... on Dashboard {
          dashboardId
          properties {
            name
            description
            customProperties {
              key
              value
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
                properties {
                  name
                }
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                properties {
                  name
                }
              }
            }
          }
          domain {
            domain {
              urn
              properties {
                name
              }
            }
          }
          subTypes {
            typeNames
          }
          browsePathV2 {
            path {
              name
            }
          }
        }
        ... on Chart {
          chartId
          properties {
            name
            description
            customProperties {
              key
              value
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
                properties {
                  name
                }
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                properties {
                  name
                }
              }
            }
          }
          domain {
            domain {
              urn
              properties {
                name
              }
            }
          }
          subTypes {
            typeNames
          }
          browsePathV2 {
            path {
              name
            }
          }
        }
        ... on DataJob {
          jobId
          properties {
            name
            description
            customProperties {
              key
              value
            }
          }
          tags {
            tags {
              tag {
                urn
                properties {
                  name
                }
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                properties {
                  name
                }
              }
            }
          }
          domain {
            domain {
              urn
              properties {
                name
              }
            }
          }
          subTypes {
            typeNames
          }
          browsePathV2 {
            path {
              name
            }
          }
        }
        ... on DataFlow {
          flowId
          properties {
            name
            description
            customProperties {
              key
              value
            }
          }
          tags {
            tags {
              tag {
                urn
                properties {
                  name
                }
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                properties {
                  name
                }
              }
            }
          }
          domain {
            domain {
              urn
              properties {
                name
              }
            }
          }
          subTypes {
            typeNames
          }
          browsePathV2 {
            path {
              name
            }
          }
        }
      }
      matchedFields {
        name
        value
      }
      score
      extraProperties {
        name
        value
      }
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
        search_flags = {
            "fulltext": True,
            "getSuggestions": False,
            "includeStructuredPropertyFacets": False,
            "includeExplain": True,
            "searchType": request.searchType,
        }

        # Add functionScoreOverride if provided (for debugging Stage 1)
        if request.functionScoreOverride:
            try:
                json.loads(request.functionScoreOverride)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid JSON in functionScoreOverride: {e}")
            search_flags["functionScoreOverride"] = request.functionScoreOverride
            logger.info(f"Using Stage 1 function score override: {request.functionScoreOverride[:100]}...")

        # Add rescoreEnabled if provided (for enabling/disabling Stage 2)
        if request.rescoreEnabled is not None:
            search_flags["rescoreEnabled"] = request.rescoreEnabled
            logger.info(f"Stage 2 rescore enabled: {request.rescoreEnabled}")

        # Add rescoreFormulaOverride if provided
        if request.rescoreFormulaOverride:
            search_flags["rescoreFormulaOverride"] = request.rescoreFormulaOverride

        # Add rescoreSignalsOverride if provided
        if request.rescoreSignalsOverride:
            try:
                json.loads(request.rescoreSignalsOverride)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid JSON in rescoreSignalsOverride: {e}")
            search_flags["rescoreSignalsOverride"] = request.rescoreSignalsOverride

        variables = {
            "input": {
                "types": request.types,
                "query": request.query,
                "start": request.start,
                "count": request.count,
                "filters": [],
                "searchFlags": search_flags,
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
            if "properties" in entity_data and entity_data["properties"] is not None:
                properties = dict(entity_data["properties"])
            if "platform" in entity_data and entity_data["platform"] is not None:
                properties["platform"] = entity_data["platform"]["name"]

            # Extract tags
            tags = _extract_tags(entity_data)

            # Extract glossary terms
            glossary_terms = _extract_glossary_terms(entity_data)

            # Extract domain
            domain = _extract_domain(entity_data)

            # Extract subTypes
            sub_types = _extract_sub_types(entity_data)

            # Extract browse path
            browse_path = _extract_browse_path(entity_data)

            # Extract custom properties
            custom_properties = _extract_custom_properties(entity_data)
            editable_properties = _extract_editable_properties(entity_data)

            # Extract explanation if present
            explanation = _extract_explanation(item)

            # Extract rescore explanation if present (Stage 2)
            rescore_explanation = _extract_rescore_explanation(item)

            # Build entity
            entity = {
                "urn": entity_data.get("urn", ""),
                "type": entity_data.get("type", ""),
                "name": _extract_entity_name(entity_data),
                "properties": properties,
                "editableProperties": editable_properties,
                "tags": tags,
                "glossaryTerms": glossary_terms,
                "domain": domain,
                "subTypes": sub_types,
                "browsePath": browse_path,
                "customProperties": custom_properties,
            }

            # Build matched fields
            matched_fields = [
                {"name": f["name"], "value": f["value"]}
                for f in item.get("matchedFields", [])
            ]

            # Build result
            result_dict = {
                "entity": entity,
                "matchedFields": matched_fields,
                "score": item.get("score"),
            }

            # Include explanation only if present
            if explanation:
                result_dict["explanation"] = explanation

            # Include rescore explanation only if present (Stage 2)
            if rescore_explanation:
                result_dict["rescoreExplanation"] = rescore_explanation

            search_results.append(result_dict)

        return SearchResponse(
            start=search_data.get("start", 0),
            count=search_data.get("count", 0),
            total=search_data.get("total", 0),
            searchResults=search_results,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/explain", response_model=ExplainResponse)
async def explain(
    request: ExplainRequest, graph: DataHubGraph = Depends(get_datahub_graph)
) -> ExplainResponse:
    """
    Get explain information for a specific search result.

    TODO: This endpoint is currently broken - the explainSearchQueryWithGlobalIdf endpoint
    has been removed. This should be updated to use the standard search API with
    includeExplain=true flag instead. The explain data will be available in the
    search response's extraFields._explain field.

    See: SearchFlags.includeExplain in metadata-models/src/main/pegasus/com/linkedin/metadata/query/SearchFlags.pdl
    """
    # This endpoint is currently placeholder as the underlying GMS endpoint was removed
    # The recommended path is to use the main search endpoint with includeExplain=true
    raise HTTPException(
        status_code=501,
        detail="Explain endpoint is deprecated. Use search with includeExplain=true"
    )


def _extract_entity_name(entity_data: Dict[str, Any]) -> str:
    """Extract entity name from various entity types."""
    if "name" in entity_data:
        return entity_data["name"]
    if "properties" in entity_data and entity_data["properties"] and "name" in entity_data["properties"]:
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


def _extract_tags(entity_data: Dict[str, Any]) -> List[str]:
    """Extract tag names from entity."""
    tags = []
    tags_data = entity_data.get("tags") or {}
    for tag_item in tags_data.get("tags") or []:
        if not tag_item:
            continue
        tag = tag_item.get("tag") or {}
        tag_props = tag.get("properties") or {}
        tag_name = tag_props.get("name")
        if tag_name:
            tags.append(tag_name)
        elif tag.get("urn"):
            # Fallback: extract tag name from URN (e.g., "urn:li:tag:dbt:core" -> "dbt:core")
            urn = tag["urn"]
            if urn.startswith("urn:li:tag:"):
                tags.append(urn.replace("urn:li:tag:", ""))
    return tags


def _extract_glossary_terms(entity_data: Dict[str, Any]) -> List[str]:
    """Extract glossary term names from entity."""
    terms = []
    terms_data = entity_data.get("glossaryTerms") or {}
    for term_item in terms_data.get("terms") or []:
        if not term_item:
            continue
        term = term_item.get("term") or {}
        term_props = term.get("properties") or {}
        term_name = term_props.get("name")
        if term_name:
            terms.append(term_name)
        elif term.get("urn"):
            # Fallback: extract term name from URN
            urn = term["urn"]
            if urn.startswith("urn:li:glossaryTerm:"):
                terms.append(urn.replace("urn:li:glossaryTerm:", ""))
    return terms


def _extract_domain(entity_data: Dict[str, Any]) -> Optional[str]:
    """Extract domain name from entity."""
    domain_data = entity_data.get("domain") or {}
    domain = domain_data.get("domain") or {}
    if domain:
        domain_props = domain.get("properties") or {}
        domain_name = domain_props.get("name")
        if domain_name:
            return domain_name
        elif domain.get("urn"):
            # Fallback: extract domain name from URN
            urn = domain["urn"]
            if urn.startswith("urn:li:domain:"):
                return urn.replace("urn:li:domain:", "")
    return None


def _extract_sub_types(entity_data: Dict[str, Any]) -> List[str]:
    """Extract subTypes from entity."""
    sub_types_data = entity_data.get("subTypes") or {}
    return sub_types_data.get("typeNames") or []


def _extract_browse_path(entity_data: Dict[str, Any]) -> List[str]:
    """Extract browse path from entity."""
    browse_path_data = entity_data.get("browsePathV2") or {}
    path = browse_path_data.get("path") or []
    return [p.get("name", "") for p in path if p and p.get("name")]


def _extract_custom_properties(entity_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract custom properties from entity."""
    properties = entity_data.get("properties") or {}
    custom_props = properties.get("customProperties") or []
    return [{"key": p.get("key", ""), "value": p.get("value", "")} for p in custom_props if p]


def _extract_editable_properties(entity_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract editable properties from entity."""
    editable_props = entity_data.get("editableProperties")
    if isinstance(editable_props, dict):
        return dict(editable_props)
    return {}


def _extract_explanation(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract explanation from GraphQL extraProperties.

    Looks for name="_explain" and parses its JSON value.
    """
    extra_props = item.get("extraProperties", [])
    for prop in extra_props:
        if prop.get("name") == "_explain":
            value_str = prop.get("value")
            if value_str:
                try:
                    return json.loads(value_str)
                except json.JSONDecodeError:
                    logger.warning("Failed to parse explanation JSON")
                    return None
    return None


def _extract_rescore_explanation(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract Stage 2 rescore explanation from GraphQL extraProperties.

    Looks for name="_rescoreExplain" and parses its JSON value.
    This contains the Java/exp4j rescore breakdown with signal values.
    """
    extra_props = item.get("extraProperties", [])
    for prop in extra_props:
        if prop.get("name") == "_rescoreExplain":
            value_str = prop.get("value")
            if value_str:
                try:
                    return json.loads(value_str)
                except json.JSONDecodeError:
                    logger.warning("Failed to parse rescore explanation JSON")
                    return None
    return None


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
                "position": item.position,  # Already 1-indexed from frontend
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


@router.get("/configuration", response_model=SearchConfigurationModel)
async def get_search_configuration(
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> SearchConfigurationModel:
    """
    Get complete search configuration including Stage 1 and Stage 2 settings.

    Returns the full configuration used by the search debug UI, including
    function score settings (Stage 1) and rescore configuration (Stage 2).
    """
    try:
        # Stage 1 Configuration (Function Scores)
        stage1_presets = [
            Stage1PresetModel(
                name="Server Default",
                description="Use server configuration (from search_config.yaml or PDL)",
                config="{}",
            ),
            Stage1PresetModel(
                name="Quality Signals Only",
                description="hasDescription +3, hasOwners +2, deprecated -10",
                config=json.dumps({
                    "functions": [
                        {"filter": {"term": {"hasDescription": True}}, "weight": 3.0},
                        {"filter": {"term": {"hasOwners": True}}, "weight": 2.0},
                        {"filter": {"term": {"deprecated": True}}, "weight": -10.0},
                    ],
                    "score_mode": "sum",
                    "boost_mode": "sum",
                }),
            ),
            Stage1PresetModel(
                name="Pure BM25",
                description="No function scores, pure BM25 text relevance",
                config=json.dumps({
                    "functions": [],
                    "score_mode": "sum",
                    "boost_mode": "replace",
                }),
            ),
        ]

        stage1_config = Stage1ConfigurationModel(
            source="pdl",
            functionScore=None,
            presets=stage1_presets,
        )

        # Stage 2 Configuration (Rescore)
        # Default values - in production, these should be fetched from GMS configuration
        stage2_signals = [
            SignalConfigModel(
                name="bm25",
                normalizedName="norm_bm25",
                fieldPath="_score",
                type="SCORE",
                boost=1.0,
                normalization=SignalNormalizationModel(
                    type="SIGMOID",
                    inputMin=0.0,
                    inputMax=500.0,
                    steepness=6.0,
                    outputMin=1.0,
                    outputMax=2.0,
                ),
            ),
            SignalConfigModel(
                name="viewCount",
                normalizedName="norm_views",
                fieldPath="viewCountLast30DaysFeature",
                type="NUMERIC",
                boost=0.8,
                normalization=SignalNormalizationModel(
                    type="SIGMOID",
                    inputMin=0.0,
                    inputMax=1000.0,
                    steepness=6.0,
                    outputMin=1.0,
                    outputMax=2.0,
                ),
            ),
            SignalConfigModel(
                name="queryCount",
                normalizedName="norm_queries",
                fieldPath="queryCountLast30DaysFeature",
                type="NUMERIC",
                boost=0.8,
                normalization=SignalNormalizationModel(
                    type="SIGMOID",
                    inputMin=0.0,
                    inputMax=1000.0,
                    steepness=6.0,
                    outputMin=1.0,
                    outputMax=1.8,
                ),
            ),
            SignalConfigModel(
                name="usageCount",
                normalizedName="norm_usage",
                fieldPath="usageCountLast30DaysFeature",
                type="NUMERIC",
                boost=0.8,
                normalization=SignalNormalizationModel(
                    type="SIGMOID",
                    inputMin=0.0,
                    inputMax=500.0,
                    steepness=6.0,
                    outputMin=1.0,
                    outputMax=1.5,
                ),
            ),
            SignalConfigModel(
                name="uniqueUserCount",
                normalizedName="norm_users",
                fieldPath="uniqueUserCountLast30DaysFeature",
                type="NUMERIC",
                boost=0.9,
                normalization=SignalNormalizationModel(
                    type="SIGMOID",
                    inputMin=0.0,
                    inputMax=20.0,
                    steepness=6.0,
                    outputMin=1.0,
                    outputMax=1.8,
                ),
            ),
            SignalConfigModel(
                name="hasDescription",
                normalizedName="hasDesc",
                fieldPath="hasDescription",
                type="BOOLEAN",
                boost=1.3,
                normalization=SignalNormalizationModel(
                    type="BOOLEAN",
                    trueValue=1.3,
                    falseValue=1.0,
                    outputMin=1.0,
                    outputMax=1.3,
                ),
            ),
            SignalConfigModel(
                name="hasOwners",
                normalizedName="hasOwners",
                fieldPath="hasOwners",
                type="BOOLEAN",
                boost=1.2,
                normalization=SignalNormalizationModel(
                    type="BOOLEAN",
                    trueValue=1.2,
                    falseValue=1.0,
                    outputMin=1.0,
                    outputMax=1.2,
                ),
            ),
            SignalConfigModel(
                name="hasTags",
                normalizedName="hasTags",
                fieldPath="hasTags",
                type="BOOLEAN",
                boost=1.1,
                normalization=SignalNormalizationModel(
                    type="BOOLEAN",
                    trueValue=1.1,
                    falseValue=1.0,
                    outputMin=1.0,
                    outputMax=1.1,
                ),
            ),
            SignalConfigModel(
                name="hasGlossaryTerms",
                normalizedName="hasTerms",
                fieldPath="hasGlossaryTerms",
                type="BOOLEAN",
                boost=1.1,
                normalization=SignalNormalizationModel(
                    type="BOOLEAN",
                    trueValue=1.1,
                    falseValue=1.0,
                    outputMin=1.0,
                    outputMax=1.1,
                ),
            ),
            SignalConfigModel(
                name="lastModified",
                normalizedName="recency",
                fieldPath="lastModified",
                type="TIMESTAMP",
                boost=1.0,
                normalization=SignalNormalizationModel(
                    type="LINEAR_DECAY",
                    scale=180.0,
                    outputMin=0.7,
                    outputMax=1.2,
                ),
            ),
            SignalConfigModel(
                name="deprecated",
                normalizedName="notDeprecated",
                fieldPath="deprecated",
                type="BOOLEAN",
                boost=1.0,
                normalization=SignalNormalizationModel(
                    type="BOOLEAN",
                    trueValue=0.7,
                    falseValue=1.0,
                    outputMin=0.7,
                    outputMax=1.0,
                ),
            ),
            SignalConfigModel(
                name="entityType",
                normalizedName="entityTypeBoost",
                fieldPath="_index",
                type="INDEX_NAME",
                boost=1.0,
                normalization=SignalNormalizationModel(
                    type="NONE",
                ),
            ),
        ]

        stage2_presets = [
            Stage2PresetModel(
                name="Server Default",
                description="Use server configuration (from rescore_config.yaml)",
                config="{}",
            ),
            Stage2PresetModel(
                name="Disabled",
                description="Disable Stage 2 rescoring entirely",
                config=json.dumps({"enabled": False}),
            ),
            Stage2PresetModel(
                name="BM25 + Quality",
                description="Emphasize BM25 and quality signals (description, owners)",
                config=json.dumps({
                    "formula": "pow(norm_bm25, 1.0) * pow(hasDesc, 1.5) * pow(hasOwners, 1.3)",
                    "signals": [
                        {
                            "name": "bm25",
                            "normalizedName": "norm_bm25",
                            "fieldPath": "_score",
                            "type": "SCORE",
                            "boost": 1.0,
                            "normalization": {
                                "type": "SIGMOID",
                                "inputMax": 500,
                                "outputMin": 1.0,
                                "outputMax": 2.0,
                            },
                        },
                        {
                            "name": "hasDescription",
                            "normalizedName": "hasDesc",
                            "fieldPath": "hasDescription",
                            "type": "BOOLEAN",
                            "boost": 1.5,
                            "normalization": {
                                "type": "BOOLEAN",
                                "trueValue": 1.5,
                                "falseValue": 1.0,
                            },
                        },
                        {
                            "name": "hasOwners",
                            "normalizedName": "hasOwners",
                            "fieldPath": "hasOwners",
                            "type": "BOOLEAN",
                            "boost": 1.3,
                            "normalization": {
                                "type": "BOOLEAN",
                                "trueValue": 1.3,
                                "falseValue": 1.0,
                            },
                        },
                    ],
                }),
            ),
        ]

        stage2_config = Stage2ConfigurationModel(
            enabled=True,
            mode="JAVA_EXP4J",
            windowSize=100,  # Default from rescore_config.yaml
            formula="pow(norm_bm25, 1.0) * pow(norm_views, 0.8) * pow(norm_queries, 0.8) * pow(norm_usage, 0.8) * pow(norm_users, 0.9) * pow(hasDesc, 1.3) * pow(recency, 1.0) * pow(notDeprecated, 1.0) * pow(entityTypeBoost, 1.0)",
            signals=stage2_signals,
            presets=stage2_presets,
        )

        return SearchConfigurationModel(
            stage1=stage1_config,
            stage2=stage2_config,
        )

    except Exception as e:
        logger.exception(f"Failed to get search configuration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
