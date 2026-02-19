"""
Search Debug API - Programmatic access to search debugging capabilities.

This module provides API endpoints for:
- Full debug search with 3-stage explanation breakdown
- Ranking validation against expected results
- Configuration comparison between baseline and experiment
- Signal and configuration inspection

Architecture:
- All data comes from GMS via GraphQL (extraProperties._explain, _rescoreExplain)
- This service adds: formatting, aggregation, comparison, validation
- No changes required to GMS or Elasticsearch
"""

import json
import time
from typing import Any, Dict, List, Optional

from api.dependencies import get_datahub_graph
from api.models import (
    AssertionResult,
    AssertionType,
    CompareRequest,
    CompareResponse,
    CompareSearchConfig,
    ComparisonMetrics,
    DebugConfigResponse,
    DebugConfiguration,
    DebugResultItem,
    DebugScores,
    DebugSummary,
    DebugTiming,
    EntityMetadata,
    MatchedField,
    PositionChange,
    SearchDebugRequest,
    SearchDebugResponse,
    SignalDebugInfo,
    SignalExtractionSummary,
    SignalImpact,
    SignalInfo,
    SignalNormalizationInfo,
    SignalsResponse,
    Stage2DebugInfo,
    Stage3DebugInfo,
    ValidateRequest,
    ValidateResponse,
    ValidationAssertion,
)
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

try:
    from datahub.ingestion.graph.client import DataHubGraph
except ImportError:
    DataHubGraph = None  # type: ignore

router = APIRouter(prefix="/api/search/debug", tags=["search-debug"])

# GraphQL query for debug search - includes comprehensive entity metadata
DEBUG_SEARCH_QUERY = """
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
          platform { name }
          properties { name description qualifiedName }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
          statsSummary { viewCount queryCountLast30Days uniqueUserCountLast30Days }
        }
        ... on Dashboard {
          dashboardId
          platform { name }
          properties { name description }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
          statsSummary { viewCount queryCountLast30Days uniqueUserCountLast30Days }
        }
        ... on Chart {
          chartId
          platform { name }
          properties { name description }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
          statsSummary { viewCount queryCountLast30Days uniqueUserCountLast30Days }
        }
        ... on DataJob {
          jobId
          platform { name }
          properties { name description }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
        }
        ... on DataFlow {
          flowId
          platform { name }
          properties { name description }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
        }
        ... on GlossaryTerm {
          name
          properties { name description }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          domain { domain { properties { name } } }
        }
        ... on Container {
          properties { name description }
          platform { name }
          deprecation { deprecated }
          ownership { owners { owner { ... on CorpUser { username } ... on CorpGroup { name } } } }
          globalTags { tags { tag { properties { name } } } }
          glossaryTerms { terms { term { properties { name } } } }
          domain { domain { properties { name } } }
        }
      }
      matchedFields { name value }
      score
      extraProperties { name value }
    }
  }
}
"""


@router.post("", response_model=SearchDebugResponse)
async def debug_search(
    request: SearchDebugRequest,
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> SearchDebugResponse:
    """
    Execute search with full debug information for all 3 stages.

    Returns detailed breakdown of:
    - Stage 1: Elasticsearch BM25 + function scores
    - Stage 2: Java/exp4j rescore with signal breakdown
    - Stage 3: Power-of-10 boost for rescored results
    """
    start_time = time.time()

    try:
        # Build GraphQL variables
        search_flags = {
            "fulltext": True,
            "getSuggestions": False,
            "includeExplain": True,  # Always include for debug
            "searchType": request.searchType,
        }

        if request.functionScoreOverride:
            try:
                json.loads(request.functionScoreOverride)
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid JSON in functionScoreOverride: {e}",
                ) from e
            search_flags["functionScoreOverride"] = request.functionScoreOverride

        if request.rescoreEnabled is not None:
            search_flags["rescoreEnabled"] = request.rescoreEnabled

        if request.rescoreFormulaOverride:
            search_flags["rescoreFormulaOverride"] = request.rescoreFormulaOverride

        if request.rescoreSignalsOverride:
            try:
                json.loads(request.rescoreSignalsOverride)
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid JSON in rescoreSignalsOverride: {e}",
                ) from e
            search_flags["rescoreSignalsOverride"] = request.rescoreSignalsOverride

        # Legacy compatibility: accept a single rescoreOverride blob and map known keys.
        if request.rescoreOverride:
            try:
                legacy_override = json.loads(request.rescoreOverride)
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid JSON in rescoreOverride: {e}",
                ) from e

            if isinstance(legacy_override, dict):
                legacy_formula = legacy_override.get("formula")
                legacy_signals = legacy_override.get("signals")

                if (
                    legacy_formula
                    and "rescoreFormulaOverride" not in search_flags
                ):
                    search_flags["rescoreFormulaOverride"] = legacy_formula
                if (
                    legacy_signals is not None
                    and "rescoreSignalsOverride" not in search_flags
                ):
                    search_flags["rescoreSignalsOverride"] = json.dumps(legacy_signals)

                # Keep legacy pass-through for backward compatibility when extra keys exist.
                legacy_keys = set(legacy_override.keys())
                if legacy_keys - {"formula", "signals"}:
                    search_flags["rescoreOverride"] = request.rescoreOverride
            else:
                search_flags["rescoreOverride"] = request.rescoreOverride

        variables = {
            "input": {
                "types": request.types,
                "query": request.query,
                "start": 0,
                "count": request.count,
                "filters": [],
                "searchFlags": search_flags,
            }
        }

        logger.info(f"Debug search: query='{request.query}', count={request.count}")

        # Execute GraphQL
        gms_start = time.time()
        result = graph.execute_graphql(DEBUG_SEARCH_QUERY, variables)
        gms_time = (time.time() - gms_start) * 1000

        if "errors" in result:
            logger.error(f"GraphQL errors: {result['errors']}")
            raise HTTPException(
                status_code=500, detail="GraphQL error from search backend"
            )

        search_data = result.get("searchAcrossEntities", {})
        total_results = search_data.get("total", 0)
        raw_results = search_data.get("searchResults", [])

        # Process results into debug format
        debug_results = []
        stage1_scores = []
        final_scores = []
        signal_data: Dict[str, List[SignalDebugInfo]] = {}
        rescore_window_used = 0

        for idx, item in enumerate(raw_results):
            debug_item = _process_debug_result(
                item,
                idx + 1,
                request.includeStage1Explain,
                request.includeStage2Explain,
            )
            debug_results.append(debug_item)

            stage1_scores.append(debug_item.scores.stage1Score)
            final_scores.append(debug_item.scores.finalScore)

            # Collect signal data for summary
            if debug_item.stage2Explanation:
                rescore_window_used += 1
                for signal in debug_item.stage2Explanation.signals:
                    if signal.name not in signal_data:
                        signal_data[signal.name] = []
                    signal_data[signal.name].append(signal)

        # Build summary
        processing_time = (time.time() - start_time) * 1000 - gms_time
        total_time = (time.time() - start_time) * 1000

        signal_stats = _compute_signal_stats(signal_data)

        # Determine configuration from first result's rescore explanation
        config = _extract_configuration(debug_results)

        return SearchDebugResponse(
            query=request.query,
            totalResults=total_results,
            returnedResults=len(debug_results),
            timing=DebugTiming(
                totalMs=round(total_time, 2),
                gmsCallMs=round(gms_time, 2),
                processingMs=round(processing_time, 2),
            ),
            configuration=config,
            results=debug_results,
            summary=DebugSummary(
                avgStage1Score=round(sum(stage1_scores) / len(stage1_scores), 3)
                if stage1_scores
                else 0,
                avgFinalScore=round(sum(final_scores) / len(final_scores), 3)
                if final_scores
                else 0,
                rescoreWindowUsed=rescore_window_used,
                signalStats=signal_stats,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Debug search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/validate", response_model=ValidateResponse)
async def validate_ranking(
    request: ValidateRequest,
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> ValidateResponse:
    """
    Validate search results against expected rankings.

    Supports assertions for:
    - Rank expectations with tolerance
    - Score thresholds
    - Signal presence
    - Relative ordering
    """
    try:
        # Execute debug search to get results
        debug_request = SearchDebugRequest(
            query=request.query,
            count=request.count,
            types=request.types,
            includeStage1Explain=False,
            includeStage2Explain=True,
        )
        debug_response = await debug_search(debug_request, graph)

        # Build URN to result mapping
        urn_to_result: Dict[str, DebugResultItem] = {}
        urn_to_rank: Dict[str, int] = {}
        for result in debug_response.results:
            urn_to_result[result.urn] = result
            urn_to_rank[result.urn] = result.rank

        assertion_results: List[AssertionResult] = []

        # Check rank expectations
        for expectation in request.expectations:
            actual_rank = urn_to_rank.get(expectation.urn)
            if actual_rank is None:
                assertion_results.append(
                    AssertionResult(
                        type="RANK_EXPECTATION",
                        urn=expectation.urn,
                        passed=False,
                        expected=expectation.expectedRank,
                        actual=None,
                        tolerance=expectation.tolerance,
                        message=f"Entity not found in top {request.count} results",
                    )
                )
            else:
                diff = abs(actual_rank - expectation.expectedRank)
                passed = diff <= expectation.tolerance
                assertion_results.append(
                    AssertionResult(
                        type="RANK_EXPECTATION",
                        urn=expectation.urn,
                        passed=passed,
                        expected=expectation.expectedRank,
                        actual=actual_rank,
                        tolerance=expectation.tolerance,
                        message=(
                            f"Rank {actual_rank} matches expectation"
                            if passed
                            else f"Rank {actual_rank} outside tolerance (expected {expectation.expectedRank} ± {expectation.tolerance})"
                        ),
                    )
                )

        # Check additional assertions
        for assertion in request.assertions:
            result = _evaluate_assertion(assertion, urn_to_result, urn_to_rank)
            assertion_results.append(result)

        passed_count = sum(1 for r in assertion_results if r.passed)
        failed_count = len(assertion_results) - passed_count

        return ValidateResponse(
            passed=failed_count == 0,
            totalAssertions=len(assertion_results),
            passedAssertions=passed_count,
            failedAssertions=failed_count,
            results=assertion_results,
            searchResults=debug_response.results,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/compare", response_model=CompareResponse)
async def compare_searches(
    request: CompareRequest,
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> CompareResponse:
    """
    Compare search results between baseline and experiment configurations.

    Returns:
    - Ranking correlation metrics (Kendall's tau, Spearman's rho)
    - Position changes for each entity
    - Signal impact analysis
    """
    try:
        # Execute both searches
        baseline_results = await _execute_comparison_search(
            request.baseline, request.count, request.types, graph
        )
        experiment_results = await _execute_comparison_search(
            request.experiment, request.count, request.types, graph
        )

        # Build URN to rank mappings
        baseline_ranks = {r.urn: r.rank for r in baseline_results}
        experiment_ranks = {r.urn: r.rank for r in experiment_results}
        baseline_scores = {r.urn: r.scores.finalScore for r in baseline_results}
        experiment_scores = {r.urn: r.scores.finalScore for r in experiment_results}

        # Find common URNs for correlation
        common_urns = set(baseline_ranks.keys()) & set(experiment_ranks.keys())
        common_urns_sorted = sorted(common_urns)
        baseline_urns = set(baseline_ranks.keys())
        experiment_urns = set(experiment_ranks.keys())

        # Compute ranking correlation
        if len(common_urns_sorted) >= 2:
            baseline_common = [baseline_ranks[u] for u in common_urns_sorted]
            experiment_common = [experiment_ranks[u] for u in common_urns_sorted]
            kendall_tau = _kendall_tau(baseline_common, experiment_common)
            spearman_rho = _spearman_rho(baseline_common, experiment_common)
        else:
            kendall_tau = 0.0
            spearman_rho = 0.0

        # Compute position changes
        position_changes = []
        unchanged = 0
        for urn in common_urns_sorted:
            b_rank = baseline_ranks[urn]
            e_rank = experiment_ranks[urn]
            change = b_rank - e_rank  # Positive = improved in experiment

            b_result = next((r for r in baseline_results if r.urn == urn), None)
            name = b_result.name if b_result else None

            if change != 0:
                position_changes.append(
                    PositionChange(
                        urn=urn,
                        name=name,
                        baselineRank=b_rank,
                        experimentRank=e_rank,
                        change=change,
                        baselineScore=baseline_scores[urn],
                        experimentScore=experiment_scores[urn],
                    )
                )
            else:
                unchanged += 1

        # Sort by absolute change
        position_changes.sort(key=lambda x: abs(x.change), reverse=True)

        # Compute signal impact
        signal_impact = _compute_signal_impact(baseline_results, experiment_results)

        return CompareResponse(
            baseline=request.baseline,
            experiment=request.experiment,
            comparison=ComparisonMetrics(
                kendallTau=round(kendall_tau, 4),
                spearmanRho=round(spearman_rho, 4),
                positionChanges=position_changes,
                newInTopK=list(experiment_urns - baseline_urns),
                droppedFromTopK=list(baseline_urns - experiment_urns),
                unchanged=unchanged,
            ),
            signalImpact=signal_impact,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Comparison failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/signals", response_model=SignalsResponse)
async def get_signals(
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> SignalsResponse:
    """
    Get available signals and their configurations.

    Note: Signal configuration comes from GMS rescore_config.yaml.
    This endpoint returns the static configuration.
    """
    # Static signal definitions based on rescore_config.yaml
    # In a full implementation, this could be fetched from GMS
    signals = [
        SignalInfo(
            name="bm25",
            normalizedName="norm_bm25",
            fieldPath="_score",
            type="SCORE",
            description="Elasticsearch BM25 text relevance score",
            normalization=SignalNormalizationInfo(
                type="SIGMOID",
                inputMin=0.0,
                inputMax=500.0,
                steepness=6.0,
                outputMin=1.0,
                outputMax=2.0,
            ),
            boost=1.0,
        ),
        SignalInfo(
            name="viewCount",
            normalizedName="norm_views",
            fieldPath="viewCount",
            type="NUMERIC",
            description="Number of times entity was viewed",
            normalization=SignalNormalizationInfo(
                type="SIGMOID",
                inputMin=0.0,
                inputMax=1000.0,
                steepness=6.0,
                outputMin=1.0,
                outputMax=2.0,
            ),
            boost=0.8,
        ),
        SignalInfo(
            name="queryCount",
            normalizedName="norm_queries",
            fieldPath="statsSummary.queryCount",
            type="NUMERIC",
            description="Number of queries against this entity",
            normalization=SignalNormalizationInfo(
                type="SIGMOID",
                inputMin=0.0,
                inputMax=1000.0,
                steepness=6.0,
                outputMin=1.0,
                outputMax=1.8,
            ),
            boost=0.8,
        ),
        SignalInfo(
            name="hasDescription",
            normalizedName="hasDesc",
            fieldPath="hasDescription",
            type="BOOLEAN",
            description="Whether entity has a description",
            normalization=SignalNormalizationInfo(
                type="BOOLEAN",
                outputMin=1.0,
                outputMax=1.3,
                trueValue=1.3,
                falseValue=1.0,
            ),
            boost=1.3,
        ),
        SignalInfo(
            name="hasOwners",
            normalizedName="hasOwners",
            fieldPath="hasOwners",
            type="BOOLEAN",
            description="Whether entity has assigned owners",
            normalization=SignalNormalizationInfo(
                type="BOOLEAN",
                outputMin=1.0,
                outputMax=1.2,
                trueValue=1.2,
                falseValue=1.0,
            ),
            boost=1.2,
        ),
        SignalInfo(
            name="deprecated",
            normalizedName="notDeprecated",
            fieldPath="deprecated",
            type="BOOLEAN",
            description="Whether entity is deprecated (penalty signal)",
            normalization=SignalNormalizationInfo(
                type="BOOLEAN",
                outputMin=0.7,
                outputMax=1.0,
                trueValue=0.7,
                falseValue=1.0,
            ),
            boost=1.0,
        ),
    ]

    formula = (
        "pow(norm_bm25, 1.0) * pow(norm_views, 0.8) * pow(norm_queries, 0.8) * "
        "pow(hasDesc, 1.3) * pow(hasOwners, 1.2) * pow(notDeprecated, 1.0)"
    )

    return SignalsResponse(
        signals=signals,
        formula=formula,
        normalizationTypes=[
            "SIGMOID",
            "LOG_SIGMOID",
            "LINEAR_DECAY",
            "BOOLEAN",
            "NONE",
        ],
    )


@router.get("/config", response_model=DebugConfigResponse)
async def get_debug_config(
    graph: DataHubGraph = Depends(get_datahub_graph),
) -> DebugConfigResponse:
    """
    Get current search and rescore configuration.
    """
    # Static configuration based on rescore_config.yaml and application.yaml
    return DebugConfigResponse(
        rescore=DebugConfiguration(
            rescoreEnabled=True,
            rescoreWindowSize=100,
            rescoreFormula=(
                "pow(norm_bm25, 1.0) * pow(norm_views, 0.8) * pow(norm_queries, 0.8) * "
                "pow(hasDesc, 1.3) * pow(hasOwners, 1.2) * pow(notDeprecated, 1.0)"
            ),
            signalCount=10,
        ),
        search={
            "maxTermBucketSize": 60,
            "exactMatch": {
                "exclusive": False,
                "exactFactor": 16.0,
                "prefixFactor": 1.1,
            },
            "partial": {
                "urnFactor": 0.5,
                "factor": 0.4,
            },
        },
        fieldWeights={
            "urn": 10.0,
            "name": 10.0,
            "description": 1.0,
            "tags": 5.0,
            "glossaryTerms": 5.0,
            "fieldPaths": 1.0,
        },
    )


# ============================================================================
# Helper Functions
# ============================================================================


def _process_debug_result(
    item: Dict[str, Any],
    rank: int,
    include_stage1: bool,
    include_stage2: bool,
) -> DebugResultItem:
    """Process a raw search result into debug format."""
    entity_data = item.get("entity", {})
    extra_props = item.get("extraProperties") or []

    # Extract entity info
    urn = entity_data.get("urn", "")
    entity_type = entity_data.get("type", "")
    name = _extract_entity_name(entity_data)
    description = _extract_description(entity_data)

    # Extract scores
    final_score = item.get("score", 0.0) or 0.0

    # Extract explanations from extraProperties
    stage1_explain = None
    stage2_explain = None
    rescore_data = None

    for prop in extra_props:
        if prop.get("name") == "_explain" and include_stage1:
            try:
                value = prop.get("value")
                if value:
                    stage1_explain = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                logger.warning(
                    "Failed to parse _explain JSON for urn={} at rank={}",
                    urn,
                    rank,
                )
        elif prop.get("name") == "_rescoreExplain" and include_stage2:
            try:
                value = prop.get("value")
                if value:
                    rescore_data = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                logger.warning(
                    "Failed to parse _rescoreExplain JSON for urn={} at rank={}",
                    urn,
                    rank,
                )

    # Parse Stage 2 explanation
    stage1_score = 0.0
    rescore_value = None
    rescore_boost = None

    if rescore_data:
        stage1_score = rescore_data.get("bm25Score") or 0.0
        rv = rescore_data.get("rescoreValue")
        rescore_value = rv if rv is not None else rescore_data.get("finalScore")
        rescore_boost = rescore_data.get("rescoreBoost") or 0.0

        signals_data = rescore_data.get("signals", {})
        signals = []
        # signals_data may be a dict (keyed by name) or a list from GMS
        if isinstance(signals_data, list):
            items = [(s.get("name", f"signal_{i}"), s) for i, s in enumerate(signals_data) if isinstance(s, dict)]
        elif isinstance(signals_data, dict):
            items = list(signals_data.items())
        else:
            items = []
        for sig_name, sig_info in items:
            if isinstance(sig_info, dict):
                signals.append(
                    SignalDebugInfo(
                        name=sig_info.get("name", sig_name),
                        rawValue=sig_info.get("rawValue"),
                        numericValue=sig_info.get("numericValue", 0.0),
                        normalizedValue=sig_info.get("normalizedValue", 1.0),
                        boost=sig_info.get("boost", 1.0),
                        contribution=sig_info.get("contribution", 1.0),
                    )
                )

        stage2_explain = Stage2DebugInfo(
            formula=rescore_data.get("formula"),
            rescoreValue=rescore_value if rescore_value is not None else 0.0,
            rescoreBoost=rescore_boost if rescore_boost is not None else 0.0,
            signals=signals,
        )
    else:
        # No rescore data - use final score as stage1 score
        stage1_score = final_score

    # Build stage 3 explanation
    stage3_explain = None
    if rescore_boost is not None and rescore_boost > 0:
        stage3_explain = Stage3DebugInfo(
            withinRescoreWindow=True,
            boostApplied=rescore_boost,
            reason=f"Top results get power-of-10 boost ({rescore_boost}) to rank above non-rescored",
        )
    elif rescore_data is None:
        stage3_explain = Stage3DebugInfo(
            withinRescoreWindow=False,
            boostApplied=0.0,
            reason="Outside rescore window - original ES score used",
        )

    # Extract matched fields
    matched_fields = [
        MatchedField(name=f["name"], value=f["value"])
        for f in item.get("matchedFields", [])
    ]

    # Build comprehensive entity metadata
    entity_metadata = _extract_entity_metadata(entity_data, rescore_data, description)

    return DebugResultItem(
        rank=rank,
        urn=urn,
        name=name,
        description=description,
        type=entity_type,
        scores=DebugScores(
            stage1Score=round(stage1_score, 3),
            stage2RescoreValue=round(rescore_value, 3) if rescore_value is not None else None,
            stage2Boost=rescore_boost,
            finalScore=round(final_score, 3),
        ),
        stage1Explanation=stage1_explain,
        stage2Explanation=stage2_explain,
        stage3Explanation=stage3_explain,
        matchedFields=matched_fields,
        entity=entity_metadata,
    )


def _extract_entity_name(entity_data: Dict[str, Any]) -> Optional[str]:
    """Extract entity name from various entity types."""
    if "name" in entity_data:
        return entity_data["name"]
    if "properties" in entity_data and entity_data["properties"]:
        return entity_data["properties"].get("name")
    for key in ["dashboardId", "chartId", "jobId", "flowId"]:
        if key in entity_data:
            return entity_data[key]
    return None


def _extract_description(entity_data: Dict[str, Any]) -> Optional[str]:
    """Extract entity description."""
    if "properties" in entity_data and entity_data["properties"]:
        return entity_data["properties"].get("description")
    return None


def _extract_entity_metadata(
    entity_data: Dict[str, Any],
    rescore_data: Optional[Dict[str, Any]],
    description: Optional[str] = None,
) -> EntityMetadata:
    """Extract comprehensive entity metadata for debugging."""

    def _to_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False
        return None

    # Platform
    platform = None
    if entity_data.get("platform"):
        platform = entity_data["platform"].get("name")

    # Description
    description = description if description is not None else _extract_description(entity_data)

    # Usage stats from statsSummary
    stats = entity_data.get("statsSummary", {}) or {}
    view_count = stats.get("viewCount")
    query_count = stats.get("queryCountLast30Days")
    unique_user_count = stats.get("uniqueUserCountLast30Days")

    # Deprecation
    deprecated = None
    if entity_data.get("deprecation"):
        deprecated = entity_data["deprecation"].get("deprecated", False)

    # Ownership
    owners: List[str] = []
    has_owners = False
    if entity_data.get("ownership") and entity_data["ownership"].get("owners"):
        for owner_entry in entity_data["ownership"]["owners"]:
            owner = owner_entry.get("owner", {})
            owner_name = owner.get("username") or owner.get("name")
            if owner_name:
                owners.append(owner_name)
        has_owners = len(owners) > 0

    # Tags
    tags: List[str] = []
    if entity_data.get("globalTags") and entity_data["globalTags"].get("tags"):
        for tag_entry in entity_data["globalTags"]["tags"]:
            tag = tag_entry.get("tag", {})
            if tag.get("properties"):
                tag_name = tag["properties"].get("name")
                if tag_name:
                    tags.append(tag_name)

    # Glossary terms
    glossary_terms: List[str] = []
    if entity_data.get("glossaryTerms") and entity_data["glossaryTerms"].get("terms"):
        for term_entry in entity_data["glossaryTerms"]["terms"]:
            term = term_entry.get("term", {})
            if term.get("properties"):
                term_name = term["properties"].get("name")
                if term_name:
                    glossary_terms.append(term_name)

    # Domain
    domain = None
    if entity_data.get("domain") and entity_data["domain"].get("domain"):
        domain_obj = entity_data["domain"]["domain"]
        if domain_obj.get("properties"):
            domain = domain_obj["properties"].get("name")

    # Determine hasDescription from actual data or rescore signals
    has_description = description is not None and len(description.strip()) > 0
    if rescore_data and rescore_data.get("signals"):
        signals_raw = rescore_data["signals"]
        # Normalize to a name-keyed dict regardless of list or dict form
        if isinstance(signals_raw, list):
            signals_map = {s["name"]: s for s in signals_raw if isinstance(s, dict) and "name" in s}
        elif isinstance(signals_raw, dict):
            signals_map = signals_raw
        else:
            signals_map = {}
        if "hasDescription" in signals_map:
            sig = signals_map["hasDescription"]
            if isinstance(sig, dict):
                parsed = _to_bool(sig.get("rawValue"))
                if parsed is not None:
                    has_description = parsed

    # Properties for debugging
    properties = entity_data.get("properties", {}) or {}

    return EntityMetadata(
        platform=platform,
        description=description,
        viewCount=view_count,
        queryCount=query_count,
        uniqueUserCount=unique_user_count,
        hasDescription=has_description,
        hasOwners=has_owners,
        deprecated=deprecated,
        owners=owners,
        tags=tags,
        glossaryTerms=glossary_terms,
        domain=domain,
        properties=properties,
    )


def _compute_signal_stats(
    signal_data: Dict[str, List[SignalDebugInfo]],
) -> List[SignalExtractionSummary]:
    """Compute summary statistics for each signal."""
    stats = []
    for signal_name, signals in signal_data.items():
        raw_values = [s.numericValue for s in signals if s.numericValue is not None]
        normalized_values = [s.normalizedValue for s in signals]
        contributions = [s.contribution for s in signals]

        stats.append(
            SignalExtractionSummary(
                signalName=signal_name,
                extractedCount=len(signals),
                avgRawValue=round(sum(raw_values) / len(raw_values), 3)
                if raw_values
                else None,
                avgNormalizedValue=round(
                    sum(normalized_values) / len(normalized_values), 3
                )
                if normalized_values
                else 0,
                avgContribution=round(sum(contributions) / len(contributions), 3)
                if contributions
                else 0,
            )
        )

    return stats


def _extract_configuration(results: List[DebugResultItem]) -> DebugConfiguration:
    """Extract configuration from debug results."""
    formula = None
    signal_count = 0
    rescore_enabled = False

    for result in results:
        if result.stage2Explanation:
            rescore_enabled = True
            if result.stage2Explanation.formula:
                formula = result.stage2Explanation.formula
            signal_count = max(signal_count, len(result.stage2Explanation.signals))
            break

    return DebugConfiguration(
        rescoreEnabled=rescore_enabled,
        rescoreWindowSize=100,  # Default
        rescoreFormula=formula,
        signalCount=signal_count,
    )


def _evaluate_assertion(
    assertion: ValidationAssertion,
    urn_to_result: Dict[str, DebugResultItem],
    urn_to_rank: Dict[str, int],
) -> AssertionResult:
    """Evaluate a single validation assertion."""
    result = urn_to_result.get(assertion.urn)
    rank = urn_to_rank.get(assertion.urn)

    if assertion.type == AssertionType.SCORE_ABOVE:
        if result is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=assertion.threshold,
                actual=None,
                message="Entity not found in results",
            )
        passed = result.scores.finalScore >= (assertion.threshold or 0)
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=assertion.threshold,
            actual=result.scores.finalScore,
            message=(
                f"Score {result.scores.finalScore} above threshold {assertion.threshold}"
                if passed
                else f"Score {result.scores.finalScore} below threshold {assertion.threshold}"
            ),
        )

    elif assertion.type == AssertionType.SCORE_BELOW:
        if result is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=assertion.threshold,
                actual=None,
                message="Entity not found in results",
            )
        passed = result.scores.finalScore <= (assertion.threshold or float("inf"))
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=assertion.threshold,
            actual=result.scores.finalScore,
            message=(
                f"Score {result.scores.finalScore} below threshold {assertion.threshold}"
                if passed
                else f"Score {result.scores.finalScore} above threshold {assertion.threshold}"
            ),
        )

    elif assertion.type == AssertionType.SIGNAL_PRESENT:
        if result is None or result.stage2Explanation is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=assertion.signal,
                actual=None,
                message="Entity or rescore explanation not found",
            )
        signal_names = [s.name for s in result.stage2Explanation.signals]
        passed = assertion.signal in signal_names
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=assertion.signal,
            actual=signal_names,
            message=(
                f"Signal '{assertion.signal}' present"
                if passed
                else f"Signal '{assertion.signal}' not found in {signal_names}"
            ),
        )

    elif assertion.type == AssertionType.RANK_EQUALS:
        if rank is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=assertion.expectedValue,
                actual=None,
                message=f"Entity {assertion.urn} not found in results",
            )
        expected_rank = int(assertion.expectedValue) if assertion.expectedValue else 1
        passed = rank == expected_rank
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=expected_rank,
            actual=rank,
            message=(
                f"Rank {rank} equals expected rank {expected_rank}"
                if passed
                else f"Rank {rank} does not equal expected rank {expected_rank}"
            ),
        )

    elif assertion.type == AssertionType.RANK_WITHIN:
        if rank is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=f"{assertion.expectedValue} +/- {assertion.threshold}",
                actual=None,
                message=f"Entity {assertion.urn} not found in results",
            )
        expected_rank = int(assertion.expectedValue) if assertion.expectedValue else 1
        tolerance = int(assertion.threshold) if assertion.threshold else 0
        min_rank = expected_rank - tolerance
        max_rank = expected_rank + tolerance
        passed = min_rank <= rank <= max_rank
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=f"{expected_rank} +/- {tolerance} (range: {min_rank}-{max_rank})",
            actual=rank,
            message=(
                f"Rank {rank} is within expected range {min_rank}-{max_rank}"
                if passed
                else f"Rank {rank} is outside expected range {min_rank}-{max_rank}"
            ),
        )

    elif assertion.type == AssertionType.RANK_BEFORE:
        if rank is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=f"before {assertion.beforeUrn}",
                actual=None,
                message=f"Entity {assertion.urn} not found",
            )
        other_rank = urn_to_rank.get(assertion.beforeUrn)
        if other_rank is None:
            return AssertionResult(
                type=assertion.type.value,
                urn=assertion.urn,
                passed=False,
                expected=f"before {assertion.beforeUrn}",
                actual=rank,
                message=f"Comparison entity {assertion.beforeUrn} not found",
            )
        passed = rank < other_rank
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=passed,
            expected=f"rank < {other_rank}",
            actual=rank,
            message=(
                f"Rank {rank} is before rank {other_rank}"
                if passed
                else f"Rank {rank} is not before rank {other_rank}"
            ),
        )

    else:
        return AssertionResult(
            type=assertion.type.value,
            urn=assertion.urn,
            passed=False,
            expected=None,
            actual=None,
            message=f"Unknown assertion type: {assertion.type}",
        )


async def _execute_comparison_search(
    config: CompareSearchConfig,
    count: int,
    types: List[str],
    graph: DataHubGraph,
) -> List[DebugResultItem]:
    """Execute a search for comparison."""
    request = SearchDebugRequest(
        query=config.query,
        count=count,
        types=types,
        rescoreEnabled=config.rescoreEnabled,
        rescoreFormulaOverride=config.rescoreFormulaOverride,
        rescoreSignalsOverride=config.rescoreSignalsOverride,
        rescoreOverride=config.rescoreOverride,
        functionScoreOverride=config.functionScoreOverride,
        includeStage1Explain=False,
        includeStage2Explain=True,
    )
    response = await debug_search(request, graph)
    return response.results


def _kendall_tau(x: List[int], y: List[int]) -> float:
    """Compute Kendall's tau rank correlation coefficient."""
    n = len(x)
    if n < 2:
        return 0.0

    concordant = 0
    discordant = 0

    for i in range(n):
        for j in range(i + 1, n):
            x_diff = x[i] - x[j]
            y_diff = y[i] - y[j]
            product = x_diff * y_diff

            if product > 0:
                concordant += 1
            elif product < 0:
                discordant += 1

    total_pairs = n * (n - 1) / 2
    if total_pairs == 0:
        return 0.0

    return (concordant - discordant) / total_pairs


def _spearman_rho(x: List[int], y: List[int]) -> float:
    """Compute Spearman's rho rank correlation coefficient.

    Re-ranks inputs to contiguous 1..n before applying the shortcut formula,
    so this works correctly even when the original ranks have gaps (partial overlap).
    """
    n = len(x)
    if n < 2:
        return 0.0

    # Re-rank to contiguous 1..n to handle non-contiguous input ranks
    def _rerank(values: List[int]) -> List[int]:
        sorted_vals = sorted(range(n), key=lambda i: values[i])
        ranks = [0] * n
        for new_rank, idx in enumerate(sorted_vals, start=1):
            ranks[idx] = new_rank
        return ranks

    rx = _rerank(x)
    ry = _rerank(y)

    d_squared_sum = sum((rxi - ryi) ** 2 for rxi, ryi in zip(rx, ry, strict=True))
    rho = 1 - (6 * d_squared_sum) / (n * (n**2 - 1))
    return rho


def _compute_signal_impact(
    baseline_results: List[DebugResultItem],
    experiment_results: List[DebugResultItem],
) -> List[SignalImpact]:
    """Compute signal impact between baseline and experiment."""
    baseline_signals: Dict[str, List[float]] = {}
    experiment_signals: Dict[str, List[float]] = {}

    for result in baseline_results:
        if result.stage2Explanation:
            for signal in result.stage2Explanation.signals:
                if signal.name not in baseline_signals:
                    baseline_signals[signal.name] = []
                baseline_signals[signal.name].append(signal.contribution)

    for result in experiment_results:
        if result.stage2Explanation:
            for signal in result.stage2Explanation.signals:
                if signal.name not in experiment_signals:
                    experiment_signals[signal.name] = []
                experiment_signals[signal.name].append(signal.contribution)

    impacts = []
    all_signals = set(baseline_signals.keys()) | set(experiment_signals.keys())

    for signal_name in all_signals:
        baseline_contribs = baseline_signals.get(signal_name, [])
        experiment_contribs = experiment_signals.get(signal_name, [])

        avg_baseline = (
            sum(baseline_contribs) / len(baseline_contribs) if baseline_contribs else 0
        )
        avg_experiment = (
            sum(experiment_contribs) / len(experiment_contribs)
            if experiment_contribs
            else 0
        )

        if avg_baseline > 0:
            change_percent = ((avg_experiment - avg_baseline) / avg_baseline) * 100
        else:
            change_percent = 100.0 if avg_experiment > 0 else 0.0

        impacts.append(
            SignalImpact(
                signalName=signal_name,
                avgBaselineContribution=round(avg_baseline, 4),
                avgExperimentContribution=round(avg_experiment, 4),
                changePercent=round(change_percent, 2),
            )
        )

    return impacts
