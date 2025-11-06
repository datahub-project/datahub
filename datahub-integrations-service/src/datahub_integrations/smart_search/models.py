"""
Pydantic models for ersatz semantic search configuration and responses.
"""

from typing import Dict, List, Optional

from pydantic import BaseModel


class SearchConfig(BaseModel):
    """Configuration for two-pass search budgets."""

    anchors_budget: int = (
        140  # Number of results to fetch in Pass A (anchors + phrases)
    )
    synonyms_budget: int = 60  # Number of results to fetch in Pass B (synonyms)
    max_candidates: int = 200  # Maximum candidates before reranking


class RerankConfig(BaseModel):
    """Configuration for Cohere Rerank via Bedrock."""

    model: str = "cohere.rerank-v3-5:0"  # Bedrock model ID
    max_docs: int = 200  # Maximum documents to send to reranker
    blend_weights: Dict[str, float] = {
        "rerank": 0.85,
        "anchors": 0.10,
        "negatives": 0.05,
    }


class SemanticSearchExplain(BaseModel):
    """Detailed explanation of search process (optional, for debugging)."""

    keywords: Dict[str, List[str]]  # anchors, phrases, synonyms used
    queries: Optional[Dict[str, str]] = None  # pass_a and pass_b query strings
    candidates: Optional[Dict[str, int]] = None  # Counts at each stage
    rerank: Optional[Dict[str, float]] = None  # Reranker info


class SemanticSearchResponse(BaseModel):
    """Response from semantic_search tool."""

    query: str  # Original user query
    results: List[Dict]  # Array of search results with scores
    total_candidates: int  # Total candidates before final cutoff
    explain: Optional[SemanticSearchExplain] = None  # Optional detailed breakdown


class SmartSearchResponse(BaseModel):
    """Response from smart_search function.

    Returns AI-reranked search results with faceted metadata.

    Terminology:
    - candidates = entities found in keyword search and reviewed by AI
    - results = entities selected and returned based on quality/budget
    """

    results: List[Dict]  # Array of DETAILED, FULL entity objects with all metadata
    facets: List[
        Dict
    ]  # Aggregated metadata (platforms, tags, domains, etc.) across all results
    candidates_reviewed: (
        int  # Number of candidates found in initial keyword search and reviewed by AI
    )
    candidates_selected: Optional[int] = (
        None  # Number selected after quality filtering (before token budget)
    )
    returned_count: int  # Number of results actually returned (may be less than candidates_selected due to token budget)
    has_more_selected_results: Optional[bool] = (
        None  # True if more quality results exist beyond what was returned
    )
    selection_cutoff_reason: Optional[str] = (
        None  # Why candidate selection was limited (e.g., "score_drop_detected", "score_plateau_detected", "token_budget_exceeded", "max_entities_reached")
    )
