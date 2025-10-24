"""
Vendor-agnostic reranker for semantic search.

Currently implements Cohere Rerank via Bedrock, but interface is designed to support
other providers (GCP Vertex AI, direct Cohere API, etc.) in the future.
"""

import json
import os
from typing import Dict, List, Protocol

from loguru import logger
from pydantic import BaseModel

from datahub_integrations.gen_ai.bedrock import get_bedrock_client
from datahub_integrations.gen_ai.model_config import (
    BedrockModel,
    get_bedrock_model_env_variable,
)


class RerankResult(BaseModel):
    """Result from reranking a single document."""

    index: int  # Original index in the input documents list
    score: float  # Relevance score (0-1, higher is better)


class Reranker(Protocol):
    """Protocol for reranker implementations."""

    def rerank(
        self,
        semantic_query: str,
        entities: List[Dict],
        keyword_search_query: str,
    ) -> List[RerankResult]:
        """
        Rerank entities by semantic relevance to query.

        Args:
            semantic_query: Natural language query for semantic understanding
            entities: List of entity dicts from search results
            keyword_search_query: The /q keyword query (for text generation if needed)

        Returns:
            List of RerankResult sorted by relevance (highest first)
        """
        ...


class CohereBedrockReranker(Reranker):
    """
    Cohere Rerank implementation via AWS Bedrock.

    This is the default reranker, using Cohere Rerank v3.5 through Bedrock.
    """

    def __init__(
        self,
        model_id: str = "cohere.rerank-v3-5:0",
    ):
        """
        Initialize Cohere Bedrock reranker.

        Args:
            model_id: Bedrock model ID for Cohere Rerank
        """
        self.model_id = model_id

    def rerank(
        self,
        semantic_query: str,
        entities: List[Dict],
        keyword_search_query: str,
    ) -> List[RerankResult]:
        """
        Rerank entities using Cohere Rerank via Bedrock.

        Args:
            semantic_query: Natural language query for semantic matching
            entities: List of entity dicts from search results
            keyword_search_query: The /q keyword query (for text generation)

        Returns:
            List of RerankResult sorted by relevance (highest first)
        """
        if not entities:
            return []

        logger.info(
            f"Reranking {len(entities)} entities with Cohere via Bedrock (model: {self.model_id})"
        )

        # Generate text representations for Cohere
        from datahub_integrations.smart_search.smart_search import (
            _extract_keywords_from_query,
        )
        from datahub_integrations.smart_search.text_generator import (
            EntityTextGenerator,
        )

        keywords = _extract_keywords_from_query(keyword_search_query)
        generator = EntityTextGenerator()

        documents = []
        for entity in entities:
            text = generator.generate(entity, search_keywords=keywords)
            documents.append(text)

        logger.debug(
            f"Generated {len(documents)} text documents, "
            f"avg length: {sum(len(d) for d in documents) / len(documents):.0f} chars"
        )

        # Call Cohere Rerank via Bedrock
        bedrock_client = get_bedrock_client()
        response = bedrock_client.invoke_model(
            modelId=self.model_id,
            body=json.dumps(
                {
                    "api_version": 2,
                    "query": semantic_query,
                    "documents": documents,
                    "top_n": len(documents),
                }
            ),
        )

        # Parse response
        response_body = json.loads(response["body"].read())
        results = response_body.get("results", [])

        logger.info(
            f"Cohere returned {len(results)} results, "
            f"top score: {results[0]['relevance_score']:.4f} if results else 0"
        )

        # Convert to RerankResult objects
        return [
            RerankResult(index=r["index"], score=r["relevance_score"]) for r in results
        ]


def create_reranker() -> Reranker:
    """
    Factory function to create a reranker instance based on environment configuration.

    Environment Variable:
        SMART_SEARCH_RERANK_MODEL: Model to use for reranking
            - If starts with "cohere.rerank": Use Cohere Rerank (e.g., "cohere.rerank-v3-5:0")
            - BedrockModel enum name: Use LLM Reranker with that model (e.g., "CLAUDE_37_SONNET")
            - Explicit model ID: Use LLM Reranker with that ID
            - Default: "cohere.rerank-v3-5:0"

    The ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX environment variable (default "us")
    is automatically applied when using BedrockModel enum values.

    Examples:
        SMART_SEARCH_RERANK_MODEL=cohere.rerank-v3-5:0  → Cohere
        SMART_SEARCH_RERANK_MODEL=CLAUDE_37_SONNET  → LLM with auto region prefix
        SMART_SEARCH_RERANK_MODEL=us.anthropic.claude-3-7-sonnet-20250219-v1:0  → LLM explicit

    Returns:
        Reranker instance (CohereBedrockReranker or LLMReranker)
    """
    # Get model value with Cohere as default
    model_value = os.environ.get("SMART_SEARCH_RERANK_MODEL", "cohere.rerank-v3-5:0")

    # Check if it's a Cohere model
    if model_value.startswith("cohere.rerank"):
        logger.info(f"Using Cohere Rerank via Bedrock (model: {model_value})")
        return CohereBedrockReranker(model_id=model_value)

    # Otherwise use LLM reranker with proper model resolution
    from datahub_integrations.smart_search.llm_reranker import LLMReranker

    # Use standard model resolution pattern with BedrockModel.CLAUDE_37_SONNET as fallback
    # This handles BedrockModel enum names and applies region prefix automatically
    model = get_bedrock_model_env_variable(
        "SMART_SEARCH_RERANK_MODEL",
        BedrockModel.CLAUDE_37_SONNET,
    )

    logger.info(f"Using LLM reranker via Bedrock (model: {model})")
    return LLMReranker(model=model)
