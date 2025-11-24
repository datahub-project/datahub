"""
LLM-based reranker using Claude via Bedrock.

Alternative to Cohere Rerank that uses LLM to directly score entities.
"""

import json
from typing import Dict, List

from datahub.utilities.perf_timer import PerfTimer
from loguru import logger

from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.gen_ai.model_config import BedrockModel
from datahub_integrations.smart_search.entity_normalizer import (
    EntityNormalizer,
)
from datahub_integrations.smart_search.reranker import (
    Reranker,
    RerankResult,
)


class LLMReranker(Reranker):
    """
    LLM-based reranker using Claude Sonnet via Bedrock.

    Uses LLM to directly score entities by semantic relevance without text generation.
    """

    def __init__(
        self,
        model: BedrockModel | str,
    ):
        """
        Initialize LLM reranker.

        Args:
            model: Bedrock model (BedrockModel enum or model ID string)
        """
        self.model_id = str(model)

    def rerank(
        self,
        semantic_query: str,
        entities: List[Dict],
        keyword_search_query: str,
    ) -> List[RerankResult]:
        """
        Rerank entities using LLM via Bedrock.

        Args:
            semantic_query: Natural language query
            entities: List of entity dicts from search results
            keyword_search_query: The /q keyword query (for context)

        Returns:
            List of RerankResult sorted by relevance (highest first)

        Raises:
            Exception: If LLM call or parsing fails
        """
        if not entities:
            return []

        # Cap at 100 entities for LLM (output token limit constraint)
        if len(entities) > 100:
            logger.warning(
                f"LLM reranker capped at 100 entities (received {len(entities)}), "
                "keeping top 100 by BM25 order"
            )
            entities = entities[:100]

        bound_logger = logger.bind(
            operation="llm_rerank",
            model=self.model_id,
            entity_count=len(entities),
        )
        bound_logger.info("Starting LLM reranking")

        with PerfTimer() as timer:
            # Create entity summaries for LLM
            entity_summaries = []
            for _i, entity in enumerate(entities):
                urn = entity.get("urn", "")
                name = EntityNormalizer.get_name(entity) or "unknown"
                desc = EntityNormalizer.get_description(entity)

                # Get key fields
                fields = entity.get("schemaMetadata", {}).get("fields", [])
                field_names = [
                    f.get("fieldPath") for f in fields[:10] if f.get("fieldPath")
                ]

                summary = {
                    "urn": urn,
                    "name": name,
                    "description": desc[:4000]
                    if desc
                    else "No description",  # Increased to 4K
                    "fields": field_names,
                }
                entity_summaries.append(summary)

            # Build prompt for LLM
            prompt = f"""You are ranking search results by relevance to a query.

Query: "{semantic_query}"

Below are {len(entity_summaries)} dataset candidates. For each entity, provide its URN and a relevance score from 0.0 to 1.0.

Candidates:
{json.dumps(entity_summaries, indent=2)}

Use the rank_entities tool to return the scored results."""

            # Define tool for structured output
            rank_tool = {
                "toolSpec": {
                    "name": "rank_entities",
                    "description": "Return entity URNs with relevance scores",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "ranked_entities": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "urn": {"type": "string"},
                                            "score": {
                                                "type": "number",
                                                "minimum": 0.0,
                                                "maximum": 1.0,
                                            },
                                        },
                                        "required": ["urn", "score"],
                                    },
                                }
                            },
                            "required": ["ranked_entities"],
                        }
                    },
                }
            }

            # Call LLM with tool
            llm_client = get_llm_client(self.model_id)

            response = llm_client.converse(
                system=[],  # No system messages for reranking task
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                toolConfig={"tools": [rank_tool]},  # type: ignore[list-item]
                inferenceConfig={
                    "temperature": 0.0,
                    "maxTokens": 8192,
                },  # Increased for 66 entities
            )

            # Extract tool use response
            content = response["output"]["message"]["content"]
            tool_use = None
            for block in content:
                if "toolUse" in block:
                    tool_use = block["toolUse"]["input"]
                    break

            if not tool_use:
                raise ValueError("LLM did not use rank_entities tool")

            ranked_entities = tool_use.get("ranked_entities", [])

            # Create URN to index mapping
            urn_to_index = {entity.get("urn"): i for i, entity in enumerate(entities)}

            # Map scored URNs back to indices
            results = []
            for scored in ranked_entities:
                urn = scored.get("urn")
                score = scored.get("score", 0.0)

                if urn in urn_to_index:
                    index = urn_to_index[urn]
                    results.append(RerankResult(index=index, score=score))
                else:
                    logger.warning(f"LLM returned unknown URN: {urn}")

            # Add missing entities with score 0.0
            scored_urns = {scored.get("urn") for scored in ranked_entities}
            for urn, index in urn_to_index.items():
                if urn not in scored_urns:
                    logger.warning(f"LLM did not score entity: {urn}")
                    results.append(RerankResult(index=index, score=0.0))

            # Sort by score descending
            results.sort(key=lambda r: r.score, reverse=True)

            logger.info(f"LLM reranking complete, top score: {results[0].score:.4f}")

        bound_logger.info(
            "Completed LLM reranking",
            duration_seconds=round(timer.elapsed_seconds(), 3),
        )
        return results
