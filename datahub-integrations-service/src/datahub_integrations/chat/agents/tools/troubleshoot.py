import json
import os
from abc import ABC, abstractmethod
from typing import List, Optional

import httpx
from httpx_sse import aconnect_sse
from loguru import logger
from pydantic import BaseModel, Field


class TroubleshootingResponse(BaseModel):
    """Response from troubleshooting provider."""

    answer: str
    sources: List[str] = Field(
        default_factory=list, description="URLs to relevant documentation"
    )


class BaseTroubleshootingProvider(ABC):
    """
    Abstract base class for troubleshooting providers.
    Implementations provide different backends for answering DataHub troubleshooting questions.
    """

    @abstractmethod
    async def query(
        self, question: str, context: Optional[str] = None
    ) -> TroubleshootingResponse:
        """
        Query the troubleshooting provider with a question.
        Args:
            question: User's question about DataHub
            context: Optional context (e.g., "Setting up BigQuery ingestion")
        Returns:
            Troubleshooting response with answer and sources
        """
        pass


class RunLLMTroubleshootingProvider(BaseTroubleshootingProvider):
    """
    RunLLM implementation for DataHub troubleshooting.
    This provider integrates with RunLLM's API to provide intelligent documentation
    search powered by their trained models. Streams responses via SSE and assembles
    the complete answer before returning.
    Configuration:
        - api_key: RunLLM API key from account settings
        - assistant_id: RunLLM assistant/pipeline ID from assistant URL
        - base_url: API endpoint (defaults to production)
    """

    def __init__(
        self,
        api_key: str,
        assistant_id: str,
        base_url: str = "https://api.runllm.com",
    ):
        """
        Initialize RunLLM troubleshooting provider.
        Args:
            api_key: RunLLM API key (from account settings)
            assistant_id: RunLLM assistant/pipeline ID (from assistant URL)
            base_url: RunLLM API base URL
        """
        self.api_key = api_key
        self.assistant_id = assistant_id
        self.base_url = base_url
        logger.info(
            f"RunLLM troubleshooting provider initialized (assistant_id: {assistant_id})"
        )

    async def query(
        self, question: str, context: Optional[str] = None
    ) -> TroubleshootingResponse:
        """
        Query RunLLM troubleshooting provider with a question.
        Args:
            question: User's question about DataHub
            context: Optional context (e.g., "Setting up BigQuery ingestion")
        Returns:
            Troubleshooting response with answer and sources
        Raises:
            Exception: If API call fails (caller should handle gracefully)
        """
        # Append context to question if provided
        full_question = question
        if context:
            full_question = f"{question} (Context: {context})"

        logger.info("Querying RunLLM for troubleshooting: %s", full_question)
        response = await self._call_runllm_api(full_question)
        logger.info(
            "RunLLM response received: %d chars, %d sources",
            len(response.answer),
            len(response.sources),
        )
        return response

    async def _call_runllm_api(self, question: str) -> TroubleshootingResponse:
        """
        Call RunLLM API and stream the response.
        The API returns Server-Sent Events (SSE) with incremental chunks.
        We consume the entire stream and assemble the complete response.
        Args:
            question: User's question
        Returns:
            Complete troubleshooting response with answer and sources
        """
        url = f"{self.base_url}/api/pipeline/{self.assistant_id}/chat"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "Access-Control-Allow-Origin": "*",
        }
        payload = {"message": question, "source": "web"}

        # Collect response chunks
        content_parts: List[str] = []
        sources: List[str] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            async with aconnect_sse(
                client, "POST", url, headers=headers, json=payload
            ) as event_source:
                async for sse in event_source.aiter_sse():
                    try:
                        data = json.loads(sse.data)
                        chunk_type = data.get("chunk_type")

                        if chunk_type == "generation_in_progress":
                            # Streaming response content
                            content = data.get("content", "")
                            if content:
                                content_parts.append(content)

                        elif chunk_type == "sources":
                            # Extract citation links from markdown format
                            # Format: [[link text](url), [link text2](url2)]
                            content = data.get("content", "")
                            if content:
                                sources = self._parse_sources(content)

                        elif chunk_type == "generation_starts":
                            logger.debug("RunLLM generation started")

                        elif chunk_type in ("retrieval", "classification"):
                            # Log but don't process these intermediate chunks
                            logger.debug(f"RunLLM {chunk_type} phase")

                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse SSE data: {e}")
                        continue

        # Assemble complete response
        full_answer = "".join(content_parts).strip()

        if not full_answer:
            raise ValueError("No answer received from RunLLM")

        return TroubleshootingResponse(answer=full_answer, sources=sources)

    def _parse_sources(self, sources_markdown: str) -> List[str]:
        """
        Parse source URLs from RunLLM's markdown citation format.
        Format: [[link text](url), [link text2](url2)]
        Args:
            sources_markdown: Markdown-formatted citation links
        Returns:
            List of source URLs
        """
        import re

        # Extract URLs from markdown links: [text](url)
        url_pattern = r"\[([^\]]+)\]\(([^\)]+)\)"
        matches = re.findall(url_pattern, sources_markdown)

        # Return just the URLs (second group in each match)
        return [url for _, url in matches]


def _create_troubleshooting_provider() -> BaseTroubleshootingProvider:
    """
    Factory function to create the troubleshooting provider.
    Currently only supports RunLLM. To add new providers:
    1. Create a new class that extends BaseTroubleshootingProvider
    2. Implement the query() method
    3. Add configuration check and instantiation here
    Returns:
        Configured troubleshooting provider
    Raises:
        RuntimeError: If no provider is configured (should not happen since
                     tool registration is conditional)
    """
    runllm_api_key = os.getenv("RUNLLM_API_KEY")
    runllm_assistant_id = os.getenv("RUNLLM_ASSISTANT_ID")

    if runllm_api_key and runllm_assistant_id:
        logger.info("Troubleshooting provider: RunLLM")
        return RunLLMTroubleshootingProvider(
            api_key=runllm_api_key, assistant_id=runllm_assistant_id
        )

    raise RuntimeError(
        "No troubleshooting provider configured. Troubleshooting tool is not available."
    )


def troubleshoot(question: str, context: Optional[str] = None) -> dict:
    """Search DataHub documentation and knowledge bases to troubleshoot issues and answer questions.
    Use this tool when users need help with:
    - Configuring ingestion sources (BigQuery, Snowflake, etc.)
    - Making sense of ingestion logs and recipes
    - Understanding DataHub features and capabilities
    - Troubleshooting common issues and errors
    - Best practices and setup instructions
    - API usage and integration patterns
    The tool searches official DataHub documentation and returns relevant answers
    with links to source documentation.
    Args:
        question: The user's question about DataHub
        context: Additional context to help narrow down the search (e.g., "BigQuery ingestion source")
    Returns:
        Dictionary with 'answer' (str) and 'sources' (list of URLs)
    Examples:
        troubleshoot("How do I configure BigQuery ingestion?")
        troubleshoot("What is the difference between tags and glossary terms?")
        troubleshoot("Error: Failed to connect to DataHub GMS", context="Docker quickstart")
    """
    import asyncer

    provider = _create_troubleshooting_provider()

    # Run async query in sync context
    response = asyncer.syncify(provider.query, raise_sync_error=False)(
        question, context or None
    )

    return {
        "answer": response.answer,
        "sources": response.sources,
    }
