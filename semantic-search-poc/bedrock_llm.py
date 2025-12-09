"""Bedrock LLM factory (LiteLLM-backed).

Provides a small helper to construct a LiteLLM chat model backed by AWS Bedrock
while remaining compatible with prior call sites that passed LangChain
`SystemMessage`/`HumanMessage` objects. The adapter converts those objects to
OpenAI-style dict messages under the hood.
"""

from __future__ import annotations

from typing import Optional, Any, Dict
import os
from chat_base import BaseChatModel
from litellm_chat import LiteLLMChatModel


# Default AWS Region for Bedrock access
DEFAULT_BEDROCK_REGION = "us-west-2"


def create_bedrock_llm(
    model_id: str,
    *,
    region_name: str = DEFAULT_BEDROCK_REGION,
    temperature: float = 0.2,
    max_tokens: Optional[int] = 2048,
    top_p: Optional[float] = None,
) -> BaseChatModel:
    """Create a LiteLLM-backed Bedrock chat model with LC-compatible invoke.

    Parameters
    - model_id: Bedrock model identifier (e.g., "anthropic.claude-3-5-sonnet-20240620-v1:0").
    - region_name: AWS region for Bedrock. Defaults to us-west-2.
    - temperature: Sampling temperature (passed to LiteLLM by default).
    - max_tokens: Optional token limit (passed to LiteLLM by default).
    - top_p: Nucleus sampling parameter (passed to LiteLLM by default).
    - Credentials/profile should be provided via the standard AWS environment/config chain.

    Returns
    - An object exposing invoke(messages, **kwargs) that accepts either
      OpenAI-style dict messages or LangChain System/Human messages and returns
      a normalized `Message`.
    """

    # Map to LiteLLM model string
    litellm_model: str = model_id if "/" in model_id else f"bedrock/{model_id}"

    default_kwargs: Dict[str, Any] = {
        "temperature": temperature,
    }
    if max_tokens is not None:
        default_kwargs["max_tokens"] = max_tokens
    if top_p is not None:
        default_kwargs["top_p"] = top_p
    # Do not forward aws_profile_name to Bedrock calls; boto uses default chain

    # Directly return a LiteLLMChatModel (implements BaseChatModel)
    llm = LiteLLMChatModel(
        model=litellm_model,
        aws_region_name=region_name,
        default_kwargs=default_kwargs,
    )
    return llm


__all__ = ["create_bedrock_llm", "DEFAULT_BEDROCK_REGION"]


