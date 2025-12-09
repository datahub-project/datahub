"""
LiteLLM-based Chat Model adapter.

Provides a minimal analogue of LangChain's `BaseChatModel` with a single
`invoke(messages, **kwargs) -> Message` method to send OpenAI-style messages and
return a normalized Message object.

Notes
- Works with any LiteLLM chat model (e.g., "gpt-4o-mini", "anthropic/claude-3-5-sonnet",
  "bedrock/anthropic.claude-3-5-sonnet-20240620-v1:0").
- For Bedrock, pass `aws_region_name` to route to a specific region.
- Supports common parameters: temperature, max_tokens, top_p, etc.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from litellm import completion as litellm_completion
from message import Message
from chat_base import BaseChatModel


class LiteLLMChatModel(BaseChatModel):
    """A minimal chat model using LiteLLM.

    Accepts OpenAI-style messages: [{"role": "user"|"system"|"assistant", "content": str}].
    Returns a normalized `Message` instance.
    """

    def __init__(
        self,
        model: str,
        *,
        aws_region_name: Optional[str] = None,
        default_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.model: str = model
        # Keep common identifier attributes for external utilities to pick up
        self.model_id: str = model
        self.model_name: str = model
        self.aws_region_name = aws_region_name
        self._default_kwargs: Dict[str, Any] = default_kwargs or {}

    def invoke(self, messages: List[Dict[str, str]], **kwargs: Any) -> Message:
        """Send chat messages and return a normalized Message.

        Parameters
        - messages: List of dicts with keys {"role", "content"}
        - kwargs: Optional generation params (temperature, max_tokens, top_p, ...)
        """
        call_kwargs: Dict[str, Any] = dict(self._default_kwargs)
        call_kwargs.update(kwargs)

        if self.aws_region_name:
            call_kwargs.setdefault("aws_region_name", self.aws_region_name)

        response = litellm_completion(model=self.model, messages=messages, **call_kwargs)
        return Message(response)
