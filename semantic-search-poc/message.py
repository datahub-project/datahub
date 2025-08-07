"""
Normalized Message wrapper for LiteLLM responses.

This class provides a stable surface for callers to consume chat responses
without worrying about provider-specific shapes or object vs dict forms.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json


@dataclass(frozen=True)
class Message:
    """Provider-agnostic chat message wrapper.

    Intention
    - Normalize differences between providers (object shapes, dicts vs objects,
      nested fields) into a simple interface.

    Current scope
    - The implementation is currently focused on the `content` property for
      assistant-visible text and `raw` to expose the original LiteLLM response.
      These are the primary, exercised parts of the API.

    Notes
    - Other helpers (e.g., `usage`, `get`, `parse_json`, `extract_code`) are
      lightly implemented, largely untested, and may evolve. They should be
      treated as placeholders until broader provider coverage is added.
    """
    _raw: Any
    _provider: Optional[str] = None

    # Internal parsed fields (lazy parsed)
    _content: Optional[str] = None
    _role: Optional[str] = None
    _tool_calls: Optional[List] = None
    _parsed: bool = False

    def __init__(self, raw_response: Any, provider: Optional[str] = None) -> None:
        """Create a normalized message wrapper.

        Parameters
        - raw_response: The provider response object or dict returned by LiteLLM
        - provider: Optional provider hint; if not provided it is auto-detected
        """
        object.__setattr__(self, "_raw", raw_response)
        object.__setattr__(self, "_provider", provider or self._detect_provider(raw_response))
        object.__setattr__(self, "_content", None)
        object.__setattr__(self, "_role", None)
        object.__setattr__(self, "_tool_calls", None)
        object.__setattr__(self, "_parsed", False)

    # ---------------------
    # Public API
    # ---------------------

    @property
    def content(self) -> str:
        """Return the assistant-visible message content as a string.

        Automatically parses the raw response on first access and normalizes
        various shapes (string, list of parts, etc.).
        """
        if not self._parsed:
            self._parse_response()
        return self._content or ""

    @property
    def text(self) -> str:
        """Alias for content for compatibility with other interfaces."""
        return self.content

    @property
    def role(self) -> str:
        """Return the message role (e.g., 'assistant', 'user', 'system', 'tool')."""
        if not self._parsed:
            self._parse_response()
        return self._role or "assistant"

    @property
    def tool_calls(self) -> Optional[List]:
        """Return OpenAI-style tool_calls if present (otherwise None)."""
        if not self._parsed:
            self._parse_response()
        return self._tool_calls

    @property
    def raw(self) -> Any:
        """Return the raw underlying provider response for debugging/inspection."""
        return self._raw

    @property
    def usage(self) -> Optional[Dict]:
        """Return token usage information (if the provider supplies it)."""
        raw = self._raw
        try:
            if hasattr(raw, "usage"):
                usage_obj = getattr(raw, "usage")
                if hasattr(usage_obj, "model_dump"):
                    return usage_obj.model_dump()
                if hasattr(usage_obj, "dict"):
                    return usage_obj.dict()
                # Fallback to attribute extraction
                result: Dict[str, int] = {}
                for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                    if hasattr(usage_obj, key):
                        result[key] = getattr(usage_obj, key)
                return result or None
            if isinstance(raw, dict):
                return raw.get("usage")
        except Exception:
            return None
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a simple dict with 'role' and 'content'."""
        return {"role": self.role, "content": self.content}

    def get(self, path: str, default: Any = None) -> Any:
        """Access nested attribute/key using dot notation.

        Supports list indices (e.g., "choices.0.message.content"). Returns
        default when a path segment is not present.
        """
        try:
            value: Any = self._raw
            for part in path.split("."):
                # List index
                if isinstance(value, list) and part.isdigit():
                    idx = int(part)
                    value = value[idx]
                    continue
                # Attribute access
                if hasattr(value, part):
                    value = getattr(value, part)
                    continue
                # Dict access
                if isinstance(value, dict) and part in value:
                    value = value[part]
                    continue
                return default
            return value
        except Exception:
            return default

    def parse_json(self) -> Dict:
        """Parse the message content as JSON.

        Strips common Markdown code fences (```json ... ```), then parses.
        Raises json.JSONDecodeError on invalid JSON.
        """
        content = self.content
        # Strip markdown fences
        if "```json" in content:
            try:
                content = content.split("```json", 1)[1].split("```", 1)[0]
            except Exception:
                pass
        elif "```" in content:
            try:
                content = content.split("```", 1)[1].split("```", 1)[0]
            except Exception:
                pass
        return json.loads(content.strip())

    def extract_code(self, language: Optional[str] = None) -> str:
        """Extract the first fenced code block from content.

        If a specific language is provided, prefer the matching fenced block.
        Returns an empty string if no code block is found.
        """
        content = self.content
        if language:
            marker = f"```{language}"
            if marker in content:
                try:
                    return content.split(marker, 1)[1].split("```", 1)[0].strip()
                except Exception:
                    return ""
        if "```" in content:
            parts = content.split("```")
            if len(parts) >= 2:
                code = parts[1]
                # Drop language hint line if present
                if "\n" in code:
                    first_line, rest = code.split("\n", 1)
                    if first_line.strip().isalpha():
                        return rest.strip()
                return code.strip()
        return ""

    def __str__(self) -> str:  # pragma: no cover - trivial
        """Return content for string conversion."""
        return self.content

    def __repr__(self) -> str:  # pragma: no cover - trivial
        """Return a concise debug representation of the message."""
        return f"Message(role={self.role}, content={self.content[:50]}...)"

    def __bool__(self) -> bool:  # pragma: no cover - trivial
        """Truthiness indicates the presence of non-empty content."""
        return bool(self.content)

    # -----------------------------
    # Private helpers (kept at end)
    # -----------------------------

    def _parse_response(self) -> None:
        if self._parsed:
            return
        content: Optional[str] = None
        role: Optional[str] = None
        tool_calls: Optional[List] = None

        raw = self._raw
        try:
            # Object form (OpenAI SDK-style)
            if hasattr(raw, "choices") and getattr(raw, "choices"):
                choice0 = raw.choices[0]
                msg = getattr(choice0, "message", None) or getattr(choice0, "delta", None)
                if msg is not None:
                    role = getattr(msg, "role", "assistant")
                    tool_calls = getattr(msg, "tool_calls", None)
                    content = self._normalize_content(getattr(msg, "content", ""))

            # Dict form
            elif isinstance(raw, dict):
                choices = raw.get("choices", [])
                if choices:
                    choice0 = choices[0]
                    msg = choice0.get("message") or choice0.get("delta", {})
                    role = msg.get("role", "assistant")
                    tool_calls = msg.get("tool_calls")
                    content = self._normalize_content(msg.get("content", ""))

            # Direct message-like object
            elif hasattr(raw, "content"):
                content = self._normalize_content(getattr(raw, "content"))
                role = getattr(raw, "role", "assistant")

            # Raw string
            elif isinstance(raw, str):
                content = raw
                role = "assistant"
        except Exception:
            # Last resort
            content = str(raw)
            role = "assistant"

        object.__setattr__(self, "_content", content or "")
        object.__setattr__(self, "_role", role or "assistant")
        object.__setattr__(self, "_tool_calls", tool_calls)
        object.__setattr__(self, "_parsed", True)

    def _normalize_content(self, value: Any) -> str:
        # String content
        if isinstance(value, str):
            return value
        # List of parts (OpenAI/Cohere style)
        if isinstance(value, list):
            parts: List[str] = []
            for part in value:
                if isinstance(part, dict):
                    # Prefer explicit text
                    if "text" in part and isinstance(part["text"], str):
                        parts.append(part["text"])
                    elif part.get("type") == "text" and isinstance(part.get("text"), str):
                        parts.append(part.get("text") or "")
                elif isinstance(part, str):
                    parts.append(part)
            return "".join(parts)
        # Fallback to string conversion
        return str(value) if value is not None else ""

    def _detect_provider(self, raw: Any) -> str:
        try:
            model_val = None
            if hasattr(raw, "model"):
                model_val = str(getattr(raw, "model"))
            elif isinstance(raw, dict):
                model_val = str(raw.get("model"))
            if model_val:
                m = model_val.lower()
                if "gpt" in m or "o3" in m:
                    return "openai"
                if "claude" in m:
                    return "anthropic"
                if "command" in m or "cohere" in m:
                    return "cohere"
                if "bedrock" in m:
                    return "bedrock"
        except Exception:
            pass
        return "unknown"
