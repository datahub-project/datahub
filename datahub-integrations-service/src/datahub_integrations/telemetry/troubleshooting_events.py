from typing import Literal, Optional

from datahub_integrations.telemetry.telemetry import BaseEvent


class TroubleshootingApiRequestEvent(BaseEvent):
    """Event representing a request to RunLLM troubleshooting API."""

    type: Literal["TroubleshootingApiRequest"] = "TroubleshootingApiRequest"

    question: str
    context: Optional[str] = None
    provider: str = "runllm"


class TroubleshootingApiResponseEvent(BaseEvent):
    """Event representing a response from RunLLM troubleshooting API."""

    type: Literal["TroubleshootingApiResponse"] = "TroubleshootingApiResponse"

    question: str
    context: Optional[str] = None
    provider: str = "runllm"

    response_time_ms: float
    response_length_chars: int
    num_sources: int
    error_msg: Optional[str] = None

    # Optional: truncated answer for analytics (full answer goes to DataHub)
    answer_preview: Optional[str] = None
