from typing import Literal, Optional

from datahub_integrations.telemetry.telemetry import BaseEvent


class S3LogStreamingRequestEvent(BaseEvent):
    """Event representing a request to stream logs from S3."""

    type: Literal["S3LogStreamingRequest"] = "S3LogStreamingRequest"

    execution_request_urn: str
    mode: str  # "grep" or "windowing"
    grep_phrase: Optional[str] = None
    lines_from_end: Optional[int] = None
    offset_from_end: Optional[int] = None
    lines_after_match: Optional[int] = None
    lines_before_match: Optional[int] = None
    max_matches_returned: Optional[int] = None
    max_total_lines: Optional[int] = None


class S3LogStreamingResponseEvent(BaseEvent):
    """Event representing the response from S3 log streaming."""

    type: Literal["S3LogStreamingResponse"] = "S3LogStreamingResponse"

    execution_request_urn: str
    mode: str  # "grep" or "windowing"

    # File metadata
    s3_file_path: Optional[str] = None  # Extracted from presigned URL
    s3_file_size_bytes: Optional[int] = None  # From Content-Length header if available

    # Streaming results
    total_lines: int
    lines_returned: int
    matches_found: Optional[int] = None  # For grep mode
    matches_returned: Optional[int] = None  # For grep mode
    truncated: Optional[bool] = None  # For grep mode

    # Performance
    stream_duration_ms: float

    # Error handling
    error_msg: Optional[str] = None
    is_s3_unavailable: bool = False  # True if S3 logs don't exist
