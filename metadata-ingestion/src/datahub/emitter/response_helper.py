import json
import logging
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Union

from requests import Response

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import APITracingWarning
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)

_TRACE_HEADER_NAME = "traceparent"


@dataclass
class TraceData:
    trace_id: str
    data: Dict[str, List[str]]

    def __post_init__(self) -> None:
        if not self.trace_id:
            raise ValueError("trace_id cannot be empty")
        if not isinstance(self.data, dict):
            raise TypeError("data must be a dictionary")


def _extract_trace_id(response: Response) -> Optional[str]:
    """
    Extract trace ID from response headers.
    Args:
        response: HTTP response object
    Returns:
        Trace ID if found and response is valid, None otherwise
    """
    if not 200 <= response.status_code < 300:
        logger.debug(f"Invalid status code: {response.status_code}")
        return None

    trace_id = response.headers.get(_TRACE_HEADER_NAME)
    if not trace_id:
        # This will only be printed if
        # 1. we're in async mode (checked by the caller)
        # 2. the server did not return a trace ID
        logger.debug(f"Missing trace header: {_TRACE_HEADER_NAME}")
        warnings.warn(
            "No trace ID found in response headers. API tracing is not active - likely due to an outdated server version.",
            APITracingWarning,
            stacklevel=3,
        )
        return None

    return trace_id


def extract_trace_data(
    response: Response,
    aspects_to_trace: Optional[List[str]] = None,
) -> Optional[TraceData]:
    """Extract trace data from a response object.

    If we run into a JSONDecodeError, we'll log an error and return None.

    Args:
        response: HTTP response object
        aspects_to_trace: Optional list of aspect names to extract. If None, extracts all aspects.

    Returns:
        TraceData object if successful, None otherwise
    """
    trace_id = _extract_trace_id(response)
    if not trace_id:
        return None

    try:
        json_data = response.json()
        if not isinstance(json_data, list):
            logger.debug("JSON data is not a list")
            return None

        data: Dict[str, List[str]] = {}

        for item in json_data:
            urn = item.get("urn")
            if not urn:
                logger.debug(f"Skipping item without URN: {item}")
                continue

            if aspects_to_trace is None:
                aspect_names = [
                    k for k, v in item.items() if k != "urn" and v is not None
                ]
            else:
                aspect_names = [
                    field for field in aspects_to_trace if item.get(field) is not None
                ]

            data[urn] = aspect_names

        return TraceData(trace_id=trace_id, data=data)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON response: {e}")
        return None


def extract_trace_data_from_mcps(
    response: Response,
    mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
    aspects_to_trace: Optional[List[str]] = None,
) -> Optional[TraceData]:
    """Extract trace data from a response object and populate data from provided MCPs.

    Args:
        response: HTTP response object used only for trace_id extraction
        mcps: List of MCP URN and aspect data
        aspects_to_trace: Optional list of aspect names to extract. If None, extracts all aspects.

    Returns:
        TraceData object if successful, None otherwise
    """
    trace_id = _extract_trace_id(response)
    if not trace_id:
        return None

    data: Dict[str, List[str]] = {}
    try:
        for mcp in mcps:
            entity_urn = getattr(mcp, "entityUrn", None)
            aspect_name = getattr(mcp, "aspectName", None)

            if not entity_urn or not aspect_name:
                logger.debug(f"Skipping MCP with missing URN or aspect name: {mcp}")
                continue

            if aspects_to_trace is not None and aspect_name not in aspects_to_trace:
                continue

            if entity_urn not in data:
                data[entity_urn] = []

            data[entity_urn].append(aspect_name)

        return TraceData(trace_id=trace_id, data=data)

    except AttributeError as e:
        logger.error(f"Error processing MCPs: {e}")
        return None
