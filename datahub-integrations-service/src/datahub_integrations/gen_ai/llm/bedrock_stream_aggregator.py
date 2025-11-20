"""
Bedrock stream aggregation utilities.

This module provides utilities for aggregating streaming responses from AWS Bedrock's
converse_stream API into complete responses that match the format of the non-streaming
converse API.
"""

import json
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger


def aggregate_converse_stream(stream: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregates converse_stream chunks into a converse-like response.

    This helper function processes the streaming events from AWS Bedrock's converse_stream
    API and accumulates them into a complete response that matches the format of the
    non-streaming converse API.

    Note: This implementation assumes that content blocks are streamed sequentially
    (one at a time), which matches AWS Bedrock's current behavior. The contentBlockIndex
    field is present in events but blocks are always completed before the next begins.

    Args:
        stream: An iterable of event dicts from the converse_stream response (the 'stream' key).
                Each event can contain: messageStart, contentBlockStart, contentBlockDelta,
                contentBlockStop, messageStop, or metadata.

    Returns:
        A dict with keys: 'output', 'stopReason', 'usage', 'metrics', 'trace' (optional)
        - output: Contains the complete message with role and content blocks
        - stopReason: The reason the model stopped generating (e.g., 'end_turn', 'max_tokens')
        - usage: Token usage information (inputTokens, outputTokens, etc.)
        - metrics: Additional metrics like latency
        - trace: Optional guardrail trace information (only present if available)
    """
    message: Dict[str, Any] = {}
    content_blocks: List[Dict[str, Any]] = []
    current_block: Optional[Dict[str, Any]] = None
    current_block_index: Optional[int] = None
    expected_next_index: int = 0
    stop_reason: str = "end_turn"
    usage: Dict[str, Any] = {}
    metrics: Dict[str, Any] = {}
    trace: Dict[str, Any] = {}

    for event in stream:
        if "messageStart" in event:
            # Capture the role from messageStart event
            message["role"] = event["messageStart"]["role"]

        elif "contentBlockStart" in event:
            # Start of a new content block (text or tool use)
            block_index = event["contentBlockStart"].get("contentBlockIndex")

            # Validate sequential block assumption
            if block_index is not None:
                if block_index != expected_next_index:
                    logger.warning(
                        f"contentBlockIndex out of sequence! Expected {expected_next_index}, "
                        f"got {block_index}. Blocks may not be sequential."
                    )
                current_block_index = block_index
                expected_next_index = block_index + 1

            start = event["contentBlockStart"]["start"]
            if "toolUse" in start:
                current_block = {
                    "toolUse": {
                        "toolUseId": start["toolUse"]["toolUseId"],
                        "name": start["toolUse"]["name"],
                        "input": "",
                    }
                }
            else:
                current_block = {"text": ""}

        elif "contentBlockDelta" in event:
            # Delta (chunk) of content
            block_index = event["contentBlockDelta"].get("contentBlockIndex")

            # Validate that delta matches current block
            if block_index is not None and current_block_index is not None:
                if block_index != current_block_index:
                    logger.warning(
                        f"contentBlockDelta index mismatch! Current block is {current_block_index}, "
                        f"but delta is for index {block_index}. Concurrent blocks detected!"
                    )

            delta = event["contentBlockDelta"]["delta"]

            # Handle case where contentBlockDelta comes without contentBlockStart
            if current_block is None:
                if "text" in delta:
                    current_block = {"text": ""}
                    logger.warning(
                        "Received contentBlockDelta without contentBlockStart, "
                        "initializing text block"
                    )
                elif "toolUse" in delta:
                    # Tool use delta without start - initialize with minimal info
                    current_block = {
                        "toolUse": {
                            "toolUseId": "",
                            "name": "",
                            "input": "",
                        }
                    }
                    logger.warning(
                        "Received toolUse delta without contentBlockStart, "
                        "initializing with empty metadata"
                    )
                else:
                    logger.warning(
                        f"Received contentBlockDelta with unknown delta type, skipping: {delta.keys()}"
                    )

            # Accumulate the delta content
            if "text" in delta and current_block is not None:
                current_block["text"] += delta["text"]
            elif "toolUse" in delta and current_block is not None:
                current_block["toolUse"]["input"] += delta["toolUse"]["input"]
            elif current_block is not None:
                logger.warning(
                    f"Received contentBlockDelta with unknown delta type for current block type, "
                    f"skipping. Delta keys: {delta.keys()}, current block keys: {current_block.keys()}"
                )

        elif "contentBlockStop" in event:
            # End of a content block - finalize and add to content_blocks
            block_index = event["contentBlockStop"].get("contentBlockIndex")

            # Validate that stop matches current block
            if block_index is not None and current_block_index is not None:
                if block_index != current_block_index:
                    logger.warning(
                        f"contentBlockStop index mismatch! Current block is {current_block_index}, "
                        f"but stop is for index {block_index}."
                    )

            if current_block is not None:
                if "toolUse" in current_block:
                    # Parse the accumulated JSON string into a dict
                    try:
                        current_block["toolUse"]["input"] = json.loads(
                            current_block["toolUse"]["input"]
                        )
                    except json.JSONDecodeError:
                        # If JSON parsing fails, keep as empty dict
                        current_block["toolUse"]["input"] = {}

                # Only append if it's a tool use OR non-empty text block
                # (Filter out empty text blocks that sometimes occur)
                should_append = "toolUse" in current_block or (
                    "text" in current_block and current_block["text"]
                )
                if should_append:
                    content_blocks.append(current_block)
                else:
                    logger.warning("Filtering out empty text block at contentBlockStop")

                current_block = None
                current_block_index = None

        elif "messageStop" in event:
            # Message complete - extract stop reason
            stop_reason = event["messageStop"]["stopReason"]

        elif "metadata" in event:
            # Extract token usage, metrics, and trace information
            if "usage" in event["metadata"]:
                usage = event["metadata"]["usage"]
            if "metrics" in event["metadata"]:
                metrics = event["metadata"]["metrics"]
            if "trace" in event["metadata"]:
                trace = event["metadata"]["trace"]

        else:
            # Unknown event type
            logger.warning(
                f"Received unknown event type, skipping. Event keys: {event.keys()}"
            )

    # Handle case where stream ends without contentBlockStop
    if current_block is not None:
        logger.warning(
            "Stream ended without contentBlockStop, finalizing incomplete block"
        )

        if "toolUse" in current_block:
            # Parse any accumulated tool input
            try:
                current_block["toolUse"]["input"] = json.loads(
                    current_block["toolUse"]["input"]
                )
            except json.JSONDecodeError:
                current_block["toolUse"]["input"] = {}

        # Apply the same filtering logic
        should_append = "toolUse" in current_block or (
            "text" in current_block and current_block["text"]
        )
        if should_append:
            content_blocks.append(current_block)
        else:
            logger.warning("Filtering out empty text block at stream end")

    message["content"] = content_blocks

    result: Dict[str, Any] = {
        "output": {"message": message},
        "stopReason": stop_reason,
        "usage": usage,
        "metrics": metrics,
    }

    # Only include trace if it was present in the stream
    if trace:
        result["trace"] = trace

    return result
