"""
Unit tests for aggregate_converse_stream helper function.

This module tests the Bedrock stream aggregation logic in isolation.
The aggregate_converse_stream function takes streaming events from AWS Bedrock's
converse_stream API and accumulates them into a complete response matching the
format of the non-streaming converse API.

Test coverage includes:
- Simple text responses
- Tool use responses with JSON parsing
- Multiple content blocks (text + tool combinations)
- Cache token tracking
- Metrics capture
- Edge cases (empty streams, malformed JSON, missing events)
"""

from typing import Any

from datahub_integrations.gen_ai.llm.bedrock_stream_aggregator import (
    aggregate_converse_stream,
)


class TestAggregateConverseStream:
    """Tests for aggregate_converse_stream helper function."""

    def test_simple_text_response(self) -> None:
        """Test aggregating a simple text response from streaming events."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Hello! How can I help you?"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {
                "metadata": {
                    "usage": {
                        "inputTokens": 20,
                        "outputTokens": 10,
                        "totalTokens": 30,
                    }
                }
            },
        ]

        result = aggregate_converse_stream(stream)

        assert result["stopReason"] == "end_turn"
        assert result["output"]["message"]["role"] == "assistant"
        assert len(result["output"]["message"]["content"]) == 1
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "Hello! How can I help you?"
        )
        assert result["usage"]["inputTokens"] == 20
        assert result["usage"]["outputTokens"] == 10
        assert result["usage"]["totalTokens"] == 30

    def test_multiple_text_chunks(self) -> None:
        """Test aggregating multiple text deltas into a single content block."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Hello! "},
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {"text": "How "},
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {"text": "can I help you?"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 20, "outputTokens": 10}}},
        ]

        result = aggregate_converse_stream(stream)

        assert len(result["output"]["message"]["content"]) == 1
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "Hello! How can I help you?"
        )

    def test_tool_use_response(self) -> None:
        """Test aggregating a tool use response with JSON parsing."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "I'll search for that"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {
                "contentBlockStart": {
                    "start": {
                        "toolUse": {
                            "toolUseId": "call_123",
                            "name": "search",
                        }
                    },
                    "contentBlockIndex": 1,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {
                        "toolUse": {
                            "input": '{"query": "test"}',
                        }
                    },
                    "contentBlockIndex": 1,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 1}},
            {"messageStop": {"stopReason": "tool_use"}},
            {"metadata": {"usage": {"inputTokens": 50, "outputTokens": 20}}},
        ]

        result = aggregate_converse_stream(stream)

        assert result["stopReason"] == "tool_use"
        assert len(result["output"]["message"]["content"]) == 2

        # First block: text
        assert (
            result["output"]["message"]["content"][0]["text"] == "I'll search for that"
        )

        # Second block: tool use
        tool_use = result["output"]["message"]["content"][1]["toolUse"]
        assert tool_use["toolUseId"] == "call_123"
        assert tool_use["name"] == "search"
        assert tool_use["input"] == {"query": "test"}

    def test_tool_use_with_multiple_json_chunks(self) -> None:
        """Test aggregating tool use input that arrives in multiple chunks."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {
                "contentBlockStart": {
                    "start": {
                        "toolUse": {
                            "toolUseId": "call_456",
                            "name": "complex_search",
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {
                        "toolUse": {
                            "input": '{"query": ',
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {
                        "toolUse": {
                            "input": '"advanced search", ',
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {
                        "toolUse": {
                            "input": '"limit": 10}',
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "tool_use"}},
            {"metadata": {"usage": {"inputTokens": 30, "outputTokens": 15}}},
        ]

        result = aggregate_converse_stream(stream)

        tool_use = result["output"]["message"]["content"][0]["toolUse"]
        assert tool_use["name"] == "complex_search"
        assert tool_use["input"] == {"query": "advanced search", "limit": 10}

    def test_malformed_tool_use_json(self) -> None:
        """Test that malformed JSON in tool use falls back to empty dict."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {
                "contentBlockStart": {
                    "start": {
                        "toolUse": {
                            "toolUseId": "call_789",
                            "name": "broken_tool",
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {
                        "toolUse": {
                            "input": '{"invalid": json syntax',
                        }
                    },
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        tool_use = result["output"]["message"]["content"][0]["toolUse"]
        assert tool_use["input"] == {}

    def test_max_tokens_stop_reason(self) -> None:
        """Test that max_tokens stop reason is properly captured."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "This is a truncated respon"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "max_tokens"}},
            {"metadata": {"usage": {"inputTokens": 100, "outputTokens": 200}}},
        ]

        result = aggregate_converse_stream(stream)

        assert result["stopReason"] == "max_tokens"
        assert (
            result["output"]["message"]["content"][0]["text"]
            == "This is a truncated respon"
        )

    def test_multiple_content_blocks(self) -> None:
        """Test aggregating multiple mixed content blocks (text + tool + text)."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            # First text block
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "First text"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            # Tool use block
            {
                "contentBlockStart": {
                    "start": {
                        "toolUse": {
                            "toolUseId": "tool_1",
                            "name": "tool_name",
                        }
                    },
                    "contentBlockIndex": 1,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {"toolUse": {"input": '{"param": "value"}'}},
                    "contentBlockIndex": 1,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 1}},
            # Second text block
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 2}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Second text"},
                    "contentBlockIndex": 2,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 2}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 50, "outputTokens": 25}}},
        ]

        result = aggregate_converse_stream(stream)

        assert len(result["output"]["message"]["content"]) == 3
        assert result["output"]["message"]["content"][0]["text"] == "First text"
        assert (
            result["output"]["message"]["content"][1]["toolUse"]["name"] == "tool_name"
        )
        assert result["output"]["message"]["content"][2]["text"] == "Second text"

    def test_cache_tokens_in_usage(self) -> None:
        """Test that cache-related token fields are captured in usage."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Response"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {
                "metadata": {
                    "usage": {
                        "inputTokens": 100,
                        "outputTokens": 50,
                        "totalTokens": 150,
                        "cacheReadInputTokens": 80,
                        "cacheWriteInputTokens": 20,
                    }
                }
            },
        ]

        result = aggregate_converse_stream(stream)

        assert result["usage"]["inputTokens"] == 100
        assert result["usage"]["outputTokens"] == 50
        assert result["usage"]["totalTokens"] == 150
        assert result["usage"]["cacheReadInputTokens"] == 80
        assert result["usage"]["cacheWriteInputTokens"] == 20

    def test_metrics_in_metadata(self) -> None:
        """Test that metrics from metadata are captured."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Response"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {
                "metadata": {
                    "usage": {"inputTokens": 10, "outputTokens": 5},
                    "metrics": {"latencyMs": 1234},
                }
            },
        ]

        result = aggregate_converse_stream(stream)

        assert result["metrics"]["latencyMs"] == 1234

    def test_empty_stream(self) -> None:
        """Test handling of an empty stream."""
        stream: list[dict[str, Any]] = []

        result = aggregate_converse_stream(stream)

        assert result["stopReason"] == "end_turn"  # default
        assert result["output"]["message"]["content"] == []
        assert result["usage"] == {}
        assert result["metrics"] == {}

    def test_stream_without_message_start(self) -> None:
        """Test stream that doesn't have messageStart event."""
        stream: list[dict[str, Any]] = [
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {"contentBlockDelta": {"delta": {"text": "Hello"}, "contentBlockIndex": 0}},
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Should still work, just no role field
        assert "role" not in result["output"]["message"]
        assert result["output"]["message"]["content"][0]["text"] == "Hello"

    def test_empty_text_blocks_filtered(self) -> None:
        """Test that empty text blocks are filtered out."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            # Empty text block
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {"contentBlockDelta": {"delta": {"text": ""}, "contentBlockIndex": 0}},
            {"contentBlockStop": {"contentBlockIndex": 0}},
            # Non-empty text block
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 1}},
            {"contentBlockDelta": {"delta": {"text": "Hello"}, "contentBlockIndex": 1}},
            {"contentBlockStop": {"contentBlockIndex": 1}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Empty text block should be filtered out
        assert len(result["output"]["message"]["content"]) == 1
        assert result["output"]["message"]["content"][0]["text"] == "Hello"

    def test_trace_support(self) -> None:
        """Test that trace information is captured when present."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Response"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {
                "metadata": {
                    "usage": {"inputTokens": 10, "outputTokens": 5},
                    "trace": {
                        "guardrail": {
                            "action": "NONE",
                            "modelOutput": ["Response"],
                        }
                    },
                }
            },
        ]

        result = aggregate_converse_stream(stream)

        assert "trace" in result
        assert result["trace"]["guardrail"]["action"] == "NONE"

    def test_no_trace_when_absent(self) -> None:
        """Test that trace key is not included when trace is absent."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Response"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Trace should not be present
        assert "trace" not in result

    def test_content_delta_without_block_start(self) -> None:
        """Test handling contentBlockDelta without preceding contentBlockStart."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            # Delta comes directly without contentBlockStart
            {
                "contentBlockDelta": {
                    "delta": {"text": "Hello! "},
                    "contentBlockIndex": 0,
                }
            },
            {
                "contentBlockDelta": {
                    "delta": {"text": "How are you?"},
                    "contentBlockIndex": 0,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Should still aggregate the text properly
        assert len(result["output"]["message"]["content"]) == 1
        assert (
            result["output"]["message"]["content"][0]["text"] == "Hello! How are you?"
        )

    def test_stream_ends_without_block_stop(self) -> None:
        """Test handling stream that ends without contentBlockStop."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Incomplete response"},
                    "contentBlockIndex": 0,
                }
            },
            # No contentBlockStop event
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Should still include the incomplete content
        assert len(result["output"]["message"]["content"]) == 1
        assert (
            result["output"]["message"]["content"][0]["text"] == "Incomplete response"
        )

    def test_sequential_block_indices(self) -> None:
        """Test that sequential contentBlockIndex values are handled correctly."""
        stream: list[dict[str, Any]] = [
            {"messageStart": {"role": "assistant"}},
            # First block (index 0)
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 0}},
            {"contentBlockDelta": {"delta": {"text": "First"}, "contentBlockIndex": 0}},
            {"contentBlockStop": {"contentBlockIndex": 0}},
            # Second block (index 1)
            {"contentBlockStart": {"start": {}, "contentBlockIndex": 1}},
            {
                "contentBlockDelta": {
                    "delta": {"text": "Second"},
                    "contentBlockIndex": 1,
                }
            },
            {"contentBlockStop": {"contentBlockIndex": 1}},
            {"messageStop": {"stopReason": "end_turn"}},
            {"metadata": {"usage": {"inputTokens": 10, "outputTokens": 5}}},
        ]

        result = aggregate_converse_stream(stream)

        # Should process both blocks successfully
        assert len(result["output"]["message"]["content"]) == 2
        assert result["output"]["message"]["content"][0]["text"] == "First"
        assert result["output"]["message"]["content"][1]["text"] == "Second"
