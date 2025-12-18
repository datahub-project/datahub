"""
Configuration for chat functionality including message and conversation limits.
"""

import os

# Maximum length of a single chat message in characters
# This prevents excessively long messages that could cause performance issues or abuse
CHAT_MAX_MESSAGE_LENGTH = int(os.environ.get("CHAT_MAX_MESSAGE_LENGTH", "10000"))

# Maximum number of messages to retain in a conversation history
# When loading conversations, only the most recent N messages will be kept
# This prevents unbounded memory growth and maintains reasonable context window sizes
CHAT_MAX_CONVERSATION_MESSAGES = int(
    os.environ.get("CHAT_MAX_CONVERSATION_MESSAGES", "1000")
)

# Keep-alive interval for SSE streaming (in seconds)
# Prevents load balancer timeouts during long-running agent operations
# Set to 20s to stay well under typical 30s load balancer timeouts
CHAT_SSE_KEEPALIVE_INTERVAL_SECONDS = int(
    os.environ.get("CHAT_SSE_KEEPALIVE_INTERVAL_SECONDS", "20")
)
