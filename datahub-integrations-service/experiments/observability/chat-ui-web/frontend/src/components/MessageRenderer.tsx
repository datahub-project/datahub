import { useMemo, useState, useEffect } from 'react';
import type { Message } from '../api/types';
import { parseAiMessage } from '../utils/messageParser';
import { ThinkingBlock } from './ThinkingBlock';
import { ToolCallBlock } from './ToolCallBlock';
import { MarkdownRenderer } from './MarkdownRenderer';

interface MessageRendererProps {
  message: Message;
  isStreaming?: boolean;
}

export function MessageRenderer({ message, isStreaming = false }: MessageRendererProps) {
  // Thinking section is expanded during streaming, collapsed after completion
  const [isThinkingExpanded, setIsThinkingExpanded] = useState(isStreaming);

  // Live elapsed time counter for streaming
  const [liveElapsedTime, setLiveElapsedTime] = useState(0);
  const [streamingStartTime, setStreamingStartTime] = useState<number | null>(null);

  const parsed = useMemo(() => {
    if (message.role === 'assistant') {
      return parseAiMessage(message.content);
    }
    return null;
  }, [message.content, message.role]);

  // Update live elapsed time every second during streaming
  useEffect(() => {
    if (!isStreaming || !parsed || message.role !== 'assistant') {
      // Reset when streaming stops
      if (!isStreaming && streamingStartTime !== null) {
        setStreamingStartTime(null);
      }
      return;
    }

    const hasEvents = parsed.events && parsed.events.length > 0;
    if (!hasEvents) {
      return;
    }

    // Calculate the highest wallClockTime we've seen from backend
    let highestWallClockTime = 0;
    parsed.events.forEach((event) => {
      if (event.type === 'thinking' && event.data.wallClockTime !== undefined) {
        highestWallClockTime = Math.max(highestWallClockTime, event.data.wallClockTime);
      } else if (event.type === 'tool_call' && event.data.wallClockTime !== undefined) {
        highestWallClockTime = Math.max(highestWallClockTime, event.data.wallClockTime);
      }
    });

    // Initialize startTime ONCE when we first see wallClockTime from backend
    if (streamingStartTime === null && highestWallClockTime > 0) {
      const calculatedStartTime = Date.now() - (highestWallClockTime * 1000);
      setStreamingStartTime(calculatedStartTime);
    }

    // Use the stored startTime for counting (doesn't change during streaming)
    const startTime = streamingStartTime || Date.now();

    // Update elapsed time every second based on our local clock
    const interval = setInterval(() => {
      const localElapsed = (Date.now() - startTime) / 1000;
      // Use max of local timer and backend's wallClockTime to stay in sync
      const elapsed = Math.max(localElapsed, highestWallClockTime);
      setLiveElapsedTime(elapsed);
    }, 1000);

    // Set initial value immediately (use backend's value as baseline)
    setLiveElapsedTime(highestWallClockTime);

    return () => clearInterval(interval);
  }, [isStreaming, parsed, message.role, streamingStartTime]);

  // User message - simple rendering
  if (message.role === 'user') {
    return (
      <div className="message message-user">
        <div className="message-header">
          <span className="message-role">{message.user_name || 'You'}</span>
          <span className="message-time">{formatTime(message.timestamp)}</span>
        </div>
        <div className="message-content">
          <MarkdownRenderer content={message.content} />
        </div>
      </div>
    );
  }

  // Assistant message - parse and render thinking, tool calls, and final response
  if (message.role === 'assistant' && parsed) {
    const hasEvents = parsed.events && parsed.events.length > 0;
    const hasFinalText = parsed.finalText && parsed.finalText.length > 0;

    // Calculate aggregated metadata for both streaming and completed messages
    let totalTokens = 0;
    let wallClockTime = 0;
    if (hasEvents) {
      parsed.events.forEach((event) => {
        if (event.type === 'thinking') {
          totalTokens += event.data.tokens || 0;
          // Use the highest wallClockTime (not sum of durations)
          if (event.data.wallClockTime !== undefined && event.data.wallClockTime > wallClockTime) {
            wallClockTime = event.data.wallClockTime;
          }
        } else if (event.type === 'tool_call') {
          totalTokens += (event.data.tokens || 0) + (event.data.resultTokens || 0);
          // Use the highest wallClockTime (not sum of durations)
          if (event.data.wallClockTime !== undefined && event.data.wallClockTime > wallClockTime) {
            wallClockTime = event.data.wallClockTime;
          }
        }
      });
    }

    return (
      <>
        {/* Thinking section - separate box */}
        {hasEvents && (
          <div className="message message-assistant message-thinking-box">
            {/* Collapsible header - always visible */}
            <div
              className="thinking-summary-header"
              onClick={() => !isStreaming && setIsThinkingExpanded(!isThinkingExpanded)}
              style={{ cursor: isStreaming ? 'default' : 'pointer' }}
            >
              <div className="thinking-summary-left">
                {isStreaming ? (
                  <span className="thinking-summary-text">
                    ⚡ Thinking for {liveElapsedTime.toFixed(1)}s{totalTokens > 0 ? ` (${totalTokens} tokens)` : ''}
                  </span>
                ) : (
                  <span className="thinking-summary-text">
                    ⚡ Thought for {wallClockTime.toFixed(1)}s{totalTokens > 0 ? ` (${totalTokens} tokens)` : ''}
                  </span>
                )}
              </div>
              {!isStreaming && (
                <button className="thinking-summary-toggle" aria-label={isThinkingExpanded ? 'Collapse' : 'Expand'}>
                  {isThinkingExpanded ? '▼' : '▶'}
                </button>
              )}
            </div>

            {/* Thinking content - collapsible after completion */}
            {(isStreaming || isThinkingExpanded) && (
              <div className="message-content">
                {/* Render events in chronological order */}
                <div className="message-events-section">
                  {parsed.events.map((event, index) => {
                    if (event.type === 'thinking') {
                      return (
                        <ThinkingBlock
                          key={`thinking-${index}`}
                          thinking={event.data}
                          index={index}
                          isLive={isStreaming && index === parsed.events.length - 1}
                        />
                      );
                    } else if (event.type === 'tool_call') {
                      return (
                        <ToolCallBlock
                          key={`tool-${index}`}
                          toolCall={event.data}
                        />
                      );
                    }
                    return null;
                  })}
                </div>

                {/* Show cursor if streaming and no final text yet */}
                {isStreaming && !hasFinalText && (
                  <div className="message-thinking-indicator">
                    <span className="thinking-dots">
                      <span>.</span>
                      <span>.</span>
                      <span>.</span>
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Response section - separate box */}
        {hasFinalText && (
          <div className="message message-assistant message-response-box">
            <div className="message-header">
              <span className="message-role">Response</span>
              <span className="message-time">{formatTime(message.timestamp)}</span>
            </div>
            <div className="message-content">
              <MarkdownRenderer content={parsed.finalText!} />
            </div>
          </div>
        )}

        {/* Show cursor if streaming but no events yet */}
        {isStreaming && !hasFinalText && !hasEvents && (
          <div className="message message-assistant">
            <div className="message-header">
              <span className="message-role">Assistant</span>
              <span className="message-time">{formatTime(message.timestamp)}</span>
              <span className="message-streaming">● Thinking...</span>
            </div>
            <div className="message-content">
              <div className="message-thinking-indicator">
                <span className="thinking-dots">
                  <span>.</span>
                  <span>.</span>
                  <span>.</span>
                </span>
              </div>
            </div>
          </div>
        )}
      </>
    );
  }

  // Fallback for unknown message types
  return (
    <div className="message">
      <div className="message-header">
        <span className="message-role">{message.role}</span>
        <span className="message-time">{formatTime(message.timestamp)}</span>
      </div>
      <div className="message-content">
        <pre>{message.content}</pre>
      </div>
    </div>
  );
}

function formatTime(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diff = now.getTime() - date.getTime();

  // Less than 1 minute ago
  if (diff < 60000) {
    return 'Just now';
  }

  // Less than 1 hour ago
  if (diff < 3600000) {
    const minutes = Math.floor(diff / 60000);
    return `${minutes}m ago`;
  }

  // Less than 24 hours ago
  if (diff < 86400000) {
    const hours = Math.floor(diff / 3600000);
    return `${hours}h ago`;
  }

  // Show date
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}
