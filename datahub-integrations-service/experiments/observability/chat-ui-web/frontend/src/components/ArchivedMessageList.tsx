import { useMemo } from 'react';
import type { ArchivedConversation, ArchivedMessage, Message, ConversationTelemetry } from '../api/types';
import { MessageList } from './MessageList';

interface ArchivedMessageListProps {
  conversation: ArchivedConversation;
}


/**
 * Convert archived messages from DataHub into the XML format expected by MessageRenderer.
 * Uses telemetry interaction events' full_history to reconstruct the exact sequence.
 */
function reconstructMessagesWithXML(
  archivedMessages: ArchivedMessage[],
  telemetry?: ConversationTelemetry
): Message[] {
  const reconstructed: Message[] = [];
  let messageIdCounter = 0;

  // If we have telemetry data, use it to add total duration metadata
  // Backend now handles all message parsing including respond_to_user extraction
  if (telemetry?.interaction_events && telemetry.interaction_events.length > 0) {
    // Build map of timestamp ranges to interaction events for adding metadata
    const interactionEvents = telemetry.interaction_events.sort((a, b) => a.timestamp - b.timestamp);

    let currentAssistantMessages: ArchivedMessage[] = [];
    let currentAssistantTimestamp: number | null = null;
    let currentEventIndex = 0;

    const flushAssistantMessage = () => {
      if (currentAssistantMessages.length > 0 && currentAssistantTimestamp) {
        const thinkingMessages = currentAssistantMessages
          .filter(m => m.message_type === 'THINKING')
          .sort((a, b) => a.timestamp - b.timestamp);

        let cumulativeWallClockTime = 0;
        const contentParts: string[] = [];
        let thinkingIndex = 0;

        // Get total duration from telemetry if available
        let totalDuration = 0;
        if (currentEventIndex < interactionEvents.length) {
          const event = interactionEvents[currentEventIndex];
          totalDuration = event.response_generation_duration_sec || 0;
          currentEventIndex++;
        }

        // Add metadata with total duration at the start
        if (totalDuration > 0) {
          contentParts.push(`<metadata totalDuration="${totalDuration.toFixed(2)}"></metadata>`);
        }

        for (const msg of currentAssistantMessages) {
          if (msg.message_type === 'THINKING') {
            const currentMsg = thinkingMessages[thinkingIndex];
            const nextMsg = thinkingMessages[thinkingIndex + 1];

            let durationAttr = '';
            if (nextMsg) {
              const durationSec = (nextMsg.timestamp - currentMsg.timestamp) / 1000;

              // Only show durations if timestamps are realistic (> 0.1s apart)
              if (durationSec >= 0.1) {
                cumulativeWallClockTime += durationSec;
                durationAttr = ` duration="${durationSec.toFixed(2)}" wallClockTime="${cumulativeWallClockTime.toFixed(2)}"`;
              }
            }

            contentParts.push(`<thinking${durationAttr}>${msg.content}</thinking>`);
            thinkingIndex++;
          } else if (msg.message_type === 'TEXT') {
            // Include TEXT messages from backend (e.g., respond_to_user responses)
            contentParts.push(msg.content);
          }
        }

        reconstructed.push({
          id: `msg-${messageIdCounter++}`,
          role: 'assistant',
          content: contentParts.filter(Boolean).join('\n\n'),
          timestamp: new Date(currentAssistantTimestamp).toISOString(),
        });
        currentAssistantMessages = [];
        currentAssistantTimestamp = null;
      }
    };

    for (const msg of archivedMessages) {
      if (msg.role === 'user') {
        flushAssistantMessage();
        reconstructed.push({
          id: `msg-${messageIdCounter++}`,
          role: 'user',
          content: msg.content,
          timestamp: new Date(msg.timestamp).toISOString(),
          user_name: msg.user_name,
        });
      } else if (msg.role === 'assistant') {
        if (currentAssistantTimestamp === null) {
          currentAssistantTimestamp = msg.timestamp;
        }
        currentAssistantMessages.push(msg);
      }
    }

    flushAssistantMessage();
  } else {
    // Fallback: reconstruct without telemetry, but calculate timing from timestamps
    let currentAssistantMessages: ArchivedMessage[] = [];
    let currentAssistantTimestamp: number | null = null;

    const flushAssistantMessage = () => {
      if (currentAssistantMessages.length > 0 && currentAssistantTimestamp) {
        const thinkingMessages = currentAssistantMessages
          .filter(m => m.message_type === 'THINKING')
          .sort((a, b) => a.timestamp - b.timestamp);

        let cumulativeWallClockTime = 0;
        const contentParts: string[] = [];
        let thinkingIndex = 0;

        for (const msg of currentAssistantMessages) {
          if (msg.message_type === 'THINKING') {
            const currentMsg = thinkingMessages[thinkingIndex];
            const nextMsg = thinkingMessages[thinkingIndex + 1];

            let durationAttr = '';
            if (nextMsg) {
              const durationSec = (nextMsg.timestamp - currentMsg.timestamp) / 1000;

              // Only show durations if timestamps are realistic (> 0.1s apart)
              if (durationSec >= 0.1) {
                cumulativeWallClockTime += durationSec;
                durationAttr = ` duration="${durationSec.toFixed(2)}" wallClockTime="${cumulativeWallClockTime.toFixed(2)}"`;
              }
            }
            // Remove fake default duration for single blocks

            contentParts.push(`<thinking${durationAttr}>${msg.content}</thinking>`);
            thinkingIndex++;
          } else if (msg.message_type === 'TEXT') {
            contentParts.push(msg.content);
          }
        }

        reconstructed.push({
          id: `msg-${messageIdCounter++}`,
          role: 'assistant',
          content: contentParts.filter(Boolean).join('\n\n'),
          timestamp: new Date(currentAssistantTimestamp).toISOString(),
        });
        currentAssistantMessages = [];
        currentAssistantTimestamp = null;
      }
    };

    for (const msg of archivedMessages) {
      if (msg.role === 'user') {
        flushAssistantMessage();
        reconstructed.push({
          id: `msg-${messageIdCounter++}`,
          role: 'user',
          content: msg.content,
          timestamp: new Date(msg.timestamp).toISOString(),
          user_name: msg.user_name,
        });
      } else if (msg.role === 'assistant') {
        if (currentAssistantTimestamp === null) {
          currentAssistantTimestamp = msg.timestamp;
        }
        currentAssistantMessages.push(msg);
      }
    }

    flushAssistantMessage();
  }

  return reconstructed;
}

export function ArchivedMessageList({
  conversation,
}: ArchivedMessageListProps) {
  // Convert archived messages to the XML format expected by MessageList/MessageRenderer
  // Pass telemetry data so full_history can be used to reconstruct the exact sequence
  const messages: Message[] = useMemo(() => {
    return reconstructMessagesWithXML(
      conversation.messages,
      conversation.telemetry
    );
  }, [conversation.messages, conversation.telemetry]);

  const originLabel = conversation.origin_type?.replace(/_/g, ' ') || 'Unknown';

  return (
    <div className="archived-message-list">
      <div className="read-only-banner">
        <div className="banner-content">
          <span className="icon">🔒</span>
          <span>
            This is an archived conversation from {originLabel}. Read-only mode.
          </span>
        </div>
        <div className="banner-id">
          <code title="Conversation URN">{conversation.id}</code>
        </div>
      </div>

      {conversation.context && (
        <div className="conversation-context">
          <h4>Context</h4>
          <p>{conversation.context.text}</p>
          {conversation.context.entityUrns &&
            conversation.context.entityUrns.length > 0 && (
              <div className="context-entities">
                <strong>Related Entities:</strong>
                <ul>
                  {conversation.context.entityUrns.map((urn, idx) => (
                    <li key={idx}>{urn}</li>
                  ))}
                </ul>
              </div>
            )}
        </div>
      )}

      <MessageList messages={messages} isStreaming={false} />

      <div className="conversation-footer">
        <div className="metadata">
          <span>
            Created:{' '}
            {new Date(conversation.created_at * 1000).toLocaleString()}
          </span>
          <span>
            Last Updated:{' '}
            {new Date(conversation.updated_at * 1000).toLocaleString()}
          </span>
          <span>Total Messages: {conversation.message_count}</span>
        </div>
      </div>
    </div>
  );
}
