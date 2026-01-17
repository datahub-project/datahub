import { useMemo } from 'react';
import type { ArchivedConversation, ArchivedMessage, Message } from '../api/types';
import { MessageList } from './MessageList';

interface ArchivedMessageListProps {
  conversation: ArchivedConversation;
}

/**
 * Convert archived messages from DataHub into the XML format expected by MessageRenderer.
 * This allows archived conversations to use the same rendering pipeline as active chats.
 */
function reconstructMessagesWithXML(archivedMessages: ArchivedMessage[]): Message[] {
  const reconstructed: Message[] = [];
  let currentAssistantContent: string[] = [];
  let currentAssistantTimestamp: number | null = null;
  let currentAssistantStartTime: number | null = null;
  let lastMessageTime: number | null = null; // Track ANY previous message (user or assistant)
  let messageIdCounter = 0;

  const flushAssistantMessage = () => {
    if (currentAssistantContent.length > 0 && currentAssistantTimestamp) {
      reconstructed.push({
        id: `msg-${messageIdCounter++}`,
        role: 'assistant',
        content: currentAssistantContent.join('\n\n'),
        timestamp: new Date(currentAssistantTimestamp).toISOString(),
      });
      currentAssistantContent = [];
      currentAssistantTimestamp = null;
      currentAssistantStartTime = null;
    }
  };

  for (const msg of archivedMessages) {
    if (msg.role === 'user') {
      // Flush any pending assistant message before adding user message
      flushAssistantMessage();

      reconstructed.push({
        id: `msg-${messageIdCounter++}`,
        role: 'user',
        content: msg.content,
        timestamp: new Date(msg.timestamp).toISOString(),
        user_name: msg.user_name,
      });

      // Track user message time for calculating first assistant response duration
      lastMessageTime = msg.timestamp;
    } else if (msg.role === 'assistant') {
      // Track first timestamp for the combined message
      if (currentAssistantTimestamp === null) {
        currentAssistantTimestamp = msg.timestamp;
        currentAssistantStartTime = msg.timestamp;
      }

      // Calculate wall clock time from start of assistant response (cumulative)
      const wallClockTime = currentAssistantStartTime
        ? ((msg.timestamp - currentAssistantStartTime) / 1000).toFixed(1)
        : '0.0';

      // Calculate duration for this specific step (time since last message - user OR assistant)
      const duration = lastMessageTime
        ? ((msg.timestamp - lastMessageTime) / 1000).toFixed(1)
        : '0.0';

      // Update last message time
      lastMessageTime = msg.timestamp;

      // Wrap different message types in XML tags with timing attributes
      if (msg.message_type === 'THINKING') {
        currentAssistantContent.push(
          `<thinking duration="${duration}" wallClockTime="${wallClockTime}">${msg.content}</thinking>`
        );
      } else if (msg.message_type === 'TOOL_CALL') {
        // Try to parse tool call format if it's structured
        currentAssistantContent.push(
          `<tool_use duration="${duration}" wallClockTime="${wallClockTime}"><tool_name>unknown</tool_name><parameters>${msg.content}</parameters></tool_use>`
        );
      } else if (msg.message_type === 'TEXT') {
        // Final text response - add without tags
        currentAssistantContent.push(msg.content);
      } else {
        // Unknown type - add as-is
        currentAssistantContent.push(msg.content);
      }
    }
  }

  // Flush any remaining assistant message
  flushAssistantMessage();

  return reconstructed;
}

export function ArchivedMessageList({
  conversation,
}: ArchivedMessageListProps) {
  // Convert archived messages to the XML format expected by MessageList/MessageRenderer
  const messages: Message[] = useMemo(() => {
    return reconstructMessagesWithXML(conversation.messages);
  }, [conversation.messages]);

  const originLabel = conversation.origin_type?.replace(/_/g, ' ') || 'Unknown';

  return (
    <div className="archived-message-list">
      <div className="read-only-banner">
        <span className="icon">🔒</span>
        <span>
          This is an archived conversation from {originLabel}. Read-only mode.
        </span>
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
