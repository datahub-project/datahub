import { useEffect, useRef } from 'react';
import type { Message } from '../api/types';
import { MessageRenderer } from './MessageRenderer';

interface MessageListProps {
  messages: Message[];
  streamingMessage?: string;
  isStreaming: boolean;
}

export function MessageList({ messages, streamingMessage, isStreaming }: MessageListProps) {
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingMessage]);

  return (
    <div className="message-list">
      {messages.map((message) => (
        <MessageRenderer
          key={message.id}
          message={message}
          isStreaming={false}
        />
      ))}
      {isStreaming && (
        <MessageRenderer
          message={{
            id: 'streaming',
            role: 'assistant',
            content: streamingMessage || '',
            timestamp: new Date().toISOString(),
          }}
          isStreaming={true}
        />
      )}
      <div ref={messagesEndRef} />
    </div>
  );
}
