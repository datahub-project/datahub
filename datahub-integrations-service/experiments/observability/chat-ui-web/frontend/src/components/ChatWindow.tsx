import { useEffect, useRef } from 'react';
import { useChat } from '../hooks/useChat';
import { MessageList } from './MessageList';
import { MessageInput, MessageInputHandle } from './MessageInput';
import { ConversationList } from './ConversationList';
import { apiClient } from '../api/client';

export function ChatWindow() {
  const {
    conversations,
    currentConversation,
    loading,
    error,
    isStreaming,
    streamingMessage,
    loadConversations,
    createConversation,
    loadConversation,
    deleteConversation,
    sendMessage,
  } = useChat();

  const messageInputRef = useRef<MessageInputHandle>(null);

  useEffect(() => {
    loadConversations();
  }, [loadConversations]);

  // Auto-focus input whenever conversation changes
  useEffect(() => {
    if (currentConversation && !isStreaming) {
      // Small delay to ensure component is rendered
      const timeoutId = setTimeout(() => {
        messageInputRef.current?.focus();
      }, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [currentConversation?.id, isStreaming]);

  const handleCreateConversation = async () => {
    await createConversation();
  };

  const handleSendMessage = async (content: string) => {
    if (!currentConversation) {
      await createConversation();
    }
    await sendMessage(content);
  };

  const handleDeleteAll = async () => {
    try {
      const result = await apiClient.deleteAllConversations();
      console.log(`Deleted ${result.deleted} conversations, ${result.failed} failed`);

      // Reload conversations to reflect changes
      await loadConversations();
    } catch (err) {
      console.error('Failed to delete all conversations:', err);
    }
  };

  return (
    <div className="chat-window">
      <ConversationList
        conversations={conversations}
        currentConversationId={currentConversation?.id || null}
        onSelect={loadConversation}
        onDelete={deleteConversation}
        onDeleteAll={handleDeleteAll}
        onCreate={handleCreateConversation}
        onSendMessage={sendMessage}
        onCreateConversation={createConversation}
      />
      <div className="chat-main">
        {error && <div className="error-banner">{error}</div>}
        {loading && <div className="loading">Loading...</div>}
        {currentConversation ? (
          <>
            <MessageList
              key={currentConversation.id}
              messages={currentConversation.messages}
              streamingMessage={streamingMessage}
              isStreaming={isStreaming}
            />
            <MessageInput
              key={`input-${currentConversation.id}`}
              ref={messageInputRef}
              onSend={handleSendMessage}
              disabled={isStreaming}
              placeholder="Ask about your data..."
            />
          </>
        ) : (
          <div className="empty-chat">
            <h2>Welcome to DataHub Chat</h2>
            <p>Start a new conversation or select an existing one</p>
            <button onClick={handleCreateConversation}>Start New Chat</button>
          </div>
        )}
      </div>
    </div>
  );
}
