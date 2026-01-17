import { useEffect, useRef, useState } from 'react';
import { useChat } from '../hooks/useChat';
import { useArchivedConversations } from '../hooks/useArchivedConversations';
import { MessageList } from './MessageList';
import { MessageInput, MessageInputHandle } from './MessageInput';
import { ConversationList } from './ConversationList';
import { ConversationTabs } from './ConversationTabs';
import { ArchivedConversationList } from './ArchivedConversationList';
import { ArchivedMessageList } from './ArchivedMessageList';
import { apiClient } from '../api/client';

export function ChatWindow() {
  const [activeTab, setActiveTab] = useState<'active' | 'archived'>('active');

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

  const {
    conversations: archivedConversations,
    currentConversation: archivedConversation,
    loading: archivedLoading,
    error: archivedError,
    total: archivedTotal,
    currentPage: archivedPage,
    pageSize: archivedPageSize,
    originFilter,
    sortBy,
    sortDesc,
    loadConversations: loadArchivedConversations,
    loadConversation: loadArchivedConversation,
    nextPage: archivedNextPage,
    prevPage: archivedPrevPage,
    filterByOrigin,
    changeSortBy,
    toggleSortDirection,
    hasNextPage: archivedHasNext,
    hasPrevPage: archivedHasPrev,
  } = useArchivedConversations();

  const messageInputRef = useRef<MessageInputHandle>(null);

  useEffect(() => {
    loadConversations();
  }, [loadConversations]);

  // Load archived conversations when switching to archived tab
  useEffect(() => {
    if (activeTab === 'archived' && archivedConversations.length === 0) {
      loadArchivedConversations();
    }
  }, [activeTab, archivedConversations.length, loadArchivedConversations]);

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
      <div className="sidebar">
        <ConversationTabs
          activeTab={activeTab}
          onTabChange={setActiveTab}
          activeCount={conversations.length}
          archivedTotal={archivedTotal}
        />

        {activeTab === 'active' ? (
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
        ) : (
          <ArchivedConversationList
            conversations={archivedConversations}
            currentConversationUrn={archivedConversation?.urn || null}
            onSelect={loadArchivedConversation}
            total={archivedTotal}
            currentPage={archivedPage}
            pageSize={archivedPageSize}
            hasNextPage={archivedHasNext}
            hasPrevPage={archivedHasPrev}
            onNextPage={archivedNextPage}
            onPrevPage={archivedPrevPage}
            originFilter={originFilter}
            onFilterChange={filterByOrigin}
            sortBy={sortBy}
            onSortChange={changeSortBy}
            sortDesc={sortDesc}
            onToggleSortDirection={toggleSortDirection}
            loading={archivedLoading}
          />
        )}
      </div>

      <div className="chat-main">
        {activeTab === 'active' ? (
          <>
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
                <button onClick={handleCreateConversation}>
                  Start New Chat
                </button>
              </div>
            )}
          </>
        ) : (
          <>
            {archivedError && <div className="error-banner">{archivedError}</div>}
            {archivedConversation ? (
              <ArchivedMessageList conversation={archivedConversation} />
            ) : (
              <div className="empty-chat">
                <h2>Conversation History</h2>
                <p>Select an archived conversation from the list to view</p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
