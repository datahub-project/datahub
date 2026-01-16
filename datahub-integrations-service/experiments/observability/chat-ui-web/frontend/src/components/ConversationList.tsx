import type { Conversation } from '../api/types';
import { AutoChatBar } from './AutoChatBar';

interface ConversationListProps {
  conversations: Conversation[];
  currentConversationId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
  onDeleteAll: () => void;
  onCreate: () => void;
  onSendMessage: (content: string, conversationId?: string) => Promise<void>;
  onCreateConversation: (title?: string) => Promise<{ id: string }>;
}

export function ConversationList({
  conversations,
  currentConversationId,
  onSelect,
  onDelete,
  onDeleteAll,
  onCreate,
  onSendMessage,
  onCreateConversation,
}: ConversationListProps) {
  return (
    <div className="conversation-list">
      <AutoChatBar
        onSendMessage={onSendMessage}
        onCreateConversation={onCreateConversation}
      />
      <div className="conversation-list-header">
        <h2>Conversations</h2>
        <div className="conversation-header-buttons">
          <button onClick={onCreate} className="btn-new">
            New Chat
          </button>
          {conversations.length > 0 && (
            <button
              onClick={() => {
                if (confirm(`Delete all ${conversations.length} conversations? This cannot be undone.`)) {
                  onDeleteAll();
                }
              }}
              className="btn-delete-all"
              title="Delete all conversations"
            >
              🗑️
            </button>
          )}
        </div>
      </div>
      <div className="conversation-list-items">
        {conversations.length === 0 ? (
          <div className="empty-state">No conversations yet</div>
        ) : (
          conversations.map((conversation) => (
            <div
              key={conversation.id}
              className={`conversation-item ${
                conversation.id === currentConversationId ? 'active' : ''
              }`}
              onClick={() => onSelect(conversation.id)}
            >
              <div className="conversation-title">{conversation.title}</div>
              <div className="conversation-meta">
                {conversation.messages.length} messages
              </div>
              <button
                className="btn-delete"
                onClick={(e) => {
                  e.stopPropagation();
                  if (confirm('Delete this conversation?')) {
                    onDelete(conversation.id);
                  }
                }}
              >
                Delete
              </button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
