import type { ArchivedConversation, ConversationOriginType, ConversationSortBy } from '../api/types';

interface ArchivedConversationListProps {
  conversations: ArchivedConversation[];
  currentConversationUrn: string | null;
  onSelect: (urn: string) => void;
  total: number;
  currentPage: number;
  pageSize: number;
  hasNextPage: boolean;
  hasPrevPage: boolean;
  onNextPage: () => void;
  onPrevPage: () => void;
  originFilter: ConversationOriginType;
  onFilterChange: (filter: ConversationOriginType) => void;
  sortBy: ConversationSortBy;
  onSortChange: (sortBy: ConversationSortBy) => void;
  sortDesc: boolean;
  onToggleSortDirection: () => void;
  loading?: boolean;
}

const ORIGIN_LABELS: Record<string, string> = {
  DATAHUB_UI: 'DataHub UI',
  SLACK: 'Slack',
  TEAMS: 'Microsoft Teams',
  INGESTION_UI: 'Ingestion UI',
};

const ORIGIN_COLORS: Record<string, string> = {
  DATAHUB_UI: '#1890ff',
  SLACK: '#611f69',
  TEAMS: '#6264a7',
  INGESTION_UI: '#52c41a',
};

function formatThinkingTime(ms: number): string {
  if (ms === 0) return '0s';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

export function ArchivedConversationList({
  conversations,
  currentConversationUrn,
  onSelect,
  total,
  currentPage,
  pageSize,
  hasNextPage,
  hasPrevPage,
  onNextPage,
  onPrevPage,
  originFilter,
  onFilterChange,
  sortBy,
  onSortChange,
  sortDesc,
  onToggleSortDirection,
  loading,
}: ArchivedConversationListProps) {
  const formatDate = (timestamp: number) => {
    const date = new Date(timestamp * 1000);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
  };

  return (
    <div className="archived-conversation-list">
      <div className="archived-list-header">
        <h2>Conversation History</h2>
        <span className="archived-badge">Read-Only</span>
      </div>

      <div className="filter-section">
        <div className="filter-group">
          <label>Filter by Source:</label>
          <select
            value={originFilter || ''}
            onChange={(e) =>
              onFilterChange((e.target.value as ConversationOriginType) || null)
            }
            className="origin-filter"
          >
            <option value="">All Sources</option>
            <option value="DATAHUB_UI">DataHub UI</option>
            <option value="SLACK">Slack</option>
            <option value="TEAMS">Microsoft Teams</option>
            <option value="INGESTION_UI">Ingestion UI</option>
          </select>
        </div>

        <div className="filter-group">
          <label>Sort by:</label>
          <div className="sort-controls">
            <select
              value={sortBy}
              onChange={(e) => onSortChange(e.target.value as ConversationSortBy)}
              className="sort-filter"
            >
              <option value="max_thinking_time">Max Thinking Time</option>
              <option value="num_turns">Number of Turns</option>
              <option value="created">Created Date</option>
            </select>
            <button
              onClick={onToggleSortDirection}
              className="sort-direction-btn"
              title={sortDesc ? 'Sorted: High to Low' : 'Sorted: Low to High'}
            >
              {sortDesc ? '↓' : '↑'}
            </button>
          </div>
        </div>
      </div>

      <div className="archived-list-items">
        {loading ? (
          <div className="loading-state">
            <div className="spinner"></div>
            <p>Loading conversation history...</p>
          </div>
        ) : conversations.length === 0 ? (
          <div className="empty-state">
            <h3>No Conversation History</h3>
            <p>
              Archived conversations from Slack, Teams, and DataHub UI will
              appear here.
            </p>
          </div>
        ) : (
          conversations.map((conversation) => (
            <div
              key={conversation.urn}
              className={`archived-item ${
                conversation.urn === currentConversationUrn ? 'active' : ''
              }`}
              onClick={() => onSelect(conversation.urn)}
            >
              <div className="archived-item-header">
                <div className="conversation-title">{conversation.title}</div>
                <span
                  className="origin-badge"
                  style={{
                    backgroundColor: ORIGIN_COLORS[conversation.origin_type],
                  }}
                >
                  {ORIGIN_LABELS[conversation.origin_type]}
                </span>
              </div>
              <div className="conversation-meta">
                <span>{conversation.num_turns} turns</span>
                <span className="thinking-time">
                  ⏱️ Max: {formatThinkingTime(conversation.max_thinking_time_ms)}
                </span>
                <span className="date">{formatDate(conversation.created_at)}</span>
              </div>
              {conversation.context?.text && (
                <div className="context-preview">
                  {conversation.context.text.substring(0, 100)}
                  {conversation.context.text.length > 100 && '...'}
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {total > pageSize && !loading && (
        <div className="pagination">
          <button
            onClick={onPrevPage}
            disabled={!hasPrevPage}
            className="btn-pagination"
          >
            Previous
          </button>
          <span className="page-info">
            Page {currentPage + 1} of {Math.ceil(total / pageSize)} ({total}{' '}
            total)
          </span>
          <button
            onClick={onNextPage}
            disabled={!hasNextPage}
            className="btn-pagination"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
