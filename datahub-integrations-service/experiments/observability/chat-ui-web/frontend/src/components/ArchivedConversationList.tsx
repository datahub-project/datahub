import React, { useState, useEffect } from 'react';
import type { ArchivedConversation, ConversationOriginType, ConversationSortBy } from '../api/types';
import { HealthStatusBadge } from './HealthStatusBadge';
import { OriginLogo } from './OriginLogo';
import { Warning, Question, XCircle, CheckCircle, Star } from 'phosphor-react';
import { apiClient } from '../api/client';

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
  onRefresh: () => void;
  loading?: boolean;
}

function formatThinkingTime(ms: number): string {
  if (ms === 0) return '0s';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

function formatRelativeTime(timestamp: number): string {
  const now = Date.now();
  const date = new Date(timestamp * 1000);
  const diffMs = now - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHour / 24);
  const diffWeek = Math.floor(diffDay / 7);
  const diffMonth = Math.floor(diffDay / 30);
  const diffYear = Math.floor(diffDay / 365);

  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffMin < 60) return `${diffMin}min ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  if (diffDay < 7) return `${diffDay}d ago`;
  if (diffWeek < 4) return `${diffWeek}w ago`;
  if (diffMonth < 12) return `${diffMonth}mo ago`;
  return `${diffYear}y ago`;
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
  onRefresh,
  loading,
}: ArchivedConversationListProps) {
  const [healthFilters, setHealthFilters] = useState<Set<string>>(new Set(['all']));
  const [favoriteUrns, setFavoriteUrns] = useState<Set<string>>(new Set());
  const [showFavoritesOnly, setShowFavoritesOnly] = useState(false);
  const [favoritesLoading, setFavoritesLoading] = useState(false);

  // Load favorites on mount and when conversations change
  useEffect(() => {
    loadFavorites();
  }, []);

  const loadFavorites = async () => {
    try {
      const result = await apiClient.getFavoriteUrns();
      setFavoriteUrns(new Set(result.urns));
    } catch (error) {
      console.error('Failed to load favorites:', error);
    }
  };

  const toggleFavorite = async (urn: string, event: React.MouseEvent) => {
    event.stopPropagation(); // Don't trigger conversation selection
    setFavoritesLoading(true);

    try {
      if (favoriteUrns.has(urn)) {
        await apiClient.removeFavorite(urn);
        setFavoriteUrns(prev => {
          const next = new Set(prev);
          next.delete(urn);
          return next;
        });
      } else {
        await apiClient.addFavorite(urn);
        setFavoriteUrns(prev => new Set(prev).add(urn));
      }
    } catch (error) {
      console.error('Failed to toggle favorite:', error);
    } finally {
      setFavoritesLoading(false);
    }
  };

  // Calculate min/max timestamps from conversations
  const dateRange = React.useMemo(() => {
    if (conversations.length === 0) {
      const now = Math.floor(Date.now() / 1000);
      return { min: now - 86400 * 7, max: now }; // Default to last 7 days
    }
    const timestamps = conversations.map(c => c.created_at);
    return {
      min: Math.min(...timestamps),
      max: Math.max(...timestamps),
    };
  }, [conversations]);

  const [dateRangeFilter, setDateRangeFilter] = useState<[number, number]>([
    dateRange.min,
    dateRange.max,
  ]);

  // Update date range when conversations change
  React.useEffect(() => {
    setDateRangeFilter([dateRange.min, dateRange.max]);
  }, [dateRange.min, dateRange.max]);

  const toggleHealthFilter = (filter: string) => {
    const newFilters = new Set(healthFilters);

    if (filter === 'all') {
      // Clicking "all" deselects everything else
      setHealthFilters(new Set(['all']));
    } else {
      // Remove "all" if selecting a specific filter
      newFilters.delete('all');

      // Toggle the clicked filter
      if (newFilters.has(filter)) {
        newFilters.delete(filter);
        // If no filters left, select "all"
        if (newFilters.size === 0) {
          newFilters.add('all');
        }
      } else {
        newFilters.add(filter);
      }

      setHealthFilters(newFilters);
    }
  };

  // Filter conversations by health status, favorites, and date range
  const filteredConversations = conversations.filter((conv) => {
    // Favorites filter
    if (showFavoritesOnly && !favoriteUrns.has(conv.urn)) {
      return false;
    }

    // Health status filter (OR logic - match ANY selected filter)
    if (!healthFilters.has('all')) {
      let matchesHealthFilter = false;

      if (healthFilters.has('abandoned')) {
        if (conv.health_status?.is_abandoned === true) matchesHealthFilter = true;
      }
      if (healthFilters.has('errors')) {
        if (conv.health_status?.has_errors === true) matchesHealthFilter = true;
      }
      if (healthFilters.has('incomplete')) {
        if ((conv.health_status?.unanswered_questions_count ?? 0) > 0) matchesHealthFilter = true;
      }
      if (healthFilters.has('complete')) {
        if (
          conv.health_status?.is_abandoned === false &&
          conv.health_status?.has_errors === false &&
          (conv.health_status?.unanswered_questions_count ?? 0) === 0 &&
          conv.health_status?.completion_rate === 1.0
        ) matchesHealthFilter = true;
      }

      if (!matchesHealthFilter) return false;
    }

    // Date range filter
    if (conv.created_at < dateRangeFilter[0] || conv.created_at > dateRangeFilter[1]) {
      return false;
    }

    return true;
  });

  return (
    <div className="archived-conversation-list">
      <div className="archived-list-header">
        <h2>Conversation History</h2>
        <div className="archived-header-actions">
          <span className="archived-badge">Read-Only</span>
          <button
            className="btn-refresh"
            onClick={onRefresh}
            disabled={loading}
            title="Refresh conversation list"
          >
            🔄 Refresh
          </button>
        </div>
      </div>

      <div className="filter-section">
        <div className="filter-group favorites-filter-group">
          <label>Favorites:</label>
          <button
            className={`favorites-filter-btn ${showFavoritesOnly ? 'active' : ''}`}
            onClick={() => setShowFavoritesOnly(!showFavoritesOnly)}
            title={showFavoritesOnly ? 'Show all conversations' : 'Show favorites only'}
            disabled={favoritesLoading}
          >
            <Star size={18} weight={showFavoritesOnly ? 'fill' : 'regular'} />
            <span>{favoriteUrns.size}</span>
          </button>
        </div>

        <div className="filter-group">
          <label>Filter by Source:</label>
          <select
            value={originFilter || ''}
            onChange={(e) =>
              onFilterChange((e.target.value as ConversationOriginType) || null)
            }
            className="origin-filter"
            title="Filter conversations by their origin platform"
          >
            <option value="" title="Show conversations from all platforms">All Sources</option>
            <option value="DATAHUB_UI" title="Conversations from DataHub web UI">DataHub UI</option>
            <option value="SLACK" title="Conversations from Slack channels and DMs">Slack</option>
            <option value="TEAMS" title="Conversations from Microsoft Teams">Microsoft Teams</option>
            <option value="INGESTION_UI" title="Conversations from ingestion UI">Ingestion UI</option>
          </select>
        </div>

        <div className="filter-group health-filter-group">
          <label>Health Status:</label>
          <div className="health-filter-buttons">
            <button
              className={`health-filter-btn ${healthFilters.has('all') ? 'active' : ''}`}
              onClick={() => toggleHealthFilter('all')}
              title="Show all conversations"
            >
              All
            </button>
            <button
              className={`health-filter-btn ${healthFilters.has('abandoned') ? 'active' : ''}`}
              onClick={() => toggleHealthFilter('abandoned')}
              title="Abandoned conversations - Bot started but never finished response"
            >
              <Warning size={18} weight="regular" />
            </button>
            <button
              className={`health-filter-btn ${healthFilters.has('errors') ? 'active' : ''}`}
              onClick={() => toggleHealthFilter('errors')}
              title="Conversations with errors - Contains error messages or failed tool calls"
            >
              <XCircle size={18} weight="regular" />
            </button>
            <button
              className={`health-filter-btn ${healthFilters.has('incomplete') ? 'active' : ''}`}
              onClick={() => toggleHealthFilter('incomplete')}
              title="Incomplete conversations - Questions without complete responses"
            >
              <Question size={18} weight="regular" />
            </button>
            <button
              className={`health-filter-btn ${healthFilters.has('complete') ? 'active' : ''}`}
              onClick={() => toggleHealthFilter('complete')}
              title="Complete conversations - All questions answered successfully"
            >
              <CheckCircle size={18} weight="regular" />
            </button>
          </div>
        </div>

        <div className="filter-group">
          <label>Sort by:</label>
          <div className="sort-controls">
            <select
              value={sortBy}
              onChange={(e) => onSortChange(e.target.value as ConversationSortBy)}
              className="sort-filter"
              title="Sort conversations by different criteria"
            >
              <option value="max_thinking_time" title="Sort by longest thinking time">Max Thinking Time</option>
              <option value="num_turns" title="Sort by number of conversation turns">Number of Turns</option>
              <option value="created" title="Sort by creation date">Created Date</option>
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

        <div className="filter-group date-range-filter">
          <label>Date Range:</label>
          <div className="date-range-display">
            <span className="date-range-label">
              {formatRelativeTime(dateRangeFilter[0])}
            </span>
            <span className="date-range-separator">→</span>
            <span className="date-range-label">
              {formatRelativeTime(dateRangeFilter[1])}
            </span>
          </div>
          <div className="date-range-slider-container">
            <input
              type="range"
              min={dateRange.min}
              max={dateRange.max}
              value={dateRangeFilter[0]}
              onChange={(e) => {
                const newStart = parseInt(e.target.value);
                if (newStart <= dateRangeFilter[1]) {
                  setDateRangeFilter([newStart, dateRangeFilter[1]]);
                }
              }}
              className="date-range-slider date-range-slider-start"
              disabled={conversations.length === 0}
            />
            <input
              type="range"
              min={dateRange.min}
              max={dateRange.max}
              value={dateRangeFilter[1]}
              onChange={(e) => {
                const newEnd = parseInt(e.target.value);
                if (newEnd >= dateRangeFilter[0]) {
                  setDateRangeFilter([dateRangeFilter[0], newEnd]);
                }
              }}
              className="date-range-slider date-range-slider-end"
              disabled={conversations.length === 0}
            />
            <div className="date-range-track">
              <div
                className="date-range-track-selected"
                style={{
                  left: `${((dateRangeFilter[0] - dateRange.min) / (dateRange.max - dateRange.min)) * 100}%`,
                  right: `${100 - ((dateRangeFilter[1] - dateRange.min) / (dateRange.max - dateRange.min)) * 100}%`,
                }}
              />
            </div>
          </div>
          {(dateRangeFilter[0] !== dateRange.min || dateRangeFilter[1] !== dateRange.max) && (
            <button
              className="btn-reset-date-range"
              onClick={() => setDateRangeFilter([dateRange.min, dateRange.max])}
              title="Reset date range"
            >
              Reset
            </button>
          )}
        </div>
      </div>

      <div className="archived-list-items">
        {loading ? (
          <div className="loading-state">
            <div className="spinner"></div>
            <p>Loading conversation history...</p>
          </div>
        ) : filteredConversations.length === 0 ? (
          <div className="empty-state">
            <h3>
              {conversations.length === 0
                ? 'No Conversation History'
                : 'No Matching Conversations'}
            </h3>
            <p>
              {conversations.length === 0
                ? 'Archived conversations from Slack, Teams, and DataHub UI will appear here.'
                : 'Try adjusting your filters to see more conversations.'}
            </p>
          </div>
        ) : (
          filteredConversations.map((conversation) => (
            <div
              key={conversation.urn}
              className={`archived-item ${
                conversation.urn === currentConversationUrn ? 'active' : ''
              }`}
              onClick={() => onSelect(conversation.urn)}
            >
              <div className="archived-item-header">
                <button
                  className={`favorite-btn ${favoriteUrns.has(conversation.urn) ? 'favorited' : ''}`}
                  onClick={(e) => toggleFavorite(conversation.urn, e)}
                  title={favoriteUrns.has(conversation.urn) ? 'Remove from favorites' : 'Add to favorites'}
                  disabled={favoritesLoading}
                >
                  <Star
                    size={18}
                    weight={favoriteUrns.has(conversation.urn) ? 'fill' : 'regular'}
                  />
                </button>
                <div className="conversation-title">{conversation.title}</div>
                <div className="header-badges">
                  <HealthStatusBadge healthStatus={conversation.health_status} compact={true} />
                  <OriginLogo originType={conversation.origin_type} size={20} />
                </div>
              </div>
              <div className="conversation-meta">
                {conversation.slack_conversation_type && (
                  <span className="conversation-type">
                    {conversation.slack_conversation_type === 'channel' && '📢 Channel'}
                    {conversation.slack_conversation_type === 'dm' && '💬 DM'}
                    {conversation.slack_conversation_type === 'private_channel' && '🔒 Private'}
                  </span>
                )}
                <span>{conversation.num_turns} turns</span>
                <span className="thinking-time">
                  ⏱️ Max: {formatThinkingTime(conversation.max_thinking_time_ms)}
                </span>
                <span className="date">{formatRelativeTime(conversation.created_at)}</span>
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
