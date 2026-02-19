import { useState } from 'react';

interface SearchConfigProps {
  searchType: 'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH';
  onSearchTypeChange: (type: 'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH') => void;
  latency: number | null;
  totalResults: number;
  rescoreEnabled: boolean;
  onRescoreEnabledChange: (enabled: boolean) => void;
  serverDefaultRescoreEnabled?: boolean;
}

export function SearchConfig({
  searchType,
  onSearchTypeChange,
  latency,
  totalResults,
  rescoreEnabled,
  onRescoreEnabledChange,
  serverDefaultRescoreEnabled = true
}: SearchConfigProps) {
  const [showInfo, setShowInfo] = useState(false);
  const [showRescoreInfo, setShowRescoreInfo] = useState(false);

  return (
    <div className="search-config">
      <div className="search-config-header">
        <h3>Search Configuration</h3>
      </div>

      <div className="search-config-body">
        {/* Search Type Selector - Compact Row */}
        <div className="config-row">
          <label className="config-label">
            Search Type
            <button
              className="info-button"
              onClick={() => setShowInfo(!showInfo)}
              title="Show information about search types"
            >
              ℹ️
            </button>
          </label>

          <div className="radio-group-horizontal">
            <label className="radio-option-compact">
              <input
                type="radio"
                value="QUERY_THEN_FETCH"
                checked={searchType === 'QUERY_THEN_FETCH'}
                onChange={(e) => onSearchTypeChange(e.target.value as any)}
              />
              <span className="option-title-compact">QUERY_THEN_FETCH</span>
            </label>

            <label className="radio-option-compact">
              <input
                type="radio"
                value="DFS_QUERY_THEN_FETCH"
                checked={searchType === 'DFS_QUERY_THEN_FETCH'}
                onChange={(e) => onSearchTypeChange(e.target.value as any)}
              />
              <span className="option-title-compact">DFS_QUERY_THEN_FETCH (Default)</span>
            </label>
          </div>
        </div>

        {showInfo && (
          <div className="info-panel">
            <h4>About Search Types</h4>
            <p>
              <strong>QUERY_THEN_FETCH:</strong> Each shard uses its local term statistics
              (IDF) for scoring. Faster but may produce inconsistent scores across shards.
              Use only when performance is critical and score consistency is not required.
            </p>
            <p>
              <strong>DFS_QUERY_THEN_FETCH:</strong> Performs a pre-query phase to gather
              term statistics across all shards, then uses global IDF for scoring.
              Provides accurate and consistent relevance scores. This is the default production mode.
            </p>
          </div>
        )}

        {/* Stage 2 Rescore Control - Toggle */}
        <div className="config-row">
          <label className="config-label">
            Stage 2 Rescore
            <button
              className="info-button"
              onClick={() => setShowRescoreInfo(!showRescoreInfo)}
              title="Show information about rescore"
            >
              ℹ️
            </button>
          </label>

          <div className="toggle-control">
            <label className="toggle-label">
              <input
                type="checkbox"
                checked={rescoreEnabled}
                onChange={(e) => onRescoreEnabledChange(e.target.checked)}
              />
              <span className="toggle-text">
                {rescoreEnabled ? 'Enabled' : 'Disabled'}
              </span>
            </label>
            <span className="server-default-hint">
              Server default: {serverDefaultRescoreEnabled ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </div>

        {showRescoreInfo && (
          <div className="info-panel">
            <h4>About Stage 2 Rescore</h4>
            <p>
              <strong>When Enabled:</strong> Applies Stage 2 reranking with multiplicative
              scoring using popularity, quality, and recency signals to the top 500 results
              from Stage 1 (BM25).
            </p>
            <p>
              <strong>When Disabled:</strong> Uses only Stage 1 scoring (BM25 + additive boosts).
              Useful for comparing ranking with/without signal-based rescoring.
            </p>
            <p className="info-note">
              <strong>Server Default:</strong> The server's global configuration is{' '}
              <strong>{serverDefaultRescoreEnabled ? 'Enabled' : 'Disabled'}</strong>. Toggle
              above to override and test different configurations.
            </p>
          </div>
        )}

        {/* Performance Metrics - Compact Row */}
        <div className="config-row">
          <label className="config-label">Performance</label>
          <div className="metrics-horizontal">
            <div className="metric-inline">
              <span className="metric-label">Latency:</span>
              <span className="metric-value">{latency !== null ? `${latency}ms` : '—'}</span>
            </div>
            <div className="metric-inline">
              <span className="metric-label">Total Results:</span>
              <span className="metric-value">{totalResults.toLocaleString()}</span>
            </div>
          </div>
        </div>

        {/* Warning banner for QUERY_THEN_FETCH mode */}
        {searchType === 'QUERY_THEN_FETCH' && (
          <div className="warning-banner">
            ⚠️ QUERY_THEN_FETCH Mode Active - This mode uses shard-local IDF which may produce inconsistent scores. Use DFS_QUERY_THEN_FETCH for accurate scoring.
          </div>
        )}
      </div>
    </div>
  );
}
