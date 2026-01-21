import { useState, useEffect } from 'react';
import { useSearch } from '../hooks/useSearch';
import { SearchSubTabs } from './SearchSubTabs';
import { SearchResultCard } from './SearchResultCard';
import { AIExplainView } from './AIExplainView';

type SearchSubTab = 'results' | 'ai-explain';

export function SearchTab() {
  const [query, setQuery] = useState('');
  const [activeSubTab, setActiveSubTab] = useState<SearchSubTab>(() => {
    const saved = localStorage.getItem('searchActiveSubTab');
    return (saved === 'ai-explain' ? 'ai-explain' : 'results');
  });

  const {
    results,
    loading,
    error,
    explainData,
    currentPage,
    currentQuery,
    selectedUrns,
    aiAnalysis,
    analyzingRanking,
    search,
    explainResult,
    nextPage,
    previousPage,
    toggleSelection,
    clearSelections,
    analyzeRanking,
  } = useSearch();

  // Persist sub-tab selection
  useEffect(() => {
    localStorage.setItem('searchActiveSubTab', activeSubTab);
  }, [activeSubTab]);

  // Switch to AI tab after analysis completes
  useEffect(() => {
    if (aiAnalysis && !analyzingRanking) {
      setActiveSubTab('ai-explain');
    }
  }, [aiAnalysis, analyzingRanking]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    search(query);
    setActiveSubTab('results');
  };

  const handleExplain = async (urn: string, entityType: string) => {
    await explainResult(currentQuery || query, urn, entityType);
  };

  const handleAnalyzeClick = async () => {
    await analyzeRanking();
  };

  return (
    <div className="search-tab">
      <div className="search-header">
        <h2>Search Admin</h2>
        <p className="search-subtitle">Test search ranking and debug scores</p>
      </div>

      <form onSubmit={handleSearch} className="search-form">
        <div className="search-input-group">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter search query..."
            className="search-input"
            disabled={loading}
          />
          <button
            type="submit"
            className="search-button"
            disabled={loading || !query.trim()}
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </form>

      {error && (
        <div className="search-error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {results && (
        <>
          <div className="search-controls">
            <SearchSubTabs
              activeTab={activeSubTab}
              onTabChange={setActiveSubTab}
              selectedCount={selectedUrns.size}
              hasAnalysis={!!aiAnalysis}
            />

            <div className="search-actions">
              {selectedUrns.size > 0 && (
                <button
                  className="clear-selection-button"
                  onClick={clearSelections}
                >
                  Clear ({selectedUrns.size})
                </button>
              )}
              <button
                className="explain-button"
                onClick={handleAnalyzeClick}
                disabled={selectedUrns.size === 0 || analyzingRanking}
              >
                {analyzingRanking ? 'Analyzing...' : `Explain (${selectedUrns.size})`}
              </button>
            </div>
          </div>

          {activeSubTab === 'results' && (
            <div className="search-results">
              <div className="results-header">
                <span className="results-count">
                  Showing {results.start + 1}-{results.start + results.searchResults.length} of {results.total} results
                </span>
                <div className="pagination-controls pagination-controls-top">
                  <button
                    className="pagination-button"
                    onClick={previousPage}
                    disabled={currentPage === 0 || loading}
                  >
                    ← Previous
                  </button>
                  <span className="pagination-info">
                    Page {currentPage + 1} of {Math.ceil(results.total / 20)}
                  </span>
                  <button
                    className="pagination-button"
                    onClick={nextPage}
                    disabled={results.start + results.count >= results.total || loading}
                  >
                    Next →
                  </button>
                </div>
              </div>

              <div className="results-list">
                {results.searchResults.map((result, idx) => (
                  <SearchResultCard
                    key={`${result.entity.urn}-${idx}`}
                    result={result}
                    onExpand={handleExplain}
                    explainData={explainData[result.entity.urn]}
                    selected={selectedUrns.has(result.entity.urn)}
                    onToggleSelect={toggleSelection}
                  />
                ))}
              </div>

              {results.searchResults.length > 0 && (
                <div className="pagination-controls">
                  <button
                    className="pagination-button"
                    onClick={previousPage}
                    disabled={currentPage === 0 || loading}
                  >
                    ← Previous
                  </button>
                  <span className="pagination-info">
                    Page {currentPage + 1} of {Math.ceil(results.total / 20)}
                  </span>
                  <button
                    className="pagination-button"
                    onClick={nextPage}
                    disabled={results.start + results.count >= results.total || loading}
                  >
                    Next →
                  </button>
                </div>
              )}

              {results.searchResults.length === 0 && (
                <div className="no-results">
                  No results found for "{query}"
                </div>
              )}
            </div>
          )}

          {activeSubTab === 'ai-explain' && (
            <AIExplainView
              analysis={aiAnalysis}
              loading={analyzingRanking}
              selectedCount={selectedUrns.size}
            />
          )}
        </>
      )}

      {!results && !loading && !error && (
        <div className="search-empty-state">
          <p>Enter a search query to get started</p>
        </div>
      )}
    </div>
  );
}
