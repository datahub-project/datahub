import { useState, useEffect } from 'react';
import { useSearch } from '../hooks/useSearch';
import { useSearchConfiguration } from '../hooks/useSearchConfiguration';
import { SearchSubTabs } from './SearchSubTabs';
import { SearchResultCard } from './SearchResultCard';
import { AIExplainView } from './AIExplainView';
import { SearchConfig } from './SearchConfig';
import { SearchStats } from './SearchStats';
import { Stage2ConfigEditor } from './Stage2ConfigEditor';
import type { SignalConfig } from '../api/types';

type SearchSubTab = 'results' | 'ai-explain';

interface QueryPreset {
  query: string;
  name?: string;
  expectedBehavior: string;
  category?: string;
  customer?: string;
  priority?: 'high' | 'medium' | 'low';
  notes?: string;
}

interface QueryPresetsFile {
  version?: string;
  description?: string;
  presets: QueryPreset[];
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(',')}}`;
}

const COMMON_ENTITY_TYPES = [
  { value: 'DATASET', label: 'Datasets' },
  { value: 'DASHBOARD', label: 'Dashboards' },
  { value: 'CHART', label: 'Charts' },
  { value: 'DATA_JOB', label: 'Data Jobs' },
  { value: 'DATA_FLOW', label: 'Data Flows' },
  { value: 'CONTAINER', label: 'Containers' },
  { value: 'GLOSSARY_TERM', label: 'Glossary Terms' },
  { value: 'TAG', label: 'Tags' },
  { value: 'DOMAIN', label: 'Domains' },
  { value: 'CORP_USER', label: 'Users' },
  { value: 'CORP_GROUP', label: 'Groups' },
];

const FUNCTION_SCORE_PRESETS = {
  'Server Default - Use production configuration': '',
  'Quality Signals Only - Description, Owners, Deprecation (Recommended for Stage 1)': JSON.stringify({
    functions: [
      { filter: { term: { hasDescription: { value: true }}}, weight: 3.0 },
      { filter: { term: { hasOwners: { value: true }}}, weight: 2.0 },
      { filter: { term: { deprecated: { value: true }}}, weight: -10.0 }
    ],
    score_mode: "sum",
    boost_mode: "sum"
  }, null, 2),
  'Quality + Popularity - Includes viewCount & usageCount (⚠️ Conflicts with Stage 2 Rescore)': JSON.stringify({
    functions: [
      { filter: { term: { hasDescription: { value: true }}}, weight: 3.0 },
      { filter: { term: { hasOwners: { value: true }}}, weight: 2.0 },
      { filter: { term: { deprecated: { value: true }}}, weight: -10.0 },
      { filter: { exists: { field: "viewCount" }}, script_score: { script: { source: "Math.log1p(doc['viewCount'].value)" }}, weight: 2.0 },
      { filter: { exists: { field: "usageCount" }}, script_score: { script: { source: "Math.log1p(doc['usageCount'].value)" }}, weight: 1.5 }
    ],
    score_mode: "sum",
    boost_mode: "sum"
  }, null, 2),
  'Legacy Multiplicative - Active/Deprecated only (Old approach, pre-Stage 2)': JSON.stringify({
    functions: [
      { filter: { term: { active: { value: true }}}, weight: 2.0 },
      { filter: { term: { deprecated: { value: true }}}, weight: 0.25 }
    ],
    score_mode: "multiply",
    boost_mode: "multiply"
  }, null, 2),
  'Pure BM25 - Text relevance only, no quality signals': JSON.stringify({
    functions: [],
    score_mode: "sum",
    boost_mode: "replace"
  }, null, 2)
};

export function SearchTab() {
  const [query, setQuery] = useState('');
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);
  const [showFilters, setShowFilters] = useState(false);
  const [activeSubTab, setActiveSubTab] = useState<SearchSubTab>(() => {
    const saved = localStorage.getItem('searchActiveSubTab');
    return (saved === 'ai-explain' ? 'ai-explain' : 'results');
  });
  const [functionScoreOverride, setFunctionScoreOverride] = useState<string>('');
  const [selectedPreset, setSelectedPreset] = useState<string>('');
  const [showFunctionScoreEditor, setShowFunctionScoreEditor] = useState(false);
  const [rescoreEnabled, setRescoreEnabled] = useState<boolean>(true);
  const [selectedQueryPreset, setSelectedQueryPreset] = useState<number>(-1);
  const [queryPresets, setQueryPresets] = useState<QueryPreset[]>(() => {
    const saved = localStorage.getItem('queryPresets');
    if (!saved) return [];
    try {
      const parsed = JSON.parse(saved);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  });
  const [showStage2Editor, setShowStage2Editor] = useState(false);

  // Stage 2 configuration state
  const { stage2Config, loading: configLoading } = useSearchConfiguration();
  const [stage2Formula, setStage2Formula] = useState<string>('');
  const [stage2Signals, setStage2Signals] = useState<SignalConfig[]>([]);

  // Initialize Stage 2 config from server defaults
  useEffect(() => {
    if (stage2Config && !configLoading) {
      setStage2Formula(stage2Config.formula || '');
      setStage2Signals(stage2Config.signals || []);
      setRescoreEnabled(stage2Config.enabled);
    }
  }, [stage2Config, configLoading]);

  const {
    results,
    loading,
    error,
    explainData,
    currentPage,
    currentQuery,
    searchType,
    setSearchType,
    latency,
    resultsPerPage,
    selectedUrns,
    aiAnalysis,
    analyzingRanking,
    search,
    explainResult,
    nextPage,
    previousPage,
    goToPage,
    changeResultsPerPage,
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

  // Helper to check if Stage 2 config differs from server defaults
  // Accepts optional overrides to avoid reading stale state in event handlers
  const hasStage2Overrides = (enabledOverride?: boolean) => {
    if (!stage2Config) return false;
    const effectiveEnabled = enabledOverride !== undefined ? enabledOverride : rescoreEnabled;
    return effectiveEnabled !== stage2Config.enabled ||
      stage2Formula !== stage2Config.formula ||
      stableStringify(stage2Signals) !== stableStringify(stage2Config.signals);
  };

  const hasStage2ValueOverrides = () => {
    if (!stage2Config) return false;
    return stage2Formula !== stage2Config.formula ||
      stableStringify(stage2Signals) !== stableStringify(stage2Config.signals);
  };

  // Build Stage 2 override params
  const getStage2Overrides = (enabledOverride?: boolean) => {
    if (!hasStage2Overrides(enabledOverride)) return { formula: undefined, signals: undefined };
    return {
      formula: stage2Formula || undefined,
      signals: stage2Signals.length > 0 ? JSON.stringify(stage2Signals) : undefined,
    };
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    const { formula, signals } = getStage2Overrides();
    search(query, selectedTypes.length > 0 ? selectedTypes : undefined, 0, undefined, undefined, functionScoreOverride || undefined, rescoreEnabled, formula, signals);
    setActiveSubTab('results');
  };

  const toggleEntityType = (type: string) => {
    setSelectedTypes(prev =>
      prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]
    );
  };

  const clearFilters = () => {
    setSelectedTypes([]);
  };

  const handleLoadPresets = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const data: QueryPresetsFile = JSON.parse(content);

        if (!data.presets || !Array.isArray(data.presets)) {
          alert('Invalid preset file format: missing "presets" array');
          return;
        }

        // Validate required fields
        const invalid = data.presets.find(p => !p.query || !p.expectedBehavior);
        if (invalid) {
          alert('Invalid preset: all presets must have "query" and "expectedBehavior" fields');
          return;
        }

        setQueryPresets(data.presets);
        localStorage.setItem('queryPresets', JSON.stringify(data.presets));
        alert(`Loaded ${data.presets.length} query presets successfully!`);
      } catch (error) {
        alert(`Failed to load presets: ${error}`);
      }
    };
    reader.readAsText(file);
    // Reset input so same file can be loaded again
    event.target.value = '';
  };

  const handleClearPresets = () => {
    if (confirm('Are you sure you want to clear all loaded query presets?')) {
      setQueryPresets([]);
      setSelectedQueryPreset(-1);
      localStorage.removeItem('queryPresets');
    }
  };

  const handleSearchTypeChange = (newType: 'QUERY_THEN_FETCH' | 'DFS_QUERY_THEN_FETCH') => {
    setSearchType(newType);
    // Re-run search with new search type if we have a current query
    if (currentQuery) {
      const { formula, signals } = getStage2Overrides();
      search(currentQuery, selectedTypes.length > 0 ? selectedTypes : undefined, currentPage, newType, undefined, functionScoreOverride || undefined, rescoreEnabled, formula, signals);
    }
  };

  const handleRescoreEnabledChange = (enabled: boolean) => {
    setRescoreEnabled(enabled);
    // Re-run search with new rescore setting if we have a current query
    if (currentQuery) {
      const { formula, signals } = hasStage2ValueOverrides()
        ? getStage2Overrides(enabled)
        : { formula: undefined, signals: undefined };
      search(currentQuery, selectedTypes.length > 0 ? selectedTypes : undefined, currentPage, undefined, undefined, functionScoreOverride || undefined, enabled, formula, signals);
    }
  };

  const handleApplyStage2Preset = (presetConfig: string) => {
    try {
      const config = JSON.parse(presetConfig);
      if (config.enabled !== undefined) {
        setRescoreEnabled(config.enabled);
      }
      if (config.formula) {
        setStage2Formula(config.formula);
      }
      if (config.signals) {
        setStage2Signals(config.signals);
      }
    } catch (err) {
      alert('Failed to apply preset: ' + (err instanceof Error ? err.message : String(err)));
    }
  };

  const handleExplain = async (urn: string, _entityType: string) => {
    await explainResult(urn);
  };

  const handleAnalyzeClick = async () => {
    await analyzeRanking();
  };

  return (
    <div className="search-tab">
      <div className="search-header">
        <h2>Search Debugging</h2>
        <p className="search-subtitle">Analyze search relevance and scoring</p>
      </div>

      {/* Query Presets Section */}
      <div style={{ marginBottom: '12px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
          <label style={{ fontSize: '0.9em', fontWeight: 600, color: '#555' }}>
            Test Query Presets {queryPresets.length > 0 && `(${queryPresets.length} loaded)`}
          </label>
          <div style={{ display: 'flex', gap: '8px' }}>
            <label
              style={{
                padding: '4px 12px',
                fontSize: '0.85em',
                backgroundColor: '#007bff',
                color: '#fff',
                borderRadius: '4px',
                cursor: 'pointer',
                border: 'none'
              }}
            >
              Load Presets
              <input
                type="file"
                accept=".json"
                onChange={handleLoadPresets}
                style={{ display: 'none' }}
              />
            </label>
            {queryPresets.length > 0 && (
              <button
                type="button"
                onClick={handleClearPresets}
                style={{
                  padding: '4px 12px',
                  fontSize: '0.85em',
                  backgroundColor: '#dc3545',
                  color: '#fff',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  border: 'none'
                }}
              >
                Clear Presets
              </button>
            )}
          </div>
        </div>

        {queryPresets.length === 0 ? (
          <div style={{
            padding: '16px',
            backgroundColor: '#f8f9fa',
            border: '1px dashed #ccc',
            borderRadius: '4px',
            textAlign: 'center',
            color: '#666',
            fontSize: '0.9em'
          }}>
            No query presets loaded. Click "Load Presets" to load a JSON file with test queries.
          </div>
        ) : (
          <>
            <select
              value={selectedQueryPreset}
              onChange={(e) => {
                const index = parseInt(e.target.value);
                setSelectedQueryPreset(index);
                if (index >= 0 && queryPresets[index]) {
                  setQuery(queryPresets[index].query);
                }
              }}
              style={{
                width: '100%',
                padding: '10px',
                borderRadius: '4px',
                border: '1px solid #ddd',
                fontSize: '0.9em',
                backgroundColor: '#fff',
                cursor: 'pointer'
              }}
            >
              <option value="-1">Choose a test query...</option>
              {/* Group by category */}
              {Array.from(new Set(queryPresets.map(p => p.category || 'Other'))).map(category => (
                <optgroup key={category} label={category}>
                  {queryPresets
                    .map((preset, idx) => ({ preset, idx }))
                    .filter(({ preset }) => (preset.category || 'Other') === category)
                    .map(({ preset, idx }) => (
                      <option key={idx} value={idx}>
                        {preset.name || preset.query} - {preset.expectedBehavior}
                      </option>
                    ))}
                </optgroup>
              ))}
            </select>

            {selectedQueryPreset >= 0 && queryPresets[selectedQueryPreset] && (
              <div className="info-panel" style={{ marginTop: '12px' }}>
                <h4>Query Details</h4>
                <div style={{ marginBottom: '8px' }}>
                  <strong>Query:</strong> <code>{queryPresets[selectedQueryPreset].query}</code>
                </div>
                <div style={{ marginBottom: '8px' }}>
                  <strong>Expected Behavior:</strong> {queryPresets[selectedQueryPreset].expectedBehavior}
                </div>
                {queryPresets[selectedQueryPreset].category && (
                  <div style={{ marginBottom: '8px' }}>
                    <strong>Category:</strong> {queryPresets[selectedQueryPreset].category}
                  </div>
                )}
                {queryPresets[selectedQueryPreset].customer && (
                  <div style={{ marginBottom: '8px' }}>
                    <strong>Customer:</strong> {queryPresets[selectedQueryPreset].customer}
                  </div>
                )}
                {queryPresets[selectedQueryPreset].priority && (
                  <div style={{ marginBottom: '8px' }}>
                    <strong>Priority:</strong> <span style={{
                      color: queryPresets[selectedQueryPreset].priority === 'high' ? '#dc3545' :
                             queryPresets[selectedQueryPreset].priority === 'medium' ? '#ffc107' : '#28a745'
                    }}>{queryPresets[selectedQueryPreset].priority?.toUpperCase()}</span>
                  </div>
                )}
                {queryPresets[selectedQueryPreset].notes && (
                  <div>
                    <strong>Notes:</strong> {queryPresets[selectedQueryPreset].notes}
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>

      <form onSubmit={handleSearch} className="search-form">
        <div className="search-input-group">
          <input
            type="text"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              // Clear selected preset if user manually edits the query
              if (selectedQueryPreset >= 0) {
                setSelectedQueryPreset(-1);
              }
            }}
            placeholder="Enter search query or choose a preset above..."
            className="search-input"
            disabled={loading}
          />
          <button
            type="button"
            className="filter-toggle-button"
            onClick={() => setShowFilters(!showFilters)}
            style={{
              padding: '0 16px',
              border: '1px solid #ddd',
              borderRadius: '4px',
              backgroundColor: selectedTypes.length > 0 ? '#e3f2fd' : '#fff',
              cursor: 'pointer',
              fontWeight: selectedTypes.length > 0 ? 'bold' : 'normal'
            }}
          >
            Filters {selectedTypes.length > 0 && `(${selectedTypes.length})`}
          </button>
          <button
            type="submit"
            className="search-button"
            disabled={loading || !query.trim()}
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </form>

      {showFilters && (
        <div style={{
          padding: '16px',
          marginBottom: '16px',
          backgroundColor: '#f5f5f5',
          borderRadius: '4px',
          border: '1px solid #ddd'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
            <strong>Entity Types</strong>
            {selectedTypes.length > 0 && (
              <button
                type="button"
                onClick={clearFilters}
                style={{
                  padding: '4px 12px',
                  fontSize: '0.85em',
                  border: 'none',
                  borderRadius: '4px',
                  backgroundColor: '#fff',
                  cursor: 'pointer',
                  color: '#666'
                }}
              >
                Clear all
              </button>
            )}
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '8px' }}>
            {COMMON_ENTITY_TYPES.map(({ value, label }) => (
              <label
                key={value}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  padding: '8px',
                  backgroundColor: '#fff',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  border: selectedTypes.includes(value) ? '2px solid #1976d2' : '1px solid #ddd',
                }}
              >
                <input
                  type="checkbox"
                  checked={selectedTypes.includes(value)}
                  onChange={() => toggleEntityType(value)}
                  style={{ marginRight: '8px' }}
                />
                {label}
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Function Score Editor */}
      <div style={{ marginBottom: '16px' }}>
        <button
          type="button"
          className="filter-toggle-button"
          onClick={() => setShowFunctionScoreEditor(!showFunctionScoreEditor)}
          style={{
            padding: '8px 16px',
            border: '1px solid #ddd',
            borderRadius: '4px',
            backgroundColor: functionScoreOverride ? '#fff3e0' : '#fff',
            cursor: 'pointer',
            fontWeight: functionScoreOverride ? 'bold' : 'normal',
            fontSize: '0.9em'
          }}
        >
          {showFunctionScoreEditor ? '▼' : '▶'} Function Score Override {functionScoreOverride && '(Active)'}
        </button>
      </div>

      {showFunctionScoreEditor && (
        <div style={{
          padding: '16px',
          marginBottom: '16px',
          backgroundColor: '#fffbf0',
          borderRadius: '4px',
          border: '2px solid #ff9800'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
            <div>
              <strong>Ranking Configuration (Debug Mode)</strong>
              <p style={{ fontSize: '0.85em', color: '#666', margin: '4px 0 0 0' }}>
                Override function scores for instant testing. Changes take effect immediately (&lt;1 sec).
              </p>
            </div>
          </div>

          <div style={{ marginBottom: '12px' }}>
            <label style={{ display: 'block', marginBottom: '4px', fontSize: '0.9em', fontWeight: 'bold' }}>
              Presets:
            </label>
            <select
              value={selectedPreset}
              onChange={(e) => {
                const preset = e.target.value;
                setSelectedPreset(preset);
                if (preset && FUNCTION_SCORE_PRESETS[preset as keyof typeof FUNCTION_SCORE_PRESETS]) {
                  setFunctionScoreOverride(FUNCTION_SCORE_PRESETS[preset as keyof typeof FUNCTION_SCORE_PRESETS]);
                } else {
                  setFunctionScoreOverride('');
                }
              }}
              style={{
                width: '100%',
                padding: '8px',
                borderRadius: '4px',
                border: '1px solid #ddd',
                fontSize: '0.9em'
              }}
            >
              <option value="">Choose a preset...</option>
              {Object.keys(FUNCTION_SCORE_PRESETS).map(preset => (
                <option key={preset} value={preset}>{preset}</option>
              ))}
            </select>
          </div>

          <div style={{ marginBottom: '12px' }}>
            <label style={{ display: 'block', marginBottom: '4px', fontSize: '0.9em', fontWeight: 'bold' }}>
              Function Score JSON:
            </label>
            <textarea
              value={functionScoreOverride}
              onChange={(e) => {
                setFunctionScoreOverride(e.target.value);
                // Reset preset selection when manually editing
                setSelectedPreset('');
              }}
              placeholder='{"functions":[{"filter":{"term":{"active":true}},"weight":5.0}],"score_mode":"sum","boost_mode":"sum"}'
              style={{
                width: '100%',
                minHeight: '200px',
                padding: '8px',
                borderRadius: '4px',
                border: '1px solid #ddd',
                fontFamily: 'monospace',
                fontSize: '0.85em',
                resize: 'vertical'
              }}
            />
          </div>

          <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
            <button
              type="button"
              onClick={() => {
                try {
                  const parsed = JSON.parse(functionScoreOverride);
                  setFunctionScoreOverride(JSON.stringify(parsed, null, 2));
                } catch (err) {
                  alert('Invalid JSON: ' + (err instanceof Error ? err.message : String(err)));
                }
              }}
              disabled={!functionScoreOverride}
              style={{
                padding: '6px 12px',
                fontSize: '0.85em',
                border: '1px solid #ddd',
                borderRadius: '4px',
                backgroundColor: '#fff',
                cursor: functionScoreOverride ? 'pointer' : 'not-allowed',
                opacity: functionScoreOverride ? 1 : 0.5
              }}
            >
              Format JSON
            </button>
            <button
              type="button"
              onClick={() => setFunctionScoreOverride('')}
              disabled={!functionScoreOverride}
              style={{
                padding: '6px 12px',
                fontSize: '0.85em',
                border: '1px solid #ddd',
                borderRadius: '4px',
                backgroundColor: '#fff',
                cursor: functionScoreOverride ? 'pointer' : 'not-allowed',
                opacity: functionScoreOverride ? 1 : 0.5
              }}
            >
              Clear
            </button>
            <button
              type="button"
              onClick={() => {
                if (functionScoreOverride) {
                  navigator.clipboard.writeText(functionScoreOverride);
                  alert('Copied to clipboard! Paste into search_config.yaml for production use.');
                }
              }}
              disabled={!functionScoreOverride}
              style={{
                padding: '6px 12px',
                fontSize: '0.85em',
                border: '1px solid #1976d2',
                borderRadius: '4px',
                backgroundColor: '#e3f2fd',
                cursor: functionScoreOverride ? 'pointer' : 'not-allowed',
                opacity: functionScoreOverride ? 1 : 0.5,
                fontWeight: 'bold'
              }}
            >
              Copy for YAML
            </button>
          </div>
        </div>
      )}

      {/* Stage 2 Rescore Editor */}
      <div style={{ marginBottom: '16px' }}>
        <button
          type="button"
          className="filter-toggle-button"
          onClick={() => setShowStage2Editor(!showStage2Editor)}
          style={{
            padding: '8px 16px',
            border: '1px solid #ddd',
            borderRadius: '4px',
            backgroundColor: hasStage2Overrides() ? '#e8f5e9' : '#fff',
            cursor: 'pointer',
            fontWeight: hasStage2Overrides() ? 'bold' : 'normal',
            fontSize: '0.9em'
          }}
        >
          {showStage2Editor ? '▼' : '▶'} Stage 2 Rescore Configuration {hasStage2Overrides() && '(Custom)'}
        </button>
      </div>

      {showStage2Editor && stage2Config && (
        <Stage2ConfigEditor
          serverConfig={stage2Config}
          enabled={rescoreEnabled}
          onEnabledChange={handleRescoreEnabledChange}
          formula={stage2Formula}
          onFormulaChange={setStage2Formula}
          signals={stage2Signals}
          onSignalsChange={setStage2Signals}
          onApplyPreset={handleApplyStage2Preset}
        />
      )}

      {error && (
        <div className="search-error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {/* Search Configuration */}
      {results && (
        <SearchConfig
          searchType={searchType}
          onSearchTypeChange={handleSearchTypeChange}
          latency={latency}
          totalResults={results.total}
          rescoreEnabled={rescoreEnabled}
          onRescoreEnabledChange={handleRescoreEnabledChange}
          serverDefaultRescoreEnabled={true}
        />
      )}

      {/* Search Statistics */}
      {results && <SearchStats results={results} totalResults={results.total} query={currentQuery} />}

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
                <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <label style={{ fontSize: '0.9em', color: '#666' }}>Per page:</label>
                    <select
                      value={resultsPerPage}
                      onChange={(e) => changeResultsPerPage(Number(e.target.value))}
                      style={{ padding: '4px 8px', borderRadius: '4px', border: '1px solid #ddd' }}
                    >
                      <option value={20}>20</option>
                      <option value={50}>50</option>
                      <option value={100}>100</option>
                      <option value={500}>500</option>
                      <option value={1000}>1000</option>
                    </select>
                  </div>
                  <div className="pagination-controls pagination-controls-top">
                    <button
                      className="pagination-button"
                      onClick={previousPage}
                      disabled={currentPage === 0 || loading}
                    >
                      ← Previous
                    </button>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span className="pagination-info">Page</span>
                      <input
                        type="number"
                        min={1}
                        max={Math.ceil(results.total / resultsPerPage)}
                        value={currentPage + 1}
                        onChange={(e) => {
                          const page = parseInt(e.target.value, 10) - 1;
                          if (page >= 0 && page < Math.ceil(results.total / resultsPerPage)) {
                            goToPage(page);
                          }
                        }}
                        style={{
                          width: '60px',
                          padding: '4px 8px',
                          borderRadius: '4px',
                          border: '1px solid #ddd',
                          textAlign: 'center'
                        }}
                      />
                      <span className="pagination-info">of {Math.ceil(results.total / resultsPerPage)}</span>
                    </div>
                    <button
                      className="pagination-button"
                      onClick={nextPage}
                      disabled={results.start + results.count >= results.total || loading}
                    >
                      Next →
                    </button>
                  </div>
                </div>
              </div>

              <div className="results-list">
                {results.searchResults.map((result, idx) => (
                  <SearchResultCard
                    key={`${result.entity.urn}-${idx}`}
                    result={result}
                    position={results.start + idx + 1}
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
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span className="pagination-info">Page</span>
                    <input
                      type="number"
                      min={1}
                      max={Math.ceil(results.total / resultsPerPage)}
                      value={currentPage + 1}
                      onChange={(e) => {
                        const page = parseInt(e.target.value, 10) - 1;
                        if (page >= 0 && page < Math.ceil(results.total / resultsPerPage)) {
                          goToPage(page);
                        }
                      }}
                      style={{
                        width: '60px',
                        padding: '4px 8px',
                        borderRadius: '4px',
                        border: '1px solid #ddd',
                        textAlign: 'center'
                      }}
                    />
                    <span className="pagination-info">of {Math.ceil(results.total / resultsPerPage)}</span>
                  </div>
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
