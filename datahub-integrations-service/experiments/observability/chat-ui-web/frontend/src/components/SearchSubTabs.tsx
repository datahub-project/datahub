interface SearchSubTabsProps {
  activeTab: 'results' | 'ai-explain';
  onTabChange: (tab: 'results' | 'ai-explain') => void;
  selectedCount: number;
  hasAnalysis: boolean;
}

export function SearchSubTabs({ activeTab, onTabChange, selectedCount, hasAnalysis }: SearchSubTabsProps) {
  return (
    <div className="search-sub-tabs">
      <button
        className={`tab ${activeTab === 'results' ? 'active' : ''}`}
        onClick={() => onTabChange('results')}
      >
        Results
        {selectedCount > 0 && <span className="count-badge">{selectedCount}</span>}
      </button>
      <button
        className={`tab ${activeTab === 'ai-explain' ? 'active' : ''}`}
        onClick={() => onTabChange('ai-explain')}
        disabled={!hasAnalysis}
      >
        AI Explain
        {hasAnalysis && <span className="status-indicator">✓</span>}
      </button>
    </div>
  );
}
