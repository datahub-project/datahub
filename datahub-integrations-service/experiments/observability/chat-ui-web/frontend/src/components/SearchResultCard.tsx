import { useState } from 'react';
import type { SearchResultItem } from '../api/types';

interface Props {
  result: SearchResultItem;
  onExpand: (urn: string, entityType: string) => Promise<void>;
  explainData?: any;
  selected: boolean;
  onToggleSelect: (urn: string, result: SearchResultItem) => void;
}

export function SearchResultCard({ result, onExpand, explainData, selected, onToggleSelect }: Props) {
  const [expanded, setExpanded] = useState(false);
  const [loadingExplain, setLoadingExplain] = useState(false);

  const handleExpand = async () => {
    if (!expanded && !explainData) {
      setLoadingExplain(true);
      try {
        await onExpand(result.entity.urn, result.entity.type);
      } catch (err) {
        console.error('Failed to load explain data:', err);
      } finally {
        setLoadingExplain(false);
      }
    }
    setExpanded(!expanded);
  };

  const entityName = result.entity.name || result.entity.urn;
  const platform = result.entity.properties?.platform || result.entity.type;

  return (
    <div className={`search-result-card ${selected ? 'selected' : ''}`}>
      <div className="search-result-header">
        <input
          type="checkbox"
          className="result-checkbox"
          checked={selected}
          onChange={() => onToggleSelect(result.entity.urn, result)}
          aria-label={`Select ${entityName}`}
        />

        <div className="search-result-main">
          <div className="search-result-title">
            <span className="entity-type-badge">{result.entity.type}</span>
            <span className="entity-name">{entityName}</span>
            {result.score !== undefined && (
              <span className="result-score">Score: {result.score.toFixed(3)}</span>
            )}
          </div>
          <div className="search-result-meta">
            <span className="platform-badge">{platform}</span>
            <span className="entity-urn">{result.entity.urn}</span>
          </div>
        </div>
        <button
          className="expand-button"
          onClick={handleExpand}
          disabled={loadingExplain}
        >
          {loadingExplain ? '...' : expanded ? '▼' : '▶'}
        </button>
      </div>

      {result.entity.properties?.description && (
        <div className="search-result-description">
          {result.entity.properties.description}
        </div>
      )}

      {result.matchedFields.length > 0 && (
        <div className="matched-fields">
          <strong>Matched fields:</strong>
          {result.matchedFields.map((field, idx) => (
            <div key={idx} className="matched-field">
              <span className="field-name">{field.name}:</span>
              <span className="field-value">{field.value}</span>
            </div>
          ))}
        </div>
      )}

      {expanded && explainData && (
        <div className="explain-section">
          <h4>Debug Information</h4>
          <div className="explain-data">
            <div className="explain-row">
              <strong>Index:</strong> {explainData.index}
            </div>
            <div className="explain-row">
              <strong>Matched:</strong> {explainData.matched ? 'Yes' : 'No'}
            </div>
            <div className="explain-row">
              <strong>Score:</strong> {explainData.score?.toFixed(3) || 'N/A'}
            </div>
            <div className="explain-row">
              <strong>Explanation:</strong>
              <pre className="explanation-json">
                {JSON.stringify(explainData.explanation, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
