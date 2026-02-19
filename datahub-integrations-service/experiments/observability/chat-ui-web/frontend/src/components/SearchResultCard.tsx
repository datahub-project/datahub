import { useState, useEffect } from 'react';
import type { ExplainResponse, SearchResultItem } from '../api/types';
import { ExplainTreeView } from './ExplainTreeView';
import { Stage2ExplainView } from './Stage2ExplainView';

interface Props {
  result: SearchResultItem;
  position: number;
  onExpand: (urn: string, entityType: string) => Promise<void>;
  explainData?: ExplainResponse;
  selected: boolean;
  onToggleSelect: (urn: string, result: SearchResultItem, position: number) => void;
}

interface ExplanationNode {
  value: number;
  description: string;
  details?: ExplanationNode[];
}

const DESCRIPTION_TRUNCATE_LENGTH = 150;
const CUSTOM_PROPS_COLLAPSED_COUNT = 5;

function truncateText(text: string, maxLength: number): { truncated: string; isTruncated: boolean } {
  if (text.length <= maxLength) {
    return { truncated: text, isTruncated: false };
  }
  return { truncated: text.slice(0, maxLength) + '...', isTruncated: true };
}

function isExplanationNode(value: unknown): value is ExplanationNode {
  if (!value || typeof value !== 'object') return false;
  const node = value as Record<string, unknown>;
  return typeof node.value === 'number' && typeof node.description === 'string';
}

export function SearchResultCard({ result, position, onExpand, explainData, selected, onToggleSelect }: Props) {
  const [expanded, setExpanded] = useState(false);
  const [loadingExplain, setLoadingExplain] = useState(false);
  const [descriptionExpanded, setDescriptionExpanded] = useState(false);
  const [customPropsExpanded, setCustomPropsExpanded] = useState(false);
  const [expandAllTree, setExpandAllTree] = useState(false);
  const [showRawJson, setShowRawJson] = useState(false);
  const [copied, setCopied] = useState(false);
  const [explainTab, setExplainTab] = useState<'stage1' | 'stage2'>('stage1');
  const stage1Explanation = explainData?.explanation;

  // Auto-select tab based on available data
  useEffect(() => {
    if (result.rescoreExplanation && !explainData) {
      // If we have Stage 2 data but no Stage 1 data, show Stage 2
      setExplainTab('stage2');
    } else if (explainData && !result.rescoreExplanation) {
      // If we have Stage 1 data but no Stage 2 data, show Stage 1
      setExplainTab('stage1');
    }
    // If both or neither available, keep current selection
  }, [result.rescoreExplanation, explainData]);

  const copyToClipboard = async () => {
    if (!explainData) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(explainData, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  const downloadJson = () => {
    if (!explainData) return;

    // Generate descriptive filename
    const timestamp = new Date().toISOString().slice(0, 16).replace('T', '_').replace(/:/g, '-');
    const entityType = result.entity.type.toLowerCase();
    const sanitizedName = (result.entity.name || 'entity').replace(/[^a-z0-9]/gi, '_').slice(0, 30);
    const filename = `explain_${entityType}_${sanitizedName}_pos${position}_${timestamp}.json`;

    // Create blob and download
    const jsonContent = JSON.stringify(explainData, null, 2);
    const blob = new Blob([jsonContent], { type: 'application/json;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

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

  // Get both source and user-edited descriptions
  const sourceDescription = result.entity.properties?.description || '';
  const editableDescription = result.entity.editableProperties?.description || '';

  // Determine which descriptions to show
  const hasSourceDesc = sourceDescription.trim().length > 0;
  const hasEditableDesc = editableDescription.trim().length > 0;
  const descriptionsAreDifferent = hasSourceDesc && hasEditableDesc && sourceDescription !== editableDescription;

  const { truncated: truncatedSourceDesc, isTruncated: isSourceDescTruncated } = truncateText(
    sourceDescription,
    DESCRIPTION_TRUNCATE_LENGTH
  );
  const { truncated: truncatedEditableDesc, isTruncated: isEditableDescTruncated } = truncateText(
    editableDescription,
    DESCRIPTION_TRUNCATE_LENGTH
  );

  // Extract ID from URN (last part after the comma for datasets)
  const entityId = result.entity.urn.includes(',')
    ? result.entity.urn.split(',').slice(1).join(',').replace(/\)$/, '')
    : result.entity.urn;

  // Extract ranking-relevant metadata
  const tags = result.entity.tags || [];
  const glossaryTerms = result.entity.glossaryTerms || [];
  const domain = result.entity.domain;
  const subTypes = result.entity.subTypes || [];
  const browsePath = result.entity.browsePath || [];
  const customProperties = result.entity.customProperties || [];

  // Check if we have any searchable metadata to display
  const hasSearchableMetadata = tags.length > 0 || glossaryTerms.length > 0 || domain || subTypes.length > 0 || browsePath.length > 0;

  // Custom properties display logic
  const visibleCustomProps = customPropsExpanded
    ? customProperties
    : customProperties.slice(0, CUSTOM_PROPS_COLLAPSED_COUNT);
  const hasMoreCustomProps = customProperties.length > CUSTOM_PROPS_COLLAPSED_COUNT;

  return (
    <div className={`search-result-card ${selected ? 'selected' : ''}`}>
      <div className="search-result-header">
        <input
          type="checkbox"
          className="result-checkbox"
          checked={selected}
          onChange={() => onToggleSelect(result.entity.urn, result, position)}
          aria-label={`Select ${entityName}`}
        />

        <div
          style={{
            minWidth: '32px',
            height: '32px',
            borderRadius: '50%',
            backgroundColor: '#1976d2',
            color: '#fff',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontWeight: 'bold',
            fontSize: '0.9em',
            marginRight: '12px',
            flexShrink: 0
          }}
          title={`Position ${position}`}
        >
          {position}
        </div>

        <div className="search-result-main">
          <div className="search-result-title">
            <span className="entity-type-badge">{result.entity.type}</span>
            <span className="entity-name">{entityName}</span>
            {result.score !== undefined && (
              <span className="result-score">Score: {result.score.toFixed(3)}</span>
            )}
          </div>
          <div className="search-result-meta">
            <span className="meta-label">Platform:</span>
            <span className="platform-badge">{platform}</span>
          </div>
          <div className="search-result-meta">
            <span className="meta-label">URN:</span>
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

      {/* Score Analysis Section - appears immediately after header when expanded */}
      {expanded && (explainData || result.rescoreExplanation) && (
        <div className="explain-section">
          <div className="explain-header">
            <h4>Score Analysis</h4>
            {/* Tab buttons - show both if we have both types of data */}
            <div className="explain-tabs">
              <button
                className={`explain-tab ${explainTab === 'stage1' ? 'active' : ''}`}
                onClick={() => setExplainTab('stage1')}
                disabled={!explainData}
              >
                Stage 1 (ES)
              </button>
              <button
                className={`explain-tab ${explainTab === 'stage2' ? 'active' : ''}`}
                onClick={() => setExplainTab('stage2')}
                disabled={!result.rescoreExplanation}
              >
                Stage 2 (Rescore)
              </button>
            </div>
          </div>

          {/* Stage 1 Content */}
          {explainTab === 'stage1' && explainData && (
            <>
              <div style={{ padding: '12px', marginBottom: '12px', backgroundColor: '#e8f4f8', borderLeft: '4px solid #0066cc', fontSize: '0.9em' }}>
                <strong>Actual Score:</strong> {explainData.score?.toFixed(3)}
                <span style={{ color: '#666', marginLeft: '8px' }}>
                  ✓ Uses cross-entity IDF (production-accurate)
                </span>
                {explainData.singleIndexScore && explainData.score && Math.abs(explainData.score - explainData.singleIndexScore) > 0.01 && (
                  <div style={{ marginTop: '8px', padding: '6px', backgroundColor: '#f5f5f5', borderRadius: '4px', fontSize: '0.85em', color: '#666' }}>
                    For reference: Single-index score would be {explainData.singleIndexScore?.toFixed(3)} (diff: {Math.abs(explainData.score - explainData.singleIndexScore).toFixed(3)})
                  </div>
                )}
              </div>
              <div className="explain-data">
                <div className="explain-row">
                  <strong>Index:</strong> {explainData.index}
                </div>
                <div className="explain-row">
                  <strong>Matched:</strong> {explainData.matched ? 'Yes' : 'No'}
                </div>
                <div style={{ marginTop: '1rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <strong>Scoring Breakdown</strong>
                    <div style={{ display: 'flex', gap: '8px' }}>
                      <button
                        onClick={() => setExpandAllTree(!expandAllTree)}
                        style={{
                          padding: '4px 12px',
                          fontSize: '0.85em',
                          border: '1px solid #ddd',
                          borderRadius: '4px',
                          backgroundColor: expandAllTree ? '#0066cc' : '#fff',
                          color: expandAllTree ? '#fff' : '#333',
                          cursor: 'pointer',
                        }}
                      >
                        {expandAllTree ? 'Collapse All' : 'Expand All'}
                      </button>
                      <button
                        onClick={() => setShowRawJson(true)}
                        style={{
                          padding: '4px 12px',
                          fontSize: '0.85em',
                          border: '1px solid #ddd',
                          borderRadius: '4px',
                          backgroundColor: '#fff',
                          color: '#333',
                          cursor: 'pointer',
                        }}
                      >
                        Show Raw JSON
                      </button>
                    </div>
                  </div>
                  <div style={{ padding: '12px', border: '1px solid #ddd', borderRadius: '4px', backgroundColor: '#fafafa', maxHeight: '600px', overflow: 'auto' }}>
                    {isExplanationNode(stage1Explanation) && <ExplainTreeView explanation={stage1Explanation} expandAll={expandAllTree} />}
                  </div>
                </div>
              </div>
            </>
          )}

          {/* Stage 2 Content */}
          {explainTab === 'stage2' && result.rescoreExplanation && (
            <Stage2ExplainView explanation={result.rescoreExplanation} />
          )}

          {/* Show message if selected tab has no data */}
          {explainTab === 'stage1' && !explainData && (
            <div className="no-explain-data">
              No Stage 1 explain data available. Click the expand button to load ES explain data.
            </div>
          )}
          {explainTab === 'stage2' && !result.rescoreExplanation && (
            <div className="no-explain-data">
              No Stage 2 rescore data available. Enable Stage 2 rescoring and set includeExplain to see rescore explanations.
            </div>
          )}
        </div>
      )}

      {/* Entity Properties Section */}
      <div className="card-section">
        <div className="section-header">ENTITY PROPERTIES</div>
        <table className="properties-table">
          <tbody>
            <tr>
              <td className="property-label">Name</td>
              <td className="property-value">{entityName}</td>
            </tr>
            <tr>
              <td className="property-label">ID</td>
              <td className="property-value monospace">{entityId}</td>
            </tr>
            {/* User-edited description (if exists and different from source) */}
            {hasEditableDesc && descriptionsAreDifferent && (
              <tr>
                <td className="property-label">Description</td>
                <td className="property-value">
                  <div style={{ fontSize: '0.85em', color: '#666', marginBottom: '4px' }}>
                    ✏️ User-edited (DataHub UI)
                  </div>
                  <span className="description-text">
                    {descriptionExpanded ? editableDescription : truncatedEditableDesc}
                  </span>
                  {isEditableDescTruncated && (
                    <button
                      className="show-more-link"
                      onClick={() => setDescriptionExpanded(!descriptionExpanded)}
                    >
                      {descriptionExpanded ? 'Show less' : 'Show more'}
                    </button>
                  )}
                </td>
              </tr>
            )}
            {/* Source description (from dbt/source system) */}
            {hasSourceDesc && descriptionsAreDifferent && (
              <tr>
                <td className="property-label">Source Documentation</td>
                <td className="property-value">
                  <div style={{ fontSize: '0.85em', color: '#666', marginBottom: '4px' }}>
                    📄 From {platform}
                  </div>
                  <span className="description-text">
                    {descriptionExpanded ? sourceDescription : truncatedSourceDesc}
                  </span>
                  {isSourceDescTruncated && (
                    <button
                      className="show-more-link"
                      onClick={() => setDescriptionExpanded(!descriptionExpanded)}
                    >
                      {descriptionExpanded ? 'Show less' : 'Show more'}
                    </button>
                  )}
                </td>
              </tr>
            )}
            {/* Single description (when only one exists or both are the same) */}
            {(hasSourceDesc || hasEditableDesc) && !descriptionsAreDifferent && (
              <tr>
                <td className="property-label">Description</td>
                <td className="property-value">
                  <span className="description-text">
                    {descriptionExpanded ? (editableDescription || sourceDescription) : (truncatedEditableDesc || truncatedSourceDesc)}
                  </span>
                  {(isEditableDescTruncated || isSourceDescTruncated) && (
                    <button
                      className="show-more-link"
                      onClick={() => setDescriptionExpanded(!descriptionExpanded)}
                    >
                      {descriptionExpanded ? 'Show less' : 'Show more'}
                    </button>
                  )}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Matched Fields Section */}
      {result.matchedFields.length > 0 && (
        <div className="card-section">
          <div className="section-header">
            MATCHED FIELDS
            <span className="count-badge">{result.matchedFields.length}</span>
          </div>
          <table className="properties-table">
            <tbody>
              {result.matchedFields.map((field, idx) => (
                <tr key={idx}>
                  <td className="property-label">{field.name}</td>
                  <td className="property-value monospace">{field.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Searchable Metadata Section */}
      {hasSearchableMetadata && (
        <div className="card-section">
          <div className="section-header">SEARCHABLE METADATA</div>
          <table className="properties-table">
            <tbody>
              {tags.length > 0 && (
                <tr>
                  <td className="property-label">Tags</td>
                  <td className="property-value">
                    <div className="tag-list">
                      {tags.map((tag, idx) => (
                        <span key={idx} className="metadata-tag tag-badge">{tag}</span>
                      ))}
                    </div>
                  </td>
                </tr>
              )}
              {glossaryTerms.length > 0 && (
                <tr>
                  <td className="property-label">Glossary Terms</td>
                  <td className="property-value">
                    <div className="tag-list">
                      {glossaryTerms.map((term, idx) => (
                        <span key={idx} className="metadata-tag term-badge">{term}</span>
                      ))}
                    </div>
                  </td>
                </tr>
              )}
              {domain && (
                <tr>
                  <td className="property-label">Domain</td>
                  <td className="property-value">
                    <span className="metadata-tag domain-badge">{domain}</span>
                  </td>
                </tr>
              )}
              {subTypes.length > 0 && (
                <tr>
                  <td className="property-label">SubType</td>
                  <td className="property-value">
                    <div className="tag-list">
                      {subTypes.map((subType, idx) => (
                        <span key={idx} className="metadata-tag subtype-badge">{subType}</span>
                      ))}
                    </div>
                  </td>
                </tr>
              )}
              {browsePath.length > 0 && (
                <tr>
                  <td className="property-label">Browse Path</td>
                  <td className="property-value monospace">
                    {browsePath.join(' / ')}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Custom Properties Section */}
      {customProperties.length > 0 && (
        <div className="card-section collapsible-section">
          <div
            className="section-header clickable"
            onClick={() => setCustomPropsExpanded(!customPropsExpanded)}
          >
            CUSTOM PROPERTIES
            <span className="count-badge">{customProperties.length}</span>
            <button className="section-toggle">
              {customPropsExpanded ? 'Hide' : 'Show'}
            </button>
          </div>
          <table className="properties-table">
            <tbody>
              {visibleCustomProps.map((prop, idx) => (
                <tr key={idx}>
                  <td className="property-label">{prop.key}</td>
                  <td className="property-value monospace">{prop.value}</td>
                </tr>
              ))}
              {!customPropsExpanded && hasMoreCustomProps && (
                <tr>
                  <td colSpan={2} className="more-props-hint">
                    ... and {customProperties.length - CUSTOM_PROPS_COLLAPSED_COUNT} more
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Raw JSON Modal */}
      {showRawJson && explainData && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            zIndex: 1000,
          }}
          onClick={() => setShowRawJson(false)}
        >
          <div
            style={{
              backgroundColor: '#fff',
              borderRadius: '8px',
              padding: '24px',
              maxWidth: '90vw',
              maxHeight: '90vh',
              overflow: 'auto',
              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
              <h3 style={{ margin: 0 }}>Raw JSON Response</h3>
              <div style={{ display: 'flex', gap: '8px' }}>
                <button
                  onClick={copyToClipboard}
                  style={{
                    padding: '6px 12px',
                    fontSize: '0.9em',
                    border: '1px solid #0066cc',
                    borderRadius: '4px',
                    backgroundColor: copied ? '#0066cc' : '#fff',
                    color: copied ? '#fff' : '#0066cc',
                    cursor: 'pointer',
                    fontWeight: 500,
                    transition: 'all 0.2s',
                  }}
                >
                  {copied ? '✓ Copied!' : '📋 Copy'}
                </button>
                <button
                  onClick={downloadJson}
                  style={{
                    padding: '6px 12px',
                    fontSize: '0.9em',
                    border: '1px solid #0066cc',
                    borderRadius: '4px',
                    backgroundColor: '#fff',
                    color: '#0066cc',
                    cursor: 'pointer',
                    fontWeight: 500,
                  }}
                  title="Download as JSON file"
                >
                  ⬇️ Download
                </button>
                <button
                  onClick={() => setShowRawJson(false)}
                  style={{
                    padding: '6px 12px',
                    fontSize: '0.9em',
                    border: '1px solid #ddd',
                    borderRadius: '4px',
                    backgroundColor: '#fff',
                    cursor: 'pointer',
                  }}
                >
                  Close
                </button>
              </div>
            </div>
            <div style={{ fontSize: '0.9em', color: '#666', marginBottom: '12px' }}>
              Complete explain response including score, index, matched status, and Elasticsearch explanation tree
            </div>
            <pre
              style={{
                backgroundColor: '#f5f5f5',
                padding: '16px',
                borderRadius: '4px',
                overflow: 'auto',
                fontSize: '0.85em',
                fontFamily: 'Monaco, Menlo, Consolas, monospace',
                maxHeight: '70vh',
              }}
            >
              {JSON.stringify(explainData, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
