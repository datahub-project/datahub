import { useState, useEffect } from 'react';
import type { ThinkingBlock as ThinkingBlockType } from '../utils/messageParser';

interface ThinkingBlockProps {
  thinking: ThinkingBlockType;
  index: number;
  isLive?: boolean;
  duration?: number;
}

export function ThinkingBlock({ thinking, index, isLive = false, duration }: ThinkingBlockProps) {
  const [isExpanded, setIsExpanded] = useState(isLive);

  // Auto-collapse when streaming completes (isLive changes from true to false)
  useEffect(() => {
    if (!isLive) {
      setIsExpanded(false);
    }
  }, [isLive]);

  const toggleExpanded = () => setIsExpanded(!isExpanded);

  const getConfidenceBadge = (confidence?: string) => {
    if (!confidence) return null;

    const classes = {
      high: 'confidence-high',
      medium: 'confidence-medium',
      low: 'confidence-low'
    };

    return (
      <span className={`confidence-badge ${classes[confidence as keyof typeof classes] || ''}`}>
        {confidence.toUpperCase()}
      </span>
    );
  };

  // Structured rendering if we have parsed reasoning
  const structured = thinking.structured;
  const hasStructured = structured && (structured.action || structured.rationale);

  // Debug logging
  console.log('ThinkingBlock:', {
    hasStructured,
    structured,
    contentPreview: thinking.content.substring(0, 100)
  });

  // Use metadata if available (don't estimate)
  const tokens = thinking.tokens; // Only use real token counts, not estimates
  const actualDuration = thinking.duration !== undefined ? thinking.duration : duration;

  return (
    <div className="thinking-block">
      <div className="thinking-header" onClick={toggleExpanded}>
        <div className="thinking-header-left">
          <span className="thinking-icon">{isLive ? '⚡' : '🧠'}</span>
          <span className="thinking-label">
            {isLive ? 'Thinking...' : hasStructured && structured.action
              ? `${structured.action.slice(0, 50)}${structured.action.length > 50 ? '...' : ''}`
              : `Reasoning ${index + 1}`}
          </span>
          {!isLive && (
            <>
              <span className="thinking-metadata">
                {tokens !== undefined && `${tokens} tokens`}
                {tokens !== undefined && actualDuration !== undefined && ', '}
                {actualDuration !== undefined && `${actualDuration.toFixed(2)}s`}
              </span>
            </>
          )}
        </div>
        <button className="thinking-toggle" aria-label={isExpanded ? 'Collapse' : 'Expand'}>
          {isExpanded ? '▼' : '▶'}
        </button>
      </div>

      {isExpanded && (
        <div className="thinking-content">
          {hasStructured ? (
            <div className="thinking-structured">
              {structured.action && (
                <div className="reasoning-section">
                  <div className="reasoning-label">🎯 Action</div>
                  <div className="reasoning-value">{structured.action}</div>
                </div>
              )}

              {structured.rationale && (
                <div className="reasoning-section">
                  <div className="reasoning-label">💭 Rationale</div>
                  <div className="reasoning-value">{structured.rationale}</div>
                </div>
              )}

              {structured.confidence && (
                <div className="reasoning-section">
                  <div className="reasoning-label">Confidence</div>
                  <div className="reasoning-value">
                    {getConfidenceBadge(structured.confidence)}
                  </div>
                </div>
              )}

              {structured.plan_id && (
                <div className="reasoning-section">
                  <div className="reasoning-label">📋 Plan ID</div>
                  <div className="reasoning-value reasoning-monospace">{structured.plan_id}</div>
                </div>
              )}

              {structured.step && (
                <div className="reasoning-section">
                  <div className="reasoning-label">Step</div>
                  <div className="reasoning-value">{structured.step}</div>
                </div>
              )}
            </div>
          ) : (
            <div className="thinking-text">
              {thinking.content}
            </div>
          )}

          <details className="thinking-raw">
            <summary className="thinking-raw-summary">View raw XML</summary>
            <pre className="thinking-raw-content">
              <code>{thinking.raw}</code>
            </pre>
          </details>
        </div>
      )}
    </div>
  );
}
