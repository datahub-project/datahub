import { useState } from 'react';
import type { ToolCall } from '../utils/messageParser';

interface ToolCallBlockProps {
  toolCall: ToolCall;
  result?: any;
  error?: string;
  duration?: number;
}

export function ToolCallBlock({ toolCall, result, error, duration }: ToolCallBlockProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  const toggleExpanded = () => setIsExpanded(!isExpanded);

  const formatJson = (obj: any) => {
    try {
      return JSON.stringify(obj, null, 2);
    } catch {
      return String(obj);
    }
  };

  // Use metadata from toolCall if available
  const tokens = toolCall.tokens || 0;
  const actualDuration = toolCall.duration !== undefined ? toolCall.duration : duration;
  const resultTokens = toolCall.resultTokens || 0;
  const totalTokens = tokens + resultTokens;

  return (
    <div className={`tool-call-block ${error ? 'tool-call-error' : result ? 'tool-call-success' : 'tool-call-pending'}`}>
      <div className="tool-call-header" onClick={toggleExpanded}>
        <div className="tool-call-header-left">
          <span className="tool-call-icon">
            {error ? '❌' : result ? '✓' : '📞'}
          </span>
          <span className="tool-call-name">{toolCall.name}</span>
          <span className="tool-call-metadata">
            {totalTokens > 0 && `${totalTokens} tokens`}
            {actualDuration !== undefined && `, ${actualDuration.toFixed(2)}s`}
          </span>
        </div>
        <button className="tool-call-toggle" aria-label={isExpanded ? 'Collapse' : 'Expand'}>
          {isExpanded ? '▼' : '▶'}
        </button>
      </div>

      {isExpanded && (
        <div className="tool-call-content">
          <div className="tool-call-section">
            <div className="tool-call-section-label">Parameters</div>
            <pre className="tool-call-code">
              <code>{formatJson(toolCall.input)}</code>
            </pre>
          </div>

          {result && !error && (
            <div className="tool-call-section">
              <div className="tool-call-section-label">Result</div>
              {typeof result === 'object' ? (
                <pre className="tool-call-code">
                  <code>{formatJson(result)}</code>
                </pre>
              ) : (
                <div className="tool-call-text">{String(result)}</div>
              )}
            </div>
          )}

          {error && (
            <div className="tool-call-section tool-call-section-error">
              <div className="tool-call-section-label">Error</div>
              <div className="tool-call-error-text">{error}</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
