import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { RankingAnalysisResponse } from '../api/types';

interface AIExplainViewProps {
  analysis: RankingAnalysisResponse | null;
  loading: boolean;
  selectedCount: number;
}

export function AIExplainView({ analysis, loading, selectedCount }: AIExplainViewProps) {
  if (loading) {
    return (
      <div className="ai-explain-loading">
        <div className="spinner"></div>
        <p>Analyzing {selectedCount} selected results...</p>
      </div>
    );
  }

  if (!analysis) {
    return (
      <div className="ai-explain-empty">
        <p>No analysis yet. Select results and click "Explain" to get AI-powered ranking insights.</p>
      </div>
    );
  }

  return (
    <div className="ai-explain-view">
      <div className="ai-explain-header">
        <h3>AI Ranking Analysis</h3>
        <div className="ai-explain-meta">
          <span className="model-badge">{analysis.model}</span>
          <span className="token-info">
            Tokens: {analysis.tokens.inputTokens || 0} in / {analysis.tokens.outputTokens || 0} out
          </span>
        </div>
      </div>

      <div className="ai-explain-content">
        <div className="markdown-content">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {analysis.analysis}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}
