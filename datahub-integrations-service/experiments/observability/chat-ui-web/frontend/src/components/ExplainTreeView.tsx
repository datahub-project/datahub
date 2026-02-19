import { useState, useEffect } from 'react';
import './ExplainTreeView.css';

interface ExplanationNode {
  value: number;
  description: string;
  match?: boolean;
  details?: ExplanationNode[];
}

interface Props {
  explanation: ExplanationNode;
  level?: number;
  expandAll?: boolean;
}

interface ParsedSummary {
  operation: string;
  field?: string;
  term?: string;
  parameters: string[];
}

/**
 * ExplainTreeView - Transforms Elasticsearch explain JSON into an interactive,
 * user-friendly tree view inspired by Galaxy Hubble's approach.
 *
 * Features:
 * - Progressive disclosure with collapsible sections
 * - Semantic one-line summaries
 * - Color-coded signal values
 * - Smart number formatting
 * - Field/term extraction from descriptions
 */
export function ExplainTreeView({ explanation, level = 0, expandAll = false }: Props) {
  const [isOpen, setIsOpen] = useState(level < 2); // Auto-expand first 2 levels

  // Reset to default state when expandAll is toggled off
  useEffect(() => {
    if (!expandAll) {
      setIsOpen(level < 2);
    }
  }, [expandAll, level]);

  if (!explanation) {
    return null;
  }

  const summary = parseSummary(explanation);
  const hasChildren = explanation.details && explanation.details.length > 0;

  // When expandAll is true, force open; otherwise use local state
  const shouldBeOpen = expandAll ? true : isOpen;

  return (
    <div className="explain-tree-node" style={{ marginLeft: `${level * 16}px` }}>
      <details open={shouldBeOpen} onToggle={(e) => setIsOpen((e.target as HTMLDetailsElement).open)}>
        <summary className="explain-summary">
          <div className="explain-summary-line">
            <span className="explain-toggle">{hasChildren ? (shouldBeOpen ? '▼' : '▶') : '·'}</span>
            <span className="explain-score">{formatScore(explanation.value)}</span>
            <span className="explain-equals"> = </span>
            <span className="explain-description">
              {renderSummary(summary, explanation)}
            </span>
          </div>
        </summary>

        {hasChildren && (
          <div className="explain-children">
            {explanation.details!.map((child, idx) => (
              <ExplainTreeView
                key={idx}
                explanation={child}
                level={level + 1}
                expandAll={expandAll}
              />
            ))}
          </div>
        )}
      </details>
    </div>
  );
}

/**
 * Parse description to extract semantic components
 */
function parseSummary(node: ExplanationNode): ParsedSummary {
  const desc = node.description || '';

  // Extract operation type
  let operation = 'unknown';
  if (desc.includes('sum of:')) operation = 'sum';
  else if (desc.includes('product of:')) operation = 'product';
  else if (desc.includes('max of:')) operation = 'max';
  else if (desc.includes('weight(')) operation = 'weight';
  else if (desc.includes('score(')) operation = 'score';
  else if (desc.includes('idf, computed as')) operation = 'idf';
  else if (desc.includes('idf')) operation = 'idf';
  else if (desc.includes('tf, computed as')) operation = 'tf';
  else if (desc.includes('tf')) operation = 'tf';
  else if (desc.includes('boost')) operation = 'boost';
  else if (desc.includes('queryWeight')) operation = 'queryWeight';
  else if (desc.includes('fieldWeight')) operation = 'fieldWeight';
  else if (desc.includes('function score')) operation = 'function';
  // BM25 parameters
  else if (desc.match(/^n,/)) operation = 'n';
  else if (desc.match(/^N,/)) operation = 'N';
  else if (desc.match(/^freq,/)) operation = 'freq';
  else if (desc.match(/^k1,/)) operation = 'k1';
  else if (desc.match(/^b,/)) operation = 'b';
  else if (desc.match(/^dl,/)) operation = 'dl';
  else if (desc.match(/^avgdl,/)) operation = 'avgdl';
  else operation = desc.split(',')[0].split('(')[0].trim();

  // Extract field name (e.g., "name", "urn", "description")
  const fieldMatch = desc.match(/\b(name|urn|description|title|tags|properties|browsePaths|fieldTags|editedFieldTags|glossaryTerms|domain)[\.:]/i);
  const field = fieldMatch ? fieldMatch[1] : undefined;

  // Extract search term
  const termMatch = desc.match(/[:=](\w+)/);
  const term = termMatch ? termMatch[1] : undefined;

  // Extract numeric parameters
  const parameters: string[] = [];
  const paramMatches = desc.matchAll(/(\w+)=([0-9.]+)/g);
  for (const match of paramMatches) {
    parameters.push(`${match[1]}=${match[2]}`);
  }

  return { operation, field, term, parameters };
}

/**
 * Render semantic summary with color coding and detailed description
 */
function renderSummary(summary: ParsedSummary, node: ExplanationNode) {
  const parts: JSX.Element[] = [];

  // Map operation types to human-readable labels
  const operationLabels: Record<string, string> = {
    'n': 'n - docs with term',
    'N': 'N - total docs',
    'tf': 'tf - term frequency (normalized)',
    'idf': 'idf - inverse doc frequency',
    'freq': 'freq - raw term count',
    'k1': 'k1 - saturation parameter',
    'b': 'b - length normalization',
    'dl': 'dl - field length',
    'avgdl': 'avgdl - avg field length',
    'boost': 'boost',
    'score': 'score',
    'weight': 'weight',
    'sum': 'sum',
    'product': 'product',
    'max': 'max',
    'queryWeight': 'queryWeight',
    'fieldWeight': 'fieldWeight',
    'function': 'function score',
  };

  const opLabel = operationLabels[summary.operation] || summary.operation;

  // Operation type with descriptive label
  parts.push(
    <span key="op" className="explain-operation">
      [{opLabel}]
    </span>
  );

  // Field name (if present)
  if (summary.field) {
    parts.push(
      <span key="field" className="explain-field">
        {' '}<strong>{summary.field}</strong>
      </span>
    );
  }

  // Term (if present)
  if (summary.term) {
    parts.push(
      <span key="term" className="explain-term">
        :{summary.term}
      </span>
    );
  }

  // Parameters
  if (summary.parameters.length > 0) {
    parts.push(
      <span key="params" className="explain-params">
        {' '}({summary.parameters.join(', ')})
      </span>
    );
  }

  // Fall back to description if parsing didn't extract useful info
  if (parts.length === 1 && summary.operation === 'unknown') {
    return (
      <>
        <span className="explain-raw-description">{node.description}</span>
      </>
    );
  }

  // Always show the full description as secondary text for context
  const showFullDescription = node.description && node.description.length > 0;

  return (
    <>
      <span className="explain-summary-main">{parts}</span>
      {showFullDescription && (
        <div className="explain-full-description">
          {node.description}
        </div>
      )}
    </>
  );
}

/**
 * Format score with appropriate precision
 * - Large numbers (>1): 2 decimal places
 * - Medium (0.01-1): 4 decimal places
 * - Small (<0.01): 6 decimal places
 */
function formatScore(value: number): string {
  if (value === 0) return '0.00';
  if (Math.abs(value) >= 1) return value.toFixed(2);
  if (Math.abs(value) >= 0.01) return value.toFixed(4);
  return value.toFixed(6);
}

/**
 * Extract signal table from explanation tree (for advanced view)
 */
export function extractSignalTable(explanation: ExplanationNode): Array<{
  field: string;
  term?: string;
  tf?: number;
  idf?: number;
  boost?: number;
  score: number;
}> {
  const signals: Array<{
    field: string;
    term?: string;
    tf?: number;
    idf?: number;
    boost?: number;
    score: number;
  }> = [];

  function traverse(node: ExplanationNode, context: Partial<{
    field: string;
    term: string;
    tf: number;
    idf: number;
    boost: number;
  }> = {}) {
    const summary = parseSummary(node);
    const nextContext = { ...context };

    // Update context with new information
    if (summary.field) nextContext.field = summary.field;
    if (summary.term) nextContext.term = summary.term;

    // Extract specific signal values
    if (summary.operation === 'tf') {
      nextContext.tf = node.value;
    } else if (summary.operation === 'idf') {
      nextContext.idf = node.value;
    } else if (summary.operation === 'boost') {
      nextContext.boost = node.value;
    }

    // If we have a field-level score, record it
    if (nextContext.field && summary.operation === 'weight') {
      signals.push({
        field: nextContext.field,
        term: nextContext.term,
        tf: nextContext.tf,
        idf: nextContext.idf,
        boost: nextContext.boost,
        score: node.value,
      });
    }

    // Traverse children
    if (node.details) {
      for (const child of node.details) {
        traverse(child, nextContext);
      }
    }
  }

  traverse(explanation);
  return signals;
}
