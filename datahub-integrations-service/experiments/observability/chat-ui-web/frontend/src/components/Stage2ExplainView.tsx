import type { RescoreExplanation } from '../api/types';

interface Stage2ExplainViewProps {
  explanation: RescoreExplanation;
}

export function Stage2ExplainView({ explanation }: Stage2ExplainViewProps) {
  // Convert signals to array (may be object/map from backend)
  const signalsArray = Array.isArray(explanation.signals)
    ? explanation.signals
    : explanation.signals
    ? Object.values(explanation.signals)
    : [];

  // Sort signals by contribution (descending)
  const sortedSignals = signalsArray.slice().sort(
    (a, b) => b.contribution - a.contribution
  );
  const maxContribution = Math.max(
    1,
    ...sortedSignals.map((signal) => signal.contribution),
  );

  // Use the reported rescore value for the footer total
  // We cannot verify locally since we don't know if the formula is multiplicative or additive
  const displayTotal = explanation.rescoreValue;

  // If no signals available, show a message
  if (signalsArray.length === 0) {
    return (
      <div className="stage2-explain-view">
        <div className="empty-state">
          <p>No Stage 2 rescoring data available for this result.</p>
          <p className="hint">This result may not have been rescored, or rescoring is disabled.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="stage2-explain-view">
      {/* Score Flow */}
      <div className="score-flow">
        <div className="score-stage">
          <span className="score-label">BM25 (ES)</span>
          <span className="score-value">{explanation.bm25Score?.toFixed(2) ?? '—'}</span>
        </div>
        <span className="score-arrow">→</span>
        <div className="score-stage">
          <span className="score-label">Rescore</span>
          <span className="score-value">{explanation.rescoreValue?.toFixed(4) ?? '—'}</span>
        </div>
        <span className="score-arrow">+</span>
        <div className="score-stage boost">
          <span className="score-label">Rescore Floor</span>
          <span className="score-value">{explanation.rescoreBoost?.toFixed(2) ?? '—'}</span>
        </div>
        <span className="score-arrow">=</span>
        <div className="score-stage highlight">
          <span className="score-label">Final</span>
          <span className="score-value">{explanation.finalScore?.toFixed(4) ?? '—'}</span>
        </div>
      </div>

      {/* Formula Display */}
      <div className="formula-section">
        <div className="section-header">Formula</div>
        <code className="formula-display">{explanation.formula}</code>
      </div>

      {/* Signal Breakdown Table */}
      <div className="signals-section">
        <div className="section-header">Signal Breakdown</div>
        <table className="signal-breakdown-table">
          <thead>
            <tr>
              <th>Signal</th>
              <th>Raw</th>
              <th>Normalized</th>
              <th>^Boost</th>
              <th>= Contribution</th>
            </tr>
          </thead>
          <tbody>
            {sortedSignals.map((signal) => (
              <tr key={signal.name}>
                <td className="signal-name-cell">
                  <span className="signal-name">{signal.name}</span>
                </td>
                <td className="raw-value-cell">
                  <span className={`raw-value ${signal.rawValue === 'true' ? 'bool-true' : signal.rawValue === 'false' ? 'bool-false' : ''}`}>
                    {signal.rawValue || '—'}
                  </span>
                </td>
                <td className="normalized-cell">
                  <span className="normalized-value">{signal.normalizedValue.toFixed(3)}</span>
                </td>
                <td className="boost-cell">
                  <span className="boost-value">^{signal.boost.toFixed(1)}</span>
                </td>
                <td className="contribution-cell">
                  <span className="contribution-value">{signal.contribution.toFixed(4)}</span>
                  <ContributionBar value={signal.contribution} maxValue={maxContribution} />
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="total-row">
              <td colSpan={4}>
                <strong>Rescore Value</strong>
              </td>
              <td className="contribution-cell">
                <strong className="total-score">{displayTotal?.toFixed(4) ?? '—'}</strong>
              </td>
            </tr>
          </tfoot>
        </table>
      </div>

      {/* Rescore Floor Explanation */}
      <div className="boost-explanation">
        <span className="boost-note">
          Rescore Floor ({explanation.rescoreBoost?.toFixed(2)}) = max non-rescored score + safety margin.
          Ensures all rescored items rank above non-rescored items.
        </span>
      </div>
    </div>
  );
}

// Mini bar chart showing relative contribution
function ContributionBar({ value, maxValue }: { value: number; maxValue: number }) {
  const normalizedPercent = Math.min(
    100,
    Math.max(0, (value / maxValue) * 100),
  );

  // Color based on whether it's a boost (>1) or penalty (<1)
  const barColor = value >= 1.0 ? '#4ade80' : '#f87171';

  return (
    <div className="contribution-bar-container">
      <div
        className="contribution-bar"
        style={{
          width: `${normalizedPercent}%`,
          backgroundColor: barColor,
        }}
      />
    </div>
  );
}
