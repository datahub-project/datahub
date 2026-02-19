import { useState, useMemo } from 'react';
import type { Stage2Configuration, SignalConfig } from '../api/types';

interface Stage2ConfigEditorProps {
  serverConfig: Stage2Configuration;
  enabled: boolean;
  onEnabledChange: (enabled: boolean) => void;
  formula: string;
  onFormulaChange: (formula: string) => void;
  signals: SignalConfig[];
  onSignalsChange: (signals: SignalConfig[]) => void;
  onApplyPreset: (presetConfig: string) => void;
}

export function Stage2ConfigEditor({
  serverConfig,
  enabled,
  onEnabledChange,
  formula,
  onFormulaChange,
  signals,
  onSignalsChange,
  onApplyPreset,
}: Stage2ConfigEditorProps) {
  const [showFormulaEditor, setShowFormulaEditor] = useState(false);
  const [showSignalEditor, setShowSignalEditor] = useState(false);
  const [showInfo, setShowInfo] = useState(false);
  const [selectedPreset, setSelectedPreset] = useState('');

  // Check if current config differs from server defaults
  const hasOverrides = useMemo(() => {
    if (enabled !== serverConfig.enabled) return true;
    if (formula !== serverConfig.formula) return true;
    if (JSON.stringify(signals) !== JSON.stringify(serverConfig.signals)) return true;
    return false;
  }, [enabled, formula, signals, serverConfig]);

  const handleResetToDefaults = () => {
    onEnabledChange(serverConfig.enabled);
    onFormulaChange(serverConfig.formula || '');
    onSignalsChange(serverConfig.signals || []);
    setSelectedPreset('');
  };

  const handlePresetChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const presetName = e.target.value;
    setSelectedPreset(presetName);
    if (presetName === '') return;

    const preset = serverConfig.presets.find(p => p.name === presetName);
    if (preset) {
      onApplyPreset(preset.config);
    }
  };

  const handleSignalBoostChange = (index: number, newBoost: number) => {
    const updatedSignals = [...signals];
    updatedSignals[index] = { ...updatedSignals[index], boost: newBoost };
    onSignalsChange(updatedSignals);
  };

  const formulaPreview = useMemo(() => {
    const text = formula?.trim() || '';
    if (!text) return '—';
    return text.length > 60 ? `${text.slice(0, 60)}...` : text;
  }, [formula]);

  return (
    <div className="stage2-config-editor">
      <div className="config-section-header">
        <h4>Stage 2 Configuration</h4>
        <div className="header-actions">
          <span className={`mode-badge ${serverConfig.mode === 'JAVA_EXP4J' ? 'mode-java' : 'mode-es'}`}>
            {serverConfig.mode === 'JAVA_EXP4J' ? 'Java/exp4j' : 'ES Painless'}
          </span>
          {hasOverrides && (
            <button className="reset-button" onClick={handleResetToDefaults}>
              Reset to Server Defaults
            </button>
          )}
          <button className="info-button" onClick={() => setShowInfo(!showInfo)} title="Show info">
            ℹ️
          </button>
        </div>
      </div>

      {showInfo && (
        <div className="info-panel">
          <h5>About Stage 2 Rescoring</h5>
          <p>
            <strong>Java/exp4j Mode:</strong> Uses a mathematical formula to combine normalized signals
            (BM25 score, quality flags, usage metrics) into a final score. Provides full explainability.
          </p>
          <p>
            <strong>Formula:</strong> The exp4j expression uses <code>pow(signal, boost)</code> to weight
            each normalized signal. Higher boost = more influence on ranking.
          </p>
          <p>
            <strong>Signals:</strong> Each signal is normalized to a range (typically 1.0-2.0) before
            being used in the formula. Boolean signals map true/false to specific values.
          </p>
        </div>
      )}

      {/* Enable/Disable Toggle */}
      <div className="config-row">
        <label className="config-label">Status</label>
        <div className="toggle-control">
          <label className="toggle-label">
            <input
              type="checkbox"
              checked={enabled}
              onChange={(e) => onEnabledChange(e.target.checked)}
            />
            <span className="toggle-text">{enabled ? 'Enabled' : 'Disabled'}</span>
          </label>
          <span className="server-default-hint">
            Server: {serverConfig.enabled ? 'Enabled' : 'Disabled'}
          </span>
        </div>
      </div>

      {/* Preset Selector */}
      <div className="config-row">
        <label className="config-label">Preset</label>
        <select className="preset-select" onChange={handlePresetChange} value={selectedPreset}>
          <option value="">Select a preset...</option>
          {serverConfig.presets.map((preset) => (
            <option key={preset.name} value={preset.name} title={preset.description}>
              {preset.name}
            </option>
          ))}
        </select>
      </div>

      {enabled && serverConfig.mode === 'JAVA_EXP4J' && (
        <>
          {/* Formula Editor */}
          <div className="config-row">
            <label className="config-label">
              Formula
              <button
                className="expand-button"
                onClick={() => setShowFormulaEditor(!showFormulaEditor)}
              >
                {showFormulaEditor ? '▼' : '▶'}
              </button>
            </label>
            {!showFormulaEditor && (
              <code className="formula-preview">{formulaPreview}</code>
            )}
          </div>

          {showFormulaEditor && (
            <div className="formula-editor-container">
              <textarea
                className="formula-editor"
                value={formula}
                onChange={(e) => onFormulaChange(e.target.value)}
                placeholder="Enter exp4j formula..."
                rows={3}
              />
              <div className="formula-help">
                Available variables: {signals.map(s => s.normalizedName).join(', ')}
              </div>
            </div>
          )}

          {/* Signal Boosts */}
          <div className="config-row">
            <label className="config-label">
              Signal Boosts
              <button
                className="expand-button"
                onClick={() => setShowSignalEditor(!showSignalEditor)}
              >
                {showSignalEditor ? '▼' : '▶'}
              </button>
            </label>
            {!showSignalEditor && (
              <span className="signals-summary">
                {signals.length} signals configured
              </span>
            )}
          </div>

          {showSignalEditor && (
            <div className="signals-editor">
              <table className="signals-table">
                <thead>
                  <tr>
                    <th>Signal</th>
                    <th>Field</th>
                    <th>Type</th>
                    <th>Boost</th>
                    <th>Normalization</th>
                  </tr>
                </thead>
                <tbody>
                  {signals.map((signal, index) => (
                    <tr key={signal.name}>
                      <td>
                        <span className="signal-name">{signal.name}</span>
                        <span className="signal-var">({signal.normalizedName})</span>
                      </td>
                      <td><code>{signal.fieldPath}</code></td>
                      <td>
                        <span className={`type-badge type-${signal.type.toLowerCase()}`}>
                          {signal.type}
                        </span>
                      </td>
                      <td>
                        <input
                          type="number"
                          className="boost-input"
                          value={signal.boost}
                          step="0.1"
                          min="0"
                          max="5"
                          onChange={(e) => {
                            const value = parseFloat(e.target.value);
                            if (Number.isFinite(value)) {
                              handleSignalBoostChange(index, value);
                            }
                          }}
                        />
                      </td>
                      <td>
                        <span className="normalization-info">
                          {signal.normalization.type}
                          {signal.normalization.type === 'BOOLEAN' && (
                            <span className="norm-values">
                              (T:{signal.normalization.trueValue}, F:{signal.normalization.falseValue})
                            </span>
                          )}
                          {signal.normalization.type === 'SIGMOID' && (
                            <span className="norm-values">
                              (max:{signal.normalization.inputMax}, out:{signal.normalization.outputMin}-{signal.normalization.outputMax})
                            </span>
                          )}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {/* Override Indicator */}
      {hasOverrides && (
        <div className="override-warning">
          ⚠️ Using custom configuration (differs from server default)
        </div>
      )}
    </div>
  );
}
