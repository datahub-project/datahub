import { useState } from 'react';
import type { ConnectionConfig } from '../api/types';
import { apiClient } from '../api/client';

interface ConfigPanelProps {
  config: ConnectionConfig;
  onUpdate: (config: ConnectionConfig) => void;
  onTest: () => Promise<{ success: boolean; error?: string }>;
  onGenerateToken: (gms_url: string) => Promise<{ token?: string; error?: string }>;
  loading: boolean;
}

export function ConfigPanel({
  config,
  onUpdate,
  onTest,
  onGenerateToken,
  loading,
}: ConfigPanelProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [localConfig, setLocalConfig] = useState(config);
  const [testResult, setTestResult] = useState<string | null>(null);
  const [showNamespaceSelector, setShowNamespaceSelector] = useState(false);
  const [contexts, setContexts] = useState<string[]>([]);
  const [currentContext, setCurrentContext] = useState<string>('');
  const [selectedContext, setSelectedContext] = useState<string>('');
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [selectedNamespace, setSelectedNamespace] = useState<string>('');

  const handleModeChange = (mode: ConnectionConfig['mode']) => {
    // Set sensible defaults based on mode
    const updates: Partial<ConnectionConfig> = { mode };

    if (mode === 'embedded') {
      // Embedded mode runs agent directly in backend, no integrations service needed
      updates.integrations_url = undefined;
    } else if (mode === 'quickstart' || mode === 'local') {
      updates.integrations_url = 'http://localhost:9003';
      updates.gms_url = 'http://localhost:8080';
    } else if (mode === 'local_service') {
      updates.integrations_url = 'http://localhost:9003';
    } else if (mode === 'remote') {
      updates.integrations_url = 'http://localhost:9003';
      updates.kube_namespace = 'datahub';
      updates.pod_label_selector = 'app=datahub-integrations';
      updates.local_port = 9003;
      updates.remote_port = 9003;
    }

    setLocalConfig({ ...localConfig, ...updates });
  };

  const handleSave = () => {
    onUpdate(localConfig);
    setTestResult(null);
  };

  const handleTest = async () => {
    const result = await onTest();
    setTestResult(result.success ? 'Connection successful!' : result.error || 'Connection failed');
  };

  const handleDiscover = async () => {
    try {
      setTestResult('Fetching kubectl contexts...');
      const result = await apiClient.listKubectlContexts();

      if (result.contexts && result.contexts.length > 0) {
        setContexts(result.contexts);
        setCurrentContext(result.current_context || '');
        setSelectedContext(result.current_context || result.contexts[0]);
        setShowNamespaceSelector(true);
        setTestResult(null);

        // Automatically load namespaces for current context
        await loadNamespacesForContext(result.current_context || result.contexts[0]);
      } else {
        setTestResult(result.error || 'No kubectl contexts found');
      }
    } catch (error) {
      setTestResult(error instanceof Error ? error.message : 'Failed to fetch contexts');
    }
  };

  const loadNamespacesForContext = async (context: string) => {
    try {
      setTestResult('Loading namespaces...');
      const result = await apiClient.listKubectlNamespaces(context);

      if (result.namespaces && result.namespaces.length > 0) {
        setNamespaces(result.namespaces);
        setSelectedNamespace(result.namespaces[0]);
        setTestResult(null);
      } else {
        setNamespaces([]);
        setSelectedNamespace('');
        setTestResult(result.error || 'No namespaces found in this context');
      }
    } catch (error) {
      setNamespaces([]);
      setSelectedNamespace('');
      setTestResult(error instanceof Error ? error.message : 'Failed to fetch namespaces');
    }
  };

  const handleContextChange = async (context: string) => {
    setSelectedContext(context);
    await loadNamespacesForContext(context);
  };

  const handleNamespaceSelect = async () => {
    if (!selectedContext) {
      setTestResult('Please select a context');
      return;
    }
    if (!selectedNamespace) {
      setTestResult('Please select a namespace');
      return;
    }

    try {
      setTestResult('Discovering GMS URL...');
      setShowNamespaceSelector(false);

      const result = await apiClient.discoverGmsUrl(selectedContext, selectedNamespace);

      if (result.gms_url) {
        setLocalConfig({ ...localConfig, gms_url: result.gms_url });
        setTestResult(`GMS URL discovered from context: ${result.context}, namespace: ${result.namespace}`);
      } else {
        setTestResult(result.error || 'Discovery failed');
      }
    } catch (error) {
      setTestResult(error instanceof Error ? error.message : 'Discovery failed');
    }
  };

  const handleGenerateToken = async () => {
    if (!localConfig.gms_url) {
      setTestResult('Please set GMS URL first');
      return;
    }
    const result = await onGenerateToken(localConfig.gms_url);
    if (result.token) {
      setLocalConfig({ ...localConfig, gms_token: result.token });
      setTestResult('Token generated!');
    } else {
      setTestResult(result.error || 'Token generation failed');
    }
  };

  const showIntegrationsUrl = ['quickstart', 'local_service', 'remote', 'local', 'custom'].includes(localConfig.mode);
  const showKubernetesConfig = localConfig.mode === 'remote';
  const showGmsConfig = localConfig.mode !== 'graphql_direct'; // All modes except future graphql_direct need GMS

  return (
    <div className="config-panel">
      <button className="btn-config" onClick={() => setIsOpen(!isOpen)}>
        Settings
      </button>
      {isOpen && (
        <div className="config-modal">
          <div className="config-modal-content">
            <div className="config-header">
              <h2>Configuration</h2>
              <button onClick={() => setIsOpen(false)}>Close</button>
            </div>

            <div className="config-section">
              <label>Connection Mode</label>
              <select
                value={localConfig.mode}
                onChange={(e) => handleModeChange(e.target.value as ConnectionConfig['mode'])}
              >
                <option value="embedded">Embedded Agent (run in backend process)</option>
                {/* Other modes temporarily hidden - backend code remains */}
                {/* <option value="quickstart">Quickstart (local GMS + integrations)</option> */}
                {/* <option value="local_service">Local Service (spawn integrations service)</option> */}
                {/* <option value="remote">Remote Service (kubectl port-forward)</option> */}
                {/* <option value="local">Local Development (all services local)</option> */}
                {/* <option value="custom">Custom (manual configuration)</option> */}
              </select>
              <small className="config-help">
                {localConfig.mode === 'quickstart' && 'Connect to locally running GMS and integrations service'}
                {localConfig.mode === 'embedded' && 'Agent runs directly in the backend - fastest but limited concurrency'}
                {localConfig.mode === 'local_service' && 'Spawns local integrations service with your GMS credentials'}
                {localConfig.mode === 'remote' && 'Port-forwards to remote integrations service via kubectl'}
                {localConfig.mode === 'local' && 'All services running locally for development'}
                {localConfig.mode === 'custom' && 'Manual configuration of all URLs'}
              </small>
            </div>

            {showGmsConfig && (
              <>
                <div className="config-section">
                  <label>GMS URL</label>
                  <input
                    type="text"
                    value={localConfig.gms_url || ''}
                    onChange={(e) => setLocalConfig({ ...localConfig, gms_url: e.target.value })}
                    placeholder="http://localhost:8080 or https://your-instance.acryl.io/api/gms"
                  />
                  <button onClick={handleDiscover} disabled={loading}>
                    Discover via kubectl
                  </button>
                </div>

                <div className="config-section">
                  <label>GMS Token</label>
                  <input
                    type="password"
                    value={localConfig.gms_token || ''}
                    onChange={(e) => setLocalConfig({ ...localConfig, gms_token: e.target.value })}
                    placeholder="Enter token..."
                  />
                  <button onClick={handleGenerateToken} disabled={loading}>
                    Generate Token
                  </button>
                </div>
              </>
            )}

            {showIntegrationsUrl && (
              <div className="config-section">
                <label>Integrations Service URL</label>
                <input
                  type="text"
                  value={localConfig.integrations_url || ''}
                  onChange={(e) => setLocalConfig({ ...localConfig, integrations_url: e.target.value })}
                  placeholder="http://localhost:9003"
                />
                <small className="config-help">
                  {localConfig.mode === 'quickstart' && 'Local integrations service running on this port'}
                  {localConfig.mode === 'local_service' && 'Service will be spawned on this port'}
                  {localConfig.mode === 'remote' && 'Local port after kubectl port-forward'}
                </small>
              </div>
            )}

            {showKubernetesConfig && (
              <>
                <div className="config-section">
                  <label>Kubernetes Namespace</label>
                  <input
                    type="text"
                    value={localConfig.kube_namespace || ''}
                    onChange={(e) => setLocalConfig({ ...localConfig, kube_namespace: e.target.value })}
                    placeholder="datahub"
                  />
                </div>

                <div className="config-section">
                  <label>Kubernetes Context (optional)</label>
                  <input
                    type="text"
                    value={localConfig.kube_context || ''}
                    onChange={(e) => setLocalConfig({ ...localConfig, kube_context: e.target.value })}
                    placeholder="Leave empty for current context"
                  />
                </div>

                <div className="config-section">
                  <label>Pod Label Selector</label>
                  <input
                    type="text"
                    value={localConfig.pod_label_selector || ''}
                    onChange={(e) => setLocalConfig({ ...localConfig, pod_label_selector: e.target.value })}
                    placeholder="app=datahub-integrations"
                  />
                </div>

                <div className="config-section">
                  <label>Port Forward Configuration</label>
                  <div className="port-config">
                    <input
                      type="number"
                      value={localConfig.local_port || 9003}
                      onChange={(e) => setLocalConfig({ ...localConfig, local_port: parseInt(e.target.value) })}
                      placeholder="Local port"
                    />
                    <span>→</span>
                    <input
                      type="number"
                      value={localConfig.remote_port || 9003}
                      onChange={(e) => setLocalConfig({ ...localConfig, remote_port: parseInt(e.target.value) })}
                      placeholder="Remote port"
                    />
                  </div>
                  <small className="config-help">Local port → Remote pod port</small>
                </div>
              </>
            )}

            <div className="config-actions">
              <button onClick={handleTest} disabled={loading}>
                Test Connection
              </button>
              <button onClick={handleSave} disabled={loading}>
                Save
              </button>
            </div>

            {testResult && (
              <div className={`config-result ${testResult.includes('success') ? 'success' : 'error'}`}>
                {testResult}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Context & Namespace Selector Modal */}
      {showNamespaceSelector && (
        <div className="config-modal">
          <div className="config-modal-content namespace-selector">
            <div className="config-header">
              <h2>Discover via kubectl</h2>
              <button onClick={() => setShowNamespaceSelector(false)}>Cancel</button>
            </div>

            <div className="config-section">
              <p>
                Found {contexts.length} kubectl context{contexts.length !== 1 ? 's' : ''}
                {currentContext && (
                  <>
                    {' '}· Current: <strong>{currentContext}</strong>
                  </>
                )}
              </p>

              <label>Select kubectl context:</label>
              <select
                value={selectedContext}
                onChange={(e) => handleContextChange(e.target.value)}
                className="namespace-list"
                style={{ marginBottom: '1rem' }}
              >
                {contexts.map((ctx) => (
                  <option key={ctx} value={ctx}>
                    {ctx} {ctx === currentContext ? '(current)' : ''}
                  </option>
                ))}
              </select>
            </div>

            <div className="config-section">
              <p>
                Found {namespaces.length} namespace{namespaces.length !== 1 ? 's' : ''} in context: <strong>{selectedContext}</strong>
              </p>
              <label>Select namespace to discover GMS URL from:</label>
              <select
                value={selectedNamespace}
                onChange={(e) => setSelectedNamespace(e.target.value)}
                size={Math.min(namespaces.length, 10)}
                className="namespace-list"
              >
                {namespaces.map((ns) => (
                  <option key={ns} value={ns}>
                    {ns}
                  </option>
                ))}
              </select>
            </div>

            <div className="config-actions">
              <button onClick={handleNamespaceSelect} disabled={!selectedContext || !selectedNamespace || loading}>
                Discover GMS URL
              </button>
              <button onClick={() => setShowNamespaceSelector(false)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
