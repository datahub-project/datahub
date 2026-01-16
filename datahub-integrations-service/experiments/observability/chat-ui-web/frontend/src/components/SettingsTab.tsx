import { useState, useEffect } from 'react';
import type { ConnectionConfig, Profile } from '../api/types';
import { apiClient } from '../api/client';
import { ProfileManager } from './ProfileManager';

interface SettingsTabProps {
  config: ConnectionConfig;
  onUpdate: (config: ConnectionConfig) => void;
  onTest: () => Promise<{ success: boolean; error?: string }>;
  onGenerateToken: (gms_url: string) => Promise<{ token?: string; error?: string }>;
  onSsoLogin: () => Promise<{ success: boolean; message?: string; profile?: string; error?: string }>;
  loading: boolean;
}

export function SettingsTab({
  config,
  onUpdate,
  onTest,
  onGenerateToken,
  onSsoLogin,
  loading,
}: SettingsTabProps) {
  const [localConfig, setLocalConfig] = useState(config);
  const [selectedProfile, setSelectedProfile] = useState<Profile | undefined>(undefined);
  const [testResult, setTestResult] = useState<string | null>(null);
  const [ssoResult, setSsoResult] = useState<string | null>(null);
  const [showNamespaceSelector, setShowNamespaceSelector] = useState(false);
  const [contexts, setContexts] = useState<string[]>([]);
  const [currentContext, setCurrentContext] = useState<string>('');
  const [selectedContext, setSelectedContext] = useState<string>('');
  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [selectedNamespace, setSelectedNamespace] = useState<string>('');
  const [awsProfiles, setAwsProfiles] = useState<string[]>([]);
  const [awsProfilesLoading, setAwsProfilesLoading] = useState(false);

  // Sync localConfig when parent config changes (e.g., after Save)
  useEffect(() => {
    setLocalConfig(config);
  }, [config]);

  // Fetch available AWS profiles on mount
  useEffect(() => {
    const fetchAwsProfiles = async () => {
      setAwsProfilesLoading(true);
      try {
        const result = await apiClient.listAwsProfiles();
        if (result.success && result.profiles.length > 0) {
          setAwsProfiles(result.profiles);
        }
      } catch (err) {
        console.error('Failed to fetch AWS profiles:', err);
      } finally {
        setAwsProfilesLoading(false);
      }
    };

    fetchAwsProfiles();
  }, []);

  const handleModeChange = (mode: ConnectionConfig['mode']) => {
    // Update mode and set appropriate integrations_url
    let integrations_url = localConfig.integrations_url;

    switch (mode) {
      case 'embedded':
        integrations_url = 'embedded://local';
        break;
      case 'local_service':
      case 'local':
      case 'quickstart':
        integrations_url = 'http://localhost:9003';
        break;
      case 'remote':
        integrations_url = `http://localhost:${localConfig.local_port || 9005}`;
        break;
      case 'custom':
        // Keep current integrations_url for custom mode
        break;
      case 'graphql_direct':
        integrations_url = 'graphql://direct';
        break;
    }

    const newConfig = { ...localConfig, mode, integrations_url };
    setLocalConfig(newConfig);
    // Auto-save when mode changes
    onUpdate(newConfig);
    setTestResult(null);
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
        const newConfig = { ...localConfig, gms_url: result.gms_url };
        setLocalConfig(newConfig);
        // Auto-save the discovered GMS URL
        await onUpdate(newConfig);
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
      const newConfig = { ...localConfig, gms_token: result.token };
      setLocalConfig(newConfig);
      // Auto-save the generated token
      await onUpdate(newConfig);
      setTestResult('Token generated and saved!');
    } else {
      setTestResult(result.error || 'Token generation failed');
    }
  };

  const handleSsoLogin = async () => {
    setSsoResult(null);
    const result = await onSsoLogin();
    if (result.success) {
      setSsoResult(`AWS SSO login started for profile: ${result.profile}. Browser should open automatically. After logging in, click "Auto-Discover" or "Generate Token" to continue.`);
    } else {
      setSsoResult(result.error || 'SSO login failed');
    }
  };

  const handleProfileSelect = async (profile: Profile) => {
    setSelectedProfile(profile);
    // Update localConfig with profile data
    // IMPORTANT: Preserve current mode and integrations_url - profiles only define WHERE, not HOW
    const newConfig: ConnectionConfig = {
      ...localConfig,
      gms_url: profile.gms_url,
      gms_token: profile.gms_token,
      kube_context: profile.kube_context,
      kube_namespace: profile.kube_namespace,
      name: profile.name,
      description: profile.description,
      // Keep current mode and integrations_url from localConfig
    };
    setLocalConfig(newConfig);

    // Automatically apply the config so "Test Connection" works immediately
    await onUpdate(newConfig);

    setTestResult(`✅ Profile "${profile.name}" activated and config applied`);
  };

  const handleAwsProfileChange = (profileName: string) => {
    const newConfig = { ...localConfig, aws_profile: profileName };
    setLocalConfig(newConfig);
    // Auto-save AWS profile change
    onUpdate(newConfig);
  };

  return (
    <div className="settings-tab">
      <h2>Connection Settings</h2>

      {/* Step 1: Select DataHub Instance (Profile) */}
      <ProfileManager
        onProfileSelect={handleProfileSelect}
        selectedProfile={selectedProfile}
      />

      <div className="settings-section sso-section">
        <h3>AWS SSO Login</h3>
        <p>If you're getting SSO token expiration errors, click the button below to open the AWS SSO login page in your browser.</p>
        <button onClick={handleSsoLogin} disabled={loading} className="sso-login-btn">
          🌐 Open AWS SSO Login
        </button>
      </div>

      {ssoResult && (
        <div className={`settings-result ${ssoResult.includes('started') ? 'success' : 'error'}`}>
          {ssoResult}
        </div>
      )}

      <div className="settings-section">
        <h3>AWS Profile</h3>
        <p>Select the AWS profile to use for all AWS operations (Bedrock, S3, etc.).</p>
        {awsProfilesLoading ? (
          <p>Loading AWS profiles...</p>
        ) : awsProfiles.length > 0 ? (
          <div className="settings-section">
            <label>AWS Profile</label>
            <select
              value={localConfig.aws_profile || 'default'}
              onChange={(e) => handleAwsProfileChange(e.target.value)}
            >
              <option value="">default</option>
              {awsProfiles.map((profile) => (
                <option key={profile} value={profile}>
                  {profile}
                </option>
              ))}
            </select>
            <small>
              Current: <strong>{localConfig.aws_profile || 'default'}</strong>
            </small>
          </div>
        ) : (
          <p>No AWS profiles found. Configure profiles in ~/.aws/config</p>
        )}
      </div>

      <div className="settings-divider"></div>

      {/* Step 2: Choose Connection Transport (Mode) */}
      <div className="connection-mode-section">
        <h3>Connection Transport</h3>
        <p className="section-help">
          Choose <strong>how</strong> to connect to the selected DataHub instance.
        </p>

        <div className="settings-section">
          <label>Connection Mode</label>
          <select
            value={localConfig.mode}
            onChange={(e) => handleModeChange(e.target.value as ConnectionConfig['mode'])}
          >
            <option value="embedded">Embedded Agent</option>
            {/* Other modes temporarily hidden - backend code remains */}
            {/* <option value="quickstart">Quickstart (local GMS + integrations)</option> */}
            {/* <option value="local_service">Local Service (spawn integrations)</option> */}
            {/* <option value="remote">Remote Service (kubectl port-forward)</option> */}
            {/* <option value="local">Local Development</option> */}
            {/* <option value="custom">Custom</option> */}
          </select>
        </div>

        {localConfig.integrations_url && (
          <div className="transport-info">
            <small>
              <strong>Integrations URL:</strong> {localConfig.integrations_url}
            </small>
          </div>
        )}
      </div>

      <div className="settings-divider"></div>

      {/* Advanced: Manual Override (Optional) */}
      <details className="advanced-settings">
        <summary>Advanced: Manual Configuration Override</summary>
        <p className="section-help">
          Override profile settings for this session only (not saved to profile).
        </p>

        <div className="settings-section">
          <label>GMS URL</label>
          <input
            type="text"
            value={localConfig.gms_url || ''}
            onChange={(e) => setLocalConfig({ ...localConfig, gms_url: e.target.value })}
            placeholder="http://localhost:8080"
          />
          <button onClick={handleDiscover} disabled={loading}>
            Discover via kubectl
          </button>
        </div>

        <div className="settings-section">
          <label>Token</label>
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
      </details>

      <div className="settings-actions">
        <button onClick={handleTest} disabled={loading}>
          Test Connection
        </button>
        <button onClick={handleSave} disabled={loading}>
          Save
        </button>
      </div>

      {testResult && (
        <div className={`settings-result ${testResult.includes('success') ? 'success' : 'error'}`}>
          {testResult}
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
