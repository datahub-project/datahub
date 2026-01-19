import { useState, useEffect } from 'react';
import type { ConnectionConfig, Profile } from '../api/types';
import { apiClient } from '../api/client';
import { ProfileManager } from './ProfileManager';
import { formatRelativeTime } from '../utils/messageParser';

interface SettingsTabProps {
  config: ConnectionConfig;
  onUpdate: (config: ConnectionConfig) => void;
  onTest: () => Promise<{ success: boolean; error?: string }>;
  onGenerateToken: (gms_url: string) => Promise<{ token?: string; error?: string }>;
  onSsoLogin: (profile?: string) => Promise<{ success: boolean; message?: string; profile?: string; error?: string }>;
  onDetectProfile: (context: string) => Promise<any>;
  onListProfiles: () => Promise<{ success: boolean; profiles: string[]; error?: string }>;
  onSetupProfile: (config: any) => Promise<{ success: boolean; instructions?: string; error?: string }>;
  onValidateProfile?: (profile: string) => Promise<{ valid: boolean; error?: string }>;
  onSwitchToChat: () => void;
  loading: boolean;
}

export function SettingsTab({
  config,
  onUpdate,
  onTest,
  onGenerateToken,
  onSsoLogin,
  onDetectProfile,
  onListProfiles,
  onSetupProfile,
  // onValidateProfile,  // Reserved for future use
  onSwitchToChat,
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
  const [selectedAwsProfile, setSelectedAwsProfile] = useState<string>('');
  const [profileDetection, setProfileDetection] = useState<any>(null);
  const [showSetupDialog, setShowSetupDialog] = useState(false);
  const [setupInstructions, setSetupInstructions] = useState<string>('');
  const [sessionStatus, setSessionStatus] = useState<any>(null);
  const [sessionStatusLoading, setSessionStatusLoading] = useState(false);

  // Sync localConfig when parent config changes (e.g., after Save)
  useEffect(() => {
    setLocalConfig(config);
  }, [config]);

  // Fetch available AWS profiles on mount
  useEffect(() => {
    const fetchAwsProfiles = async () => {
      setAwsProfilesLoading(true);
      try {
        const result = await onListProfiles();
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

  // Check AWS session status on mount
  useEffect(() => {
    checkSessionStatus();
  }, []);

  const checkSessionStatus = async (profile?: string) => {
    setSessionStatusLoading(true);
    try {
      const params = profile ? `?profile=${encodeURIComponent(profile)}` : '';
      const response = await fetch(`/api/config/aws/session-status${params}`);
      const status = await response.json();
      setSessionStatus(status);
    } catch (err) {
      console.error('Failed to check AWS session status:', err);
      setSessionStatus({ valid: false, error: 'Failed to check session status' });
    } finally {
      setSessionStatusLoading(false);
    }
  };

  // Auto-detect AWS profile when context changes
  useEffect(() => {
    const detectAwsProfile = async () => {
      if (!selectedContext) return;

      try {
        const result = await onDetectProfile(selectedContext);
        setProfileDetection(result);

        // Auto-select recommended profile if available
        if (result.recommended_profile) {
          setSelectedAwsProfile(result.recommended_profile);
        }

        // Show setup dialog if no profiles found
        if (result.setup_needed) {
          setSsoResult(`⚠️ AWS profile setup required for account ${result.account_id}. Click "Set Up Profile" below to continue.`);
        } else if (result.matching_profiles.length > 0) {
          setSsoResult(`✅ Found ${result.matching_profiles.length} matching AWS profile(s) for this cluster.`);
        }
      } catch (err) {
        console.error('Failed to detect AWS profile:', err);
      }
    };

    detectAwsProfile();
  }, [selectedContext]);

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
    const result = await onSsoLogin(selectedAwsProfile || undefined);
    if (result.success) {
      setSsoResult(`AWS SSO login started for profile: ${result.profile}. Browser should open automatically. After logging in, click "Auto-Discover" or "Generate Token" to continue.`);

      // Refresh session status after SSO login
      // Wait a bit for SSO to complete before checking
      setTimeout(() => {
        checkSessionStatus(result.profile);
      }, 2000);
    } else {
      setSsoResult(result.error || 'SSO login failed');
    }
  };

  const handleSetupProfile = async () => {
    if (!profileDetection || !profileDetection.account_info) {
      setSsoResult('Cannot set up profile - account information not available');
      return;
    }

    const accountInfo = profileDetection.account_info;
    const result = await onSetupProfile({
      profile_name: accountInfo.suggested_profile_name,
      account_id: profileDetection.account_id,
      sso_start_url: accountInfo.sso_start_url,
      sso_region: accountInfo.sso_region,
      role_name: accountInfo.default_role,
      region: profileDetection.region || 'us-west-2',
    });

    if (result.success && result.instructions) {
      setSetupInstructions(result.instructions);
      setShowSetupDialog(true);
    } else {
      setSsoResult(`Failed to generate setup instructions: ${result.error}`);
    }
  };

  const handleRefreshProfiles = async () => {
    setAwsProfilesLoading(true);
    try {
      const result = await onListProfiles();
      if (result.success) {
        setAwsProfiles(result.profiles);
        // Re-run detection to update matching profiles
        if (selectedContext) {
          const detection = await onDetectProfile(selectedContext);
          setProfileDetection(detection);
          if (detection.recommended_profile) {
            setSelectedAwsProfile(detection.recommended_profile);
          }
        }
        setSsoResult(`✅ Refreshed profiles - found ${result.profiles.length} profile(s)`);
      }
    } catch (err) {
      setSsoResult('Failed to refresh profiles');
    } finally {
      setAwsProfilesLoading(false);
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
      <div className="settings-layout">
        {/* Main Content Area - Scrollable */}
        <div className="settings-main-content">
          <h2>Connection Settings</h2>

          {/* Step 1: Choose Connection Transport (Mode) */}
          <div className="connection-mode-section">
            <div className="settings-section compact">
              <label>Connection Mode</label>
              <select
                value={localConfig.mode}
                onChange={(e) => handleModeChange(e.target.value as ConnectionConfig['mode'])}
              >
                <option value="embedded">Embedded Agent</option>
              </select>
              <p className="field-help">Uses local Agent class, bypasses Integrations Service (default)</p>
            </div>
          </div>

          <div className="settings-divider"></div>

          {/* Step 2: Select DataHub Instance (Profile) */}
          <ProfileManager
            onProfileSelect={handleProfileSelect}
            selectedProfile={selectedProfile}
            onSwitchToChat={onSwitchToChat}
          />

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
        </div>

        {/* Right Rail - Sticky AWS Section */}
        <div className="settings-right-rail">
          <div className="aws-sticky-section">
            <h3>AWS SSO Login</h3>
            <p className="section-description">Select an AWS profile to authenticate with AWS services.</p>

        {/* Session Status Display */}
        {sessionStatus && (
          <div className={`session-status ${sessionStatus.valid ? 'session-valid' : 'session-invalid'}`}>
            <div className="session-status-header">
              <span className="session-status-icon">
                {sessionStatusLoading ? '⏳' : sessionStatus.valid ? '🟢' : '🔴'}
              </span>
              <span className="session-status-text">
                {sessionStatusLoading ? 'Checking...' : sessionStatus.valid ? 'Active' : 'Expired'}
              </span>
              <button
                onClick={() => checkSessionStatus(selectedAwsProfile || undefined)}
                disabled={sessionStatusLoading}
                className="btn-refresh-status"
                title="Refresh status"
              >
                🔄
              </button>
            </div>
            {sessionStatus.valid && (
              <div className="session-status-details">
                <span className="session-status-compact">
                  {sessionStatus.profile} • {sessionStatus.account}
                </span>
                {sessionStatus.checked_at && (
                  <span className="session-status-timestamp">
                    {formatRelativeTime(sessionStatus.checked_at)}
                  </span>
                )}
              </div>
            )}
            {!sessionStatus.valid && sessionStatus.error && (
              <div className="session-status-error">
                {sessionStatus.expired ? '🔑 Session expired - please login' : sessionStatus.error}
              </div>
            )}
          </div>
        )}

        {/* Profile detection info */}
        {profileDetection && profileDetection.account_id && (
          <div className="profile-detection-info">
            <p><strong>Detected AWS Account:</strong> {profileDetection.account_id}</p>
            {profileDetection.account_info && (
              <p><small>{profileDetection.account_info.name} - {profileDetection.account_info.description}</small></p>
            )}
          </div>
        )}

        {/* AWS Profile Selector */}
        {awsProfiles.length > 0 ? (
          <div className="form-group">
            <label htmlFor="aws-profile">AWS Profile:</label>
            <div style={{ display: 'flex', gap: '10px' }}>
              <select
                id="aws-profile"
                value={selectedAwsProfile}
                onChange={(e) => setSelectedAwsProfile(e.target.value)}
                disabled={awsProfilesLoading}
              >
                <option value="">Select a profile...</option>
                {awsProfiles.map(profile => (
                  <option key={profile} value={profile}>{profile}</option>
                ))}
              </select>
              <button onClick={handleRefreshProfiles} disabled={awsProfilesLoading} className="btn-secondary">
                🔄 Refresh
              </button>
            </div>
            {profileDetection?.matching_profiles && profileDetection.matching_profiles.length > 0 && (
              <small style={{ color: '#28a745' }}>
                ✓ {profileDetection.matching_profiles.length} profile(s) match this cluster
              </small>
            )}
          </div>
        ) : (
          <div className="no-profiles-warning">
            <p>⚠️ No AWS profiles found. You need to set up an AWS profile first.</p>
          </div>
        )}

        {/* Setup or Login buttons */}
        <div style={{ display: 'flex', gap: '10px', marginTop: '10px' }}>
          <button
            onClick={handleSsoLogin}
            disabled={loading || !selectedAwsProfile}
            className="sso-login-btn"
          >
            🌐 AWS SSO Login {selectedAwsProfile && `(${selectedAwsProfile})`}
          </button>

          {profileDetection?.setup_needed && profileDetection?.account_info && (
            <button
              onClick={handleSetupProfile}
              disabled={loading}
              className="btn-secondary"
            >
              📝 Set Up Profile
            </button>
          )}
        </div>

        {ssoResult && (
              <div className={`settings-result ${ssoResult.includes('✅') || ssoResult.includes('started') ? 'success' : ssoResult.includes('⚠️') ? 'warning' : 'error'}`}>
                {ssoResult}
              </div>
            )}

            <div className="settings-subsection-divider"></div>

            <h4>AWS Profile for Bedrock/S3</h4>
            <p className="section-description">Profile used for AI operations.</p>
            {awsProfilesLoading ? (
              <p className="loading-text">Loading AWS profiles...</p>
            ) : awsProfiles.length > 0 ? (
              <>
                <select
                  value={localConfig.aws_profile || 'default'}
                  onChange={(e) => handleAwsProfileChange(e.target.value)}
                  style={{ width: '100%', marginBottom: '8px' }}
                >
                  <option value="">default</option>
                  {awsProfiles.map((profile) => (
                    <option key={profile} value={profile}>
                      {profile}
                    </option>
                  ))}
                </select>
                <small style={{ fontSize: '12px', color: '#666' }}>
                  Current: <strong>{localConfig.aws_profile || 'default'}</strong>
                </small>
              </>
            ) : (
              <p className="warning-text">No AWS profiles found</p>
            )}
          </div>
        </div>
      </div>

      {/* Setup Dialog */}
      {showSetupDialog && setupInstructions && (
        <div className="setup-dialog">
          <div className="setup-dialog-content">
            <h3>AWS Profile Setup Instructions</h3>
            <pre>{setupInstructions}</pre>
            <div style={{ display: 'flex', gap: '10px', marginTop: '15px' }}>
              <button onClick={() => {
                navigator.clipboard.writeText(setupInstructions);
                setSsoResult('✓ Setup instructions copied to clipboard');
              }} className="btn-secondary">
                📋 Copy Instructions
              </button>
              <button onClick={() => setShowSetupDialog(false)} className="btn-secondary">
                Close
              </button>
            </div>
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
