import { useState, useEffect, useRef } from 'react';
import { apiClient } from '../api/client';
import type { Profile, ClusterInfo } from '../api/types';
import { Autocomplete } from './Autocomplete';
import { ActiveProfileSummary } from './ActiveProfileSummary';
import { ClusterSelector } from './ClusterSelector';

interface ProfileManagerProps {
  onProfileSelect: (profile: Profile) => void;
  selectedProfile?: Profile;
  onSwitchToChat: () => void;
}

export function ProfileManager({ onProfileSelect, selectedProfile, onSwitchToChat }: ProfileManagerProps) {
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [showAddModal, setShowAddModal] = useState(false);
  const [editingProfile, setEditingProfile] = useState<Profile | null>(null);
  const [searchQuery, setSearchQuery] = useState('');

  // Refs for scrolling to profiles
  const profileListRef = useRef<HTMLDivElement>(null);
  const profileRefs = useRef<Map<string, HTMLDivElement>>(new Map());

  // Kubectl discovery state
  const [kubectlContexts, setKubectlContexts] = useState<string[]>([]);
  const [kubectlNamespaces, setKubectlNamespaces] = useState<string[]>([]);
  const [loadingKubectl, setLoadingKubectl] = useState(false);

  // Cluster selection state
  const [clusterMode, setClusterMode] = useState<'search' | 'manual'>('search');
  const [clusterSelectionMode, setClusterSelectionMode] = useState<'cloud' | 'trials'>('cloud');
  const [selectedCluster, setSelectedCluster] = useState<ClusterInfo | null>(null);

  // Form state for add/edit
  const [formData, setFormData] = useState({
    name: '',
    description: '',
    gms_url: '',
    gms_token: '',
    kube_context: '',
    kube_namespace: '',
  });

  const formatRelativeTime = (expiresAt: string): string => {
    const now = new Date();
    const expiration = new Date(expiresAt);
    const diffMs = expiration.getTime() - now.getTime();

    if (diffMs < 0) {
      // Already expired
      const absDiffMs = Math.abs(diffMs);
      const days = Math.floor(absDiffMs / (1000 * 60 * 60 * 24));
      const hours = Math.floor(absDiffMs / (1000 * 60 * 60));
      const minutes = Math.floor(absDiffMs / (1000 * 60));

      if (days > 0) return `${days} day${days > 1 ? 's' : ''} ago`;
      if (hours > 0) return `${hours} hour${hours > 1 ? 's' : ''} ago`;
      if (minutes > 0) return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
      return 'just now';
    }

    // Not expired yet
    const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const hours = Math.floor(diffMs / (1000 * 60 * 60));
    const minutes = Math.floor(diffMs / (1000 * 60));

    if (days > 0) return `in ${days} day${days > 1 ? 's' : ''}`;
    if (hours > 0) return `in ${hours} hour${hours > 1 ? 's' : ''}`;
    if (minutes > 0) return `in ${minutes} minute${minutes > 1 ? 's' : ''}`;
    return 'in less than a minute';
  };

  useEffect(() => {
    loadProfiles();
  }, []);

  const loadProfiles = async () => {
    try {
      setLoading(true);
      const loadedProfiles = await apiClient.listProfiles();
      setProfiles(loadedProfiles);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to load profiles');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveProfile = async () => {
    try {
      setLoading(true);
      setMessage(null);

      if (!formData.name || !formData.gms_url) {
        setMessage('Name and GMS URL are required');
        return;
      }

      await apiClient.createOrUpdateProfile(formData);
      setMessage(`Profile "${formData.name}" saved successfully`);
      setShowAddModal(false);
      setEditingProfile(null);
      setFormData({
        name: '',
        description: '',
        gms_url: '',
        gms_token: '',
        kube_context: '',
        kube_namespace: '',
      });
      await loadProfiles();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to save profile');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteProfile = async (name: string) => {
    if (!confirm(`Delete profile "${name}"?`)) {
      return;
    }

    try {
      setLoading(true);
      await apiClient.deleteProfile(name);
      setMessage(`Profile "${name}" deleted`);
      await loadProfiles();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to delete profile');
    } finally {
      setLoading(false);
    }
  };

  const handleTestProfile = async (name: string) => {
    try {
      setLoading(true);
      setMessage('Testing connection...');
      const result = await apiClient.testProfile(name);

      if (result.success) {
        setMessage(`✅ ${result.message}`);
      } else {
        setMessage(`❌ ${result.error}`);
      }
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Test failed');
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshToken = async (name: string) => {
    try {
      setLoading(true);
      setMessage('Refreshing token...');
      await apiClient.refreshProfileToken(name);
      setMessage(`✅ Token refreshed for "${name}"`);
      await loadProfiles();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Token refresh failed');
    } finally {
      setLoading(false);
    }
  };

  const handleSelectProfile = async (profile: Profile) => {
    try {
      setLoading(true);
      setMessage(`Activating profile "${profile.name}"...`);

      // Activate the profile in the backend
      await apiClient.activateProfile(profile.name);

      // Update the UI
      onProfileSelect(profile);

      // Reload profiles to show updated active status
      await loadProfiles();

      setMessage(`✅ Profile "${profile.name}" activated`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to activate profile');
    } finally {
      setLoading(false);
    }
  };

  const handleChatWithProfile = async (profile: Profile) => {
    try {
      setLoading(true);
      setMessage(`Activating profile "${profile.name}" and switching to chat...`);

      // Activate the profile in the backend
      await apiClient.activateProfile(profile.name);

      // Update the UI
      onProfileSelect(profile);

      // Reload profiles to show updated active status
      await loadProfiles();

      // Switch to chat tab
      onSwitchToChat();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to activate profile');
    } finally {
      setLoading(false);
    }
  };

  const handleEditProfile = (profile: Profile) => {
    setEditingProfile(profile);
    setFormData({
      name: profile.name,
      description: profile.description || '',
      gms_url: profile.gms_url,
      gms_token: profile.gms_token || '',
      kube_context: profile.kube_context || '',
      kube_namespace: profile.kube_namespace || '',
    });
    setShowAddModal(true);

    // Load kubectl contexts
    loadKubectlContexts();

    // If the profile already has a context, load its namespaces
    if (profile.kube_context) {
      loadKubectlNamespaces(profile.kube_context);
    }
  };

  const loadKubectlContexts = async () => {
    try {
      setLoadingKubectl(true);
      const contexts = await apiClient.getKubectlContexts();
      setKubectlContexts(contexts);
    } catch (error) {
      console.error('Failed to load kubectl contexts:', error);
      setKubectlContexts([]);
    } finally {
      setLoadingKubectl(false);
    }
  };

  const loadKubectlNamespaces = async (context: string) => {
    if (!context) {
      setKubectlNamespaces([]);
      return;
    }

    try {
      setLoadingKubectl(true);
      const namespaces = await apiClient.getKubectlNamespaces(context);
      setKubectlNamespaces(namespaces);
    } catch (error) {
      console.error('Failed to load kubectl namespaces:', error);
      setKubectlNamespaces([]);
    } finally {
      setLoadingKubectl(false);
    }
  };

  const handleScrollToProfile = () => {
    const activeProfile = profiles.find((p) => p.is_active);
    if (!activeProfile) return;

    const profileElement = profileRefs.current.get(activeProfile.name);
    if (profileElement) {
      profileElement.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
      });

      // Flash effect to highlight
      profileElement.classList.add('flash-highlight');
      setTimeout(() => {
        profileElement.classList.remove('flash-highlight');
      }, 2000);
    }
  };

  const handleAutoDiscover = async () => {
    if (!formData.kube_context || !formData.kube_namespace) {
      setMessage('Please select both kubectl context and namespace first');
      return;
    }

    try {
      setLoading(true);
      setMessage('Auto-discovering GMS URL and generating token...');

      const result = await apiClient.discoverProfileFromKubectl(
        formData.kube_context,
        formData.kube_namespace
      );

      // Fill in the discovered values
      setFormData({
        ...formData,
        gms_url: result.gms_url,
        gms_token: result.gms_token,
      });

      setMessage('✅ Successfully discovered GMS URL and generated token!');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Failed to auto-discover');
    } finally {
      setLoading(false);
    }
  };

  const handleClusterSelect = (cluster: ClusterInfo) => {
    setSelectedCluster(cluster);

    // Pre-fill context and namespace
    setFormData({
      ...formData,
      kube_context: cluster.context,
      kube_namespace: cluster.namespace,
      // Also pre-fill profile name if empty and customer name is available
      name: formData.name || cluster.customer_name || '',
    });

    // Load namespaces for the selected context (for manual mode switching)
    loadKubectlNamespaces(cluster.context);
  };

  const handleAddNew = () => {
    setEditingProfile(null);
    setFormData({
      name: '',
      description: '',
      gms_url: '',
      gms_token: '',
      kube_context: '',
      kube_namespace: '',
    });
    setShowAddModal(true);
    setClusterMode('search');
    setSelectedCluster(null);

    // Load kubectl contexts when opening modal
    loadKubectlContexts();
  };

  // Filter profiles based on search query
  const filteredProfiles = profiles.filter((profile) => {
    if (!searchQuery) return true;

    const query = searchQuery.toLowerCase();
    return (
      profile.name.toLowerCase().includes(query) ||
      profile.gms_url.toLowerCase().includes(query) ||
      (profile.description && profile.description.toLowerCase().includes(query)) ||
      (profile.kube_namespace && profile.kube_namespace.toLowerCase().includes(query))
    );
  });

  // Find active profile
  const activeProfile = profiles.find((p) => p.is_active) || null;

  return (
    <div className="profile-manager">
      <div className="profile-manager-header">
        <h3>DataHub Instance Profiles</h3>
        <button onClick={handleAddNew} disabled={loading} className="btn-add-profile">
          + Add Profile
        </button>
      </div>

      <p className="profile-help">
        Profiles define <strong>where</strong> to connect (DataHub instance + credentials).
      </p>

      {message && (
        <div className={`profile-message ${message.includes('✅') ? 'success' : 'error'}`}>
          {message}
        </div>
      )}

      {/* Active Profile Summary */}
      <ActiveProfileSummary
        profile={activeProfile}
        onScrollToProfile={handleScrollToProfile}
        onRefreshToken={handleRefreshToken}
        formatRelativeTime={formatRelativeTime}
      />

      {/* Search input */}
      {profiles.length > 0 && (
        <div className="profile-search">
          <input
            type="text"
            placeholder="🔍 Search profiles by name, URL, or namespace..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="profile-search-input"
          />
          {searchQuery && (
            <button
              className="profile-search-clear"
              onClick={() => setSearchQuery('')}
              title="Clear search"
            >
              ✕
            </button>
          )}
        </div>
      )}

      <div className="profile-list" ref={profileListRef}>
        {profiles.length === 0 ? (
          <div className="profile-empty">
            No profiles saved. Click "Add Profile" to create one.
          </div>
        ) : filteredProfiles.length === 0 ? (
          <div className="profile-empty">
            No profiles match "{searchQuery}". <button onClick={() => setSearchQuery('')} className="link-button">Clear search</button>
          </div>
        ) : (
          filteredProfiles.map((profile) => (
            <div
              key={profile.name}
              ref={(el) => {
                if (el) profileRefs.current.set(profile.name, el);
              }}
              className={`profile-item ${selectedProfile?.name === profile.name ? 'selected' : ''} ${profile.is_active ? 'active' : ''}`}
            >
              <div className="profile-item-header">
                <div className="profile-item-info">
                  <h4>
                    {profile.name}
                    {profile.is_active && <span className="active-badge">● Active</span>}
                    {profile.source === 'datahubenv' && <span className="datahubenv-badge">~/.datahubenv</span>}
                  </h4>
                  {profile.description && <p className="profile-description">{profile.description}</p>}
                  <p className="profile-url">{profile.gms_url}</p>
                  {profile.kube_context && (
                    <p className="profile-kubectl">
                      kubectl: {profile.kube_context} / {profile.kube_namespace}
                    </p>
                  )}
                  {profile.token_expires_at && (
                    <p className={`profile-token-status ${profile.token_expired ? 'expired' : profile.token_expiring_soon ? 'expiring-soon' : 'valid'}`}>
                      {profile.token_expired ? (
                        <>⚠️ Token expired {formatRelativeTime(profile.token_expires_at)}</>
                      ) : profile.token_expiring_soon ? (
                        <>⏰ Token expires {formatRelativeTime(profile.token_expires_at)}</>
                      ) : (
                        <>✅ Token expires {formatRelativeTime(profile.token_expires_at)}</>
                      )}
                    </p>
                  )}
                </div>
                <div className="profile-item-actions">
                  <button
                    onClick={() => handleSelectProfile(profile)}
                    disabled={loading}
                    className="btn-select"
                  >
                    {profile.is_active ? '✓ Active' : 'Select'}
                  </button>
                </div>
              </div>

              <div className="profile-item-footer">
                <button onClick={() => handleChatWithProfile(profile)} disabled={loading} className="btn-small btn-primary">
                  💬 Chat
                </button>
                <button onClick={() => handleTestProfile(profile.name)} disabled={loading} className="btn-small">
                  Test
                </button>
                {profile.kube_context && !profile.is_readonly && (
                  <button onClick={() => handleRefreshToken(profile.name)} disabled={loading} className="btn-small">
                    Refresh Token
                  </button>
                )}
                {!profile.is_readonly && (
                  <>
                    <button onClick={() => handleEditProfile(profile)} disabled={loading} className="btn-small">
                      Edit
                    </button>
                    <button onClick={() => handleDeleteProfile(profile.name)} disabled={loading} className="btn-small btn-delete">
                      Delete
                    </button>
                  </>
                )}
              </div>
            </div>
          ))
        )}
      </div>

      {/* Add/Edit Profile Modal */}
      {showAddModal && (
        <div className="config-modal">
          <div className="config-modal-content">
            <div className="config-header">
              <h2>{editingProfile ? 'Edit Profile' : 'Add New Profile'}</h2>
              <button onClick={() => setShowAddModal(false)}>Cancel</button>
            </div>

            <div className="config-section">
              <label>Profile Name *</label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="e.g., Dev01, Staging, Production"
                disabled={!!editingProfile}
              />
              {editingProfile && <small>Profile name cannot be changed</small>}
            </div>

            <div className="config-section">
              <label>Description</label>
              <input
                type="text"
                value={formData.description}
                onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                placeholder="e.g., Development environment"
              />
            </div>

            <div className="config-section">
              <label>Cluster Selection</label>
              <div className="cluster-mode-toggle">
                <button
                  type="button"
                  className={clusterMode === 'search' ? 'active' : ''}
                  onClick={() => setClusterMode('search')}
                >
                  🔍 Search Clusters
                </button>
                <button
                  type="button"
                  className={clusterMode === 'manual' ? 'active' : ''}
                  onClick={() => setClusterMode('manual')}
                >
                  ⚙️ Manual Entry
                </button>
              </div>

              {clusterMode === 'search' ? (
                <ClusterSelector
                  mode={clusterSelectionMode}
                  onModeChange={setClusterSelectionMode}
                  onClusterSelect={handleClusterSelect}
                  selectedCluster={selectedCluster}
                />
              ) : (
                <>
                  <Autocomplete
                    label="Kubectl Context"
                    options={kubectlContexts}
                    value={formData.kube_context}
                    onChange={(context) => {
                      setFormData({ ...formData, kube_context: context, kube_namespace: '' });
                      if (context) {
                        loadKubectlNamespaces(context);
                      }
                    }}
                    placeholder="Type to search contexts..."
                    disabled={loadingKubectl}
                  />
                  <small>Select the kubectl context where DataHub is deployed</small>

                  <div style={{ marginTop: '1rem' }}>
                    <Autocomplete
                      label="Kubectl Namespace"
                      options={kubectlNamespaces}
                      value={formData.kube_namespace}
                      onChange={(namespace) => setFormData({ ...formData, kube_namespace: namespace })}
                      placeholder="Type to search namespaces..."
                      disabled={!formData.kube_context || loadingKubectl}
                    />
                    <small>Select the namespace where DataHub pods are running</small>
                  </div>
                </>
              )}
            </div>

            <div className="config-section">
              <button
                type="button"
                onClick={handleAutoDiscover}
                disabled={!formData.kube_context || !formData.kube_namespace || loading}
                className="btn-auto-discover"
              >
                🔍 Auto-discover GMS URL & Token
              </button>
              <small>Automatically discover GMS URL and generate token from kubectl</small>
            </div>

            <div className="config-section">
              <label>GMS URL *</label>
              <input
                type="text"
                value={formData.gms_url}
                onChange={(e) => setFormData({ ...formData, gms_url: e.target.value })}
                placeholder="https://your-instance.acryl.io/api/gms"
              />
              <small>Or enter manually if not using kubectl auto-discovery</small>
            </div>

            <div className="config-section">
              <label>GMS Token</label>
              <input
                type="password"
                value={formData.gms_token}
                onChange={(e) => setFormData({ ...formData, gms_token: e.target.value })}
                placeholder="Optional: Enter token"
              />
              <small>Or enter manually if not using kubectl auto-discovery</small>
            </div>

            <div className="config-actions">
              <button onClick={handleSaveProfile} disabled={loading}>
                Save Profile
              </button>
              <button onClick={() => setShowAddModal(false)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
