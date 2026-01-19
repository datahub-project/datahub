import { useState, useEffect, useRef } from 'react';
import type { ConnectionConfig, Profile } from '../api/types';
import { apiClient } from '../api/client';

interface ConnectionBannerProps {
  config: ConnectionConfig;
}

export function ConnectionBanner({ config }: ConnectionBannerProps) {
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [activeProfile, setActiveProfile] = useState<Profile | null>(null);
  const [showDropdown, setShowDropdown] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Load profiles on mount
  useEffect(() => {
    loadProfiles();
  }, []);

  // Click outside to close dropdown
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    };

    if (showDropdown) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [showDropdown]);

  const loadProfiles = async () => {
    try {
      const profilesList = await apiClient.listProfiles();
      setProfiles(profilesList);

      // Find active profile (matches current config)
      const active = profilesList.find(p => p.name === config.name);
      setActiveProfile(active || null);
    } catch (error) {
      console.error('Failed to load profiles:', error);
    }
  };

  const handleProfileSelect = async (profile: Profile) => {
    if (profile.token_expired) {
      return; // Don't allow selecting expired profiles
    }

    setLoading(true);
    try {
      await apiClient.activateProfile(profile.name);
      // Reload the page to refresh all data with new profile
      window.location.reload();
    } catch (error) {
      console.error('Failed to switch profile:', error);
      setLoading(false);
    }
  };

  // Filter profiles by search query
  const filteredProfiles = profiles.filter(profile => {
    if (!searchQuery) return true;
    const query = searchQuery.toLowerCase();
    return (
      profile.name.toLowerCase().includes(query) ||
      (profile.description && profile.description.toLowerCase().includes(query)) ||
      (profile.gms_url && profile.gms_url.toLowerCase().includes(query))
    );
  });

  // Extract a friendly name from GMS URL
  const getFriendlyName = (): string => {
    if (config.name) {
      return config.name;
    }

    if (config.gms_url) {
      try {
        const url = new URL(config.gms_url);
        // Extract hostname without .acryl.io or domain
        const hostname = url.hostname.replace(/\.acryl\.io$/, '');
        return hostname;
      } catch {
        return config.gms_url;
      }
    }

    return 'Unknown';
  };

  // Determine if we're connected
  const isConnected = !!config.gms_url;

  return (
    <div className={`connection-banner ${isConnected ? 'connected' : 'disconnected'}`}>
      <div className="connection-banner-content">
        <span className="connection-status-icon">
          {isConnected ? '🟢' : '🔴'}
        </span>
        <span className="connection-info">
          <strong>{isConnected ? 'Connected to:' : 'Not connected'}</strong>
          {isConnected && (
            <>
              <span className="connection-name">{getFriendlyName()}</span>
              {config.description && (
                <span className="connection-description">({config.description})</span>
              )}
            </>
          )}
        </span>

        {isConnected && profiles.length > 1 && (
          <div className="profile-switcher" ref={dropdownRef}>
            <span className="switcher-label">Switch to:</span>
            <button
              className="switcher-trigger"
              onClick={() => setShowDropdown(!showDropdown)}
              disabled={loading}
            >
              {loading ? 'Switching...' : 'Select profile ▾'}
            </button>

            {showDropdown && (
              <div className="switcher-dropdown">
                <input
                  type="text"
                  className="switcher-search"
                  placeholder="Search profiles..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  autoFocus
                />
                <div className="switcher-profiles">
                  {filteredProfiles.length === 0 && (
                    <div className="switcher-no-results">No profiles found</div>
                  )}
                  {filteredProfiles.map((profile) => {
                    const isActive = activeProfile?.name === profile.name;
                    const isExpired = profile.token_expired;
                    return (
                      <div
                        key={profile.name}
                        className={`switcher-profile ${isActive ? 'active' : ''} ${isExpired ? 'expired' : ''}`}
                        onClick={() => !isExpired && handleProfileSelect(profile)}
                        style={{ cursor: isExpired ? 'not-allowed' : 'pointer' }}
                      >
                        <div className="switcher-profile-name">
                          {profile.name}
                          {isActive && ' (current)'}
                          {isExpired && ' (token expired)'}
                        </div>
                        {profile.description && (
                          <div className="switcher-profile-description">
                            {profile.description}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
