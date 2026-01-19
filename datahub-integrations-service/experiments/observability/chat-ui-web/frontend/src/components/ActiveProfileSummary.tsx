import type { Profile } from '../api/types';

interface ActiveProfileSummaryProps {
  profile: Profile | null;
  onScrollToProfile: () => void;
  onRefreshToken: (profileName: string) => Promise<void>;
  formatRelativeTime: (expiresAt: string) => string;
}

export function ActiveProfileSummary({
  profile,
  onScrollToProfile,
  onRefreshToken,
  formatRelativeTime,
}: ActiveProfileSummaryProps) {
  if (!profile) {
    return (
      <div className="active-profile-summary empty">
        <p>⚠️ No active profile. Select a profile below to connect.</p>
      </div>
    );
  }

  const handleRefreshClick = async () => {
    await onRefreshToken(profile.name);
  };

  return (
    <div className="active-profile-summary">
      <div className="active-profile-header">
        <span className="active-profile-icon">📌</span>
        <h4>Active Profile</h4>
      </div>

      <div className="active-profile-content">
        <div className="active-profile-info">
          <h5>{profile.name}</h5>
          <p className="active-profile-url">{profile.gms_url}</p>

          {profile.token_expired && (
            <p className="token-status expired">
              ⚠️ Token expired {formatRelativeTime(profile.token_expires_at!)}
            </p>
          )}
          {profile.token_expiring_soon && !profile.token_expired && profile.token_expires_at && (
            <p className="token-status expiring">
              ⏰ Token expires {formatRelativeTime(profile.token_expires_at)}
            </p>
          )}
          {!profile.token_expired && !profile.token_expiring_soon && profile.token_expires_at && (
            <p className="token-status valid">
              ✅ Token expires {formatRelativeTime(profile.token_expires_at)}
            </p>
          )}
        </div>

        <div className="active-profile-actions">
          <button
            onClick={onScrollToProfile}
            className="btn-secondary btn-sm"
            title="Scroll to this profile in the list"
          >
            📍 View in List
          </button>

          {profile.kube_context && profile.kube_namespace && (
            <button
              onClick={handleRefreshClick}
              className="btn-secondary btn-sm"
              title="Generate new token from kubectl"
            >
              🔄 Refresh Token
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
