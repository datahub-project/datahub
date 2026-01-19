import { useEffect, useState } from 'react';
import { apiClient } from '../api/client';
import type { ClusterInfo } from '../api/types';

interface ClusterSelectorProps {
  mode: 'cloud' | 'trials';
  onModeChange: (mode: 'cloud' | 'trials') => void;
  onClusterSelect: (cluster: ClusterInfo) => void;
  selectedCluster: ClusterInfo | null;
}

export function ClusterSelector({
  mode,
  onModeChange,
  onClusterSelect,
  selectedCluster
}: ClusterSelectorProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [clusters, setClusters] = useState<ClusterInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [cached, setCached] = useState(false);
  const [cacheAge, setCacheAge] = useState<number | undefined>();
  const [error, setError] = useState<string | null>(null);
  const [vpnRequired, setVpnRequired] = useState(false);

  // Load clusters on mount or mode change
  useEffect(() => {
    loadClusters();
  }, [mode]);

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => {
      loadClusters();
    }, 300);

    return () => clearTimeout(timer);
  }, [searchQuery]);

  const loadClusters = async (forceRefresh: boolean = false) => {
    setLoading(true);
    setError(null);
    setVpnRequired(false);
    try {
      const response = await apiClient.listClusters(
        mode === 'cloud' ? 'all' : 'trials',
        searchQuery || undefined,
        forceRefresh
      );
      setClusters(response.clusters);
      setCached(response.cached);
      setCacheAge(response.cache_age_seconds);

      // Check for backend-reported errors
      if (response.error) {
        setError(response.error);
        setVpnRequired(response.vpn_required || false);
      }
    } catch (err) {
      console.error('Failed to load clusters:', err);
      setError(err instanceof Error ? err.message : 'Failed to load clusters');
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadClusters(true); // Force refresh
  };

  return (
    <div className="cluster-selector">
      <div className="cluster-mode-tabs">
        <button
          className={mode === 'cloud' ? 'active' : ''}
          onClick={() => onModeChange('cloud')}
        >
          🌐 Cloud Customers
        </button>
        <button
          className={mode === 'trials' ? 'active' : ''}
          onClick={() => onModeChange('trials')}
        >
          🧪 Free Trials
        </button>
      </div>

      {mode === 'cloud' && (
        <div className="cluster-search">
          <input
            type="text"
            placeholder="Search by customer name or namespace..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
      )}

      <div className="cluster-list">
        {loading && <p className="cluster-loading">Loading clusters...</p>}

        {!loading && error && (
          <div className={`cluster-error ${vpnRequired ? 'vpn-error' : ''}`}>
            <p>{error}</p>
            {vpnRequired && (
              <p className="vpn-help">
                Connect to VPN and click "Refresh" to load clusters.
              </p>
            )}
          </div>
        )}

        {!loading && !error && clusters.length === 0 && (
          <p className="no-results">
            {mode === 'cloud'
              ? 'No clusters found. Try a different search term.'
              : 'No trial clusters available.'}
          </p>
        )}

        {!loading && clusters.length > 0 && clusters.map((cluster) => (
          <div
            key={`${cluster.context}:${cluster.namespace}`}
            className={`cluster-item ${selectedCluster?.context === cluster.context && selectedCluster?.namespace === cluster.namespace ? 'selected' : ''}`}
            onClick={() => onClusterSelect(cluster)}
          >
            <div className="cluster-name">
              {cluster.customer_name || cluster.namespace}
              {cluster.customer_name && cluster.customer_name !== cluster.namespace && (
                <span className="cluster-namespace">({cluster.namespace})</span>
              )}
            </div>
            <div className="cluster-context">
              {cluster.context_name} · {cluster.cluster_region}
              {cluster.cluster_env && <span className="cluster-env"> · {cluster.cluster_env}</span>}
            </div>
          </div>
        ))}
      </div>

      {cached && (
        <div className="cache-status">
          ℹ️ Using cached data
          {cacheAge !== undefined && ` (${cacheAge}s old)`}.{' '}
          <button onClick={handleRefresh} className="refresh-button">
            Refresh
          </button>
        </div>
      )}
    </div>
  );
}
