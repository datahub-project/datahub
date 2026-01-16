import { useState, useEffect } from 'react';
import { apiClient } from '../api/client';
import type { ConnectionConfig } from '../api/types';

// Helper function to get the correct integrations_url for a given mode
function getIntegrationsUrlForMode(mode: ConnectionConfig['mode'], currentUrl?: string, localPort?: number): string {
  switch (mode) {
    case 'embedded':
      return 'embedded://local';
    case 'local_service':
    case 'local':
    case 'quickstart':
      return 'http://localhost:9003';
    case 'remote':
      return `http://localhost:${localPort || 9005}`;
    case 'custom':
      // Keep current URL for custom mode
      return currentUrl || 'http://localhost:9003';
    case 'graphql_direct':
      return 'graphql://direct';
    default:
      return 'http://localhost:9003';
  }
}

export function useConfig() {
  const [config, setConfig] = useState<ConnectionConfig>({
    mode: 'embedded',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadConfig = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await apiClient.getConfig();

      // Fix integrations_url if it doesn't match the mode
      const expectedUrl = getIntegrationsUrlForMode(data.mode, data.integrations_url, data.local_port);
      if (data.integrations_url !== expectedUrl) {
        console.log('[CONFIG] Fixing integrations_url mismatch:', {
          mode: data.mode,
          current: data.integrations_url,
          expected: expectedUrl
        });
        data.integrations_url = expectedUrl;
      }

      setConfig(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load config');
    } finally {
      setLoading(false);
    }
  };

  const updateConfig = async (newConfig: ConnectionConfig) => {
    try {
      setLoading(true);
      setError(null);
      const data = await apiClient.updateConfig(newConfig);
      setConfig(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update config');
      throw err;
    } finally {
      setLoading(false);
    }
  };

  const testConnection = async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await apiClient.testConnection();
      if (!result.success) {
        setError(result.error || 'Connection test failed');
      }
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Connection test failed';
      setError(message);
      return { success: false, error: message };
    } finally {
      setLoading(false);
    }
  };

  const discoverGmsUrl = async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await apiClient.discoverGmsUrl();
      if (result.gms_url) {
        setConfig((prev) => ({ ...prev, gms_url: result.gms_url }));
      } else {
        setError(result.error || 'Failed to discover GMS URL');
      }
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to discover GMS URL';
      setError(message);
      return { error: message };
    } finally {
      setLoading(false);
    }
  };

  const generateToken = async (gms_url: string) => {
    try {
      setLoading(true);
      setError(null);
      const result = await apiClient.generateToken(gms_url);
      if (result.token) {
        setConfig((prev) => ({ ...prev, gms_token: result.token }));
      } else {
        setError(result.error || 'Failed to generate token');
      }
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to generate token';
      setError(message);
      return { error: message };
    } finally {
      setLoading(false);
    }
  };

  const ssoLogin = async () => {
    try {
      setLoading(true);
      setError(null);
      const result = await apiClient.ssoLogin();
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to start SSO login';
      setError(message);
      return { success: false, error: message };
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadConfig();
  }, []);

  return {
    config,
    loading,
    error,
    updateConfig,
    testConnection,
    discoverGmsUrl,
    generateToken,
    ssoLogin,
    reloadConfig: loadConfig,
  };
}
