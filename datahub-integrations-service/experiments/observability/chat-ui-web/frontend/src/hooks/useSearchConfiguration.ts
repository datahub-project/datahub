import { useState, useEffect, useCallback } from 'react';
import { apiClient } from '../api/client';
import type { SearchConfiguration, Stage1Configuration, Stage2Configuration } from '../api/types';

// Default configuration used when server config is unavailable
const DEFAULT_STAGE1_CONFIG: Stage1Configuration = {
  source: 'pdl',
  functionScore: undefined,
  presets: [
    {
      name: 'Server Default',
      description: 'Use server configuration (from search_config.yaml or PDL)',
      config: '{}',
    },
    {
      name: 'Quality Signals Only',
      description: 'hasDescription +3, hasOwners +2, deprecated -10',
      config: JSON.stringify({
        functions: [
          { filter: { term: { hasDescription: true } }, weight: 3.0 },
          { filter: { term: { hasOwners: true } }, weight: 2.0 },
          { filter: { term: { deprecated: true } }, weight: -10.0 },
        ],
        score_mode: 'sum',
        boost_mode: 'sum',
      }),
    },
    {
      name: 'Pure BM25',
      description: 'No function scores, pure BM25 text relevance',
      config: JSON.stringify({ functions: [], score_mode: 'sum', boost_mode: 'replace' }),
    },
  ],
};

const DEFAULT_STAGE2_CONFIG: Stage2Configuration = {
  enabled: true,
  mode: 'JAVA_EXP4J',
  windowSize: 100,
  formula: 'pow(norm_bm25, 1.0) * pow(hasDesc, 1.3) * pow(hasOwners, 1.2) * pow(notDeprecated, 1.0)',
  signals: [
    {
      name: 'bm25',
      normalizedName: 'norm_bm25',
      fieldPath: '_score',
      type: 'SCORE',
      boost: 1.0,
      normalization: { type: 'SIGMOID', inputMax: 500, outputMin: 1.0, outputMax: 2.0 },
    },
    {
      name: 'hasDescription',
      normalizedName: 'hasDesc',
      fieldPath: 'hasDescription',
      type: 'BOOLEAN',
      boost: 1.3,
      normalization: { type: 'BOOLEAN', trueValue: 1.3, falseValue: 1.0, outputMin: 1.0, outputMax: 1.3 },
    },
    {
      name: 'hasOwners',
      normalizedName: 'hasOwners',
      fieldPath: 'hasOwners',
      type: 'BOOLEAN',
      boost: 1.2,
      normalization: { type: 'BOOLEAN', trueValue: 1.2, falseValue: 1.0, outputMin: 1.0, outputMax: 1.2 },
    },
    {
      name: 'deprecated',
      normalizedName: 'notDeprecated',
      fieldPath: 'deprecated',
      type: 'BOOLEAN',
      boost: 1.0,
      normalization: { type: 'BOOLEAN', trueValue: 0.7, falseValue: 1.0, outputMin: 0.7, outputMax: 1.0 },
    },
  ],
  presets: [
    {
      name: 'Server Default',
      description: 'Use server configuration (from rescore_config.yaml)',
      config: '{}',
    },
    {
      name: 'Disabled',
      description: 'Disable Stage 2 rescoring entirely',
      config: JSON.stringify({ enabled: false }),
    },
    {
      name: 'BM25 + Quality',
      description: 'Emphasize BM25 and quality signals (description, owners)',
      config: JSON.stringify({
        formula: 'pow(norm_bm25, 1.0) * pow(hasDesc, 1.5) * pow(hasOwners, 1.3)',
        signals: [
          {
            name: 'bm25',
            normalizedName: 'norm_bm25',
            fieldPath: '_score',
            type: 'SCORE',
            boost: 1.0,
            normalization: { type: 'SIGMOID', inputMax: 500, outputMin: 1.0, outputMax: 2.0 },
          },
          {
            name: 'hasDescription',
            normalizedName: 'hasDesc',
            fieldPath: 'hasDescription',
            type: 'BOOLEAN',
            boost: 1.5,
            normalization: { type: 'BOOLEAN', trueValue: 1.5, falseValue: 1.0 },
          },
          {
            name: 'hasOwners',
            normalizedName: 'hasOwners',
            fieldPath: 'hasOwners',
            type: 'BOOLEAN',
            boost: 1.3,
            normalization: { type: 'BOOLEAN', trueValue: 1.3, falseValue: 1.0 },
          },
        ],
      }),
    },
  ],
};

const DEFAULT_CONFIG: SearchConfiguration = {
  stage1: DEFAULT_STAGE1_CONFIG,
  stage2: DEFAULT_STAGE2_CONFIG,
};

export function useSearchConfiguration() {
  const [config, setConfig] = useState<SearchConfiguration>(DEFAULT_CONFIG);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchConfig = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const serverConfig = await apiClient.getSearchConfiguration();
      setConfig(serverConfig);
    } catch (err) {
      // Use default config if server is unavailable
      console.warn('Failed to fetch search configuration, using defaults:', err);
      setError(err instanceof Error ? err.message : 'Failed to fetch configuration');
      setConfig(DEFAULT_CONFIG);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const refresh = useCallback(() => {
    fetchConfig();
  }, [fetchConfig]);

  return {
    config,
    stage1Config: config.stage1,
    stage2Config: config.stage2,
    loading,
    error,
    refresh,
    isUsingDefaults: error !== null,
  };
}
