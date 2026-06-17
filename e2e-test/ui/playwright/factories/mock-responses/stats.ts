/**
 * Stats Mock Factory — Generate and set up mock profile and usage data for testing stats tab.
 *
 * Provides factory functions to generate sample profile data, usage stats, and
 * operations stats, matching the format expected by DataHub's GraphQL API.
 *
 * Setup functions orchestrate these factories with the test API mock infrastructure.
 *
 * This mirrors the Cypress approach (ApiResponseHelpers) but for Playwright's
 * apiMock.interceptGraphQLResponse pattern.
 */

import type { ApiMocker } from '../../fixtures/mocking.fixture';

export interface DatasetFieldProfile {
  fieldPath: string;
  max: string | number | null;
  mean: number | null;
  median: number | null;
  min: string | number | null;
  nullCount: number;
  nullProportion: number;
  quantiles: unknown | null;
  sampleValues: string[];
  distinctValueFrequencies: unknown | null;
  stdev: number | null;
  uniqueCount: number;
  uniqueProportion: number;
  __typename: 'DatasetFieldProfile';
}

export interface SchemaField {
  fieldPath: string;
  type: string;
  nativeDataType: string;
  nullable: boolean;
  __typename: 'SchemaField';
}

export interface SchemaMetadata {
  __typename: 'SchemaMetadata';
  fields: SchemaField[];
}

export interface DatasetProfile {
  rowCount: number;
  columnCount: number;
  sizeInBytes: number;
  timestampMillis: number;
  partitionSpec: unknown | null;
  fieldProfiles: DatasetFieldProfile[];
  __typename: 'DatasetProfile';
}

export interface UsageAggregation {
  bucket: number;
  metrics: {
    totalSqlQueries: number;
    __typename: 'UsageAggregationMetrics';
  };
  __typename: 'UsageAggregation';
}

export interface UsageQueryResult {
  buckets: UsageAggregation[];
  aggregations: {
    uniqueUserCount: number;
    totalSqlQueries: number;
    fields: Array<{
      fieldName: string;
      count: number;
      __typename: 'FieldUsageCounts';
    }>;
    users?: Array<Record<string, unknown>>;
    __typename: 'UsageQueryResultAggregations';
  };
  __typename: 'UsageQueryResult';
}

// ── Setup Helpers ──────────────────────────────────────────────────────────

/**
 * Set up basic dataset mock with profile data
 * Required to enable the stats tab in tests
 */
export async function setupBasicDataset(
  apiMock: ApiMocker,
  timestamp: number,
  datasetUrn: string,
  privileges: Record<string, unknown>,
): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);

  await apiMock.mockGraphQL('getDataset', {
    dataset: {
      __typename: 'Dataset',
      urn: datasetUrn,
      latestFullTableProfile: [sampleProfile],
      latestPartitionProfile: [],
      privileges,
    },
  });

  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: {
      __typename: 'Dataset',
      datasetProfiles: [sampleProfile],
    },
  });

  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats: {
          __typename: 'AssetStats',
          oldestDatasetProfileTime: timestamp,
          oldestDatasetUsageTime: timestamp,
        },
      },
    },
  });
}

/**
 * Set up usage stats mocks (queries and users)
 */
export async function setupUsageStats(apiMock: ApiMocker, timestamp: number): Promise<void> {
  const sampleUsageStats = getSampleUsageStats(timestamp);

  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: {
        __typename: 'UsageAggregation',
        buckets: sampleUsageStats.buckets,
      },
    },
  });

  await apiMock.mockGraphQL('getLastMonthUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: sampleUsageStats,
    },
  });
}

/**
 * Set up operations stats mocks (change history)
 *
 * @param apiMock - API mock instance
 * @param timestamp - Timestamp for bucket data (optional, defaults to now)
 * @param useAllOperations - Set to true to include all operation types (for complex tests),
 *                          false (default) to only include creates (for simple tests)
 * @param operationCounts - Operation count configuration (uses defaults if not provided)
 * @param customOperationType - Custom operation type name (defaults to 'custom_type')
 */
export async function setupOperationsStats(
  apiMock: ApiMocker,
  timestamp?: number,
  useAllOperations: boolean = false,
  operationCounts?: {
    CREATES: number;
    UPDATES: number;
    DELETES: number;
    INSERTS: number;
    ALTERS: number;
    DROPS: number;
    CUSTOMS: number;
    TOTAL: number;
  },
  customOperationType: string = 'custom_type',
): Promise<void> {
  const now = timestamp || Date.now();
  const counts = operationCounts || {
    CREATES: 1,
    UPDATES: 1,
    DELETES: 1,
    INSERTS: 1,
    ALTERS: 1,
    DROPS: 1,
    CUSTOMS: 1,
    TOTAL: 7,
  };

  // For simple tests, only create 1 total operation (creates)
  // For complex tests, create all operation types (7 total)
  const aggregations = useAllOperations
    ? {
        __typename: 'OperationsAggregationMetrics',
        totalCreates: counts.CREATES,
        totalUpdates: counts.UPDATES,
        totalDeletes: counts.DELETES,
        totalInserts: counts.INSERTS,
        totalAlters: counts.ALTERS,
        totalDrops: counts.DROPS,
        totalCustoms: counts.CUSTOMS,
        totalOperations: counts.TOTAL,
        customOperationsMap: [{ key: customOperationType, value: 1 }],
      }
    : {
        __typename: 'OperationsAggregationMetrics',
        totalCreates: 1,
        totalOperations: 1,
      };

  await apiMock.mockGraphQL('getOperationsStats', {
    dataset: {
      __typename: 'Dataset',
      operationsStats: {
        __typename: 'OperationsAggregation',
        aggregations,
      },
    },
  });

  // Mock the buckets query for time series data
  await apiMock.mockGraphQL('getOperationsStatsBuckets', {
    dataset: {
      __typename: 'Dataset',
      operationsStats: {
        __typename: 'OperationsAggregation',
        aggregations: useAllOperations
          ? aggregations
          : {
              __typename: 'OperationsAggregationMetrics',
              totalCreates: 1,
            },
        buckets: [
          {
            bucket: now,
            aggregations: {
              __typename: 'OperationsAggregationMetrics',
              totalCreates: 1,
            },
            __typename: 'OperationsAggregation',
          },
        ],
      },
    },
  });
}

/**
 * Set up all chart mocks with full data and default privileges
 * Used for testing normal operation of statistics charts
 */
export async function setupChartsData(apiMock: ApiMocker, timestamp: number, datasetUrn: string): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);
  const sampleUsageStats = getSampleUsageStats(timestamp);

  await apiMock.mockGraphQL('getDataset', {
    dataset: {
      __typename: 'Dataset',
      urn: datasetUrn,
      latestFullTableProfile: [sampleProfile],
      latestPartitionProfile: [],
      privileges: {
        __typename: 'DatasetPrivileges',
        canViewDatasetProfile: true,
        canViewDatasetUsage: true,
        canViewDatasetOperations: true,
        canEditDatasetProperties: true,
      },
    },
  });

  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: {
      __typename: 'Dataset',
      datasetProfiles: [sampleProfile],
    },
  });

  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: {
        __typename: 'UsageAggregation',
        buckets: sampleUsageStats.buckets,
      },
    },
  });

  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats: {
          __typename: 'AssetStats',
          oldestDatasetProfileTime: timestamp,
          oldestDatasetUsageTime: timestamp,
        },
      },
    },
  });
}

/**
 * Set up empty data scenario mocks (no profiles, no usage stats)
 * Used for testing chart empty states
 */
export async function setupChartsDataEmpty(apiMock: ApiMocker, datasetUrn: string): Promise<void> {
  const timestamp = Math.floor(Date.now() / 1000) * 1000;
  const sampleProfile = getSampleProfile(timestamp);

  await apiMock.mockGraphQL('getDataset', {
    dataset: {
      __typename: 'Dataset',
      urn: datasetUrn,
      latestFullTableProfile: [sampleProfile],
      latestPartitionProfile: [],
      privileges: {
        __typename: 'DatasetPrivileges',
        canViewDatasetProfile: true,
        canViewDatasetUsage: true,
        canViewDatasetOperations: true,
        canEditDatasetProperties: true,
      },
    },
  });

  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: {
      __typename: 'Dataset',
      datasetProfiles: [],
    },
  });

  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: {
        __typename: 'UsageAggregation',
        buckets: [],
      },
    },
  });

  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats: {
          __typename: 'AssetStats',
          oldestDatasetProfileTime: null,
          oldestDatasetUsageTime: null,
        },
      },
    },
  });
}

/**
 * Set up no-permissions scenario mocks
 * Used for testing permission-based visibility
 */
export async function setupChartsDataNoPermissions(apiMock: ApiMocker, datasetUrn: string): Promise<void> {
  const timestamp = Math.floor(Date.now() / 1000) * 1000;
  const sampleProfile = getSampleProfile(timestamp);

  await apiMock.mockGraphQL('getDataset', {
    dataset: {
      __typename: 'Dataset',
      urn: datasetUrn,
      latestFullTableProfile: [sampleProfile],
      latestPartitionProfile: [],
      privileges: {
        __typename: 'DatasetPrivileges',
        canViewDatasetProfile: false,
        canViewDatasetUsage: false,
        canViewDatasetOperations: false,
        canEditDatasetProperties: false,
      },
    },
  });
}

// ── Data Factories ─────────────────────────────────────────────────────────

/**
 * Generate an empty DatasetProfile for testing no-data states
 */
export function getEmptyProfile(timestamp: number): Record<string, unknown> {
  return {
    __typename: 'DatasetProfile',
    rowCount: null,
    columnCount: null,
    sizeInBytes: 0,
    timestampMillis: timestamp,
    partitionSpec: null,
    fieldProfiles: [],
  };
}

/**
 * Generate a sample DatasetProfile with comprehensive column stats
 */
export function getSampleProfile(timestamp: number): DatasetProfile {
  return {
    __typename: 'DatasetProfile',
    rowCount: 100,
    columnCount: 7,
    sizeInBytes: 10240,
    timestampMillis: timestamp,
    partitionSpec: null,
    fieldProfiles: [
      {
        fieldPath: 'user_id',
        max: 100,
        mean: 50.5,
        median: 50,
        min: 1,
        nullCount: 0,
        nullProportion: 0,
        quantiles: null,
        sampleValues: ['1', '50', '100'],
        distinctValueFrequencies: null,
        stdev: 29.15,
        uniqueCount: 100,
        uniqueProportion: 1,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'user_name',
        max: null,
        mean: null,
        median: null,
        min: null,
        nullCount: 0,
        nullProportion: 0,
        quantiles: null,
        sampleValues: ['Alice', 'Bob', 'Charlie'],
        distinctValueFrequencies: null,
        stdev: null,
        uniqueCount: 79,
        uniqueProportion: 0.79,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'email',
        max: null,
        mean: null,
        median: null,
        min: null,
        nullCount: 0,
        nullProportion: 0,
        quantiles: null,
        sampleValues: ['alice@example.com', 'bob@example.com'],
        distinctValueFrequencies: null,
        stdev: null,
        uniqueCount: 19,
        uniqueProportion: 0.19,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'created_at',
        max: '2018-04-07',
        mean: null,
        median: null,
        min: '2018-01-01',
        nullCount: 38,
        nullProportion: 0.38,
        quantiles: null,
        sampleValues: ['2018-01-01', '2018-04-07'],
        distinctValueFrequencies: null,
        stdev: null,
        uniqueCount: 46,
        uniqueProportion: 0.46,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'updated_at',
        max: '2018-04-09',
        mean: null,
        median: null,
        min: '2018-01-09',
        nullCount: 38,
        nullProportion: 0.38,
        quantiles: null,
        sampleValues: ['2018-01-09', '2018-04-09'],
        distinctValueFrequencies: null,
        stdev: null,
        uniqueCount: 52,
        uniqueProportion: 0.52,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'order_count',
        max: 5,
        mean: 2.5,
        median: 2,
        min: 1,
        nullCount: 38,
        nullProportion: 0.38,
        quantiles: null,
        sampleValues: ['1', '3', '5'],
        distinctValueFrequencies: null,
        stdev: 1.41,
        uniqueCount: 4,
        uniqueProportion: 0.04,
        __typename: 'DatasetFieldProfile',
      },
      {
        fieldPath: 'lifetime_value',
        max: 99.0,
        mean: 50.5,
        median: 50,
        min: 1.0,
        nullCount: 38,
        nullProportion: 0.38,
        quantiles: null,
        sampleValues: ['1.0', '50.0', '99.0'],
        distinctValueFrequencies: null,
        stdev: 28.4,
        uniqueCount: 35,
        uniqueProportion: 0.35,
        __typename: 'DatasetFieldProfile',
      },
    ],
  };
}

/**
 * Generate sample usage stats with SQL query data and 5 unique users
 */
export function getSampleUsageStats(timestamp: number): UsageQueryResult {
  const users = Array.from({ length: 5 }, (_, i) => ({
    count: 1,
    userEmail: `user${i + 1}@example.com`,
    user: {
      __typename: 'CorpUser',
      urn: `urn:li:corpuser:user${i + 1}`,
      username: `user${i + 1}`,
      type: 'CORP_USER',
      properties: {
        __typename: 'CorpUserProperties',
        displayName: `User ${String.fromCharCode(65 + i)}`,
        firstName: 'User',
        lastName: String.fromCharCode(65 + i),
        fullName: `User ${String.fromCharCode(65 + i)}`,
      },
      editableProperties: {
        __typename: 'CorpUserEditableProperties',
        displayName: `User ${String.fromCharCode(65 + i)}`,
        pictureLink: '',
      },
    },
    __typename: 'UsageAggregationUser',
  }));

  return {
    buckets: [
      {
        bucket: timestamp,
        metrics: {
          totalSqlQueries: 25,
          __typename: 'UsageAggregationMetrics',
        },
        __typename: 'UsageAggregation',
      },
    ],
    aggregations: {
      uniqueUserCount: 5,
      totalSqlQueries: 25,
      fields: [
        {
          fieldName: 'user_id',
          count: 15,
          __typename: 'FieldUsageCounts',
        },
        {
          fieldName: 'user_name',
          count: 10,
          __typename: 'FieldUsageCounts',
        },
      ],
      users,
      __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
  };
}

/**
 * Generate schema metadata with field types matching field profiles
 */
export function getSchemaMetadata(): SchemaMetadata {
  return {
    __typename: 'SchemaMetadata',
    fields: [
      {
        fieldPath: 'user_id',
        nativeDataType: 'int',
        type: 'Number',
        nullable: false,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'user_name',
        nativeDataType: 'varchar',
        type: 'String',
        nullable: false,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'email',
        nativeDataType: 'varchar',
        type: 'String',
        nullable: false,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'created_at',
        nativeDataType: 'date',
        type: 'Date',
        nullable: true,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'updated_at',
        nativeDataType: 'date',
        type: 'Date',
        nullable: true,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'order_count',
        nativeDataType: 'int',
        type: 'Number',
        nullable: true,
        __typename: 'SchemaField',
      },
      {
        fieldPath: 'lifetime_value',
        nativeDataType: 'decimal',
        type: 'Number',
        nullable: true,
        __typename: 'SchemaField',
      },
    ],
  };
}

/**
 * Extract expected column field values from mock data for test assertions
 * This ensures test assertions always match the mock data without hardcoding
 */
export function getExpectedColumnStats(): Record<string, Record<string, string>> {
  const profile = getSampleProfile(Date.now());
  const schema = getSchemaMetadata();

  const buildFieldStats = (fieldPath: string): Record<string, string> => {
    const fieldProfile = profile.fieldProfiles.find((f) => f.fieldPath === fieldPath);
    const schemaField = schema.fields.find((f) => f.fieldPath === fieldPath);

    if (!fieldProfile || !schemaField) {
      throw new Error(`Field ${fieldPath} not found in mock data`);
    }

    const nullPercentage = formatNullPercentage(fieldProfile.nullProportion);

    const uniqueValues = String(fieldProfile.uniqueCount);
    const stats: Record<string, string> = {
      column: fieldPath,
      type: schemaField.type,
      nullPercentage,
      uniqueValues,
    };

    if (fieldProfile.min !== null) {
      stats.min = String(fieldProfile.min);
    }
    if (fieldProfile.max !== null) {
      stats.max = String(fieldProfile.max);
    }

    return stats;
  };

  return {
    user_id: buildFieldStats('user_id'),
    user_name: buildFieldStats('user_name'),
    email: buildFieldStats('email'),
    created_at: buildFieldStats('created_at'),
    updated_at: buildFieldStats('updated_at'),
    order_count: buildFieldStats('order_count'),
    lifetime_value: buildFieldStats('lifetime_value'),
  };
}

/**
 * Calculate null percentage string for display
 * Handles formatting: 0 → "0%", 0.38 → "38.00%", 1 → "100%"
 */
export function formatNullPercentage(nullProportion: number): string {
  const percentageValue = nullProportion * 100;
  return percentageValue === 0 || percentageValue === 100
    ? `${Math.round(percentageValue)}%`
    : `${percentageValue.toFixed(2)}%`;
}
