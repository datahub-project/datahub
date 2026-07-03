/**
 * Stats Mock Factory — Generate and set up mock profile and usage data for testing stats tab.
 *
 * Provides factory functions to generate sample profile data, usage stats, and
 * operations stats.
 *
 */

import type { ApiMocker } from '../../fixtures/mocking.fixture';
export { getNormalizedTimestamp } from '../../utils/time-utils';

// ── Interfaces ─────────────────────────────────────────────────────────────

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

// ── Field Definitions (Single Source of Truth) ─────────────────────────────

const SAMPLE_FIELDS = {
  user_id: {
    path: 'user_id',
    profile: {
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
    },
    schema: { nativeDataType: 'int', type: 'Number', nullable: false },
  },
  user_name: {
    path: 'user_name',
    profile: {
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
    },
    schema: { nativeDataType: 'varchar', type: 'String', nullable: false },
  },
  email: {
    path: 'email',
    profile: {
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
    },
    schema: { nativeDataType: 'varchar', type: 'String', nullable: false },
  },
  created_at: {
    path: 'created_at',
    profile: {
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
    },
    schema: { nativeDataType: 'date', type: 'Date', nullable: true },
  },
  updated_at: {
    path: 'updated_at',
    profile: {
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
    },
    schema: { nativeDataType: 'date', type: 'Date', nullable: true },
  },
  order_count: {
    path: 'order_count',
    profile: {
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
    },
    schema: { nativeDataType: 'int', type: 'Number', nullable: true },
  },
  lifetime_value: {
    path: 'lifetime_value',
    profile: {
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
    },
    schema: { nativeDataType: 'decimal', type: 'Number', nullable: true },
  },
};

// ── Privilege Presets ──────────────────────────────────────────────────────

/** Full read/write access to all dataset properties and statistics */
export const DEFAULT_PRIVILEGES = {
  __typename: 'DatasetPrivileges',
  canViewDatasetProfile: true,
  canViewDatasetUsage: true,
  canViewDatasetOperations: true,
  canEditDatasetProperties: true,
};

/** No access to any dataset properties or statistics */
const NO_PERMISSIONS_PRIVILEGES = {
  __typename: 'DatasetPrivileges',
  canViewDatasetProfile: false,
  canViewDatasetUsage: false,
  canViewDatasetOperations: false,
  canEditDatasetProperties: false,
};

/** Can view profile/usage/properties but not operations (tests permission-based visibility) */
const LIMITED_OPERATIONS_PRIVILEGES = {
  __typename: 'DatasetPrivileges',
  canViewDatasetProfile: true,
  canViewDatasetUsage: true,
  canViewDatasetOperations: false,
  canEditDatasetProperties: true,
};

// ── Operation Stats Presets ─────────────────────────────────────────────────

/** Single create operation for testing with data scenarios */
const SINGLE_OPERATION_AGGREGATIONS = { totalCreates: 1, totalOperations: 1 };

/** No operations for testing empty state scenarios */
const EMPTY_OPERATION_AGGREGATIONS = { totalCreates: 0, totalOperations: 0 };

// ── Data Factories ─────────────────────────────────────────────────────────

/** Creates a dataset profile with all 7 sample fields and typical metrics */
export function getSampleProfile(timestamp: number): DatasetProfile {
  return {
    __typename: 'DatasetProfile',
    rowCount: 100,
    columnCount: 7,
    sizeInBytes: 10240,
    timestampMillis: timestamp,
    partitionSpec: null,
    fieldProfiles: Object.values(SAMPLE_FIELDS).map((f) => ({
      fieldPath: f.path,
      ...f.profile,
      __typename: 'DatasetFieldProfile',
    })),
  };
}

/** Creates usage stats with 5 sample users and field usage counts */
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
        metrics: { totalSqlQueries: 25, __typename: 'UsageAggregationMetrics' },
        __typename: 'UsageAggregation',
      },
    ],
    aggregations: {
      uniqueUserCount: 5,
      totalSqlQueries: 25,
      fields: [
        { fieldName: 'user_id', count: 15, __typename: 'FieldUsageCounts' },
        { fieldName: 'user_name', count: 10, __typename: 'FieldUsageCounts' },
      ],
      users,
      __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
  };
}

/** Creates schema metadata for all 7 sample fields (type, nullability) */
export function getSchemaMetadata(): SchemaMetadata {
  return {
    __typename: 'SchemaMetadata',
    fields: Object.values(SAMPLE_FIELDS).map((f) => ({ fieldPath: f.path, ...f.schema, __typename: 'SchemaField' })),
  };
}

/** Creates a minimal dataset mock with URN and default privileges (no profiles/stats) */
export function getMinimalDatasetMock(datasetUrn: string): Record<string, unknown> {
  return {
    __typename: 'Dataset',
    urn: datasetUrn,
    latestFullTableProfile: [],
    latestPartitionProfile: [],
    privileges: DEFAULT_PRIVILEGES,
  };
}

/** Creates a dataset mock with profile (supports overrides for custom scenarios) */
export function getDatasetMockWithProfile(
  datasetUrn: string,
  timestamp: number,
  overrides?: Record<string, unknown>,
): Record<string, unknown> {
  return { ...getMinimalDatasetMock(datasetUrn), latestFullTableProfile: [getSampleProfile(timestamp)], ...overrides };
}

/** Creates a complete dataset mock with profile, schema, and usage stats */
export function getFullDatasetMock(
  datasetUrn: string,
  timestamp: number,
  overrides?: Record<string, unknown>,
): Record<string, unknown> {
  return {
    ...getMinimalDatasetMock(datasetUrn),
    latestFullTableProfile: [getSampleProfile(timestamp)],
    schemaMetadata: getSchemaMetadata(),
    usageStats: getSampleUsageStats(timestamp),
    ...overrides,
  };
}

/** Builds expected column stats values from profile and schema for test assertions */
export function getExpectedColumnStats(): Record<string, Record<string, string>> {
  const profile = getSampleProfile(Date.now());
  const schema = getSchemaMetadata();
  const buildFieldStats = (fieldPath: string): Record<string, string> => {
    const fieldProfile = profile.fieldProfiles.find((f) => f.fieldPath === fieldPath);
    const schemaField = schema.fields.find((f) => f.fieldPath === fieldPath);
    if (!fieldProfile || !schemaField) throw new Error(`Field ${fieldPath} not found`);
    const nullPercentage =
      fieldProfile.nullProportion === 0 || fieldProfile.nullProportion === 1
        ? `${Math.round(fieldProfile.nullProportion * 100)}%`
        : `${(fieldProfile.nullProportion * 100).toFixed(2)}%`;
    const stats: Record<string, string> = {
      column: fieldPath,
      type: schemaField.type,
      nullPercentage,
      uniqueValues: String(fieldProfile.uniqueCount),
    };
    if (fieldProfile.min !== null) stats.min = String(fieldProfile.min);
    if (fieldProfile.max !== null) stats.max = String(fieldProfile.max);
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

// ── Private Helpers ───────────────────────────────────────────────────────

/** Mocks getDataset GraphQL query with dataset metadata, profile, and privileges */
async function buildDatasetMock(
  apiMock: ApiMocker,
  datasetUrn: string,
  profile: DatasetProfile | Record<string, unknown> | null,
  privileges: Record<string, unknown>,
): Promise<void> {
  await apiMock.mockGraphQL('getDataset', {
    dataset: {
      __typename: 'Dataset',
      urn: datasetUrn,
      latestFullTableProfile: profile ? [profile] : [],
      latestPartitionProfile: [],
      privileges,
    },
  });
}

/** Mocks both getOperationsStats and getOperationsStatsBuckets GraphQL queries */
async function mockOperationsStats(
  apiMock: ApiMocker,
  aggregations: Record<string, unknown>,
  buckets?: Array<Record<string, unknown>>,
): Promise<void> {
  await apiMock.mockGraphQL('getOperationsStats', {
    dataset: {
      __typename: 'Dataset',
      operationsStats: {
        __typename: 'OperationsAggregation',
        aggregations: { __typename: 'OperationsAggregationMetrics', ...aggregations },
      },
    },
  });
  await apiMock.mockGraphQL('getOperationsStatsBuckets', {
    dataset: {
      __typename: 'Dataset',
      operationsStats: {
        __typename: 'OperationsAggregation',
        aggregations: { __typename: 'OperationsAggregationMetrics', ...aggregations },
        buckets: buckets?.map((b) => ({ ...b, __typename: 'OperationsAggregation' })) || [],
      },
    },
  });
}

/** Mocks getDatasetTimeseriesCapability query with oldest data timestamps */
async function mockTimeseriesCapability(
  apiMock: ApiMocker,
  timestamp: number,
  includeOperationTime: boolean = false,
): Promise<void> {
  const assetStats: Record<string, unknown> = {
    __typename: 'AssetStats',
    oldestDatasetProfileTime: timestamp,
    oldestDatasetUsageTime: timestamp,
  };
  if (includeOperationTime) {
    assetStats.oldestOperationTime = timestamp;
  }
  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats,
      },
    },
  });
}

/** Mocks getDataProfiles GraphQL query with historical profile data */
async function mockDataProfiles(
  apiMock: ApiMocker,
  profiles: DatasetProfile[] | Record<string, unknown>[],
): Promise<void> {
  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: { __typename: 'Dataset', datasetProfiles: profiles },
  });
}

/** Mocks both time-range and last-month usage stats GraphQL queries */
async function mockUsageStats(apiMock: ApiMocker, usageStats: UsageQueryResult): Promise<void> {
  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: { __typename: 'UsageAggregation', buckets: usageStats.buckets },
    },
  });
  await apiMock.mockGraphQL('getLastMonthUsageAggregations', {
    dataset: { __typename: 'Dataset', usageStats },
  });
}

// ── Setup Helpers ──────────────────────────────────────────────────────────

/** Mocks a dataset with profile, historical profiles, and timeseries capability */
export async function setupBasicDataset(
  apiMock: ApiMocker,
  timestamp: number,
  datasetUrn: string,
  privileges: Record<string, unknown>,
): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, privileges);
  await mockDataProfiles(apiMock, [sampleProfile]);
  await mockTimeseriesCapability(apiMock, timestamp);
}

/** Mocks usage statistics (time-range and last-month) with sample user data */
export async function setupUsageStats(apiMock: ApiMocker, timestamp: number): Promise<void> {
  const sampleUsageStats = getSampleUsageStats(timestamp);
  await mockUsageStats(apiMock, sampleUsageStats);
}

/** Mocks dataset operations with configurable create/update/delete counts and custom operation types */
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
  const aggregations = useAllOperations
    ? {
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
    : SINGLE_OPERATION_AGGREGATIONS;
  const buckets = [
    {
      bucket: now,
      aggregations: { totalCreates: 1 },
    },
  ];
  await mockOperationsStats(apiMock, aggregations, buckets);
}

/** Mocks a dataset with empty stats (no usage, no operations, no schema fields) */
export async function setupEmptyDatasetForStats(
  apiMock: ApiMocker,
  timestamp: number,
  datasetUrn: string,
  privileges: Record<string, unknown>,
): Promise<void> {
  const emptyProfile: Record<string, unknown> = {
    __typename: 'DatasetProfile',
    rowCount: null,
    columnCount: null,
    sizeInBytes: 0,
    timestampMillis: timestamp,
    partitionSpec: null,
    fieldProfiles: [],
  };
  await buildDatasetMock(apiMock, datasetUrn, emptyProfile as unknown as DatasetProfile, privileges);
  const emptyUsageStats: UsageQueryResult = {
    buckets: [],
    aggregations: {
      totalSqlQueries: 0,
      uniqueUserCount: 0,
      fields: [],
      users: [],
      __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
  };
  await mockUsageStats(apiMock, emptyUsageStats);
  await mockOperationsStats(apiMock, EMPTY_OPERATION_AGGREGATIONS, []);
  await apiMock.mockGraphQL('getDatasetSchema', {
    dataset: {
      __typename: 'Dataset',
      schemaMetadata: { __typename: 'SchemaMetadata', fields: [] },
      siblingsSearch: {
        __typename: 'SearchResults',
        searchResults: [
          { entity: { __typename: 'Dataset', schemaMetadata: { __typename: 'SchemaMetadata', fields: [] } } },
        ],
      },
    },
  });
}

/** Mocks complete stats for charts (profile, usage, and timeseries data) */
export async function setupChartsData(apiMock: ApiMocker, timestamp: number, datasetUrn: string): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);
  const sampleUsageStats = getSampleUsageStats(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, DEFAULT_PRIVILEGES);
  await mockDataProfiles(apiMock, [sampleProfile]);
  await mockUsageStats(apiMock, sampleUsageStats);
  await mockTimeseriesCapability(apiMock, timestamp);
}

/** Mocks charts with empty historical profile data (tests empty state UI) */
export async function setupChartsDataEmpty(apiMock: ApiMocker, datasetUrn: string): Promise<void> {
  const timestamp = Math.floor(Date.now() / 1000) * 1000;
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, DEFAULT_PRIVILEGES);
  await mockDataProfiles(apiMock, []);
  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: { __typename: 'Dataset', usageStats: { __typename: 'UsageAggregation', buckets: [] } },
  });
  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats: { __typename: 'AssetStats', oldestDatasetProfileTime: null, oldestDatasetUsageTime: null },
      },
    },
  });
}

/** Mocks charts with no view permissions (tests permission-based chart visibility) */
export async function setupChartsDataNoPermissions(apiMock: ApiMocker, datasetUrn: string): Promise<void> {
  const timestamp = Math.floor(Date.now() / 1000) * 1000;
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, NO_PERMISSIONS_PRIVILEGES);
}

/** Mocks change history with one operation (tests calendar with data) */
export async function setupChangeHistoryWithData(
  apiMock: ApiMocker,
  timestamp: number,
  datasetUrn: string,
  privileges: Record<string, unknown>,
): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, privileges);
  const buckets = [{ bucket: timestamp, aggregations: { totalCreates: 1 } }];
  await mockOperationsStats(apiMock, SINGLE_OPERATION_AGGREGATIONS, buckets);
}

/** Mocks change history with no operations (tests empty calendar state) */
export async function setupChangeHistoryEmpty(
  apiMock: ApiMocker,
  datasetUrn: string,
  privileges: Record<string, unknown>,
): Promise<void> {
  const sampleProfile = getSampleProfile(Date.now());
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, privileges);
  await mockOperationsStats(apiMock, EMPTY_OPERATION_AGGREGATIONS, []);
}

/** Mocks change history without operation view permissions (tests permission UI) */
export async function setupChangeHistoryNoPermissions(apiMock: ApiMocker, datasetUrn: string): Promise<void> {
  const timestamp = Date.now();
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, LIMITED_OPERATIONS_PRIVILEGES);
  await mockOperationsStats(apiMock, SINGLE_OPERATION_AGGREGATIONS);
}

/** Mocks change history with custom aggregations, buckets, and optional operation details */
export async function setupChangeHistoryWithOperations(
  apiMock: ApiMocker,
  datasetUrn: string,
  timestamp: number,
  aggregations: Record<string, unknown>,
  buckets: Array<Record<string, unknown>>,
  operations?: Array<Record<string, unknown>>,
): Promise<void> {
  const sampleProfile = getSampleProfile(timestamp);
  await buildDatasetMock(apiMock, datasetUrn, sampleProfile, DEFAULT_PRIVILEGES);
  await mockTimeseriesCapability(apiMock, timestamp, true);
  if (operations) {
    await apiMock.mockGraphQL('getOperations', { dataset: { __typename: 'Dataset', operations } });
  }
  await mockOperationsStats(apiMock, aggregations, buckets);
}
