/**
 * Stats Mock Helper — Generate mock profile and usage data for testing stats tab.
 *
 * Provides helper functions to generate sample profile data, usage stats, and
 * operations stats, matching the format expected by DataHub's GraphQL API.
 *
 * This mirrors the Cypress approach (ApiResponseHelpers) but for Playwright's
 * apiMock.interceptGraphQLResponse pattern.
 */

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
    users?: Array<{
      count: number;
      user: {
        __typename: 'CorpUser';
        urn: string;
        username: string;
        type: string;
      };
    }>;
    __typename: 'UsageQueryResultAggregations';
  };
  __typename: 'UsageQueryResult';
}

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
 * Generate sample usage stats with SQL query data
 */
export function getSampleUsageStats(timestamp: number): UsageQueryResult {
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
      __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
  };
}

/**
 * Generate schema metadata with field types matching field profiles
 */
export function getSchemaMetadata() {
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

/**
 * Create a complete Dataset response with all required GraphQL schema fields.
 * Apollo validates responses against the schema and rejects missing required fields.
 *
 * Minimal fields to satisfy Apollo validation for nonSiblingDatasetFields fragment.
 * Only includes fields needed to pass validation, avoiding fields that might break rendering.
 */
export function getMinimalDatasetMock(urn: string, overrides?: Record<string, unknown>) {
  const base = {
    __typename: 'Dataset',
    urn,
    name: 'Test Dataset',
    type: 'DATASET',
    origin: null,
    uri: null,
    lastIngested: Math.floor(Date.now() / 1000),
    platform: {
      __typename: 'DataPlatform',
      urn: 'urn:li:dataPlatform:postgres',
    },
    dataPlatformInstance: null,
    platformNativeType: null,
    properties: {
      __typename: 'DatasetProperties',
      name: 'Test Dataset',
      qualifiedName: 'test.dataset',
      description: null,
      customProperties: [],
      externalUrl: null,
      lastModified: null,
    },
    structuredProperties: {
      __typename: 'StructuredProperties',
      properties: [],
    },
    editableProperties: {
      __typename: 'EditableDatasetProperties',
      name: null,
      description: null,
    },
    ownership: {
      __typename: 'Ownership',
      owners: [],
    },
    institutionalMemory: {
      __typename: 'InstitutionalMemory',
      elements: [],
    },
    globalTags: {
      __typename: 'GlobalTags',
      tags: [],
    },
    glossaryTerms: {
      __typename: 'GlossaryTerms',
      terms: [],
    },
    subTypes: {
      __typename: 'SubTypes',
      typeNames: [],
    },
    domain: null,
    applications: {
      __typename: 'Applications',
      applications: [],
    },
    container: null,
    deprecation: null,
    embed: null,
    browsePathV2: null,
    exists: true,
    parentContainers: {
      __typename: 'ParentContainers',
      containers: [],
    },
    datasetProfiles: [],
    health: null,
    assertions: {
      __typename: 'AssertionResult',
      start: 0,
      count: 0,
      total: 0,
    },
    access: {
      __typename: 'Access',
      roles: [],
    },
    operations: null,
    viewProperties: {
      __typename: 'ViewProperties',
      materialized: false,
      logic: null,
      formattedLogic: null,
      language: null,
    },
    autoRenderAspects: {
      __typename: 'AspectResults',
      aspects: [],
    },
    status: {
      __typename: 'Status',
      removed: false,
    },
    runs: {
      __typename: 'RunResults',
      count: 0,
      start: 0,
      total: 0,
    },
    testResults: {
      __typename: 'TestResults',
      passing: [],
      failing: [],
    },
    statsSummary: null,
    siblings: {
      __typename: 'SiblingProperties',
      isPrimary: true,
    },
    activeIncidents: {
      __typename: 'IncidentResults',
      total: 0,
    },
    ...overrides,
  };
  return base;
}

/**
 * Deep patch utility — merge nested objects recursively
 */
export function patchObject(target: Record<string, unknown>, patch: Record<string, unknown>): unknown {
  const result = JSON.parse(JSON.stringify(target)) as Record<string, unknown>; // Deep copy
  const apply = (obj: Record<string, unknown>, path: string[], value: unknown): void => {
    const key = path[0];
    if (path.length === 1) {
      obj[key] = value;
    } else {
      if (!obj[key]) obj[key] = {};
      apply(obj[key] as Record<string, unknown>, path.slice(1), value);
    }
  };

  for (const [path, value] of Object.entries(patch)) {
    apply(result, path.split('.'), value);
  }
  return result;
}
