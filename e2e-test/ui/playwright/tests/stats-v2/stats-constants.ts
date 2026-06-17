/**
 * Stats V2 Test Constants
 *
 * Centralized definitions for operation types, test data, and other constants
 * used across stats-v2 test suite to prevent magic strings and improve maintainability.
 */

// Operation types matching the GraphQL schema
export const OPERATION_TYPES = {
  CREATE: 'operation-CREATE',
  UPDATE: 'operation-UPDATE',
  DELETE: 'operation-DELETE',
  INSERT: 'operation-INSERT',
  ALTER: 'operation-ALTER',
  DROP: 'operation-DROP',
} as const;

// Custom operation type prefix and default custom operation type
export const CUSTOM_OPERATION_PREFIX = 'operation-custom_' as const;
export const CUSTOM_OPERATION_TYPE = 'custom_type' as const;
export const CUSTOM_OPERATION_DISPLAY = `custom_${CUSTOM_OPERATION_TYPE}` as const;

// Time range options
export const TIME_RANGES = {
  WEEK: 'WEEK',
  MONTH: 'MONTH',
  QUARTER: 'QUARTER',
  HALF_OF_YEAR: 'HALF_OF_YEAR',
  YEAR: 'YEAR',
} as const;

// Default dataset for testing
export const TEST_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_stats_test,PROD)' as const;

// Default privileges for tests
export const DEFAULT_PRIVILEGES = {
  __typename: 'DatasetPrivileges',
  canViewDatasetProfile: true,
  canViewDatasetUsage: true,
  canViewDatasetOperations: true,
  canEditDatasetProperties: true,
} as const;

// Expected mock values for stat cards
export const EXPECTED_STATS = {
  ROWS: '100',
  COLUMNS: '7',
  USERS: '5',
  QUERIES: '25',
  CHANGES: '1',
} as const;
