/**
 * Stats V2 Test Constants
 *
 * Centralized definitions for operation types, test data, and other constants
 * used across stats-v2 test suite to prevent magic strings and improve maintainability.
 */

import { getTimestampDaysAgo } from '../../utils/time-utils';

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

// Stat card test IDs (Latest Stats)
export const LATEST_STATS_CARDS = {
  ROWS: 'rows-card',
  COLUMNS: 'columns-card',
} as const;

// Stat card test IDs (Last Month Stats)
export const LAST_MONTH_STATS_CARDS = {
  USERS: 'users-card',
  QUERIES: 'queries-card',
} as const;

// Chart test IDs
export const CHART_IDS = {
  ROW_COUNT: 'row-count-card',
  QUERY_COUNT: 'query-count-card',
  STORAGE_SIZE: 'storage-size-card',
  CHANGE_HISTORY: 'change-history-card',
} as const;

// Select element test IDs
export const SELECT_IDS = {
  USERS: 'users-select',
  TYPES: 'types-select',
  DATE_SWITCHER: 'date-switcher',
} as const;

// Column names for column stats tests
export const COLUMN_NAMES = {
  USER_ID: 'user_id',
  USER_NAME: 'user_name',
  EMAIL: 'email',
  CREATED_AT: 'created_at',
  UPDATED_AT: 'updated_at',
  ORDER_COUNT: 'order_count',
  LIFETIME_VALUE: 'lifetime_value',
} as const;

// Timestamp offsets for time-range testing
export const TIMESTAMP_OFFSETS = {
  ONE_YEAR_AGO: getTimestampDaysAgo(365),
  SIX_MONTHS_AGO: getTimestampDaysAgo(6 * 30 + 7),
  THREE_MONTHS_AGO: getTimestampDaysAgo(90),
  ONE_MONTH_ONE_WEEK_AGO: getTimestampDaysAgo(30 + 7),
  ONE_WEEK_ONE_DAY_AGO: getTimestampDaysAgo(7 + 1),
} as const;
