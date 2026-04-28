/**
 * Derive the GMS REST URL from a DataHub frontend URL.
 * DataHub runs the frontend on :9002 and GMS on :8080 by convention.
 */
export function gmsUrl(baseUrl?: string): string {
  return (baseUrl ?? process.env.BASE_URL ?? 'http://localhost:9002').replace(':9002', ':8080');
}

export const DEFAULT_TIMEOUT = 30000;
export const NETWORK_IDLE_TIMEOUT = 10000;
export const ANIMATION_TIMEOUT = 500;

// TEST_CREDENTIALS removed — use data/users.ts (users) as the
// single source of truth for user credentials.

export const ROUTES = {
  login: '/login',
  home: '/',
  search: '/search',
  datasets: '/datasets',
  dashboards: '/dashboards',
  businessAttributes: '/business-attributes',
};

export const ENTITY_TYPES = {
  DATASET: 'dataset',
  DASHBOARD: 'dashboard',
  CHART: 'chart',
  DATA_FLOW: 'dataFlow',
  DATA_JOB: 'dataJob',
};

export const DATA_SOURCES = {
  HIVE: 'hive',
  KAFKA: 'kafka',
  POSTGRES: 'postgres',
  MYSQL: 'mysql',
  SNOWFLAKE: 'snowflake',
};
