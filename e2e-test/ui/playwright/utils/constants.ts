export const DEFAULT_TIMEOUT = 30000;
export const NETWORK_IDLE_TIMEOUT = 10000;
export const ANIMATION_TIMEOUT = 500;

export const TEST_CREDENTIALS = {
  username: process.env.TEST_USERNAME || 'datahub',
  password: process.env.TEST_PASSWORD || 'datahub',
};

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

export const PLATFORMS = {
  HIVE: 'hive',
  KAFKA: 'kafka',
  POSTGRES: 'postgres',
  MYSQL: 'mysql',
  SNOWFLAKE: 'snowflake',
};
