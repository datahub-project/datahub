/**
 * Defines the root path for wherehows front-end api requests
 * @type {string}
 */
const apiRoot = '/api/v1';

/**
 * Defines the literal possible string enum values for the an api response status
 * @type {string}
 */
enum ApiStatus {
  OK = 'ok',
  FAILED = 'failed'
}

export * from 'wherehows-web/utils/api/datasets';
export { apiRoot, ApiStatus };
