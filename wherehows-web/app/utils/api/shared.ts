/**
 * Defines the root path for wherehows front-end api requests
 * @type {string}
 */
export const ApiRoot = '/api/v1';

/**
 * Defines the literal possible string enum values for the an api response status
 * @type {string}
 */
export enum ApiStatus {
  OK = 'ok',
  // Adds support for success response used in api's like /comments, will refactored to use ok
  SUCCESS = 'success',
  FAILED = 'failed',
  ERROR = 'error'
}
