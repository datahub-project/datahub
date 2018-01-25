/**
 * Defines available api version types
 */
export type ApiVersion = 'v1' | 'v2';

/**
 * Defines the root path for wherehows front-end api requests
 * @param {ApiVersion} [version = 'v1'] the
 * @return {string}
 */
export const getApiRoot = (version: ApiVersion = 'v1') => `/api/${version}`;

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
