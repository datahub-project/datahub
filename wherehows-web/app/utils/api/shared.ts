/**
 * Defines available api version types
 */
import { ApiError } from 'wherehows-web/utils/api/errors/errors';

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

/**
 * Enumerates the currently available Api statuses
 * @type {number}
 */
export enum ApiResponseStatus {
  NotFound = 404,
  UnAuthorized = 401,
  InternalServerError = 500
}

/**
 * Convenience function to ascertain if an api error is a not found code
 * @param {Error} e
 * @return {boolean}
 */
export const notFoundApiError = (e: Error) => e instanceof ApiError && e.status === ApiResponseStatus.NotFound;
