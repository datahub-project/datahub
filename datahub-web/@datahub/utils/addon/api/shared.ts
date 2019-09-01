import { ApiError } from '@datahub/utils/api/error';

/**
 * Defines available api version types
 * @type {string}
 */
export enum ApiVersion {
  v1 = 'v1',
  v2 = 'v2'
}

/**
 * Defines the root path for wherehows front-end api requests
 * @param {ApiVersion} [version = ApiVersion.v1] the
 * @return {string}
 */
export const getApiRoot = (version: ApiVersion = ApiVersion.v1) => `/api/${version}`;

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
 * Enumerates the currently available Api status errors
 * @type {number}
 */
export enum ApiResponseStatus {
  Ok = 200,
  NotFound = 404,
  UnAuthorized = 401,
  InternalServerError = 500
}

/**
 * Type guard discriminates an error object as an instance of ApiError
 * @param {Error} e
 * @return {boolean}
 */
const isApiError = (e: Error): e is ApiError => e instanceof ApiError;

/**
 * Curried function further checks if the error instance is an ApiStatusError
 * @param {Error} e he error object to check
 */
const isApiStatusError = (e: Error) => (apiResponseStatus: ApiResponseStatus) =>
  isApiError(e) && e.status === apiResponseStatus;

/**
 * Convenience function to ascertain if an api error is a not found code
 * @param {Error} e
 * @return {boolean}
 */
export const isNotFoundApiError = (e: Error) => isApiStatusError(e)(ApiResponseStatus.NotFound);

/**
 * Checks that a server response status is a server exception
 * @param {Error} e
 * @return {boolean}
 */
export const isServerExceptionApiError = (e: Error) =>
  isApiError(e) && e.status >= ApiResponseStatus.InternalServerError;

/**
 * Checks that a server response status is an unauthorized error
 * @param {Error} e
 * @return {boolean}
 */
export const isUnAuthorizedApiError = (e: Error) => isApiStatusError(e)(ApiResponseStatus.UnAuthorized);
