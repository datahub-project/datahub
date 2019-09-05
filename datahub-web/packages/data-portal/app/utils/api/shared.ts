import { ApiError } from 'wherehows-web/utils/api/errors/errors';
import { typeOf } from '@ember/utils';

/**
 * Defines available api version types
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
export const getApiRoot = (version: ApiVersion = ApiVersion.v1): string => `/api/${version}`;

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
 * Type guard discriminates an error object as an instance of ApiError
 * @param {Error} e
 * @return {boolean}
 */
const isApiError = (e: Error): e is ApiError =>
  typeOf(((e as unknown) as Record<string, unknown>).status) !== 'undefined';

/**
 * Convenience function to ascertain if an api error is a not found code
 * @param {Error} e
 * @return {boolean}
 */
export const isNotFoundApiError = (e: Error): boolean => {
  // We assert ApiError here as it is an extension of Erorr and we don't always expect to be able to specifically
  // check against instanceof ApiError
  return isApiError(e) && e.status === ApiResponseStatus.NotFound;
};

/**
 * Checks that a server response status is a server exception
 * @param {Error} e
 * @return {boolean}
 */
export const isServerExceptionApiError = (e: Error): boolean =>
  isApiError(e) && e.status >= ApiResponseStatus.InternalServerError;

/**
 * Checks that a server response status is an unauthorized error
 * @param {Error} e
 * @return {boolean}
 */
export const isUnAuthorizedApiError = (e: Error): boolean =>
  isApiError(e) && e.status === ApiResponseStatus.UnAuthorized;
