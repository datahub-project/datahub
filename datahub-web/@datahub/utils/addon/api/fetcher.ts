import fetch from 'fetch';
import { apiErrorStatusMessage, throwIfApiError, ApiError } from '@datahub/utils/api/error';
import { isNotFoundApiError } from '@datahub/utils/api/shared';
import { IFetchConfig, IFetchOptions } from '@datahub/utils/types/api/fetcher';
import { typeOf } from '@ember/utils';
import getCSRFToken from '@datahub/utils/helpers/csrf-token';
/**
 * Augments the user supplied headers with the default accept and content-type headers
 * @param {IFetchConfig.headers} headers additional headers to add onto the request
 */
const withBaseFetchHeaders = (headers: IFetchConfig['headers']): { headers: IFetchConfig['headers'] } => ({
  headers: {
    // https://github.com/playframework/playframework/blob/9e9e26f40a4941fa306116471694d67040331b28/web/play-filters-helpers/src/main/scala/play/filters/csrf/csrf.scala#L99
    Accept: '*/*',
    'Content-Type': 'application/json', // Content-Type is required to prevent a unsupported media type exception
    'Csrf-Token': getCSRFToken(),
    ...headers
  }
});

/**
 * Sends a HTTP request and resolves with the JSON response
 * @template T
 * @param {string} url the url for the endpoint to request a response from
 * @param {IFetchOptions} fetchConfig A configuration object with optional attributes for the Fetch object
 */
const json = <T>(url = '', fetchConfig: IFetchOptions = {}): Promise<T> =>
  fetch(url, fetchConfig).then<T>(
    (response: Response): Promise<T> => throwIfApiError<T>(response, (response): Promise<T> => response.json())
  );

/**
 * Conveniently gets a JSON response using the fetch api
 * @template T
 * @param {IFetchConfig} config
 * @return {Promise<T>}
 */
export const getJSON = <T>(config: IFetchConfig): Promise<T> => {
  const fetchConfig = {
    ...withBaseFetchHeaders(config.headers),
    method: 'GET'
  };

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a POST request using the Fetch api
 * @template T
 * @param {IFetchConfig} config
 * @returns {Promise<T>}
 */
export const postJSON = <T>(config: IFetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};
  const fetchConfig = Object.assign(
    requestBody,
    config.data && { body: JSON.stringify(config.data) },
    withBaseFetchHeaders(config.headers),
    { method: 'POST' }
  );
  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a DELETE request using the Fetch api
 * @template T
 * @param {IFetchConfig} config
 * @return {Promise<T>}
 */
export const deleteJSON = <T>(config: IFetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};
  const fetchConfig = Object.assign(requestBody, withBaseFetchHeaders(config.headers), { method: 'DELETE' });

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a PUT request using the Fetch api
 * @template T
 * @param {IFetchConfig} config
 * @return {Promise<T>}
 */
export const putJSON = <T>(config: IFetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};

  const fetchConfig = Object.assign(requestBody, withBaseFetchHeaders(config.headers), { method: 'PUT' });

  return json<T>(config.url, fetchConfig);
};

/**
 * Requests the headers from a resource endpoint
 * @param {IFetchConfig} config
 * @return {Promise<Headers>}
 */
export const getHeaders = async (config: IFetchConfig): Promise<Headers> => {
  const fetchConfig = {
    ...withBaseFetchHeaders(config.headers),
    method: 'HEAD'
  };
  const response = await fetch(config.url, fetchConfig);
  const { ok, headers, status } = response;

  if (ok) {
    return headers;
  }

  throw new ApiError(status, apiErrorStatusMessage(status));
};

/**
 * Wraps an api request or Promise that resolves a value, if the promise rejects with an
 * @link ApiError and
 * @link ApiResponseStatus.NotFound
 * then the default value is returned then resolve with the default value
 * @param {Promise<T>} request the request or promise to wrap
 * @param {T} defaultValue resolved value if request throws ApiResponseStatus.NotFound
 * @return {Promise<T>}
 */
export const returnDefaultIfNotFound = async <T>(request: Promise<T>, defaultValue: T): Promise<T> => {
  try {
    return await request;
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return defaultValue;
    }

    throw e;
  }
};

/**
 * Helper function to convert any object or type into a string that is not [Object object]
 * @param arg
 */
const argToString = (arg: unknown): string => {
  // @ts-ignore https://github.com/typed-ember/ember-cli-typescript/issues/799
  if (typeOf(arg) === 'object') {
    return JSON.stringify(arg);
  } else {
    return `${arg}`;
  }
};

/**
 * Helper fn to convert arguments of a fn to a string so we can use it as a key for
 * the cache in cacheApi
 * @param args
 */
const argsToKey = (args: Array<unknown>): string =>
  args
    .filter((arg): boolean => typeOf(arg) !== 'undefined')
    .map((arg): string => argToString(arg))
    .join('.');

/**
 * Workaround to enable or disable cache during tests. See CacheEnabler instance-initializer
 */
let CACHE_ENABLED = false;
export const setCacheEnabled = (enabled: boolean): void => {
  CACHE_ENABLED = enabled;
};

/**
 * This fn will cache API request, so only 1 request will be made. This is useful for some
 * configuration APIs where the data is not going to change. This way we make 1 api call
 * and cache the result in memory.
 *
 * @param fn Fn that will call api, returns a promise.
 */
export const cacheApi = <T, R>(fn: (...args: Array<T>) => Promise<R>): ((...args: Array<T>) => Promise<R>) => {
  const cachedResult: Record<string, R> = {};
  const promises: Record<string, Promise<R>> = {};
  return async (...args: Array<T>): Promise<R> => {
    const key = argsToKey(args);
    // We don't want to cache in test
    if (CACHE_ENABLED) {
      // if result is already cached, return data
      if (cachedResult[key]) {
        return cachedResult[key];
      }

      // if call is being made, just wait
      if (promises[key]) {
        return await promises[key];
      }
    }

    // looks like you are the first one
    // make the api call and wait for results
    promises[key] = fn(...args);
    cachedResult[key] = await promises[key];
    return cachedResult[key];
  };
};
