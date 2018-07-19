import fetch from 'fetch';
import { apiErrorStatusMessage } from 'wherehows-web/constants/errors/errors';
import { ApiError, throwIfApiError } from 'wherehows-web/utils/api/errors/errors';
import { isNotFoundApiError } from 'wherehows-web/utils/api/shared';

/**
 * Describes the attributes on the fetch configuration object
 */
interface IFetchConfig {
  url: string;
  headers?: { [key: string]: string } | Headers;
  data?: object;
}

/**
 * Describes the available options on an option bag to be passed into a fetch call
 * @interface IFetchOptions
 */
interface IFetchOptions {
  method?: string;
  body?: any;
  headers?: object | Headers;
  credentials?: RequestCredentials;
}

/**
 * Augments the user supplied headers with the default accept and content-type headers
 * @param {IFetchConfig.headers} headers
 */
const withBaseFetchHeaders = (headers: IFetchConfig['headers']): { headers: IFetchConfig['headers'] } => ({
  headers: Object.assign(
    {
      Accept: 'application/json',
      'Content-Type': 'application/json'
    },
    headers
  )
});

/**
 * Sends a HTTP request and resolves with the JSON response
 * @template T
 * @param {string} url the url for the endpoint to request a response from
 * @param {object} fetchConfig
 * @returns {Promise<T>}
 */
const json = <T>(url: string = '', fetchConfig: IFetchOptions = {}): Promise<T> =>
  fetch(url, fetchConfig).then<T>(response => throwIfApiError(response));

/**
 * Conveniently gets a JSON response using the fetch api
 * @template T
 * @param {IFetchConfig} config
 * @return {Promise<T>}
 */
const getJSON = <T>(config: IFetchConfig): Promise<T> => {
  const fetchConfig = { ...withBaseFetchHeaders(config.headers), method: 'GET' };

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a POST request using the Fetch api
 * @template T
 * @param {IFetchConfig} config
 * @returns {Promise<T>}
 */
const postJSON = <T>(config: IFetchConfig): Promise<T> => {
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
const deleteJSON = <T>(config: IFetchConfig): Promise<T> => {
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
const putJSON = <T>(config: IFetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};

  const fetchConfig = Object.assign(requestBody, withBaseFetchHeaders(config.headers), { method: 'PUT' });

  return json<T>(config.url, fetchConfig);
};

/**
 * Requests the headers from a resource endpoint
 * @param {IFetchConfig} config
 * @return {Promise<Headers>}
 */
const getHeaders = async (config: IFetchConfig): Promise<Headers> => {
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
const returnDefaultIfNotFound = async <T>(request: Promise<T>, defaultValue: T): Promise<T> => {
  try {
    return await request;
  } catch (e) {
    if (isNotFoundApiError(e)) {
      return defaultValue;
    }

    throw e;
  }
};

export { getJSON, postJSON, deleteJSON, putJSON, getHeaders, returnDefaultIfNotFound };
