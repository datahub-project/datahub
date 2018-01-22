import fetch from 'fetch';

/**
 * Describes the attributes on the fetch configuration object
 */
interface FetchConfig {
  url: string;
  headers?: { [key: string]: string } | Headers;
  data?: object;
}

/**
 * Desribes the available options on an option bag to be passed into a fetch call
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
 * @param {FetchConfig.headers} headers
 */
const withBaseFetchHeaders = (headers: FetchConfig['headers']): { headers: FetchConfig['headers'] } => ({
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
  fetch(url, fetchConfig).then<T>(response => response.json());

/**
 * Conveniently gets a JSON response using the fetch api
 * @template T
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const getJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = { ...withBaseFetchHeaders(config.headers), method: 'GET' };

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a POST request using the Fetch api
 * @template T
 * @param {FetchConfig} config 
 * @returns {Promise<T>} 
 */
const postJSON = <T>(config: FetchConfig): Promise<T> => {
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
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const deleteJSON = <T>(config: FetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};
  const fetchConfig = Object.assign(requestBody, withBaseFetchHeaders(config.headers), { method: 'DELETE' });

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a PUT request using the Fetch api
 * @template T
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const putJSON = <T>(config: FetchConfig): Promise<T> => {
  const requestBody = config.data ? { body: JSON.stringify(config.data) } : {};

  const fetchConfig = Object.assign(requestBody, withBaseFetchHeaders(config.headers), { method: 'PUT' });

  return json<T>(config.url, fetchConfig);
};

/**
 * Requests the headers from a resource endpoint
 * @param {FetchConfig} config
 * @return {Promise<Headers>}
 */
const getHeaders = async (config: FetchConfig): Promise<Headers> => {
  const fetchConfig = {
    ...withBaseFetchHeaders(config.headers),
    method: 'HEAD'
  };
  const { ok, headers, statusText } = await fetch(config.url, fetchConfig);

  if (ok) {
    return headers;
  }

  throw new Error(statusText);
};

/**
 * Wraps a request Promise, pass-through response if successful, otherwise handle the error and rethrow if not api error
 * @template T
 * @param {Promise<T>} fetcher the api request to wrap
 * @param {T} defaultValue
 * @returns {Promise<T|null>}
 */
const fetchAndHandleIfApiError = async <T>(fetcher: Promise<T>, defaultValue: T): Promise<T | null> => {
  let result = typeof defaultValue === 'undefined' ? null : defaultValue;
  try {
    result = await fetcher;
  } catch (e) {
    // TODO: if error is an api error, display notification and allow default return
    // otherwise throw
  }
  return result;
};

export { getJSON, postJSON, deleteJSON, putJSON, getHeaders, fetchAndHandleIfApiError };
