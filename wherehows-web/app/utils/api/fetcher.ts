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
 * Augments the user supplied headers with the default accept and content-type headers
 * @param {FetchConfig.headers} headers
 */
const baseFetchHeaders = (headers: FetchConfig['headers']) => ({
  headers: {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...headers
  }
});

/**
 * Sends a HTTP request and resolves with the JSON response
 * @template T
 * @param {string} url the url for the endpoint to request a response from
 * @param {object} fetchConfig 
 * @returns {Promise<T>} 
 */
const json = <T>(url: string, fetchConfig: object): Promise<T> =>
  fetch(url, fetchConfig).then<T>(response => response.json());

/**
 * Conveniently gets a JSON response using the fetch api
 * @template T
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const getJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = { ...baseFetchHeaders(config.headers), method: 'GET' };

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a POST request using the Fetch api
 * @template T
 * @param {FetchConfig} config 
 * @returns {Promise<T>} 
 */
const postJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
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
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
    { method: 'DELETE' }
  );

  return json<T>(config.url, fetchConfig);
};

/**
 * Initiates a PUT request using the Fetch api
 * @template T
 * @param {FetchConfig} config
 * @return {Promise<T>}
 */
const putJSON = <T>(config: FetchConfig): Promise<T> => {
  const fetchConfig = Object.assign(
    config.data && { body: JSON.stringify(config.data) },
    baseFetchHeaders(config.headers),
    { method: 'PUT' }
  );

  return json<T>(config.url, fetchConfig);
};

/**
 * Requests the headers from a resource endpoint
 * @param {FetchConfig} config
 * @return {Promise<Headers>}
 */
const getHeaders = async (config: FetchConfig): Promise<Headers> => {
  const fetchConfig = {
    ...baseFetchHeaders(config.headers),
    method: 'HEAD'
  };
  const { ok, headers, statusText } = await fetch(config.url, fetchConfig);

  if (ok) {
    return headers;
  }

  throw new Error(statusText);
};

/**
 * Wraps a request Promise, passthrough response if successful, otherwise handle the error and rethrow if not api error
 * @param {Promise<T>} fetcher the api request to wrap
 * @param {K} defaultValue
 * @return {Promise<K | T>}
 */
const fetchAndHandleIfApiError = async <T, K>(fetcher: Promise<T>, defaultValue: K): Promise<T | K | null> => {
  let result: T | K | null = typeof defaultValue === 'undefined' ? null : defaultValue;
  try {
    result = await fetcher;
  } catch (e) {
    //handle error
  }
  return result;
};

export { getJSON, postJSON, deleteJSON, putJSON, getHeaders, fetchAndHandleIfApiError };
